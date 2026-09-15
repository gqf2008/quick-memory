//! Embeddable full-corpus search over object storage.
//!
//! The contract this crate exists to prove: a machine that has never built an
//! index can still search the whole corpus by materialising someone else's
//! index files from the object store and opening them locally — no cluster, no
//! metastore RPC, no coordinator. That is what makes the multi-machine shape
//! work when machines appear and disappear.
//!
//! Materialising (download-then-open) is deliberate for the first cut: it is
//! byte-exact, works with the plain tantivy format, and the local cache is
//! needed anyway. Ranged reads against the object store are a later
//! optimisation, not a correctness requirement.

use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail};
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;
use serde::{Deserialize, Serialize};
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{FAST, STORED, STRING, Schema, TEXT, Value};
use tantivy::{Index, doc};
use walkdir::WalkDir;

/// One searchable page version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PageDoc {
    /// Workspace identity.
    pub workspace_id: String,
    /// Project identity.
    pub project_id: String,
    /// Page path inside the project.
    pub path: String,
    /// Immutable page version id.
    pub page_id: String,
    /// Page title.
    pub title: String,
    /// Page body (markdown source).
    pub body: String,
    /// Last update time in milliseconds since the Unix epoch.
    pub updated_at_ms: i64,
}

/// A single search hit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Hit {
    /// Page path.
    pub path: String,
    /// Page version id.
    pub page_id: String,
    /// Page title.
    pub title: String,
    /// BM25 score.
    pub score: f32,
}

fn schema() -> Schema {
    let mut builder = Schema::builder();
    builder.add_text_field("workspace_id", STRING | FAST | STORED);
    builder.add_text_field("project_id", STRING | FAST | STORED);
    builder.add_text_field("path", STRING | FAST | STORED);
    builder.add_text_field("page_id", STRING | STORED);
    builder.add_text_field("title", TEXT | STORED);
    builder.add_text_field("body", TEXT);
    builder.add_i64_field("updated_at_ms", FAST | STORED);
    builder.build()
}

/// Build a tantivy index at `dir` from `docs`, replacing any existing index.
///
/// # Errors
/// Propagates tantivy IO/commit failures.
pub fn build_index(dir: &Path, docs: &[PageDoc]) -> Result<()> {
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    let index = Index::create_in_dir(dir, schema()).context("creating tantivy index")?;
    let mut writer = index
        .writer_with_num_threads(1, 32_000_000)
        .context("opening index writer")?;
    let s = index.schema();
    let f = |name: &str| s.get_field(name).expect("schema field");
    for page in docs {
        writer.add_document(doc!(
            f("workspace_id") => page.workspace_id.clone(),
            f("project_id") => page.project_id.clone(),
            f("path") => page.path.clone(),
            f("page_id") => page.page_id.clone(),
            f("title") => page.title.clone(),
            f("body") => page.body.clone(),
            f("updated_at_ms") => page.updated_at_ms,
        ))?;
    }
    writer.commit().context("committing index")?;
    Ok(())
}

/// Upload every file of a local index to `prefix`, returning the object keys.
///
/// Lock files are skipped: they are process-local and would only confuse a
/// reader that materialises the prefix elsewhere.
///
/// # Errors
/// Propagates IO/object-store failures.
pub async fn upload_dir(store: &dyn ObjectStore, prefix: &str, dir: &Path) -> Result<Vec<String>> {
    let prefix = prefix.trim_matches('/');
    if prefix.is_empty() {
        bail!("split prefix must not be empty");
    }
    let mut keys = Vec::new();
    for entry in WalkDir::new(dir).sort_by_file_name() {
        let entry = entry?;
        if !entry.file_type().is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if name.ends_with(".lock") {
            continue;
        }
        let rel = entry
            .path()
            .strip_prefix(dir)?
            .to_string_lossy()
            .replace('\\', "/");
        let key = format!("{prefix}/{rel}");
        let bytes = std::fs::read(entry.path())?;
        let path = ObjectPath::parse(&key).with_context(|| format!("parsing key {key}"))?;
        store
            .put(&path, bytes.into())
            .await
            .with_context(|| format!("uploading {key}"))?;
        keys.push(key);
    }
    Ok(keys)
}

/// Download every object under `prefix` into `dest`, returning the file count.
///
/// # Errors
/// Fails on listing failures, unsafe relative paths, or write errors.
pub async fn materialize(store: &dyn ObjectStore, prefix: &str, dest: &Path) -> Result<usize> {
    let prefix = prefix.trim_matches('/');
    if prefix.is_empty() {
        bail!("split prefix must not be empty");
    }
    let base = ObjectPath::parse(prefix).context("parsing split prefix")?;
    let mut stream = store.list(Some(&base));
    let mut count = 0usize;
    while let Some(item) = futures::StreamExt::next(&mut stream).await {
        let meta = item.context("listing split objects")?;
        let full = meta.location.as_ref();
        let rel = full
            .strip_prefix(prefix)
            .unwrap_or(full)
            .trim_start_matches('/');
        let rel_path = PathBuf::from(rel);
        if rel_path.components().any(|c| {
            matches!(
                c,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        }) {
            bail!("refusing unsafe split path {rel:?}");
        }
        let target = dest.join(&rel_path);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let bytes = store
            .get(&meta.location)
            .await
            .with_context(|| format!("downloading {full}"))?
            .bytes()
            .await?;
        std::fs::write(&target, &bytes).with_context(|| format!("writing {}", target.display()))?;
        count += 1;
    }
    if count == 0 {
        bail!("no index files found under {prefix:?}");
    }
    Ok(count)
}

/// Search a materialised index directory.
///
/// # Errors
/// Propagates index-open/query failures.
pub fn search(dir: &Path, query: &str, limit: usize) -> Result<Vec<Hit>> {
    let index = Index::open_in_dir(dir).context("opening index")?;
    let s = index.schema();
    let title = s.get_field("title").expect("title field");
    let body = s.get_field("body").expect("body field");
    let reader = index.reader().context("opening reader")?;
    let searcher = reader.searcher();
    let parser = QueryParser::for_index(&index, vec![title, body]);
    let parsed = parser.parse_query(query).context("parsing query")?;
    let top = searcher
        .search(&parsed, &TopDocs::with_limit(limit).order_by_score())
        .context("running query")?;
    let path_field = s.get_field("path").expect("path field");
    let page_field = s.get_field("page_id").expect("page_id field");
    let mut hits = Vec::with_capacity(top.len());
    for (score, addr) in top {
        let doc: tantivy::TantivyDocument = searcher.doc(addr)?;
        let get = |field| {
            doc.get_first(field)
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string()
        };
        hits.push(Hit {
            path: get(path_field),
            page_id: get(page_field),
            title: get(title),
            score,
        });
    }
    Ok(hits)
}

#[cfg(test)]
mod tests {

    use object_store::memory::InMemory;
    use tempfile::TempDir;

    use super::*;

    fn docs() -> Vec<PageDoc> {
        vec![
            PageDoc {
                workspace_id: "acme".into(),
                project_id: "ai-memory".into(),
                path: "notes/raft.md".into(),
                page_id: "p1".into(),
                title: "Raft consensus".into(),
                body: "leader election and log replication".into(),
                updated_at_ms: 1,
            },
            PageDoc {
                workspace_id: "acme".into(),
                project_id: "ai-memory".into(),
                path: "notes/quickwit.md".into(),
                page_id: "p2".into(),
                title: "Quickwit splits".into(),
                body: "tantivy splits stored in object storage".into(),
                updated_at_ms: 2,
            },
        ]
    }

    /// The S0 question: can a machine that never built the index search the
    /// whole corpus from object storage alone?
    #[tokio::test]
    async fn another_machine_can_search_the_published_index() {
        let builder = TempDir::new().unwrap();
        build_index(builder.path(), &docs()).unwrap();

        let store = InMemory::new();
        let prefix = "v1/ws/acme/proj/ai-memory/index/splits/writer-1/0000000001";
        let uploaded = upload_dir(&store, prefix, builder.path()).await.unwrap();
        assert!(uploaded.iter().any(|k| k.ends_with("meta.json")));

        // A different machine: fresh cache dir, only object storage in common.
        let reader_cache = TempDir::new().unwrap();
        let count = materialize(&store, prefix, reader_cache.path())
            .await
            .unwrap();
        assert_eq!(count, uploaded.len());

        let hits = search(reader_cache.path(), "tantivy", 5).unwrap();
        assert_eq!(hits.len(), 1, "{hits:?}");
        assert_eq!(hits[0].path, "notes/quickwit.md");
        assert_eq!(hits[0].page_id, "p2");

        let hits = search(reader_cache.path(), "leader", 5).unwrap();
        assert_eq!(hits.len(), 1, "{hits:?}");
        assert_eq!(hits[0].path, "notes/raft.md");
    }

    #[tokio::test]
    async fn materialize_refuses_empty_prefix_and_empty_result() {
        let store = InMemory::new();
        let dest = TempDir::new().unwrap();
        assert!(materialize(&store, "", dest.path()).await.is_err());
        assert!(
            materialize(&store, "nothing/here", dest.path())
                .await
                .is_err()
        );
    }
}
