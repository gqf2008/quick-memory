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

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail};
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;
use qm_core::{PageVersion, ProjectId, SplitEntry, WorkspaceId, WriterId, content_hash};
use qm_store::{ProjectStore, PublishOutcome};
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

impl PageDoc {
    /// Project a page version into a searchable document.
    #[must_use]
    pub fn from_version(
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
        page: &PageVersion,
        updated_at_ms: i64,
    ) -> Self {
        Self {
            workspace_id: workspace_id.to_string(),
            project_id: project_id.to_string(),
            path: page.path.as_str().to_string(),
            page_id: page.page_id.as_str().to_string(),
            title: page.title.clone(),
            body: page.body.clone(),
            updated_at_ms,
        }
    }
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

/// Deterministic content hash of an index directory.
///
/// Covers file names and bytes in sorted order, so two machines that build the
/// same split agree on its identity and publishing becomes idempotent.
///
/// # Errors
/// Propagates IO failures.
pub fn hash_dir(dir: &Path) -> Result<String> {
    let mut hasher_input = Vec::new();
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
        hasher_input.extend_from_slice(rel.as_bytes());
        hasher_input.push(0);
        hasher_input.extend_from_slice(&std::fs::read(entry.path())?);
        hasher_input.push(0xff);
    }
    Ok(content_hash(&hasher_input))
}

/// Build a split from `pages`, upload it, and publish it into the catalog.
///
/// Uploading before publishing is deliberate: an unpublished split is
/// invisible, so a crash anywhere in here leaves an orphan rather than a
/// half-visible index.
///
/// # Errors
/// Propagates build, upload, and catalog failures.
#[allow(clippy::too_many_arguments)]
pub async fn publish_split_index(
    store: &dyn ObjectStore,
    project_store: &ProjectStore,
    workspace_id: &WorkspaceId,
    project_id: &ProjectId,
    writer_id: &WriterId,
    seq: u64,
    pages: &[PageDoc],
    build_dir: &Path,
    now_ms: i64,
) -> Result<PublishOutcome> {
    build_index(build_dir, pages)?;
    let prefix = project_store
        .layout()
        .split_prefix(workspace_id, project_id, writer_id, seq);
    upload_dir(store, &prefix, build_dir).await?;
    let split = SplitEntry {
        writer_id: writer_id.clone(),
        seq,
        prefix,
        doc_count: pages.len() as u64,
        content_hash: hash_dir(build_dir)?,
    };
    project_store
        .publish_split(workspace_id, project_id, split, now_ms)
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))
}

/// Materialise several splits into `cache_root`, returning their directories.
///
/// # Errors
/// Fails when any split cannot be materialised; a partial index would silently
/// answer from an incomplete corpus.
pub async fn materialize_splits(
    store: &dyn ObjectStore,
    prefixes: &[String],
    cache_root: &Path,
) -> Result<Vec<PathBuf>> {
    let mut dirs = Vec::with_capacity(prefixes.len());
    for (index, prefix) in prefixes.iter().enumerate() {
        let dir = cache_root.join(format!("split-{index:04}"));
        materialize(store, prefix, &dir).await?;
        dirs.push(dir);
    }
    Ok(dirs)
}

/// Reciprocal-rank fusion across result lists.
///
/// Every split is searched independently and contributes by rank, so a page
/// that several machines indexed ranks above one that a single split happens
/// to score highly. Ties break on page id to keep output stable.
#[must_use]
pub fn fuse_rrf(lists: Vec<Vec<Hit>>, k: f32) -> Vec<Hit> {
    let mut scores: HashMap<String, (Hit, f32)> = HashMap::new();
    for list in lists {
        for (rank, hit) in list.into_iter().enumerate() {
            let weight = 1.0 / (k + rank as f32 + 1.0);
            scores
                .entry(hit.page_id.clone())
                .and_modify(|(existing, score)| {
                    *score += weight;
                    existing.score = existing.score.max(hit.score);
                })
                .or_insert((hit, weight));
        }
    }
    let mut fused: Vec<Hit> = scores
        .into_values()
        .map(|(mut hit, score)| {
            hit.score = score;
            hit
        })
        .collect();
    fused.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.page_id.cmp(&b.page_id))
    });
    fused
}

/// What a project search did.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchOutcome {
    /// Fused, authority-checked hits.
    pub hits: Vec<Hit>,
    /// Splits that were searched.
    pub splits_searched: usize,
    /// Raw candidates before fusion and filtering.
    pub candidates: usize,
    /// Candidates the manifest rejected as stale, deleted, or unknown.
    pub filtered_out: usize,
}

/// Search a project across every published split, then check the authority.
///
/// The index only proposes candidates: a hit survives only when the manifest
/// still names exactly that page version. That is what lets old splits keep
/// their stale copies around without ever answering from them.
///
/// # Errors
/// Propagates catalog, materialisation, and query failures.
#[allow(clippy::too_many_arguments)]
pub async fn search_project(
    store: &dyn ObjectStore,
    project_store: &ProjectStore,
    workspace_id: &WorkspaceId,
    project_id: &ProjectId,
    cache_root: &Path,
    query: &str,
    limit: usize,
) -> Result<SearchOutcome> {
    let loaded = project_store
        .load_catalog(workspace_id, project_id)
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    let prefixes: Vec<String> = loaded
        .catalog
        .splits
        .iter()
        .map(|split| split.prefix.clone())
        .collect();
    if prefixes.is_empty() {
        return Ok(SearchOutcome {
            hits: Vec::new(),
            splits_searched: 0,
            candidates: 0,
            filtered_out: 0,
        });
    }

    let dirs = materialize_splits(store, &prefixes, cache_root).await?;
    let mut lists = Vec::with_capacity(dirs.len());
    let mut candidates = 0usize;
    for dir in &dirs {
        let hits = search(dir, query, limit)?;
        candidates += hits.len();
        lists.push(hits);
    }
    let fused = fuse_rrf(lists, 60.0);

    let manifest = project_store
        .load(workspace_id, project_id)
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?
        .manifest;
    let before = fused.len();
    let mut hits: Vec<Hit> = fused
        .into_iter()
        .filter(|hit| {
            manifest
                .pages
                .get(&hit.path)
                .is_some_and(|entry| entry.page_id.as_str() == hit.page_id)
        })
        .collect();
    let filtered_out = before - hits.len();
    hits.truncate(limit);

    Ok(SearchOutcome {
        hits,
        splits_searched: dirs.len(),
        candidates,
        filtered_out,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use object_store::memory::InMemory;
    use qm_core::{PagePath, WriterId};
    use qm_store::{CommitPageRequest, RetryPolicy};
    use tempfile::TempDir;

    use super::*;

    struct Machine<'a> {
        project: &'a ProjectStore,
        bucket: &'a Arc<dyn ObjectStore>,
        build_root: &'a Path,
        workspace: &'a WorkspaceId,
        project_id: &'a ProjectId,
    }

    impl Machine<'_> {
        /// Commit a page version, then publish a split containing it.
        async fn write_and_publish(
            &self,
            writer: &str,
            seq: u64,
            path: &str,
            body: &str,
            now_ms: i64,
        ) -> PageVersion {
            let writer_id = WriterId::new(writer).unwrap();
            let page_path = PagePath::new(path).unwrap();
            self.project
                .commit_page(CommitPageRequest {
                    workspace_id: self.workspace.clone(),
                    project_id: self.project_id.clone(),
                    path: page_path.clone(),
                    title: path.to_string(),
                    body: body.to_string(),
                    writer_id: writer_id.clone(),
                    now_ms,
                })
                .await
                .expect("commit");
            let page = self
                .project
                .read_page(self.workspace, self.project_id, &page_path)
                .await
                .expect("read")
                .expect("page");
            let doc = PageDoc::from_version(self.workspace, self.project_id, &page, now_ms);
            let build_dir = self.build_root.join(format!("{writer}-{seq}"));
            publish_split_index(
                self.bucket.as_ref(),
                self.project,
                self.workspace,
                self.project_id,
                &writer_id,
                seq,
                &[doc],
                &build_dir,
                now_ms,
            )
            .await
            .expect("publish split");
            page
        }
    }

    /// A fresh handle on the bucket: what another machine looks like.
    fn reader(bucket: &Arc<dyn ObjectStore>) -> ProjectStore {
        ProjectStore::new(Arc::clone(bucket), "v1").with_retry(RetryPolicy {
            max_attempts: 64,
            base_delay: Duration::ZERO,
        })
    }

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

    /// S2 acceptance: three machines write alternately, one of them is never
    /// heard from again, and independent readers still converge on the same
    /// results — because the index only proposes and the manifest decides.
    #[tokio::test]
    async fn three_machines_with_one_offline_converge_on_the_same_results() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer_store = reader(&bucket);
        let build_root = TempDir::new().unwrap();
        let workspace = WorkspaceId::new("acme").unwrap();
        let project_id = ProjectId::new("ai-memory").unwrap();
        let machine = Machine {
            project: &writer_store,
            bucket: &bucket,
            build_root: build_root.path(),
            workspace: &workspace,
            project_id: &project_id,
        };

        // A writes the first version of a shared page and publishes it.
        machine
            .write_and_publish(
                "mbp-a",
                1,
                "shared/raft.md",
                "leader election and joint consensus",
                10,
            )
            .await;
        // B writes its own page, publishes, and is never heard from again.
        machine
            .write_and_publish(
                "mbp-b",
                1,
                "notes/quickwit.md",
                "tantivy splits in object storage",
                20,
            )
            .await;
        // C supersedes A's page: the old version stays in A's split forever.
        let latest = machine
            .write_and_publish(
                "mbp-c",
                1,
                "shared/raft.md",
                "leader election and snapshots",
                30,
            )
            .await;

        let cache = TempDir::new().unwrap();
        let outcome = search_project(
            bucket.as_ref(),
            &reader(&bucket),
            &workspace,
            &project_id,
            cache.path(),
            "election",
            10,
        )
        .await
        .unwrap();
        assert_eq!(outcome.splits_searched, 3);
        assert_eq!(outcome.hits.len(), 1, "{:?}", outcome.hits);
        assert_eq!(outcome.hits[0].path, "shared/raft.md");
        assert_eq!(
            outcome.hits[0].page_id,
            latest.page_id.as_str(),
            "the stale copy from A's split must lose to the manifest head"
        );
        assert_eq!(
            outcome.filtered_out, 1,
            "A's superseded copy must be filtered"
        );

        // A term that only exists in the superseded body must return nothing:
        // the reader did not merely pick the newest hit, it refused the old one.
        let stale_only = search_project(
            bucket.as_ref(),
            &reader(&bucket),
            &workspace,
            &project_id,
            cache.path(),
            "consensus",
            10,
        )
        .await
        .unwrap();
        assert!(stale_only.hits.is_empty(), "{:?}", stale_only.hits);
        assert_eq!(stale_only.filtered_out, 1);

        // The offline machine's content is still searchable: its split is in
        // the bucket, and nothing about it needs that machine to be alive.
        let offline_machine_content = search_project(
            bucket.as_ref(),
            &reader(&bucket),
            &workspace,
            &project_id,
            TempDir::new().unwrap().path(),
            "tantivy",
            10,
        )
        .await
        .unwrap();
        assert_eq!(offline_machine_content.hits.len(), 1);
        assert_eq!(offline_machine_content.hits[0].path, "notes/quickwit.md");

        // Two independent readers converge on identical results.
        let first = search_project(
            bucket.as_ref(),
            &reader(&bucket),
            &workspace,
            &project_id,
            TempDir::new().unwrap().path(),
            "election",
            10,
        )
        .await
        .unwrap();
        let second = search_project(
            bucket.as_ref(),
            &reader(&bucket),
            &workspace,
            &project_id,
            TempDir::new().unwrap().path(),
            "election",
            10,
        )
        .await
        .unwrap();
        assert_eq!(first.hits, second.hits);
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
