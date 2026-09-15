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
    /// Identifiers the page declares (wiki links, backticked tokens, paths, tags).
    #[serde(default)]
    pub entities: Vec<String>,
    /// Link targets the page points at.
    #[serde(default)]
    pub links: Vec<String>,
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
            entities: entities::extract(&page.body).entities,
            links: entities::extract(&page.body).links,
        }
    }
}

/// A single search hit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Hit {
    /// Workspace the hit belongs to.
    #[serde(default)]
    pub workspace_id: String,
    /// Project the hit belongs to.
    #[serde(default)]
    pub project_id: String,
    /// Page path.
    pub path: String,
    /// Page version id.
    pub page_id: String,
    /// Page title.
    pub title: String,
    /// BM25 score.
    pub score: f32,
    /// Retrieval streams that produced this hit, e.g. `body` and `entities`.
    #[serde(default)]
    pub streams: Vec<String>,
}

fn schema() -> Schema {
    let mut builder = Schema::builder();
    builder.add_text_field("workspace_id", STRING | FAST | STORED);
    builder.add_text_field("project_id", STRING | FAST | STORED);
    builder.add_text_field("path", STRING | FAST | STORED);
    builder.add_text_field("page_id", STRING | STORED);
    builder.add_text_field("title", TEXT | STORED);
    builder.add_text_field("body", TEXT);
    builder.add_text_field("entities", TEXT | STORED);
    builder.add_text_field("links", TEXT | STORED);
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
            f("entities") => page.entities.join(" "),
            f("links") => page.links.join(" "),
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
        // A Quickwit indexer publishes one `.split` container instead of a
        // tantivy directory; unpack it in place so both shapes search the same.
        if rel.ends_with(".split") {
            crate::quickwit_split::extract_split(&bytes, dest)
                .with_context(|| format!("extracting split {rel}"))?;
        }
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
    search_stream(dir, &["title", "body"], "body", query, limit)
}

/// Search one field set, labelling the hits with the stream that found them.
///
/// Fields are named rather than passed as a slice of `Field`s so callers (and
/// tests) can talk about streams the same way the outcome does.
///
/// # Errors
/// Propagates index-open and query failures.
pub fn search_stream(
    dir: &Path,
    fields: &[&str],
    stream: &str,
    query: &str,
    limit: usize,
) -> Result<Vec<Hit>> {
    let index = Index::open_in_dir(dir).context("opening index")?;
    let s = index.schema();
    let title = s.get_field("title").expect("title field");
    let body = s.get_field("body").expect("body field");
    let entities_field = s.get_field("entities").expect("entities field");
    let links_field = s.get_field("links").expect("links field");
    let selected: Vec<tantivy::schema::Field> = fields
        .iter()
        .map(|name| match *name {
            "title" => title,
            "body" => body,
            "entities" => entities_field,
            "links" => links_field,
            other => panic!("unknown search field {other}"),
        })
        .collect();
    let reader = index.reader().context("opening reader")?;
    let searcher = reader.searcher();
    let parser = QueryParser::for_index(&index, selected);
    let parsed = parser.parse_query(query).context("parsing query")?;
    let top = searcher
        .search(&parsed, &TopDocs::with_limit(limit).order_by_score())
        .context("running query")?;
    let path_field = s.get_field("path").expect("path field");
    let page_field = s.get_field("page_id").expect("page_id field");
    let workspace_field = s.get_field("workspace_id").expect("workspace_id field");
    let project_field = s.get_field("project_id").expect("project_id field");
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
            workspace_id: get(workspace_field),
            project_id: get(project_field),
            path: get(path_field),
            page_id: get(page_field),
            title: get(title),
            score,
            streams: vec![stream.to_string()],
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

/// Marker written inside a cached split once it is complete.
const CACHE_MARKER: &str = ".qm-cache-complete";

/// Materialise splits into a cache that survives across commands.
///
/// A search re-reads the catalog every time, so without a cache every query
/// would re-download every split. The cache is keyed by the split's content
/// hash: an immutable split maps to one directory, and a hit means there is
/// nothing to download at all. Staging into a unique directory and renaming it
/// into place means a crash never leaves a half-populated cache entry looking
/// complete.
///
/// # Errors
/// Fails when a split cannot be materialised, or when the cache directory
/// cannot be created.
pub async fn materialize_splits_cached(
    store: &dyn ObjectStore,
    splits: &[(String, String)],
    cache_root: &Path,
) -> Result<Vec<PathBuf>> {
    std::fs::create_dir_all(cache_root)
        .with_context(|| format!("creating cache dir {}", cache_root.display()))?;
    let mut dirs = Vec::with_capacity(splits.len());
    for (index, (prefix, content_hash)) in splits.iter().enumerate() {
        let key: String = content_hash.chars().take(24).collect();
        let dir = cache_root.join(format!("split-{key}"));
        if dir.join(CACHE_MARKER).is_file() {
            dirs.push(dir);
            continue;
        }

        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or(0);
        let staging = cache_root.join(format!(".staging-{}-{index}-{nanos}", std::process::id()));
        materialize(store, prefix, &staging)
            .await
            .with_context(|| format!("materialising split {prefix}"))?;
        std::fs::write(staging.join(CACHE_MARKER), b"complete")
            .context("marking a cached split complete")?;

        match std::fs::rename(&staging, &dir) {
            Ok(()) => dirs.push(dir),
            Err(_) if dir.join(CACHE_MARKER).is_file() => {
                // Another process published this split first; use theirs and
                // drop our staging copy.
                let _ = std::fs::remove_dir_all(&staging);
                dirs.push(dir);
            }
            Err(error) => {
                let _ = std::fs::remove_dir_all(&staging);
                return Err(error).context("publishing a cached split");
            }
        }
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
                    for stream in &hit.streams {
                        if !existing.streams.contains(stream) {
                            existing.streams.push(stream.clone());
                        }
                    }
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

/// Search several projects of one workspace and fuse the results.
///
/// Each project is searched independently (its own catalog, its own authority
/// check) and the per-project lists are fused with the same RRF used inside a
/// project, so a global query ranks by agreement across projects rather than by
/// raw scores that are not comparable between them.
///
/// # Errors
/// Propagates each project's search failures.
pub async fn search_workspace(
    store: &dyn ObjectStore,
    project_store: &ProjectStore,
    workspace_id: &WorkspaceId,
    projects: &[ProjectId],
    cache_root: &Path,
    query: &str,
    limit: usize,
) -> Result<SearchOutcome> {
    let mut lists = Vec::new();
    let mut splits_searched = 0usize;
    let mut candidates = 0usize;
    let mut filtered_out = 0usize;
    let mut stream_candidates = std::collections::BTreeMap::new();
    for project_id in projects {
        let project_cache = cache_root.join(format!("{workspace_id}-{project_id}"));
        let outcome = search_project(
            store,
            project_store,
            workspace_id,
            project_id,
            &project_cache,
            query,
            limit,
        )
        .await?;
        splits_searched += outcome.splits_searched;
        candidates += outcome.candidates;
        filtered_out += outcome.filtered_out;
        for (stream, count) in outcome.stream_candidates {
            *stream_candidates.entry(stream).or_insert(0) += count;
        }
        if !outcome.hits.is_empty() {
            lists.push(outcome.hits);
        }
    }
    let streams_active: Vec<String> = stream_candidates
        .iter()
        .filter(|(_, count)| **count > 0)
        .map(|(stream, _)| stream.clone())
        .collect();
    let mut hits = fuse_rrf(lists, 60.0);
    hits.truncate(limit);
    Ok(SearchOutcome {
        hits,
        splits_searched,
        candidates,
        filtered_out,
        streams_active,
        stream_candidates,
    })
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
    /// Streams that returned at least one candidate, e.g. `body`, `entities`.
    pub streams_active: Vec<String>,
    /// Candidate count per stream, for diagnosing recall.
    pub stream_candidates: std::collections::BTreeMap<String, usize>,
}

/// What a compaction did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactOutcome {
    /// True when another machine held the lease and nothing was done.
    pub skipped: bool,
    /// Page versions rebuilt into the new index.
    pub pages: usize,
    /// Splits in the catalog before the rebuild.
    pub splits_before: usize,
    /// Splits in the catalog after the rebuild.
    pub splits_after: usize,
    /// Catalog generation after the rebuild.
    pub generation: u64,
}

/// Rebuild a project's index from its authoritative pages.
///
/// Compaction is optional and abandonable: it holds a lease, reads the
/// manifest, rebuilds one split from the pages the manifest still names, and
/// swaps the catalog pointer in a single CAS. Nothing about a search requires
/// it to have run — it exists to shrink the split set and to drop superseded
/// and tombstoned content from the index.
///
/// # Errors
/// Propagates build, upload, and catalog failures. A lost lease is not an
/// error; it is reported as `skipped`.
#[allow(clippy::too_many_arguments)]
pub async fn compact_project(
    store: &dyn ObjectStore,
    project_store: &ProjectStore,
    workspace_id: &WorkspaceId,
    project_id: &ProjectId,
    compactor: &WriterId,
    build_dir: &Path,
    now_ms: i64,
    lease_ttl_ms: i64,
) -> Result<CompactOutcome> {
    let scope = format!("compact/{workspace_id}/{project_id}");
    let Some(lease) = project_store
        .acquire_lease(&scope, compactor, now_ms, lease_ttl_ms)
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?
    else {
        let catalog = project_store
            .load_catalog(workspace_id, project_id)
            .await
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        return Ok(CompactOutcome {
            skipped: true,
            pages: 0,
            splits_before: catalog.catalog.splits.len(),
            splits_after: catalog.catalog.splits.len(),
            generation: catalog.catalog.generation,
        });
    };

    let loaded = project_store
        .load(workspace_id, project_id)
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    let splits_before = project_store
        .load_catalog(workspace_id, project_id)
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?
        .catalog
        .splits
        .len();

    // Rebuild from what the manifest still names: superseded versions and
    // tombstoned paths are dropped by construction, not by filtering.
    let mut docs = Vec::with_capacity(loaded.manifest.pages.len());
    for (path, entry) in &loaded.manifest.pages {
        let page_path = qm_core::PagePath::new(path)?;
        let page = project_store
            .read_page_version(workspace_id, project_id, &page_path, &entry.page_id)
            .await
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        docs.push(PageDoc::from_version(
            workspace_id,
            project_id,
            &page,
            entry.created_at_ms,
        ));
    }

    let seq = loaded.manifest.seq + 1;
    let prefix = project_store
        .layout()
        .split_prefix(workspace_id, project_id, compactor, seq);
    build_index(build_dir, &docs)?;
    upload_dir(store, &prefix, build_dir).await?;
    let split = SplitEntry {
        writer_id: compactor.clone(),
        seq,
        prefix,
        doc_count: docs.len() as u64,
        content_hash: hash_dir(build_dir)?,
    };

    let replaced = project_store
        .replace_catalog(workspace_id, project_id, vec![split], now_ms)
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    lease
        .release(project_store, now_ms)
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?;

    Ok(CompactOutcome {
        skipped: false,
        pages: docs.len(),
        splits_before,
        splits_after: replaced.splits,
        generation: replaced.generation,
    })
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
            streams_active: Vec::new(),
            stream_candidates: std::collections::BTreeMap::new(),
        });
    }

    // Three streams per split, fused together. Entities and links are declared
    // by the page rather than merely mentioned in prose, so a match there is a
    // stronger signal; RRF lets that show up without hand-tuned weights.
    let streams: [(&str, &[&str]); 3] = [
        ("body", &["title", "body"]),
        ("entities", &["entities"]),
        ("links", &["links"]),
    ];
    let split_keys: Vec<(String, String)> = loaded
        .catalog
        .splits
        .iter()
        .map(|split| (split.prefix.clone(), split.content_hash.clone()))
        .collect();
    let dirs = materialize_splits_cached(store, &split_keys, cache_root).await?;
    let mut lists = Vec::new();
    let mut candidates = 0usize;
    let mut stream_candidates = std::collections::BTreeMap::new();
    for dir in &dirs {
        for (stream, fields) in streams {
            let hits = search_stream(dir, fields, stream, query, limit)?;
            candidates += hits.len();
            *stream_candidates.entry(stream.to_string()).or_insert(0) += hits.len();
            if !hits.is_empty() {
                lists.push(hits);
            }
        }
    }
    let streams_active: Vec<String> = stream_candidates
        .iter()
        .filter(|(_, count)| **count > 0)
        .map(|(stream, _)| stream.clone())
        .collect();
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
        streams_active,
        stream_candidates,
    })
}

pub mod compile;
pub mod consolidate;
pub mod entities;
pub mod quickwit_split;

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::consolidate::consolidate_session;
    use object_store::memory::InMemory;
    use qm_core::{PagePath, SessionId, WriterId};
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
                entities: Vec::new(),
                links: Vec::new(),
            },
            PageDoc {
                workspace_id: "acme".into(),
                project_id: "ai-memory".into(),
                path: "notes/quickwit.md".into(),
                page_id: "p2".into(),
                title: "Quickwit splits".into(),
                body: "tantivy splits stored in object storage".into(),
                updated_at_ms: 2,
                entities: Vec::new(),
                links: Vec::new(),
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

    /// The cache is what makes repeat searches cheap: after the first query the
    /// split objects can vanish from the bucket and results still come back.
    #[tokio::test]
    async fn a_cached_split_is_reused_without_touching_the_bucket() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let project = reader(&bucket);
        let workspace = WorkspaceId::new("acme").unwrap();
        let project_id = ProjectId::new("ai-memory").unwrap();
        let build_root = TempDir::new().unwrap();
        let cache_root = TempDir::new().unwrap();

        let page_path = PagePath::new("notes/quickwit.md").unwrap();
        project
            .commit_page(CommitPageRequest {
                workspace_id: workspace.clone(),
                project_id: project_id.clone(),
                path: page_path.clone(),
                title: "Quickwit".into(),
                body: "tantivy splits in object storage".into(),
                writer_id: WriterId::new("mbp-a").unwrap(),
                now_ms: 1,
            })
            .await
            .unwrap();
        let page = project
            .read_page(&workspace, &project_id, &page_path)
            .await
            .unwrap()
            .unwrap();
        let doc = PageDoc::from_version(&workspace, &project_id, &page, 1);
        publish_split_index(
            bucket.as_ref(),
            &project,
            &workspace,
            &project_id,
            &WriterId::new("mbp-a").unwrap(),
            1,
            &[doc],
            &build_root.path().join("s1"),
            1,
        )
        .await
        .unwrap();

        let first = search_project(
            bucket.as_ref(),
            &reader(&bucket),
            &workspace,
            &project_id,
            cache_root.path(),
            "tantivy",
            5,
        )
        .await
        .unwrap();
        assert_eq!(first.hits.len(), 1);
        assert!(
            std::fs::read_dir(cache_root.path())
                .unwrap()
                .any(|entry| entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with("split-")),
            "a complete split must be cached"
        );

        // Delete every split object: only the cache can answer now.
        let catalog = project.load_catalog(&workspace, &project_id).await.unwrap();
        for split in &catalog.catalog.splits {
            let prefix = object_store::path::Path::parse(&split.prefix).unwrap();
            let mut stream = bucket.list(Some(&prefix));
            let mut keys = Vec::new();
            while let Some(item) = futures::StreamExt::next(&mut stream).await {
                keys.push(item.unwrap().location);
            }
            for key in keys {
                bucket.delete(&key).await.unwrap();
            }
        }

        let second = search_project(
            bucket.as_ref(),
            &reader(&bucket),
            &workspace,
            &project_id,
            cache_root.path(),
            "tantivy",
            5,
        )
        .await
        .unwrap();
        assert_eq!(
            second.hits, first.hits,
            "the second search must be served from the cache"
        );
    }

    /// Multi-stream retrieval: a page that *declares* the identifier should
    /// outrank one that merely mentions it in prose.
    #[tokio::test]
    async fn declared_entities_outrank_body_only_mentions() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let project = reader(&bucket);
        let workspace = WorkspaceId::new("acme").unwrap();
        let project_id = ProjectId::new("ai-memory").unwrap();
        let writer = WriterId::new("mbp-a").unwrap();
        let build_root = TempDir::new().unwrap();

        // Both bodies contain the word; only one declares it as an entity
        // (a backticked identifier), which extraction picks up automatically.
        let pages = [
            ("notes/engine.md", "Search engine", "we run `tantivy` here"),
            (
                "notes/other.md",
                "Other notes",
                "a passing mention of tantivy in prose",
            ),
        ];
        for (seq, (path, title, body)) in pages.iter().enumerate() {
            let page_path = PagePath::new(*path).unwrap();
            project
                .commit_page(CommitPageRequest {
                    workspace_id: workspace.clone(),
                    project_id: project_id.clone(),
                    path: page_path.clone(),
                    title: (*title).to_string(),
                    body: (*body).to_string(),
                    writer_id: writer.clone(),
                    now_ms: seq as i64,
                })
                .await
                .unwrap();
            let page = project
                .read_page(&workspace, &project_id, &page_path)
                .await
                .unwrap()
                .unwrap();
            let doc = PageDoc::from_version(&workspace, &project_id, &page, seq as i64);
            assert_eq!(
                doc.entities.contains(&"tantivy".to_string()),
                seq == 0,
                "only the declaring page should carry the entity: {doc:?}"
            );
            publish_split_index(
                bucket.as_ref(),
                &project,
                &workspace,
                &project_id,
                &writer,
                seq as u64 + 1,
                &[doc],
                &build_root.path().join(format!("s{seq}")),
                seq as i64,
            )
            .await
            .unwrap();
        }

        let outcome = search_project(
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
        assert_eq!(outcome.hits.len(), 2, "{:?}", outcome.hits);
        assert!(
            outcome.streams_active.contains(&"entities".to_string()),
            "the entity stream must have contributed: {:?}",
            outcome.streams_active
        );
        assert_eq!(
            outcome.hits[0].path, "notes/engine.md",
            "the declaring page must rank first: {:?}",
            outcome.hits
        );
        assert!(
            outcome.hits[0].streams.contains(&"entities".to_string()),
            "and the hit must say why: {:?}",
            outcome.hits[0].streams
        );
        assert!(
            outcome.hits[1].streams == vec!["body".to_string()],
            "the prose-only page matched the body stream alone: {:?}",
            outcome.hits[1].streams
        );
    }

    /// S3 acceptance: compaction shrinks the split set without changing what
    /// a search returns, and a tombstoned page never comes back.
    #[tokio::test]
    async fn compaction_shrinks_the_index_without_changing_results() {
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

        machine
            .write_and_publish(
                "mbp-a",
                1,
                "shared/raft.md",
                "leader election and consensus",
                10,
            )
            .await;
        machine
            .write_and_publish(
                "mbp-b",
                1,
                "notes/quickwit.md",
                "tantivy splits in object storage",
                20,
            )
            .await;
        machine
            .write_and_publish(
                "mbp-c",
                1,
                "shared/raft.md",
                "leader election and snapshots",
                30,
            )
            .await;

        // One page is deleted before compaction: it must not survive the rebuild.
        let deleted_path = PagePath::new("notes/quickwit.md").unwrap();
        writer_store
            .delete_page(
                &workspace,
                &project_id,
                &deleted_path,
                &WriterId::new("mbp-c").unwrap(),
                40,
            )
            .await
            .unwrap();

        let cache = TempDir::new().unwrap();
        let before = search_project(
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
        assert_eq!(before.splits_searched, 3);
        assert_eq!(before.hits.len(), 1);

        let compactor = WriterId::new("mbp-a").unwrap();
        let outcome = compact_project(
            bucket.as_ref(),
            &reader(&bucket),
            &workspace,
            &project_id,
            &compactor,
            &build_root.path().join("compact-1"),
            50,
            60_000,
        )
        .await
        .unwrap();
        assert!(!outcome.skipped);
        assert_eq!(outcome.splits_before, 3);
        assert_eq!(
            outcome.splits_after, 1,
            "compaction must shrink the catalog"
        );
        assert_eq!(outcome.pages, 1, "only the live page is rebuilt");

        let after = search_project(
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
        assert_eq!(after.splits_searched, 1);
        assert_eq!(after.hits, before.hits, "results must not change");

        // The tombstoned page was not resurrected by the rebuild.
        let deleted = search_project(
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
        assert!(deleted.hits.is_empty(), "{:?}", deleted.hits);
    }

    /// Compaction is abandonable work: a machine that cannot get the lease
    /// must walk away, and the system must be unaffected.
    #[tokio::test]
    async fn compaction_defers_to_an_existing_lease() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = reader(&bucket);
        let workspace = WorkspaceId::new("acme").unwrap();
        let project_id = ProjectId::new("ai-memory").unwrap();
        let holder = WriterId::new("holder").unwrap();
        let scope = format!("compact/{workspace}/{project_id}");
        let lease = store
            .acquire_lease(&scope, &holder, 1_000, 60_000)
            .await
            .unwrap()
            .expect("lease acquired");

        let build_root = TempDir::new().unwrap();
        let outcome = compact_project(
            bucket.as_ref(),
            &store,
            &workspace,
            &project_id,
            &WriterId::new("other").unwrap(),
            build_root.path(),
            2_000,
            60_000,
        )
        .await
        .unwrap();
        assert!(outcome.skipped, "a held lease must defer the work");
        assert_eq!(outcome.splits_after, outcome.splits_before);
        lease.release(&store, 3_000).await.unwrap();
    }

    /// The same round trip as above, but the publisher wrote one `.split`
    /// container (the shape a Quickwit indexer produces) instead of a
    /// directory of files.
    #[tokio::test]
    async fn a_published_split_container_is_searchable_by_another_machine() {
        let builder = TempDir::new().unwrap();
        build_index(builder.path(), &docs()).unwrap();
        let split_bytes = {
            let mut files: Vec<(String, Vec<u8>)> = std::fs::read_dir(builder.path())
                .unwrap()
                .map(|entry| {
                    let entry = entry.unwrap();
                    let name = entry.file_name().to_string_lossy().to_string();
                    (name, std::fs::read(entry.path()).unwrap())
                })
                .filter(|(name, _)| !name.ends_with(".lock"))
                .collect();
            files.sort();
            crate::quickwit_split::tests_support::bundle_index(&files)
        };
        let store = InMemory::new();
        let prefix = "v1/ws/acme/proj/ai-memory/index/splits/writer-2/0000000001";
        let key = object_store::path::Path::parse(format!("{prefix}/0000000001.split")).unwrap();
        store.put(&key, split_bytes.into()).await.unwrap();

        let cache = TempDir::new().unwrap();
        materialize(&store, prefix, cache.path()).await.unwrap();
        let hits = search(cache.path(), "tantivy", 5).unwrap();
        assert_eq!(hits.len(), 1, "{hits:?}");
        assert_eq!(hits[0].path, "notes/quickwit.md");
    }

    fn observation(session: &SessionId, text: &str, at: i64) -> qm_core::Observation {
        qm_core::Observation {
            schema: qm_core::MANIFEST_SCHEMA,
            observation_id: qm_core::derive_observation_id(session, "codex", "tool_use", text, at),
            session_id: session.clone(),
            actor: "codex".into(),
            kind: "tool_use".into(),
            text: text.into(),
            created_at_ms: at,
        }
    }

    async fn ingest(store: &ProjectStore, session: &SessionId, texts: &[&str], now_ms: i64) {
        store
            .ingest_observations(qm_store::IngestObservationsRequest {
                workspace_id: WorkspaceId::new("acme").unwrap(),
                project_id: ProjectId::new("ai-memory").unwrap(),
                session_id: session.clone(),
                writer_id: WriterId::new("mbp-a").unwrap(),
                observations: texts
                    .iter()
                    .enumerate()
                    .map(|(index, text)| observation(session, text, now_ms + index as i64))
                    .collect(),
                now_ms,
            })
            .await
            .expect("ingest");
    }

    /// S4 acceptance: raw capture becomes a durable, searchable page, and
    /// re-running the compiler over an unchanged chain writes nothing.
    #[tokio::test]
    async fn a_session_is_compiled_into_a_searchable_page() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let project = reader(&bucket);
        let workspace = WorkspaceId::new("acme").unwrap();
        let project_id = ProjectId::new("ai-memory").unwrap();
        let session = SessionId::new("sess-1").unwrap();
        let consolidator = WriterId::new("mbp-a").unwrap();

        ingest(
            &project,
            &session,
            &[
                "switched the index to tantivy splits",
                "ran the full test suite",
            ],
            100,
        )
        .await;
        ingest(&project, &session, &["published the catalog"], 200).await;

        let outcome = consolidate_session(
            &project,
            &workspace,
            &project_id,
            &session,
            &consolidator,
            300,
            60_000,
        )
        .await
        .unwrap();
        assert!(!outcome.skipped && !outcome.already_up_to_date);
        assert_eq!(outcome.observations, 3);
        assert_eq!(outcome.segments, 2);
        assert_eq!(outcome.manifest_seq, 1);

        let page_path = PagePath::new("sessions/sess-1.md").unwrap();
        let page = project
            .read_page(&workspace, &project_id, &page_path)
            .await
            .unwrap()
            .expect("compiled page");
        assert!(page.body.contains("tantivy splits"), "{}", page.body);
        assert!(page.body.contains("published the catalog"), "{}", page.body);
        assert!(
            crate::compile::fingerprint_of(&page.body).is_some(),
            "the page must record the chain it was compiled from: {}",
            page.body
        );

        // The compiled page is searchable like any other page.
        let build_root = TempDir::new().unwrap();
        let doc = PageDoc::from_version(&workspace, &project_id, &page, 300);
        publish_split_index(
            bucket.as_ref(),
            &project,
            &workspace,
            &project_id,
            &WriterId::new("mbp-a").unwrap(),
            1,
            &[doc],
            &build_root.path().join("sess"),
            300,
        )
        .await
        .unwrap();
        let found = search_project(
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
        assert_eq!(found.hits.len(), 1, "{:?}", found.hits);
        assert_eq!(found.hits[0].path, "sessions/sess-1.md");

        // Recompiling an unchanged chain must not append a version.
        let again = consolidate_session(
            &project,
            &workspace,
            &project_id,
            &session,
            &consolidator,
            400,
            60_000,
        )
        .await
        .unwrap();
        assert!(again.already_up_to_date);
        assert_eq!(again.manifest_seq, 1, "no new commit for an unchanged page");

        // A different machine can compile a session it has never seen, and a
        // held lease makes consolidation defer.
        let other_session = SessionId::new("sess-2").unwrap();
        ingest(&project, &other_session, &["only observation"], 500).await;
        let second = reader(&bucket);
        let compiled = consolidate_session(
            &second,
            &workspace,
            &project_id,
            &other_session,
            &WriterId::new("mbp-b").unwrap(),
            600,
            60_000,
        )
        .await
        .unwrap();
        assert!(!compiled.skipped && !compiled.already_up_to_date);

        let holder = WriterId::new("holder").unwrap();
        let scope = format!("consolidate/{workspace}/{project_id}/{other_session}");
        let lease = second
            .acquire_lease(&scope, &holder, 1_000, 60_000)
            .await
            .unwrap()
            .expect("lease");
        let deferred = consolidate_session(
            &second,
            &workspace,
            &project_id,
            &other_session,
            &WriterId::new("mbp-c").unwrap(),
            1_100,
            60_000,
        )
        .await
        .unwrap();
        assert!(deferred.skipped, "a held lease must defer consolidation");
        lease.release(&second, 1_200).await.unwrap();
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
