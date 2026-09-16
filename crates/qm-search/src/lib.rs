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
use qm_llm::Embedder;
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
    /// Dense embedding of the page text, when a provider was configured at
    /// publish time. Absent is the normal case: a bucket with no embedding
    /// provider still indexes and searches, just without the vector stream.
    ///
    /// `serde(default)` keeps previously published JSONL documents readable —
    /// a corpus written before this field existed must not become unparseable.
    #[serde(default)]
    pub embedding: Option<Vec<f32>>,
    /// Which embedder produced [`PageDoc::embedding`], when one did.
    ///
    /// `None` is the normal state for a batch built without a provider, and
    /// for documents constructed before this field existed: `serde(default)`
    /// keeps them readable. A batch is embedded by one embedder at a time, so
    /// every vector in a split carries the same identity, and it is the
    /// *split* that records it — see [`EmbeddingIdentity`].
    ///
    /// Note the granularity boundary: this field is per **document**, while the
    /// record on disk is per **split**. Only the first document that carries an
    /// identity speaks for the split (see [`batch_identity`]), which is sound
    /// for a batch [`attach_embeddings`] produced — it stamps one identity
    /// across the whole batch — and silently lossy for a batch assembled by
    /// hand with two of them.
    #[serde(default)]
    pub embedding_identity: Option<EmbeddingIdentity>,
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
            // The embedding is not a function of the page alone — it needs a
            // provider — so it is filled in by whoever builds the split, and
            // so is the identity of that provider.
            embedding: None,
            embedding_identity: None,
        }
    }

    /// Attach an embedding to this document.
    #[must_use]
    pub fn with_embedding(mut self, embedding: Option<Vec<f32>>) -> Self {
        self.embedding = embedding;
        self
    }

    /// Record which embedder produced this document's embedding.
    #[must_use]
    pub fn with_embedding_identity(mut self, identity: Option<EmbeddingIdentity>) -> Self {
        self.embedding_identity = identity;
        self
    }
}

/// Name of the file a split carries to state which embedder built its vectors.
///
/// It lives *inside* the split directory rather than in the catalog, so a
/// reader picks it up with the same materialisation that fetches the index.
/// That keeps the catalog schema untouched and gives the compatibility rule
/// for free: a split published before this file existed simply says nothing
/// about its provenance, and is never treated as a mismatch.
const SPLIT_IDENTITY_FILE: &str = "embedding-identity.json";

/// Which embedder produced a split's vectors.
///
/// A vector is only comparable to a query vector from the *same* embedder.
/// Width alone does not say that: two models of equal width — the common case
/// when swapping a provider or a model revision — produce unrelated
/// coordinates, so comparing them succeeds numerically and answers with a
/// ranking that has no meaning. Recording the identity on the split is what
/// makes that state visible instead of silent.
///
/// Every field carries `#[serde(default)]` so a partially written or older
/// record still parses; the record as a whole is optional, because a split
/// built before this existed, or built without a provider, has nothing to
/// record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbeddingIdentity {
    /// Provider family, e.g. `openai-compatible` or `deterministic`.
    #[serde(default)]
    pub provider: String,
    /// Model name as the provider knows it.
    #[serde(default)]
    pub model: String,
    /// Width of every vector in the split.
    #[serde(default)]
    pub dim: usize,
}

impl From<qm_llm::EmbedderIdentity> for EmbeddingIdentity {
    fn from(identity: qm_llm::EmbedderIdentity) -> Self {
        Self {
            provider: identity.provider,
            model: identity.model,
            dim: identity.dim,
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
    /// Final score: the fused rank score after any recency adjustment.
    pub score: f32,
    /// The fused score before the recency adjustment, for explainability.
    #[serde(default)]
    pub fused_score: f32,
    /// Multiplier applied for recency (1.0 when the tuning is disabled).
    #[serde(default = "default_multiplier")]
    pub recency_multiplier: f32,
    /// When the page version was committed, in milliseconds.
    #[serde(default)]
    pub updated_at_ms: i64,
    /// Retrieval streams that produced this hit, e.g. `body` and `entities`.
    #[serde(default)]
    pub streams: Vec<String>,
}

fn default_multiplier() -> f32 {
    1.0
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
    // Same values, raw tokenizer and one value per link: neighbour lookup asks
    // "who links to exactly this path", which a tokenised field cannot answer.
    builder.add_text_field("links_exact", STRING | STORED);
    builder.add_i64_field("updated_at_ms", FAST | STORED);
    // The embedding is a column, not a searchable field: the vector stream
    // scans it by doc id, so `FAST` alone is what it needs. Raw little-endian
    // f32 bytes rather than a string, because f32->text->f32 is lossy.
    builder.add_bytes_field("embedding", FAST);
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
    let links_exact = f("links_exact");
    for page in docs {
        let mut document = tantivy::TantivyDocument::default();
        document.add_text(f("workspace_id"), &page.workspace_id);
        document.add_text(f("project_id"), &page.project_id);
        document.add_text(f("path"), &page.path);
        document.add_text(f("page_id"), &page.page_id);
        document.add_text(f("title"), &page.title);
        document.add_text(f("body"), &page.body);
        document.add_text(f("entities"), page.entities.join(" "));
        document.add_text(f("links"), page.links.join(" "));
        // One stored value per link, so an exact-path term exists for each.
        for link in &page.links {
            document.add_text(links_exact, link);
        }
        document.add_i64(f("updated_at_ms"), page.updated_at_ms);
        if let Some(embedding) = &page.embedding {
            // An empty vector is not a "no vector": it is a value nothing can
            // be compared against, and every search over this split would fail
            // on it. Refuse it here, where it is still cheap to notice.
            if embedding.is_empty() {
                bail!(
                    "page {} has a zero-width embedding; a split containing it could never be searched",
                    page.path
                );
            }
            document.add_bytes(f("embedding"), &vector::encode(embedding));
        }
        writer.add_document(document)?;
    }
    writer.commit().context("committing index")?;
    // The split's provenance is written next to its index, so uploading,
    // hashing and materialising the split carry the record with the vectors it
    // describes. A batch with no vectors writes nothing: a keyword-only bucket
    // stays byte-identical to one published before this existed.
    if let Some(identity) = batch_identity(docs) {
        write_split_identity(dir, &identity)?;
    }
    Ok(())
}

/// The text a page is embedded from.
///
/// Title and body together: the title is often the most information-dense part
/// of a note, and dropping it would make short pages relatively hard to find.
/// Changing this function changes every stored vector, so it is versioned by
/// the split rather than mutated in place — a rebuild is the migration.
#[must_use]
pub fn embedding_text(doc: &PageDoc) -> String {
    format!("{}\n\n{}", doc.title, doc.body)
}

/// Attach embeddings to a batch of documents when a provider is configured.
///
/// One request for the whole batch: providers bill and rate-limit per call, so
/// embedding a page at a time would turn a publish into as many round trips as
/// there are changed pages. Without a provider the documents are returned
/// untouched — an unconfigured bucket keeps publishing, just without vectors.
///
/// # Errors
/// Fails when the provider fails, or when it breaches its contract (see
/// [`vector::embed_texts`]). Publication is then abandoned rather than
/// publishing a split whose vectors are silently wrong.
pub async fn attach_embeddings(
    docs: Vec<PageDoc>,
    embedder: Option<&dyn Embedder>,
) -> Result<Vec<PageDoc>> {
    let Some(embedder) = embedder else {
        return Ok(docs);
    };
    // The identity is part of the batch, not of a single document: one call
    // embeds everything, and a split is the unit a reader opens. Stamping it
    // here means the vector and its provenance cannot drift apart.
    let identity = EmbeddingIdentity::from(embedder.identity());
    let texts: Vec<String> = docs.iter().map(embedding_text).collect();
    let vectors = vector::embed_texts(embedder, &texts).await?;
    Ok(docs
        .into_iter()
        .zip(vectors)
        .map(|(doc, vector)| {
            doc.with_embedding(Some(vector))
                .with_embedding_identity(Some(identity.clone()))
        })
        .collect())
}

/// The embedder identity a batch of documents agrees on, if any.
///
/// `None` is the answer for a batch with no vectors: a split with no vector
/// column has no provenance to state. Documents are embedded in one batch by
/// one embedder, so the first record speaks for the split.
///
/// "Agrees on" is an assumption, not a check: [`PageDoc::embedding_identity`]
/// is a per-document field, and `find_map` below returns the *first* document
/// that carries one and never looks at the rest. That is sound for a batch
/// [`attach_embeddings`] produced — it is the only producer that stamps a
/// non-`None` identity, and it stamps one identity across the whole batch,
/// pinned by `attach_embeddings_stamps_the_whole_batch_with_one_identity` — but
/// a batch a caller assembled with mixed identities would publish the first one
/// and record nothing about the second.
fn batch_identity(docs: &[PageDoc]) -> Option<EmbeddingIdentity> {
    docs.iter().find_map(|doc| doc.embedding_identity.clone())
}

/// Write a split's embedder identity into its index directory.
///
/// Called while the split is being built, before it is hashed and uploaded, so
/// the record is part of the split's bytes: a reader cannot materialise the
/// vectors without also materialising who produced them.
///
/// # Errors
/// Fails when the record cannot be encoded or written.
fn write_split_identity(dir: &Path, identity: &EmbeddingIdentity) -> Result<()> {
    let path = dir.join(SPLIT_IDENTITY_FILE);
    let encoded =
        serde_json::to_vec_pretty(identity).context("encoding a split's embedding identity")?;
    std::fs::write(&path, encoded).with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

/// Read a split's embedder identity, if it recorded one.
///
/// `Ok(None)` is the compatible answer, not a failure: a split published
/// before this record existed — or built without a provider — carries no file,
/// and callers must not turn "no record" into "a different embedder". A record
/// that exists but cannot be parsed *is* a failure: provenance that is present
/// yet unreadable must not degrade into "unjudged".
///
/// # Errors
/// Fails when the file exists but cannot be read or parsed.
fn read_split_identity(dir: &Path) -> Result<Option<EmbeddingIdentity>> {
    let path = dir.join(SPLIT_IDENTITY_FILE);
    let raw = match std::fs::read(&path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).with_context(|| format!("reading {}", path.display())),
    };
    let identity = serde_json::from_slice(&raw)
        .with_context(|| format!("{} is not a valid embedding identity", path.display()))?;
    Ok(Some(identity))
}

/// Refuse to compare a query vector against vectors from a different model.
///
/// This is the half `vector::cosine` cannot see. A *width* change is already
/// fail-closed with its own precise message ("query has 8, stored value has
/// 4"), so this guard is deliberately about `(provider, model)` — the case
/// where two unrelated coordinate systems happen to have the same shape. Their
/// cosine would be a number between -1 and 1 that means nothing, and the
/// ranking built from it would look perfectly normal, so the mismatch is an
/// error naming both identities rather than a warning or a filtered split.
///
/// The recorded width is still reported, and is part of what a split states
/// about itself; for the dimension itself this leans on the width check, so a
/// pure dimension change keeps reporting its own, narrower cause.
///
/// # Errors
/// Fails when the split records a provider or model that differs from
/// `current`.
fn ensure_split_model_matches(
    dir: &Path,
    split_prefix: &str,
    current: &EmbeddingIdentity,
) -> Result<()> {
    let Some(recorded) = read_split_identity(dir)? else {
        return Ok(());
    };
    if recorded.provider == current.provider && recorded.model == current.model {
        return Ok(());
    }
    bail!(
        "the split {split_prefix} was indexed with provider '{}', model '{}', dim {}, but this machine would query with provider '{}', model '{}', dim {}; embeddings from two models are not comparable, so the vector stream is refused rather than ranked — re-embed the index by running `qm compact` with the current provider",
        recorded.provider,
        recorded.model,
        recorded.dim,
        current.provider,
        current.model,
        current.dim,
    )
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

    // A split carries whatever schema its producer used. Our own splits have
    // path/page_id/title/body/entities/links; a split another indexer produced
    // has its own fields. Resolve what exists and fall back to the split's own
    // indexed text fields rather than refusing to search it at all.
    let selected: Vec<tantivy::schema::Field> = fields
        .iter()
        .filter_map(|name| s.get_field(name).ok())
        .collect();
    let search_fields = if selected.is_empty() {
        let all: Vec<tantivy::schema::Field> = s
            .fields()
            .filter(|(_, entry)| entry.is_indexed() && !entry.is_fast())
            .map(|(field, _)| field)
            .collect();
        if all.is_empty() {
            bail!("split has no indexed fields to search");
        }
        all
    } else {
        selected
    };

    let reader = index.reader().context("opening reader")?;
    let searcher = reader.searcher();
    let parser = QueryParser::for_index(&index, search_fields);
    let parsed = parser.parse_query(query).context("parsing query")?;
    let top = searcher
        .search(&parsed, &TopDocs::with_limit(limit).order_by_score())
        .context("running query")?;

    let optional = |name: &str| s.get_field(name).ok();
    let path_field = optional("path");
    let page_field = optional("page_id");
    let workspace_field = optional("workspace_id");
    let project_field = optional("project_id");
    let title_field = optional("title");
    let updated_field = optional("updated_at_ms");
    // Foreign splits have no title: use the first stored text field so a hit is
    // still identifiable to a human.
    let title_fallback: Option<tantivy::schema::Field> = s
        .fields()
        .find(|(_, entry)| entry.is_stored())
        .map(|(field, _)| field);

    let mut hits = Vec::with_capacity(top.len());
    for (score, addr) in top {
        let doc: tantivy::TantivyDocument = searcher.doc(addr)?;
        let get = |field: Option<tantivy::schema::Field>| {
            field
                .and_then(|field| doc.get_first(field))
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .to_string()
        };
        hits.push(Hit {
            workspace_id: get(workspace_field),
            project_id: get(project_field),
            path: get(path_field),
            page_id: get(page_field),
            title: {
                let title = get(title_field);
                if title.is_empty() {
                    get(title_fallback)
                } else {
                    title
                }
            },
            score,
            fused_score: score,
            recency_multiplier: 1.0,
            updated_at_ms: updated_field
                .and_then(|field| doc.get_first(field))
                .and_then(|value| value.as_i64())
                .unwrap_or_default(),
            streams: vec![stream.to_string()],
        });
    }
    Ok(hits)
}

/// Search a field for any of `terms`, as exact terms rather than parsed text.
///
/// Used by neighbour lookup: "which pages link to exactly this path" is a set
/// membership question, and handing it to the query parser would reintroduce
/// the tokenisation that makes paths ambiguous.
///
/// # Errors
/// Propagates index-open and query failures.
pub fn search_terms(
    dir: &Path,
    field: &str,
    terms: &[String],
    stream: &str,
    limit: usize,
) -> Result<Vec<Hit>> {
    use tantivy::Term;
    use tantivy::query::{BooleanQuery, Occur, Query, TermQuery};
    use tantivy::schema::IndexRecordOption;

    if terms.is_empty() {
        return Ok(Vec::new());
    }
    let index = Index::open_in_dir(dir).context("opening index")?;
    let s = index.schema();
    let Some(field_ref) = s.get_field(field).ok() else {
        return Ok(Vec::new());
    };
    let subqueries: Vec<(Occur, Box<dyn Query>)> = terms
        .iter()
        .map(|term| {
            (
                Occur::Should,
                Box::new(TermQuery::new(
                    Term::from_field_text(field_ref, term),
                    IndexRecordOption::Basic,
                )) as Box<dyn Query>,
            )
        })
        .collect();
    let query = BooleanQuery::new(subqueries);

    let reader = index.reader().context("opening reader")?;
    let searcher = reader.searcher();
    let top = searcher
        .search(&query, &TopDocs::with_limit(limit).order_by_score())
        .context("running term query")?;
    let optional = |name: &str| s.get_field(name).ok();
    let path_field = optional("path");
    let page_field = optional("page_id");
    let workspace_field = optional("workspace_id");
    let project_field = optional("project_id");
    let title_field = optional("title");
    let updated_field = optional("updated_at_ms");
    let mut hits = Vec::with_capacity(top.len());
    for (score, addr) in top {
        let doc: tantivy::TantivyDocument = searcher.doc(addr)?;
        let get = |field: Option<tantivy::schema::Field>| {
            field
                .and_then(|field| doc.get_first(field))
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .to_string()
        };
        hits.push(Hit {
            workspace_id: get(workspace_field),
            project_id: get(project_field),
            path: get(path_field),
            page_id: get(page_field),
            title: get(title_field),
            score,
            fused_score: score,
            recency_multiplier: 1.0,
            updated_at_ms: updated_field
                .and_then(|field| doc.get_first(field))
                .and_then(|value| value.as_i64())
                .unwrap_or_default(),
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
    fuse_rrf_weighted(lists.into_iter().map(|list| (list, 1.0)).collect(), k)
}

/// Reciprocal-rank fusion with a per-list weight.
///
/// Neighbour hits arrive through a weaker signal than direct matches, so their
/// list contributes less and they land behind the pages that actually matched.
#[must_use]
pub fn fuse_rrf_weighted(lists: Vec<(Vec<Hit>, f32)>, k: f32) -> Vec<Hit> {
    let mut scores: HashMap<String, (Hit, f32)> = HashMap::new();
    for (list, weight) in lists {
        for (rank, hit) in list.into_iter().enumerate() {
            let contribution = weight / (k + rank as f32 + 1.0);
            // Our splits identify a page by id; a foreign split has none, so
            // fall back to the path and then the title rather than collapsing
            // every hit into one bucket.
            let key = if !hit.page_id.is_empty() {
                hit.page_id.clone()
            } else if !hit.path.is_empty() {
                hit.path.clone()
            } else {
                hit.title.clone()
            };
            scores
                .entry(key)
                .and_modify(|(existing, score)| {
                    *score += contribution;
                    existing.score = existing.score.max(hit.score);
                    for stream in &hit.streams {
                        if !existing.streams.contains(stream) {
                            existing.streams.push(stream.clone());
                        }
                    }
                })
                .or_insert((hit, contribution));
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
    search_workspace_tuned(
        store,
        project_store,
        workspace_id,
        projects,
        cache_root,
        query,
        limit,
        &SearchTuning::default(),
        None,
    )
    .await
}

/// Workspace search with an explicit relevance/recency trade-off.
///
/// The tuning is applied once, after the cross-project fusion: applying it per
/// project as well would count the same prior twice.
///
/// # Errors
/// Propagates each project's search failures.
#[allow(clippy::too_many_arguments)]
pub async fn search_workspace_tuned(
    store: &dyn ObjectStore,
    project_store: &ProjectStore,
    workspace_id: &WorkspaceId,
    projects: &[ProjectId],
    cache_root: &Path,
    query: &str,
    limit: usize,
    tuning: &SearchTuning,
    embedder: Option<&dyn Embedder>,
) -> Result<SearchOutcome> {
    let mut lists = Vec::new();
    let mut splits_searched = 0usize;
    let mut candidates = 0usize;
    let mut filtered_out = 0usize;
    let mut stream_candidates = std::collections::BTreeMap::new();
    for project_id in projects {
        let project_cache = cache_root.join(format!("{workspace_id}-{project_id}"));
        // Each project fuses its own streams and the per-project lists are
        // fused again below. Neighbour expansion and the vector stream are
        // deliberately per-project; the recency prior is not, because applying
        // it here as well would count the same prior once per project. The
        // caller's remaining knobs are forwarded, so `--no-vector` and
        // `--no-neighbors` mean the same thing with `--global` as without it.
        let inner = SearchTuning {
            recency_half_life_ms: 0,
            ..*tuning
        };
        let outcome = search_project_tuned(
            store,
            project_store,
            workspace_id,
            project_id,
            &project_cache,
            query,
            limit,
            &inner,
            embedder,
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
    tuning.apply(&mut hits);
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

/// How retrieval trades relevance against recency.
///
/// The borrowed design applies a *bounded* authority adjustment after fusion.
/// The bound is on the multiplier (0.5 = at most +50%), and one property of
/// reciprocal-rank fusion has to be stated plainly: RRF scores are compressed
/// (rank 1 and rank 2 differ by ~1.6% for k=60), so **any** positive boost can
/// reorder adjacent ranks. Recency here is a tie-breaker between comparably
/// relevant pages — not a licence for stale-but-irrelevant content to win, and
/// not a strong preference input. A page that agrees across *more streams*
/// (body + entities, say) is roughly twice as strong and keeps its lead.
/// A zero half-life disables the adjustment entirely.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SearchTuning {
    /// Age at which the boost halves, in milliseconds.
    pub recency_half_life_ms: i64,
    /// Maximum relative boost, e.g. 0.5 for "+50% at most".
    pub max_boost: f32,
    /// Reference time; hits newer than this get the full boost.
    pub now_ms: i64,
    /// Recall pages that link to the pages which matched ("second hop").
    pub neighbor_expansion: bool,
    /// How many top hits seed the neighbour lookup.
    pub neighbor_seeds: usize,
    /// Weight of the neighbour list in the fusion (direct matches are 1.0).
    pub neighbor_weight: f32,
    /// Run the vector stream when an embedder is available.
    ///
    /// Off is the answer for a caller that wants keyword recall only, and for
    /// `--no-vector`; it is not a way to hide a broken provider, because a
    /// configured-but-failing provider is an error either way.
    pub vector_search: bool,
    /// Weight of the vector list in the fusion.
    ///
    /// Below 1.0 on purpose: a semantic neighbour is a weaker signal than a
    /// page that actually contains the query terms, so the vector stream may
    /// add recall without displacing the direct matches.
    pub vector_weight: f32,
}

impl Default for SearchTuning {
    fn default() -> Self {
        Self {
            recency_half_life_ms: 30 * 24 * 60 * 60 * 1_000,
            max_boost: 0.5,
            now_ms: 0,
            neighbor_expansion: true,
            neighbor_seeds: 3,
            neighbor_weight: 0.4,
            vector_search: true,
            vector_weight: 0.6,
        }
    }
}

impl SearchTuning {
    /// Whether the adjustment does anything.
    #[must_use]
    pub fn is_disabled(&self) -> bool {
        self.recency_half_life_ms <= 0 || self.max_boost <= 0.0 || self.now_ms <= 0
    }

    /// Bounded recency multiplier for a page committed at `updated_at_ms`.
    #[must_use]
    pub fn multiplier(&self, updated_at_ms: i64) -> f32 {
        if self.is_disabled() || updated_at_ms <= 0 {
            return 1.0;
        }
        let age_ms = (self.now_ms - updated_at_ms).max(0) as f32;
        let half_life = self.recency_half_life_ms as f32;
        // 2^(-age/half_life): 1.0 for a brand-new page, 0.5 after one half-life.
        let decay = (-age_ms / half_life).exp2();
        1.0 + self.max_boost * decay
    }

    /// Apply the multiplier to a fused result list, re-sorting it.
    fn apply(&self, hits: &mut [Hit]) {
        if self.is_disabled() {
            return;
        }
        for hit in hits.iter_mut() {
            hit.recency_multiplier = self.multiplier(hit.updated_at_ms);
            hit.fused_score = hit.score;
            hit.score *= hit.recency_multiplier;
        }
        hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.page_id.cmp(&b.page_id))
        });
    }
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
    embedder: Option<&dyn Embedder>,
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
    // A rebuild that dropped vectors would silently delete the vector stream:
    // the index is derived, but it must derive the *same* documents.
    let docs = attach_embeddings(docs, embedder).await?;

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
    search_project_tuned(
        store,
        project_store,
        workspace_id,
        project_id,
        cache_root,
        query,
        limit,
        &SearchTuning::default(),
        None,
    )
    .await
}

/// Search a project with an explicit relevance/recency trade-off.
///
/// # Errors
/// Propagates catalog, materialisation, and query failures.
#[allow(clippy::too_many_arguments)]
pub async fn search_project_tuned(
    store: &dyn ObjectStore,
    project_store: &ProjectStore,
    workspace_id: &WorkspaceId,
    project_id: &ProjectId,
    cache_root: &Path,
    query: &str,
    limit: usize,
    tuning: &SearchTuning,
    embedder: Option<&dyn Embedder>,
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
    // Neighbour expansion: pages that link to the pages which matched. The
    // seed paths come from a provisional fusion of the direct streams; the
    // neighbour list joins the real fusion with a lower weight so it can add
    // recall without displacing direct matches.
    let mut weighted: Vec<(Vec<Hit>, f32)> =
        lists.iter().cloned().map(|list| (list, 1.0)).collect();
    if tuning.neighbor_expansion {
        let provisional = fuse_rrf(lists.clone(), 60.0);
        let seeds: Vec<String> = provisional
            .iter()
            .take(tuning.neighbor_seeds)
            .filter(|hit| !hit.path.is_empty())
            .map(|hit| hit.path.clone())
            .collect();
        if !seeds.is_empty() {
            for dir in &dirs {
                let neighbors = search_terms(dir, "links_exact", &seeds, "neighbors", limit)?;
                if !neighbors.is_empty() {
                    *stream_candidates
                        .entry("neighbors".to_string())
                        .or_insert(0) += neighbors.len();
                    weighted.push((neighbors, tuning.neighbor_weight));
                }
            }
        }
    }
    // Vector stream: pages that are semantically close to the query without
    // sharing its words. The query is embedded once and every split is scored
    // against the same vector. A split published before a provider existed has
    // no embedding column and contributes nothing, which is why a corpus can
    // mix split generations without failing a search.
    if let Some(embedder) = embedder.filter(|_| tuning.vector_search && tuning.vector_weight > 0.0)
    {
        // Before any vector is compared: a split whose vectors came from a
        // different embedder would score against the query and return a
        // meaningless ranking, so it is refused by name. The check sits inside
        // the vector branch on purpose — a caller that disabled the stream
        // with `--no-vector` is not comparing vectors, so it is not at risk,
        // and legacy splits with no record are not judged at all.
        let current = EmbeddingIdentity::from(embedder.identity());
        for (dir, split) in dirs.iter().zip(&loaded.catalog.splits) {
            ensure_split_model_matches(dir, &split.prefix, &current)?;
        }
        let query_vector = vector::embed_one(embedder, query).await?;
        for dir in &dirs {
            let hits = vector::search_vector(dir, &query_vector, "vector", limit)?;
            if !hits.is_empty() {
                *stream_candidates.entry("vector".to_string()).or_insert(0) += hits.len();
                weighted.push((hits, tuning.vector_weight));
            }
        }
    }
    let streams_active: Vec<String> = stream_candidates
        .iter()
        .filter(|(_, count)| **count > 0)
        .map(|(stream, _)| stream.clone())
        .collect();
    let fused = fuse_rrf_weighted(weighted, 60.0);

    // The authority check reads what it has to and nothing else: the whole form
    // is one object, and the sharded form is the root plus the shards the
    // *candidate paths* hash to. A query that matched a handful of pages
    // therefore does not pay for every shard in the scope, which is the read
    // amplification the sharded form exists to avoid.
    let candidate_paths: Vec<String> = fused.iter().map(|hit| hit.path.clone()).collect();
    let heads = project_store
        .load_path_heads(workspace_id, project_id, &candidate_paths)
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    let before = fused.len();
    let mut fused = fused;
    tuning.apply(&mut fused);
    let mut hits: Vec<Hit> = fused
        .into_iter()
        .filter(|hit| heads.is_current(&hit.path, &hit.page_id))
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
pub mod vector;

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::future::Future;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use crate::consolidate::{
        CompilerChoice, ConsolidationRequest, consolidate_session, consolidate_session_with,
    };
    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::{
        GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, PutMultipartOptions,
        PutOptions, PutPayload, PutResult,
    };
    use qm_core::{PagePath, SessionId, WriterId, manifest_shard_index};
    use qm_llm::{EmbedFuture, Embedder};
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
                embedding: None,
                embedding_identity: None,
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
                embedding: None,
                embedding_identity: None,
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

    #[test]
    fn recency_boost_is_bounded_and_disabled_cleanly() {
        let half_life = 30 * 24 * 60 * 60 * 1_000i64;
        let tuning = SearchTuning {
            recency_half_life_ms: half_life,
            max_boost: 0.5,
            now_ms: 10 * half_life,
            ..Default::default()
        };
        // Brand new: the full boost, never more.
        assert!((tuning.multiplier(tuning.now_ms) - 1.5).abs() < 1e-6);
        // One half-life old: half the boost.
        assert!((tuning.multiplier(tuning.now_ms - half_life) - 1.25).abs() < 1e-6);
        // Very old: the boost tends to zero, so relevance dominates.
        assert!(tuning.multiplier(1).abs() < 1.01);
        // A future timestamp is treated as new, not as a huge bonus.
        assert!((tuning.multiplier(tuning.now_ms + half_life) - 1.5).abs() < 1e-6);

        let disabled = SearchTuning {
            recency_half_life_ms: 0,
            ..tuning
        };
        assert_eq!(disabled.multiplier(tuning.now_ms), 1.0);
        assert!(disabled.is_disabled());
    }

    /// Second-hop recall: a page that *links to* the page you matched should be
    /// found too, ranked behind it, and only when the neighbour is still
    /// current according to the manifest.
    #[tokio::test]
    async fn link_neighbors_are_recalled_behind_the_direct_hit() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let project = reader(&bucket);
        let workspace = WorkspaceId::new("acme").unwrap();
        let project_id = ProjectId::new("ai-memory").unwrap();
        let writer = WriterId::new("mbp-a").unwrap();
        let build_root = TempDir::new().unwrap();

        // A answers the query; B only links to A. The link target deliberately
        // avoids the query term: if it contained it, B would be a direct hit and
        // the test would prove nothing about neighbour expansion.
        let pages = [
            ("notes/raft.md", "consensus algorithms in one place"),
            ("notes/index.md", "see [[notes/raft.md]] for the details"),
            (
                "notes/unrelated.md",
                "caching notes that link nowhere relevant",
            ),
        ];
        let mut docs = Vec::new();
        for (seq, (path, body)) in pages.iter().enumerate() {
            let page_path = PagePath::new(*path).unwrap();
            project
                .commit_page(CommitPageRequest {
                    workspace_id: workspace.clone(),
                    project_id: project_id.clone(),
                    path: page_path.clone(),
                    title: (*path).to_string(),
                    body: (*body).to_string(),
                    writer_id: writer.clone(),
                    now_ms: seq as i64 + 1,
                })
                .await
                .unwrap();
            let page = project
                .read_page(&workspace, &project_id, &page_path)
                .await
                .unwrap()
                .unwrap();
            let doc = PageDoc::from_version(&workspace, &project_id, &page, seq as i64 + 1);
            assert_eq!(
                doc.links.iter().any(|link| link == "notes/raft.md"),
                *path == "notes/index.md",
                "only the index page links to the answer: {doc:?}"
            );
            assert!(
                !doc.body.contains("consensus") || *path == "notes/raft.md",
                "only the answer may mention the query term: {doc:?}"
            );
            docs.push(doc);
        }
        publish_split_index(
            bucket.as_ref(),
            &project,
            &workspace,
            &project_id,
            &writer,
            1,
            &docs,
            &build_root.path().join("s"),
            1,
        )
        .await
        .unwrap();

        let tuning = SearchTuning {
            now_ms: 100,
            ..Default::default()
        };
        let outcome = search_project_tuned(
            bucket.as_ref(),
            &project,
            &workspace,
            &project_id,
            TempDir::new().unwrap().path(),
            "consensus",
            10,
            &tuning,
            None,
        )
        .await
        .unwrap();
        let order: Vec<&str> = outcome.hits.iter().map(|hit| hit.path.as_str()).collect();
        assert_eq!(order.first(), Some(&"notes/raft.md"), "{order:?}");
        let neighbor = outcome
            .hits
            .iter()
            .position(|hit| hit.path == "notes/index.md");
        assert!(
            neighbor.is_some(),
            "the linking page must be recalled: {order:?}"
        );
        assert!(
            neighbor.unwrap() > 0,
            "and ranked behind the page it links to"
        );
        assert!(
            outcome.hits[neighbor.unwrap()]
                .streams
                .contains(&"neighbors".to_string()),
            "the hit must say which stream found it: {:?}",
            outcome.hits[neighbor.unwrap()]
        );
        assert!(
            !order.contains(&"notes/unrelated.md"),
            "a page that links nowhere relevant must not be pulled in: {order:?}"
        );

        // With the expansion off, only the direct hit remains.
        let off = SearchTuning {
            neighbor_expansion: false,
            ..tuning
        };
        let outcome = search_project_tuned(
            bucket.as_ref(),
            &project,
            &workspace,
            &project_id,
            TempDir::new().unwrap().path(),
            "consensus",
            10,
            &off,
            None,
        )
        .await
        .unwrap();
        let order: Vec<&str> = outcome.hits.iter().map(|hit| hit.path.as_str()).collect();
        assert_eq!(order, vec!["notes/raft.md"], "{order:?}");
    }

    /// What the recency prior actually promises, checked as three properties:
    /// equally relevant pages are ordered by freshness, agreement across more
    /// streams still wins, and disabling the prior changes nothing at all.
    #[tokio::test]
    async fn recency_breaks_ties_without_overturning_stream_agreement() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let project = reader(&bucket);
        let workspace = WorkspaceId::new("acme").unwrap();
        let project_id = ProjectId::new("ai-memory").unwrap();
        let writer = WriterId::new("mbp-a").unwrap();
        let build_root = TempDir::new().unwrap();
        let now = 1_700_000_000_000i64;
        let long_ago = now - 365 * 24 * 60 * 60 * 1_000;

        // old-strong declares the entity (two streams), the others do not.
        let pages = [
            ("notes/old-plain.md", "consensus in prose", long_ago),
            ("notes/new-plain.md", "consensus in prose", now),
            ("notes/old-strong.md", "consensus in `consensus`", long_ago),
        ];
        let mut docs = Vec::new();
        for (path, body, at) in pages {
            let page_path = PagePath::new(path).unwrap();
            project
                .commit_page(CommitPageRequest {
                    workspace_id: workspace.clone(),
                    project_id: project_id.clone(),
                    path: page_path.clone(),
                    title: path.to_string(),
                    body: body.to_string(),
                    writer_id: writer.clone(),
                    now_ms: at,
                })
                .await
                .unwrap();
            let page = project
                .read_page(&workspace, &project_id, &page_path)
                .await
                .unwrap()
                .unwrap();
            // Keep the real page id: a synthetic one would be filtered out by
            // the authority check and the test would measure nothing.
            docs.push(PageDoc::from_version(&workspace, &project_id, &page, at));
        }
        publish_split_index(
            bucket.as_ref(),
            &project,
            &workspace,
            &project_id,
            &writer,
            1,
            &docs,
            &build_root.path().join("s"),
            now,
        )
        .await
        .unwrap();

        let tuning = SearchTuning {
            now_ms: now,
            ..Default::default()
        };
        let outcome = search_project_tuned(
            bucket.as_ref(),
            &project,
            &workspace,
            &project_id,
            TempDir::new().unwrap().path(),
            "consensus",
            10,
            &tuning,
            None,
        )
        .await
        .unwrap();
        let order: Vec<&str> = outcome.hits.iter().map(|hit| hit.path.as_str()).collect();
        let rank = |path: &str| {
            order
                .iter()
                .position(|candidate| *candidate == path)
                .unwrap()
        };

        // Agreeing across two streams is a stronger signal than being fresh in
        // one, and the bounded boost cannot overturn it.
        assert!(
            rank("notes/old-strong.md") < rank("notes/new-plain.md"),
            "stream agreement must beat freshness: {order:?}"
        );
        // Between equally relevant pages, the fresher one comes first.
        assert!(
            rank("notes/new-plain.md") < rank("notes/old-plain.md"),
            "equally relevant pages order by freshness: {order:?}"
        );
        // The boost is reported, not hidden.
        let fresh = outcome
            .hits
            .iter()
            .find(|hit| hit.path == "notes/new-plain.md")
            .unwrap();
        assert!(fresh.recency_multiplier > 1.0, "{fresh:?}");
        assert!(fresh.fused_score > 0.0, "{fresh:?}");

        // Disabling the prior leaves every multiplier at 1.0.
        let untuned = search_project(
            bucket.as_ref(),
            &project,
            &workspace,
            &project_id,
            TempDir::new().unwrap().path(),
            "consensus",
            10,
        )
        .await
        .unwrap();
        assert!(
            untuned
                .hits
                .iter()
                .all(|hit| (hit.recency_multiplier - 1.0).abs() < f32::EPSILON)
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
            None,
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
            None,
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
    /// Asking for the LLM compiler with nothing configured is a configuration
    /// error, not a quiet rules compilation.
    ///
    /// The report used to name the rule compiler with `used_fallback: false`,
    /// which reads as "rules were chosen" rather than "the LLM you asked for
    /// was not there". A *configured* provider that fails still falls back —
    /// that is what `used_fallback` records — but an unconfigured one is not a
    /// failure to recover from.
    #[tokio::test]
    async fn asking_for_the_llm_compiler_without_a_provider_is_an_error() {
        if std::env::var("QM_LLM_BASE_URL").is_ok_and(|value| !value.trim().is_empty()) {
            // The premise is a property of the environment this test runs in,
            // so say so rather than passing for a different reason.
            eprintln!("not exercised: QM_LLM_BASE_URL is set in this environment");
            return;
        }

        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let project = reader(&bucket);
        let workspace = WorkspaceId::new("acme").unwrap();
        let project_id = ProjectId::new("ai-memory").unwrap();
        let session = SessionId::new("sess-llm").unwrap();
        let consolidator = WriterId::new("mbp-a").unwrap();
        ingest(&project, &session, &["this session needs a compiler"], 100).await;

        let error = consolidate_session_with(
            CompilerChoice::Llm,
            &project,
            ConsolidationRequest {
                workspace_id: &workspace,
                project_id: &project_id,
                session_id: &session,
                consolidator: &consolidator,
                now_ms: 200,
                lease_ttl_ms: 60_000,
            },
        )
        .await
        .expect_err("an unconfigured provider cannot compile with the LLM");

        assert!(
            error.to_string().contains("no provider is configured"),
            "the error has to name what is missing: {error}"
        );

        // The refusal must not leave behind a page compiled by something the
        // caller did not ask for.
        let page = project
            .read_page(
                &workspace,
                &project_id,
                &PagePath::new("sessions/sess-llm.md").unwrap(),
            )
            .await
            .unwrap();
        assert!(
            page.is_none(),
            "a refused LLM request must not commit a rule-rendered page"
        );
    }

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

    /// Recall evaluation over a synthetic corpus with known ground truth.
    ///
    /// The corpus is built so that the answer is *declared* by exactly one page
    /// while the same token also appears in prose in several distractors. That
    /// makes the evaluation able to distinguish retrieval configurations: the
    /// same queries are run twice, once with entity/link streams active and once
    /// against a corpus with the declared fields stripped. If the numbers do not
    /// move, the harness is measuring nothing.
    #[tokio::test]
    async fn recall_eval_separates_declared_matches_from_prose() {
        use std::collections::BTreeMap;

        const PAGES: usize = 20;
        const DISTRACTORS: usize = 3;
        const TOP_K: usize = 5;

        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let project = reader(&bucket);
        let workspace = WorkspaceId::new("acme").unwrap();
        let writer = WriterId::new("eval").unwrap();
        let build_root = TempDir::new().unwrap();

        // Two projects: one keeps the declared fields, one has them stripped.
        // Every competing page is committed, because the authority check drops
        // anything the manifest does not name — an evaluation whose distractors
        // are filtered away measures nothing.
        let mut expected: BTreeMap<usize, (String, String)> = BTreeMap::new();
        for (project_name, keep_declared) in [("declared", true), ("stripped", false)] {
            let project_id = ProjectId::new(project_name).unwrap();
            let mut docs = Vec::new();
            let mut now_ms = 0i64;
            for index in 0..PAGES {
                let token = format!("qm-eval-{index:03}");
                let answer = format!("notes/answer-{index:03}.md");
                expected.insert(index, (token.clone(), answer.clone()));

                // The answer mentions the token once, but declares it.
                let answer_body = format!("the canonical note for `{token}`");
                // Distractors repeat it in prose, which plain BM25 rewards.
                let distractor_body = format!(
                    "{token} {token} {token} {token} appears while discussing unrelated caching work"
                );
                let mut pages = vec![(answer.clone(), answer_body.clone())];
                for d in 0..DISTRACTORS {
                    pages.push((
                        format!("notes/distract-{index:03}-{d}.md"),
                        distractor_body.clone(),
                    ));
                }

                for (path, body) in pages {
                    now_ms += 1;
                    let page_path = PagePath::new(&path).unwrap();
                    let outcome = project
                        .commit_page(CommitPageRequest {
                            workspace_id: workspace.clone(),
                            project_id: project_id.clone(),
                            path: page_path,
                            title: path.clone(),
                            body: body.clone(),
                            writer_id: writer.clone(),
                            now_ms,
                        })
                        .await
                        .unwrap();
                    let mut doc = PageDoc {
                        workspace_id: workspace.to_string(),
                        project_id: project_id.to_string(),
                        path: path.clone(),
                        page_id: outcome.page_id.as_str().to_string(),
                        title: path.clone(),
                        body,
                        updated_at_ms: now_ms,
                        entities: Vec::new(),
                        links: Vec::new(),
                        embedding: None,
                        embedding_identity: None,
                    };
                    if keep_declared {
                        doc.entities = entities::extract(&doc.body).entities;
                    }
                    docs.push(doc);
                }
            }
            publish_split_index(
                bucket.as_ref(),
                &project,
                &workspace,
                &project_id,
                &writer,
                1,
                &docs,
                &build_root.path().join(project_name),
                0,
            )
            .await
            .unwrap();
        }

        // Measure recall@k and MRR for each configuration.
        let mut metrics: BTreeMap<&str, (f32, f32)> = BTreeMap::new();
        for project_name in ["declared", "stripped"] {
            let project_id = ProjectId::new(project_name).unwrap();
            let cache = TempDir::new().unwrap();
            let mut hits_at_k = 0usize;
            let mut reciprocal_rank = 0.0f32;
            for index in 0..PAGES {
                let (token, answer) = &expected[&index];
                let outcome = search_project(
                    bucket.as_ref(),
                    &project,
                    &workspace,
                    &project_id,
                    cache.path(),
                    token,
                    TOP_K,
                )
                .await
                .unwrap();
                let rank = outcome
                    .hits
                    .iter()
                    .position(|hit| &hit.path == answer)
                    .map(|position| position + 1);
                match rank {
                    Some(rank) if rank <= TOP_K => {
                        hits_at_k += 1;
                        reciprocal_rank += 1.0 / rank as f32;
                    }
                    _ => {}
                }
            }
            let recall = hits_at_k as f32 / PAGES as f32;
            let mrr = reciprocal_rank / PAGES as f32;
            println!("{project_name}: recall@{TOP_K}={recall:.3} mrr={mrr:.3}");
            metrics.insert(project_name, (recall, mrr));
        }

        let (declared_recall, declared_mrr) = metrics["declared"];
        let (stripped_recall, stripped_mrr) = metrics["stripped"];
        // The declared corpus must answer almost every query in the top few…
        assert!(
            declared_recall >= 0.95,
            "recall@{TOP_K} with declared entities was {declared_recall}"
        );
        assert!(declared_mrr >= 0.80, "mrr was {declared_mrr}");
        // …and stripping the declared fields must actually hurt, or the metric
        // is not measuring the streams it claims to.
        assert!(
            declared_mrr > stripped_mrr || stripped_recall < declared_recall,
            "the evaluation cannot tell the configurations apart: declared={metrics:?}"
        );
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
    /// An embedder whose answers the test writes down.
    ///
    /// Not `DeterministicEmbedder`: that would make "close" a property of
    /// SHA-256, and a real provider would make it a property of the model.
    /// What these tests ask is whether a vector travels intact from publish,
    /// through the index, into the fusion — so the numbers are literal, and a
    /// wrong answer is expressible on purpose (see `answer`).
    struct FakeEmbedder {
        dim: usize,
        model: String,
        answers: std::collections::HashMap<String, Vec<Vec<f32>>>,
        default: Vec<f32>,
    }

    impl FakeEmbedder {
        fn new(dim: usize, default: Vec<f32>) -> Self {
            assert_eq!(default.len(), dim);
            Self {
                dim,
                model: "fake-model".to_string(),
                answers: std::collections::HashMap::new(),
                default,
            }
        }

        /// Name the model this fake stands in for. Two fakes with the same
        /// width and different models are the case the split identity exists
        /// to separate.
        fn model(mut self, model: &str) -> Self {
            self.model = model.to_string();
            self
        }

        /// Answer `text` with exactly these rows — as many, and as wide, as
        /// the test asks for, so a contract breach can be expressed as data.
        fn answer(mut self, text: &str, rows: Vec<Vec<f32>>) -> Self {
            self.answers.insert(text.to_string(), rows);
            self
        }
    }

    impl Embedder for FakeEmbedder {
        fn dim(&self) -> usize {
            self.dim
        }

        fn identity(&self) -> qm_llm::EmbedderIdentity {
            qm_llm::EmbedderIdentity {
                provider: "fake".to_string(),
                model: self.model.clone(),
                dim: self.dim,
            }
        }

        fn embed<'a>(&'a self, texts: &'a [String]) -> EmbedFuture<'a> {
            Box::pin(async move {
                Ok(texts
                    .iter()
                    .flat_map(|text| {
                        self.answers
                            .get(text)
                            .cloned()
                            .unwrap_or_else(|| vec![self.default.clone()])
                    })
                    .collect())
            })
        }
    }

    /// The text a page is embedded from, spelled out so a test can key a fake
    /// answer on it without duplicating the formatting rule.
    fn text_for(title: &str, body: &str) -> String {
        format!("{title}\n\n{body}")
    }

    /// Commit `pages` and publish one split, embedding them when asked.
    async fn publish_pages(
        bucket: &Arc<dyn ObjectStore>,
        workspace: &WorkspaceId,
        project_id: &ProjectId,
        build_root: &Path,
        pages: &[(&str, &str, &str)],
        embedder: Option<&dyn Embedder>,
    ) {
        let project = reader(bucket);
        let writer = WriterId::new("mbp-a").unwrap();
        let mut docs = Vec::new();
        for (index, (path, title, body)) in pages.iter().enumerate() {
            let page_path = PagePath::new(*path).unwrap();
            let now_ms = index as i64 + 1;
            project
                .commit_page(CommitPageRequest {
                    workspace_id: workspace.clone(),
                    project_id: project_id.clone(),
                    path: page_path.clone(),
                    title: (*title).to_string(),
                    body: (*body).to_string(),
                    writer_id: writer.clone(),
                    now_ms,
                })
                .await
                .unwrap();
            let page = project
                .read_page(workspace, project_id, &page_path)
                .await
                .unwrap()
                .unwrap();
            docs.push(PageDoc::from_version(workspace, project_id, &page, now_ms));
        }
        let docs = attach_embeddings(docs, embedder).await.unwrap();
        publish_split_index(
            bucket,
            &project,
            workspace,
            project_id,
            &writer,
            1,
            &docs,
            &build_root.join("split"),
            1,
        )
        .await
        .unwrap();
    }

    async fn search_with_embedder(
        bucket: &Arc<dyn ObjectStore>,
        workspace: &WorkspaceId,
        project_id: &ProjectId,
        query: &str,
        embedder: Option<&dyn Embedder>,
    ) -> SearchOutcome {
        search_with_embedder_result(bucket, workspace, project_id, query, embedder)
            .await
            .unwrap()
    }

    /// The same search, but a caller that expects the read path to refuse.
    async fn search_with_embedder_result(
        bucket: &Arc<dyn ObjectStore>,
        workspace: &WorkspaceId,
        project_id: &ProjectId,
        query: &str,
        embedder: Option<&dyn Embedder>,
    ) -> Result<SearchOutcome> {
        // No recency prior: these tests are about recall, not ranking drift.
        let tuning = SearchTuning {
            now_ms: 0,
            ..Default::default()
        };
        search_project_tuned(
            bucket.as_ref(),
            &reader(bucket),
            workspace,
            project_id,
            TempDir::new().unwrap().path(),
            query,
            10,
            &tuning,
            embedder,
        )
        .await
    }

    /// How many splits the catalog currently names.
    async fn splits_in_catalog(
        bucket: &Arc<dyn ObjectStore>,
        workspace: &WorkspaceId,
        project_id: &ProjectId,
    ) -> usize {
        reader(bucket)
            .load_catalog(workspace, project_id)
            .await
            .unwrap()
            .catalog
            .splits
            .len()
    }

    /// Publish one more split of the manifest's live pages, embedded by
    /// `embedder` when one is given.
    ///
    /// Nothing new is committed: this is what a machine does when it
    /// re-publishes the same authoritative pages under a different embedding
    /// model.
    async fn publish_generation(
        bucket: &Arc<dyn ObjectStore>,
        workspace: &WorkspaceId,
        project_id: &ProjectId,
        build_dir: &Path,
        writer: &str,
        seq: u64,
        embedder: Option<&dyn Embedder>,
    ) {
        let project = reader(bucket);
        let loaded = project.load(workspace, project_id).await.unwrap();
        let mut docs = Vec::with_capacity(loaded.manifest.pages.len());
        for (path, entry) in &loaded.manifest.pages {
            let page_path = PagePath::new(path).unwrap();
            let page = project
                .read_page_version(workspace, project_id, &page_path, &entry.page_id)
                .await
                .unwrap();
            docs.push(PageDoc::from_version(
                workspace,
                project_id,
                &page,
                entry.created_at_ms,
            ));
        }
        let docs = attach_embeddings(docs, embedder).await.unwrap();
        publish_split_index(
            bucket,
            &project,
            workspace,
            project_id,
            &WriterId::new(writer).unwrap(),
            seq,
            &docs,
            build_dir,
            1,
        )
        .await
        .unwrap();
    }

    const QUERY: &str = "consensus";
    const KEYWORD: (&str, &str, &str) = (
        "notes/keyword.md",
        "Raft consensus",
        "leader election and log replication",
    );
    const PARAPHRASE: (&str, &str, &str) = (
        "notes/paraphrase.md",
        "Choosing a leader",
        "a cluster picks one node to direct writes",
    );

    /// A vector that points the same way as the query.
    fn near_query() -> Vec<f32> {
        vec![0.95, 0.05, 0.0, 0.0]
    }

    /// The query vector itself.
    fn query_vector() -> Vec<f32> {
        vec![1.0, 0.0, 0.0, 0.0]
    }

    /// A vector that shares nothing with the query's direction.
    fn orthogonal() -> Vec<f32> {
        vec![0.0, 1.0, 0.0, 0.0]
    }

    /// The acceptance case: `PARAPHRASE` shares no words with the query, so
    /// only the vector stream can reach it.
    fn corpus() -> [(&'static str, &'static str, &'static str); 2] {
        [KEYWORD, PARAPHRASE]
    }

    fn fake_for_corpus() -> FakeEmbedder {
        FakeEmbedder::new(4, orthogonal())
            .answer(QUERY, vec![query_vector()])
            .answer(&text_for(KEYWORD.1, KEYWORD.2), vec![orthogonal()])
            .answer(&text_for(PARAPHRASE.1, PARAPHRASE.2), vec![near_query()])
    }

    #[tokio::test]
    async fn a_page_without_the_query_words_is_recalled_by_the_vector_stream() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let workspace = WorkspaceId::new("acme").unwrap();
        let project_id = ProjectId::new("ai-memory").unwrap();
        let build_root = TempDir::new().unwrap();
        let embedder = fake_for_corpus();
        publish_pages(
            &bucket,
            &workspace,
            &project_id,
            build_root.path(),
            &corpus(),
            Some(&embedder),
        )
        .await;

        // The premise: the paraphrase really does not contain the query.
        assert!(
            !text_for(PARAPHRASE.1, PARAPHRASE.2).contains(QUERY),
            "the paraphrase must not mention the query word, or the vector \
             stream is not what found it"
        );

        let outcome =
            search_with_embedder(&bucket, &workspace, &project_id, QUERY, Some(&embedder)).await;
        assert_eq!(
            outcome.streams_active,
            vec!["body".to_string(), "vector".to_string()],
            "both streams must have contributed: {outcome:?}"
        );
        let paraphrase = outcome
            .hits
            .iter()
            .find(|hit| hit.path == PARAPHRASE.0)
            .unwrap_or_else(|| panic!("the paraphrase was not recalled: {outcome:?}"));
        assert!(
            paraphrase.streams.contains(&"vector".to_string()),
            "the paraphrase is only reachable through its embedding: {paraphrase:?}"
        );
        assert_eq!(outcome.stream_candidates["vector"], 1);
        // Both pages are present, and the keyword match is not displaced.
        assert_eq!(outcome.hits.len(), 2, "{outcome:?}");
        assert_eq!(outcome.hits[0].path, KEYWORD.0, "{outcome:?}");
    }

    /// The vector stream proposes candidates; the manifest still decides.
    ///
    /// The first review of the vector stream verified this order with a
    /// throwaway probe. It belongs in the suite: a page only its embedding can
    /// reach must vanish the moment it is superseded, even though the split
    /// that still holds it is never rebuilt. Otherwise the index would be
    /// answering questions about the corpus, which is authority's job.
    #[tokio::test]
    async fn a_superseded_page_is_filtered_even_when_only_the_vector_stream_recalls_it() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let workspace = WorkspaceId::new("acme").unwrap();
        let project_id = ProjectId::new("ai-memory").unwrap();
        let build_root = TempDir::new().unwrap();
        let project = reader(&bucket);
        let writer = WriterId::new("mbp-a").unwrap();
        let path = PagePath::new(PARAPHRASE.0).unwrap();

        // The page shares no words with the query, so only its embedding can
        // put it in front of the filter: a body hit would explain the recall.
        assert!(
            !text_for(PARAPHRASE.1, PARAPHRASE.2).contains(QUERY),
            "the page must not contain the query word, or the vector stream \
             would not be what found it"
        );
        let embedder = FakeEmbedder::new(4, orthogonal())
            .answer(QUERY, vec![query_vector()])
            .answer(&text_for(PARAPHRASE.1, PARAPHRASE.2), vec![near_query()]);

        // Publish a split that holds the first version, embeddings included.
        let commit = |body: &str, now_ms: i64| CommitPageRequest {
            workspace_id: workspace.clone(),
            project_id: project_id.clone(),
            path: path.clone(),
            title: PARAPHRASE.1.to_string(),
            body: body.to_string(),
            writer_id: writer.clone(),
            now_ms,
        };
        project.commit_page(commit(PARAPHRASE.2, 1)).await.unwrap();
        let first = project
            .read_page(&workspace, &project_id, &path)
            .await
            .unwrap()
            .unwrap();
        let doc = PageDoc::from_version(&workspace, &project_id, &first, 1);
        let docs = attach_embeddings(vec![doc], Some(&embedder)).await.unwrap();
        publish_split_index(
            &bucket,
            &project,
            &workspace,
            &project_id,
            &writer,
            1,
            &docs,
            &build_root.path().join("split"),
            1,
        )
        .await
        .unwrap();

        // Premise: while the first version is authoritative the vector stream
        // is what finds the page, and the page is returned.
        let live =
            search_with_embedder(&bucket, &workspace, &project_id, QUERY, Some(&embedder)).await;
        assert!(
            live.stream_candidates.get("vector").copied().unwrap_or(0) >= 1,
            "the premise: only the vector stream can reach this page: {live:?}"
        );
        assert_eq!(
            live.stream_candidates.get("body"),
            Some(&0),
            "the premise: the page's words must not match the query, or a body \
             hit would explain the recall instead of the embedding: {live:?}"
        );
        assert_eq!(live.hits.len(), 1, "{live:?}");
        assert_eq!(live.hits[0].page_id, first.page_id.as_str());

        // Supersede the path without republishing. The split still holds the
        // old page_id — exactly what a reader sees between a write and the
        // next publish.
        let outcome = project
            .commit_page(commit("a cluster elects one node for each term", 2))
            .await
            .unwrap();
        assert_ne!(
            outcome.page_id, first.page_id,
            "the second commit must be a new version"
        );

        let stale =
            search_with_embedder(&bucket, &workspace, &project_id, QUERY, Some(&embedder)).await;
        assert!(
            stale.stream_candidates.get("vector").copied().unwrap_or(0) >= 1,
            "the vector stream must still propose the stale copy; that is the \
             point of the test: {stale:?}"
        );
        assert!(
            stale.hits.is_empty(),
            "the manifest superseded this page, so nothing may be returned: {stale:?}"
        );
        assert!(
            stale.filtered_out >= 1,
            "the stale candidate must be counted as filtered, not silently \
             dropped: {stale:?}"
        );
    }

    /// The other half of the acceptance case: with no vector stream running —
    /// because no provider is configured, or because the split was published
    /// without one — the paraphrase must not appear at all.
    #[tokio::test]
    async fn the_paraphrase_is_absent_whenever_the_vector_stream_does_not_run() {
        let workspace = WorkspaceId::new("acme").unwrap();
        let project_id = ProjectId::new("ai-memory").unwrap();
        let embedder = fake_for_corpus();

        // (a) The split has embeddings, but the search does not run the stream.
        let embedded_bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let build_root = TempDir::new().unwrap();
        publish_pages(
            &embedded_bucket,
            &workspace,
            &project_id,
            build_root.path(),
            &corpus(),
            Some(&embedder),
        )
        .await;
        let outcome =
            search_with_embedder(&embedded_bucket, &workspace, &project_id, QUERY, None).await;
        assert_eq!(
            outcome.hits.len(),
            1,
            "only the keyword match may survive: {outcome:?}"
        );
        assert_eq!(outcome.hits[0].path, KEYWORD.0);
        assert!(
            !outcome.streams_active.contains(&"vector".to_string()),
            "no provider was passed, so no vector stream ran: {outcome:?}"
        );

        // (b) The search would run the stream, but the split has no vectors.
        let plain_bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let build_root = TempDir::new().unwrap();
        publish_pages(
            &plain_bucket,
            &workspace,
            &project_id,
            build_root.path(),
            &corpus(),
            None,
        )
        .await;
        let outcome = search_with_embedder(
            &plain_bucket,
            &workspace,
            &project_id,
            QUERY,
            Some(&embedder),
        )
        .await;
        assert_eq!(outcome.hits.len(), 1, "{outcome:?}");
        assert_eq!(outcome.hits[0].path, KEYWORD.0);
        assert!(
            !outcome.stream_candidates.contains_key("vector"),
            "a split published without embeddings contributes no vector \
             candidates and must not fail the search: {outcome:?}"
        );
    }

    /// An orthogonal page carries no evidence that it is about the query, so it
    /// must not enter the fusion — with the opposite answer as the positive
    /// control, so the check cannot pass by refusing everything.
    #[tokio::test]
    async fn only_positively_correlated_pages_become_vector_candidates() {
        let workspace = WorkspaceId::new("acme").unwrap();
        let project_id = ProjectId::new("ai-memory").unwrap();
        let pages = [PARAPHRASE];

        let orthogonal_answer =
            FakeEmbedder::new(4, orthogonal()).answer(QUERY, vec![query_vector()]);
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let build_root = TempDir::new().unwrap();
        publish_pages(
            &bucket,
            &workspace,
            &project_id,
            build_root.path(),
            &pages,
            Some(&orthogonal_answer),
        )
        .await;
        let outcome = search_with_embedder(
            &bucket,
            &workspace,
            &project_id,
            QUERY,
            Some(&orthogonal_answer),
        )
        .await;
        assert!(
            outcome.hits.is_empty(),
            "an orthogonal embedding is not a candidate: {outcome:?}"
        );

        let near_answer = FakeEmbedder::new(4, orthogonal())
            .answer(QUERY, vec![query_vector()])
            .answer(&text_for(PARAPHRASE.1, PARAPHRASE.2), vec![near_query()]);
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let build_root = TempDir::new().unwrap();
        publish_pages(
            &bucket,
            &workspace,
            &project_id,
            build_root.path(),
            &pages,
            Some(&near_answer),
        )
        .await;
        let outcome =
            search_with_embedder(&bucket, &workspace, &project_id, QUERY, Some(&near_answer)).await;
        assert_eq!(
            outcome.hits.len(),
            1,
            "the same corpus with a near answer must recall the page: {outcome:?}"
        );
    }

    /// A provider that returns the wrong width or the wrong number of rows is a
    /// contract breach. Repairing it silently would corrupt every later
    /// comparison, so the search fails instead.
    #[tokio::test]
    async fn a_provider_that_breaches_its_contract_fails_the_search() {
        let workspace = WorkspaceId::new("acme").unwrap();
        let project_id = ProjectId::new("ai-memory").unwrap();
        let build_root = TempDir::new().unwrap();
        let good = fake_for_corpus();
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        publish_pages(
            &bucket,
            &workspace,
            &project_id,
            build_root.path(),
            &corpus(),
            Some(&good),
        )
        .await;

        let tuning = SearchTuning {
            now_ms: 0,
            ..Default::default()
        };
        let search_with = |embedder: &'static dyn Embedder| {
            let bucket = Arc::clone(&bucket);
            let workspace = workspace.clone();
            let project_id = project_id.clone();
            async move {
                search_project_tuned(
                    bucket.as_ref(),
                    &reader(&bucket),
                    &workspace,
                    &project_id,
                    TempDir::new().unwrap().path(),
                    QUERY,
                    10,
                    &tuning,
                    Some(embedder),
                )
                .await
            }
        };

        // Wrong width: the provider advertises four dimensions, answers with three.
        let narrow = Box::leak(Box::new(
            FakeEmbedder::new(4, orthogonal()).answer(QUERY, vec![vec![1.0, 0.0, 0.0]]),
        ));
        let error = search_with(narrow)
            .await
            .expect_err("a three-wide answer to a four-wide provider must fail");
        assert!(
            format!("{error:#}").contains("configured width is 4"),
            "the error must name the breach: {error:#}"
        );

        // Wrong count: two rows for one input.
        let duplicated = Box::leak(Box::new(
            FakeEmbedder::new(4, orthogonal()).answer(QUERY, vec![query_vector(), query_vector()]),
        ));
        let error = search_with(duplicated)
            .await
            .expect_err("two rows for one input must fail");
        assert!(
            format!("{error:#}").contains("returned 2 vector(s) for 1 input(s)"),
            "the error must name the breach: {error:#}"
        );
    }

    /// The publish path enforces the same contract, so a breach cannot be
    /// baked into an immutable split and discovered only at query time.
    #[tokio::test]
    async fn attaching_embeddings_fails_closed_on_a_contract_breach() {
        let docs = docs();
        // One of the two inputs gets no row: the batch is short by one.
        let short = FakeEmbedder::new(4, vec![0.0, 0.0, 0.0, 0.0])
            .answer(&embedding_text(&docs[0]), Vec::new());
        let error = attach_embeddings(docs.clone(), Some(&short))
            .await
            .expect_err("a missing row must fail the batch");
        assert!(
            format!("{error:#}").contains("returned 1 vector(s) for 2 input(s)"),
            "{error:#}"
        );

        let wide = FakeEmbedder::new(4, vec![0.0, 0.0, 0.0, 0.0])
            .answer(&embedding_text(&docs[0]), vec![vec![1.0, 2.0, 3.0]]);
        let error = attach_embeddings(docs, Some(&wide))
            .await
            .expect_err("a wrong width must fail the batch");
        assert!(
            format!("{error:#}").contains("3-wide vector at position 0"),
            "{error:#}"
        );
    }

    /// The one text rule, checked directly: a change here changes every stored
    /// vector, so it must not drift silently.
    #[test]
    fn a_page_is_embedded_from_its_title_and_body() {
        assert_eq!(
            embedding_text(&docs()[0]),
            "Raft consensus\n\nleader election and log replication"
        );
    }

    /// A split records **one** identity, and [`batch_identity`] takes it from
    /// the first document that has one — so the producer has to stamp the whole
    /// batch, not merely the first document. Pin that split of the bargain:
    /// every document, and the split-level record, carry the one identity.
    #[tokio::test]
    async fn attach_embeddings_stamps_the_whole_batch_with_one_identity() {
        let embedder = fake_for_corpus();
        let expected = EmbeddingIdentity::from(embedder.identity());
        let batch = attach_embeddings(docs(), Some(&embedder)).await.unwrap();

        // Premise: more than one document, each of them really vectorised —
        // otherwise "the batch agrees" would hold by having nothing to agree
        // on, and the assertions below could not fail for the stated reason.
        assert!(
            batch.len() > 1,
            "the premise: a batch of one agrees with itself"
        );
        for (index, doc) in batch.iter().enumerate() {
            assert!(
                doc.embedding.is_some(),
                "the premise: document {index} carries a vector to describe"
            );
            assert_eq!(
                doc.embedding_identity.as_ref(),
                Some(&expected),
                "document {index} must carry the batch's identity: \
                 `batch_identity` reads the first record and stops looking, so a \
                 document stamped with a stale or missing identity would be \
                 published under the wrong provenance"
            );
        }

        // And the identity the split will write down is that same one, so the
        // per-document field and the per-split file agree by construction.
        assert_eq!(batch_identity(&batch), Some(expected));
    }

    /// Compaction rebuilds the index from authoritative pages. If it dropped
    /// the vectors, semantic recall would disappear on the first compaction —
    /// silently, since the keyword streams would still answer.
    #[tokio::test]
    async fn compaction_keeps_the_vector_stream_alive() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let workspace = WorkspaceId::new("acme").unwrap();
        let project_id = ProjectId::new("ai-memory").unwrap();
        let build_root = TempDir::new().unwrap();
        let embedder = fake_for_corpus();
        publish_pages(
            &bucket,
            &workspace,
            &project_id,
            build_root.path(),
            &corpus(),
            Some(&embedder),
        )
        .await;

        let outcome = compact_project(
            bucket.as_ref(),
            &reader(&bucket),
            &workspace,
            &project_id,
            &WriterId::new("compactor").unwrap(),
            &build_root.path().join("compact"),
            100,
            60_000,
            Some(&embedder),
        )
        .await
        .unwrap();
        assert!(!outcome.skipped, "{outcome:?}");

        let outcome =
            search_with_embedder(&bucket, &workspace, &project_id, QUERY, Some(&embedder)).await;
        assert!(
            outcome
                .hits
                .iter()
                .any(|hit| hit.path == PARAPHRASE.0 && hit.streams.contains(&"vector".to_string())),
            "the rebuilt split must still carry embeddings: {outcome:?}"
        );

        // Compaction is a write path like publish, and it must state the
        // provenance of the vectors it just rebuilt — otherwise a split could
        // carry vectors whose origin nothing records, which is the hole this
        // feature closes. Premise first: the rebuild really did replace the
        // catalog with one split.
        assert_eq!(splits_in_catalog(&bucket, &workspace, &project_id).await, 1);
        let prefix = reader(&bucket)
            .load_catalog(&workspace, &project_id)
            .await
            .unwrap()
            .catalog
            .splits[0]
            .prefix
            .clone();
        let materialised = TempDir::new().unwrap();
        materialize(&bucket, &prefix, materialised.path())
            .await
            .unwrap();
        let recorded = read_split_identity(materialised.path())
            .unwrap()
            .expect("a compacted split records the embedder that rebuilt it");
        assert_eq!(recorded.provider, "fake");
        assert_eq!(recorded.model, "fake-model");
        assert_eq!(recorded.dim, 4);
    }

    /// Without a provider there is nothing to state: no vectors, no record,
    /// and nothing for the read path to judge — the keyword streams carry the
    /// corpus exactly as before this feature.
    #[tokio::test]
    async fn a_split_built_without_a_provider_carries_no_record() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let workspace = WorkspaceId::new("acme").unwrap();
        let project_id = ProjectId::new("ai-memory").unwrap();
        let build_root = TempDir::new().unwrap();
        publish_pages(
            &bucket,
            &workspace,
            &project_id,
            build_root.path(),
            &corpus(),
            None,
        )
        .await;

        // Premise: the split was published and holds no vector column, so it
        // has no provenance to record.
        assert_eq!(splits_in_catalog(&bucket, &workspace, &project_id).await, 1);
        let prefix = reader(&bucket)
            .load_catalog(&workspace, &project_id)
            .await
            .unwrap()
            .catalog
            .splits[0]
            .prefix
            .clone();
        let materialised = TempDir::new().unwrap();
        materialize(&bucket, &prefix, materialised.path())
            .await
            .unwrap();
        assert!(
            !materialised.path().join(SPLIT_IDENTITY_FILE).exists(),
            "a split with no vectors must not claim an embedder"
        );

        // Conclusion: a machine that *does* have a provider still searches the
        // corpus. There is nothing to compare, so nothing is refused, and the
        // keyword streams answer on their own.
        let late = FakeEmbedder::new(4, orthogonal())
            .model("model-a")
            .answer(QUERY, vec![query_vector()]);
        let outcome =
            search_with_embedder(&bucket, &workspace, &project_id, QUERY, Some(&late)).await;
        assert_eq!(
            outcome.stream_candidates.get("vector"),
            None,
            "a split with no vector column contributes nothing: {outcome:?}"
        );
        assert!(
            outcome.hits.iter().any(|hit| hit.path == KEYWORD.0),
            "the keyword stream must still answer: {outcome:?}"
        );
    }

    /// The published split states which embedder built its vectors, and a
    /// model change at the *same* width is refused by name.
    ///
    /// Two models of equal width produce unrelated coordinates, so the vector
    /// stream would otherwise compare them, succeed numerically, and answer
    /// with a ranking that carries no meaning. Recording the identity on the
    /// split is what turns that into an error — this is the regression that
    /// record exists for.
    #[tokio::test]
    async fn a_same_width_model_change_is_refused_by_name() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let workspace = WorkspaceId::new("acme").unwrap();
        let project_id = ProjectId::new("ai-memory").unwrap();
        let build_root = TempDir::new().unwrap();
        let first = FakeEmbedder::new(4, orthogonal())
            .model("model-a")
            .answer(QUERY, vec![query_vector()])
            .answer(&text_for(KEYWORD.1, KEYWORD.2), vec![orthogonal()])
            .answer(&text_for(PARAPHRASE.1, PARAPHRASE.2), vec![near_query()]);
        publish_pages(
            &bucket,
            &workspace,
            &project_id,
            build_root.path(),
            &corpus(),
            Some(&first),
        )
        .await;

        // Premise 1: the split really holds vectors, so what follows is about
        // comparability, not about an empty vector column.
        let matched =
            search_with_embedder(&bucket, &workspace, &project_id, QUERY, Some(&first)).await;
        assert!(
            matched
                .stream_candidates
                .get("vector")
                .copied()
                .unwrap_or(0)
                > 0,
            "the split must really hold embeddings: {matched:?}"
        );

        // Premise 2: the split records the builder's identity — provider,
        // model and width — so the record is a fact about the split rather
        // than a constant the reader could have assumed.
        let prefix = reader(&bucket)
            .load_catalog(&workspace, &project_id)
            .await
            .unwrap()
            .catalog
            .splits[0]
            .prefix
            .clone();
        let materialised = TempDir::new().unwrap();
        materialize(&bucket, &prefix, materialised.path())
            .await
            .unwrap();
        let recorded = read_split_identity(materialised.path())
            .unwrap()
            .expect("a split built with a provider records its identity");
        assert_eq!(recorded.provider, "fake");
        assert_eq!(recorded.model, "model-a");
        assert_eq!(recorded.dim, 4);

        // Conclusion: same width, different model, refused and named.
        let second = FakeEmbedder::new(4, orthogonal())
            .model("model-b")
            .answer(QUERY, vec![query_vector()]);
        let error =
            search_with_embedder_result(&bucket, &workspace, &project_id, QUERY, Some(&second))
                .await
                .expect_err("a same-width model change must not be ranked");
        let message = format!("{error:#}");
        assert!(
            message.contains("model-a") && message.contains("model-b"),
            "the failure must name both models: {message}"
        );
        assert!(
            message.contains("dim 4"),
            "the failure must name the width: {message}"
        );
        assert!(
            message.contains(&prefix),
            "the failure must name the split it refused: {message}"
        );
    }

    /// A split published before the identity record existed says nothing about
    /// its provenance, and "no record" must never be read as "a different
    /// embedder": an existing corpus has to stay searchable.
    #[tokio::test]
    async fn a_split_without_a_recorded_identity_is_not_judged() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let workspace = WorkspaceId::new("acme").unwrap();
        let project_id = ProjectId::new("ai-memory").unwrap();
        let build_root = TempDir::new().unwrap();
        let project = reader(&bucket);
        let writer = WriterId::new("mbp-a").unwrap();
        let embedded = FakeEmbedder::new(4, orthogonal())
            .model("model-a")
            .answer(QUERY, vec![query_vector()])
            .answer(&text_for(KEYWORD.1, KEYWORD.2), vec![orthogonal()])
            .answer(&text_for(PARAPHRASE.1, PARAPHRASE.2), vec![near_query()]);

        let mut docs = Vec::new();
        for (index, (path, title, body)) in corpus().iter().enumerate() {
            let page_path = PagePath::new(*path).unwrap();
            let now_ms = index as i64 + 1;
            project
                .commit_page(CommitPageRequest {
                    workspace_id: workspace.clone(),
                    project_id: project_id.clone(),
                    path: page_path.clone(),
                    title: (*title).to_string(),
                    body: (*body).to_string(),
                    writer_id: writer.clone(),
                    now_ms,
                })
                .await
                .unwrap();
            let page = project
                .read_page(&workspace, &project_id, &page_path)
                .await
                .unwrap()
                .unwrap();
            docs.push(PageDoc::from_version(
                &workspace,
                &project_id,
                &page,
                now_ms,
            ));
        }
        // Vectors, but no record of who produced them: exactly the shape a
        // split published before this feature has.
        let docs: Vec<PageDoc> = attach_embeddings(docs, Some(&embedded))
            .await
            .unwrap()
            .into_iter()
            .map(|doc| doc.with_embedding_identity(None))
            .collect();
        publish_split_index(
            &bucket,
            &project,
            &workspace,
            &project_id,
            &writer,
            1,
            &docs,
            &build_root.path().join("legacy"),
            1,
        )
        .await
        .unwrap();

        // Premise: vectors present, record absent.
        let prefix = project
            .load_catalog(&workspace, &project_id)
            .await
            .unwrap()
            .catalog
            .splits[0]
            .prefix
            .clone();
        let materialised = TempDir::new().unwrap();
        materialize(&bucket, &prefix, materialised.path())
            .await
            .unwrap();
        assert!(
            !materialised.path().join(SPLIT_IDENTITY_FILE).exists(),
            "the legacy split must carry no identity record"
        );
        let matched =
            search_with_embedder(&bucket, &workspace, &project_id, QUERY, Some(&embedded)).await;
        assert!(
            matched
                .stream_candidates
                .get("vector")
                .copied()
                .unwrap_or(0)
                > 0,
            "the legacy split must still hold vectors: {matched:?}"
        );

        // Conclusion: a different model neither matches nor mismatches; the
        // reader has no claim to judge, so the search proceeds as before.
        let other = FakeEmbedder::new(4, orthogonal()).model("model-b");
        let outcome =
            search_with_embedder(&bucket, &workspace, &project_id, QUERY, Some(&other)).await;
        assert_eq!(
            outcome.splits_searched, 1,
            "a split with no recorded identity must not be refused: {outcome:?}"
        );
    }

    /// The guard guards a comparison. A caller with no provider, or one that
    /// turned the vector stream off, is not comparing vectors, so a recorded
    /// identity must not turn into an error for them.
    #[tokio::test]
    async fn the_identity_guard_only_applies_to_a_live_vector_stream() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let workspace = WorkspaceId::new("acme").unwrap();
        let project_id = ProjectId::new("ai-memory").unwrap();
        let build_root = TempDir::new().unwrap();
        let first = FakeEmbedder::new(4, orthogonal())
            .model("model-a")
            .answer(QUERY, vec![query_vector()])
            .answer(&text_for(KEYWORD.1, KEYWORD.2), vec![orthogonal()])
            .answer(&text_for(PARAPHRASE.1, PARAPHRASE.2), vec![near_query()]);
        publish_pages(
            &bucket,
            &workspace,
            &project_id,
            build_root.path(),
            &corpus(),
            Some(&first),
        )
        .await;

        // No provider configured: the vector stream does not run at all.
        let outcome = search_with_embedder(&bucket, &workspace, &project_id, QUERY, None).await;
        assert_eq!(
            outcome.stream_candidates.get("vector"),
            None,
            "no provider means no vector stream: {outcome:?}"
        );

        // A different model, but the caller turned the stream off: the vectors
        // are never compared, so there is nothing to refuse.
        let tuning = SearchTuning {
            now_ms: 0,
            vector_search: false,
            ..Default::default()
        };
        let second = FakeEmbedder::new(4, orthogonal()).model("model-b");
        let outcome = search_project_tuned(
            bucket.as_ref(),
            &reader(&bucket),
            &workspace,
            &project_id,
            TempDir::new().unwrap().path(),
            QUERY,
            10,
            &tuning,
            Some(&second),
        )
        .await
        .expect("a disabled vector stream cannot mismatch");
        assert_eq!(
            outcome.stream_candidates.get("vector"),
            None,
            "the disabled stream must not have run: {outcome:?}"
        );
    }

    /// A model change is a re-embedding, not a republish.
    ///
    /// Publishing appends, so a second generation of vectors lands as a second
    /// split and the catalog then names two widths. Every search fails closed
    /// at that point rather than comparing coordinates from two models. Only a
    /// compaction replaces the catalog and drops the old generation, which is
    /// what `docs/ops.md` tells operators to run after a model change — this
    /// test is what keeps that instruction honest.
    #[tokio::test]
    async fn a_dimension_change_needs_a_compaction_because_publishing_appends() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let workspace = WorkspaceId::new("acme").unwrap();
        let project_id = ProjectId::new("ai-memory").unwrap();
        let build_root = TempDir::new().unwrap();
        let project = reader(&bucket);

        // The authoritative pages both generations will embed.
        for (index, (path, title, body)) in corpus().iter().enumerate() {
            project
                .commit_page(CommitPageRequest {
                    workspace_id: workspace.clone(),
                    project_id: project_id.clone(),
                    path: PagePath::new(*path).unwrap(),
                    title: (*title).to_string(),
                    body: (*body).to_string(),
                    writer_id: WriterId::new("mbp-a").unwrap(),
                    now_ms: index as i64 + 1,
                })
                .await
                .unwrap();
        }

        // First generation: the pages embedded by a four-wide model.
        let four = FakeEmbedder::new(4, orthogonal());
        publish_generation(
            &bucket,
            &workspace,
            &project_id,
            &build_root.path().join("gen-4"),
            "mbp-a",
            1,
            Some(&four),
        )
        .await;
        assert_eq!(
            splits_in_catalog(&bucket, &workspace, &project_id).await,
            1,
            "the first generation is one split"
        );

        // A different model over the same pages. Publishing appends, so the
        // four-wide generation is still in the catalog.
        let eight = FakeEmbedder::new(8, vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]);
        publish_generation(
            &bucket,
            &workspace,
            &project_id,
            &build_root.path().join("gen-8"),
            "mbp-b",
            2,
            Some(&eight),
        )
        .await;
        assert_eq!(
            splits_in_catalog(&bucket, &workspace, &project_id).await,
            2,
            "publishing appends: the old generation is still in the catalog"
        );

        // An eight-wide query against a four-wide stored vector is an error.
        // Skipping the split instead would answer from half the corpus and
        // look like a working search.
        let error =
            search_with_embedder_result(&bucket, &workspace, &project_id, QUERY, Some(&eight))
                .await
                .expect_err("a mixed-width catalog must fail closed, not answer");
        let message = format!("{error:#}");
        assert!(
            message.contains("cannot compare embeddings of different widths"),
            "the failure must name the mismatch: {message}"
        );
        assert!(
            message.contains("query has 8") && message.contains("stored value has 4"),
            "the error must name both widths: {message}"
        );

        // Compaction rebuilds every live page into one split and replaces the
        // catalog pointer, so the old generation leaves the index.
        let outcome = compact_project(
            bucket.as_ref(),
            &reader(&bucket),
            &workspace,
            &project_id,
            &WriterId::new("compactor").unwrap(),
            &build_root.path().join("compact"),
            100,
            60_000,
            Some(&eight),
        )
        .await
        .unwrap();
        assert!(!outcome.skipped, "{outcome:?}");
        assert_eq!(outcome.splits_before, 2, "{outcome:?}");
        assert_eq!(
            outcome.splits_after, 1,
            "compaction replaces the catalog rather than appending to it: {outcome:?}"
        );
        assert_eq!(
            splits_in_catalog(&bucket, &workspace, &project_id).await,
            1,
            "the old generation must be gone from the catalog"
        );

        // The same query that failed a moment ago now answers.
        let after =
            search_with_embedder(&bucket, &workspace, &project_id, QUERY, Some(&eight)).await;
        assert_eq!(after.splits_searched, 1, "{after:?}");
        assert!(
            after.hits.iter().any(|hit| hit.path == KEYWORD.0),
            "the rebuilt index must still answer: {after:?}"
        );
    }

    // ------------------------------------------------------------------
    // The end-to-end chain: a configured process against a real HTTP endpoint.
    // ------------------------------------------------------------------

    /// The model the chain starts with — the one `QM_EMBEDDING_MODEL` names
    /// when the child publishes — and the one it switches to afterwards. Equal
    /// width on purpose: that is the change [`ensure_split_model_matches`]
    /// exists for.
    const FIRST_MODEL: &str = "model-a";
    const SECOND_MODEL: &str = "model-b";

    /// What `QM_EMBEDDING_API_KEY` holds. Not a credential: the endpoint is the
    /// stub below, in this process.
    const STUB_KEY: &str = "stub-key";

    /// What `QM_EMBEDDING_DIM` holds, and the width the stub answers with.
    const DIM: usize = 4;

    /// Prefixed to the child's report line so the parent can find it among the
    /// test harness's own output.
    const CHILD_MARKER: &str = "VECTOR_E2E_REPORT:";

    /// How long the stub waits for a client to finish sending one request.
    ///
    /// A client that connects and then stalls is the only way this
    /// single-threaded accept loop could block forever, and the parent waits on
    /// the child with no deadline of its own. The deadline turns that stall into
    /// a short request, which [`answer`] turns into a 400 the child fails on.
    /// Generous next to a loopback round trip, so a healthy run cannot reach it.
    ///
    /// This is the value the chain test runs with.
    /// [`a_client_that_stalls_mid_request_is_answered_not_waited_on`] proves the
    /// deadline is there at all, on a short one of its own so that the everyday
    /// loop does not pay this one waiting for a timeout it already knows about.
    const STUB_READ_TIMEOUT: Duration = Duration::from_secs(5);

    /// One request, as it arrived.
    #[derive(Debug, Clone)]
    struct SeenRequest {
        /// The request line, e.g. `POST /v1/embeddings HTTP/1.1`.
        line: String,
        /// The raw `authorization` header.
        authorization: String,
        /// The model the client asked for.
        model: String,
        /// The body it sent.
        body: String,
    }

    /// A stub OpenAI-compatible embeddings endpoint, in this process.
    ///
    /// It answers from a hand-written "model" rather than a learned one: the
    /// paraphrase page points the same way as the query, the keyword page is
    /// orthogonal to it, and the query is itself. **Both model names get the
    /// same numbers**, deliberately — that is the failure mode this area is
    /// about. With equal widths and predictable coordinates, a removed identity
    /// guard does not fail loudly: it returns the ranking that means nothing.
    /// Whatever the guard contributes has to be visible against an endpoint
    /// that would happily answer.
    ///
    /// One request per connection (the response says `connection: close`), so
    /// the accept loop is one iteration per request and never has to multiplex
    /// or time anything out.
    struct StubModel {
        address: SocketAddr,
        seen: Arc<Mutex<Vec<SeenRequest>>>,
        shutdown: Arc<AtomicBool>,
        handle: Option<std::thread::JoinHandle<()>>,
    }

    impl StubModel {
        fn start() -> Self {
            Self::with_read_deadline(STUB_READ_TIMEOUT)
        }

        /// A stub that waits `read_deadline` for each request to finish.
        ///
        /// The deadline is a parameter only so the stall test can use a short
        /// one: what it checks is that a half-sent request is answered rather
        /// than parked on, and a smaller deadline checks that without spending
        /// [`STUB_READ_TIMEOUT`] of the suite's time on it.
        fn with_read_deadline(read_deadline: Duration) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("binding the stub endpoint");
            let address = listener.local_addr().expect("the stub's address");
            let seen = Arc::new(Mutex::new(Vec::new()));
            let shutdown = Arc::new(AtomicBool::new(false));
            let handle = std::thread::spawn({
                let seen = Arc::clone(&seen);
                let shutdown = Arc::clone(&shutdown);
                move || {
                    for stream in listener.incoming() {
                        if shutdown.load(Ordering::SeqCst) {
                            break;
                        }
                        let Ok(mut stream) = stream else { break };
                        // Bound the read: a client that connects and stops
                        // talking would otherwise pin this loop, and the parent
                        // has no deadline of its own to fall back on.
                        stream.set_read_timeout(Some(read_deadline)).expect(
                            "the stub must take a read deadline: a platform that refuses one \
                             leaves a client that stalls mid-request able to park this accept \
                             loop for good, and nothing else in this suite would notice",
                        );
                        let (record, response) = answer(&read_request(&mut stream));
                        seen.lock().expect("the stub's request log").push(record);
                        // Answer before anything else can go wrong here: a
                        // client left waiting would hang the child instead of
                        // failing it.
                        let _ = stream.write_all(response.as_bytes());
                    }
                }
            });
            Self {
                address,
                seen,
                shutdown,
                handle: Some(handle),
            }
        }

        fn base_url(&self) -> String {
            format!("http://{}/v1", self.address)
        }

        /// Stop the accept loop and hand back everything it saw.
        ///
        /// The wake-up connection is what makes the blocking `accept` return —
        /// no polling, and no timeout to tune.
        fn finish(mut self) -> Vec<SeenRequest> {
            self.shutdown.store(true, Ordering::SeqCst);
            let _ = TcpStream::connect(self.address);
            let handle = self.handle.take().expect("the stub's thread");
            handle.join().expect("the stub endpoint must not panic");
            self.seen.lock().expect("the stub's request log").clone()
        }
    }

    impl Drop for StubModel {
        fn drop(&mut self) {
            self.shutdown.store(true, Ordering::SeqCst);
            let _ = TcpStream::connect(self.address);
            if let Some(handle) = self.handle.take() {
                let _ = handle.join();
            }
        }
    }

    /// Read one HTTP request: its head, then exactly the `content-length` bytes
    /// of body.
    ///
    /// A connection that ends early yields a short string rather than a panic,
    /// so an unparseable request becomes a 400 the client sees — a failure, not
    /// a hang in this thread.
    fn read_request(stream: &mut TcpStream) -> String {
        let mut buffer = Vec::new();
        let mut chunk = [0u8; 4096];
        let header_end = loop {
            match stream.read(&mut chunk) {
                Ok(0) | Err(_) => break buffer.len(),
                Ok(read) => {
                    buffer.extend_from_slice(&chunk[..read]);
                    if let Some(head) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
                        break head + 4;
                    }
                }
            }
        };
        let head = String::from_utf8_lossy(&buffer[..header_end]).to_lowercase();
        let length = head
            .lines()
            .find_map(|line| line.strip_prefix("content-length:"))
            .and_then(|value| value.trim().parse::<usize>().ok())
            .unwrap_or(0);
        while buffer.len() < header_end + length {
            match stream.read(&mut chunk) {
                Ok(0) | Err(_) => break,
                Ok(read) => buffer.extend_from_slice(&chunk[..read]),
            }
        }
        String::from_utf8_lossy(&buffer).to_string()
    }

    /// What one request was, and what to answer it.
    ///
    /// A request the stub cannot serve gets a 400 naming the problem, so a
    /// client that sent the wrong thing fails through the provider's own error
    /// path instead of panicking here.
    fn answer(request: &str) -> (SeenRequest, String) {
        let (head, body) = request.split_once("\r\n\r\n").unwrap_or((request, ""));
        let parsed: serde_json::Value =
            serde_json::from_str(body).unwrap_or(serde_json::Value::Null);
        let model = parsed
            .get("model")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string();
        let inputs: Vec<&str> = parsed
            .get("input")
            .and_then(serde_json::Value::as_array)
            .map(|inputs| {
                inputs
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .collect()
            })
            .unwrap_or_default();
        let record = SeenRequest {
            line: head.lines().next().unwrap_or_default().to_string(),
            authorization: head
                .lines()
                .find(|line| line.to_ascii_lowercase().starts_with("authorization:"))
                .unwrap_or_default()
                .to_string(),
            model: model.clone(),
            body: body.to_string(),
        };
        if model.is_empty() || inputs.is_empty() {
            return (
                record,
                response(
                    400,
                    "Bad Request",
                    r#"{"error":"the stub needs a model and inputs"}"#,
                ),
            );
        }
        let mut data = Vec::with_capacity(inputs.len());
        for input in inputs {
            let Some(vector) = vector_for(input) else {
                let complaint = serde_json::json!({
                    "error": format!("the stub was sent an input it does not know: {input:?}"),
                })
                .to_string();
                return (record, response(400, "Bad Request", &complaint));
            };
            data.push(serde_json::json!({ "embedding": vector }));
        }
        let payload = serde_json::json!({ "model": model, "data": data }).to_string();
        (record, response(200, "OK", &payload))
    }

    fn response(status: u16, reason: &str, payload: &str) -> String {
        format!(
            "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{payload}",
            payload.len()
        )
    }

    /// The stub's "model": three texts it recognises and no others.
    ///
    /// Anything else is a 400, so a client that embedded the wrong text fails
    /// loudly instead of scoring something plausible. `None` is what makes that
    /// possible — there is no fallback vector that would quietly stand in.
    fn vector_for(text: &str) -> Option<Vec<f32>> {
        if text == text_for(PARAPHRASE.1, PARAPHRASE.2) {
            Some(near_query())
        } else if text == text_for(KEYWORD.1, KEYWORD.2) {
            Some(orthogonal())
        } else if text == QUERY {
            Some(query_vector())
        } else {
            None
        }
    }

    /// How `text` appears inside a JSON body: quoted, with newlines and anything
    /// else serde escapes already escaped. Matching this against the raw request
    /// pins the *exact* text that was embedded, where a bare substring would
    /// also match a different one.
    fn json_fragment(text: &str) -> String {
        let quoted = serde_json::to_string(text).expect("a string always serialises");
        quoted[1..quoted.len() - 1].to_string()
    }

    /// What the only split in the catalog says about the embedder that built
    /// its vectors: the machine's own record of its provenance.
    async fn recorded_identity(
        bucket: &Arc<dyn ObjectStore>,
        workspace: &WorkspaceId,
        project_id: &ProjectId,
    ) -> EmbeddingIdentity {
        let catalog = reader(bucket)
            .load_catalog(workspace, project_id)
            .await
            .unwrap()
            .catalog;
        assert_eq!(
            catalog.splits.len(),
            1,
            "premise: exactly one split to read the record from"
        );
        let materialised = TempDir::new().unwrap();
        materialize(bucket, &catalog.splits[0].prefix, materialised.path())
            .await
            .unwrap();
        read_split_identity(materialised.path())
            .unwrap()
            .expect("the split must record the embedder that built its vectors")
    }

    /// The whole chain over real HTTP: a *configured* process publishes,
    /// searches, switches models, is refused, compacts, and recovers.
    ///
    /// Every other vector test here stands in for the provider with an
    /// in-process `FakeEmbedder`, which pins the pieces but not the seams
    /// between them: `QM_EMBEDDING_*` into [`vector::embedder_from_env`], a
    /// provider that speaks actual HTTP, the identity it leaves on the split,
    /// the guard that reads it back, and a rebuild that rewrites it. This test
    /// runs that chain against a stub endpoint and asserts from both ends —
    /// what the stub received, and what the child recorded.
    ///
    /// The chain runs in a child process because the configuration *is* the
    /// environment, and `unsafe_code = "forbid"` makes `std::env::set_var` a
    /// compile error: spawning a process with `QM_EMBEDDING_*` set is the only
    /// way to hand them to `embedder_from_env`. The stub stays here, so this
    /// test compares the model it put in the environment, the model the stub
    /// saw on the wire, and the model the child wrote onto the split — three
    /// independent ends of one value.
    ///
    /// The stub is not a model and says nothing about embedding quality. What
    /// it proves is that a configured process, a real HTTP provider and the
    /// split record fit together.
    ///
    /// The wait for the child is a plain `output()` with no deadline of its own,
    /// because every step the child takes is either local or answered by a stub
    /// that always responds — including when it refuses a request. Two things
    /// keep that true: a stub that died mid-request closes the connection, and
    /// the stub reads with a deadline ([`STUB_READ_TIMEOUT`]), so a client that
    /// connects and stalls becomes a 400 instead of pinning the accept loop.
    /// Either way a missing answer is an error rather than a wait.
    #[test]
    fn a_configured_process_runs_the_whole_vector_chain_over_real_http() {
        let stub = StubModel::start();
        let child = std::process::Command::new(std::env::current_exe().expect("the test binary"))
            .args([
                "child_runs_the_configured_vector_chain",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("QM_E2E_CHILD", "1")
            .env("QM_EMBEDDING_BASE_URL", stub.base_url())
            .env("QM_EMBEDDING_API_KEY", STUB_KEY)
            .env("QM_EMBEDDING_MODEL", FIRST_MODEL)
            .env("QM_EMBEDDING_DIM", DIM.to_string())
            .output()
            .expect("running the child chain");
        let stdout = String::from_utf8_lossy(&child.stdout).to_string();
        let stderr = String::from_utf8_lossy(&child.stderr).to_string();
        let seen = stub.finish();

        // Premises: the child really ran, and it ran exactly the one test it was
        // asked for and reached the end of the chain.
        assert!(child.status.success(), "the child chain failed:\n{stderr}");
        // One test ran, and it ran rather than being ignored. A name that
        // matched nothing would report `0 passed`; a chain that quietly stopped
        // walking would still say `1 passed`, which is why the report below has
        // to be present and the step receipt complete.
        assert!(
            stdout.contains("1 passed; 0 failed; 0 ignored"),
            "the child must run exactly `child_runs_the_configured_vector_chain`, and run it:\n{stdout}"
        );
        // The harness prints the test's name and then, under `--nocapture`, the
        // captured output on the same line, so the marker is found rather than
        // anchored — and the report ends where that line does.
        let report: serde_json::Value = {
            let start = stdout.find(CHILD_MARKER).unwrap_or_else(|| {
                panic!("the child never wrote its report, so it did not walk the chain:\n{stdout}\n{stderr}")
            }) + CHILD_MARKER.len();
            let rest = &stdout[start..];
            let line = rest.split('\n').next().unwrap_or(rest);
            serde_json::from_str(line).expect("the child's report must be JSON")
        };

        // What crossed the wire. Four calls in one deterministic order: publish
        // with the configured model, search with it, the rebuild with the second
        // model, the search that follows. The refused search in between is
        // *absent* from this list — the identity guard runs before the query is
        // embedded, so a mismatch costs no request at all.
        let models: Vec<&str> = seen.iter().map(|request| request.model.as_str()).collect();
        assert_eq!(
            models,
            vec![FIRST_MODEL, FIRST_MODEL, SECOND_MODEL, SECOND_MODEL],
            "the configured model must be what the provider was asked for, in order: {seen:#?}"
        );
        assert!(
            seen.iter()
                .all(|request| request.line == "POST /v1/embeddings HTTP/1.1"),
            "the provider must call the endpoint the base url names: {seen:#?}"
        );
        assert!(
            seen.iter().all(|request| request
                .authorization
                .to_ascii_lowercase()
                .contains(&format!("bearer {STUB_KEY}"))),
            "every call must present the key from QM_EMBEDDING_API_KEY: {seen:#?}"
        );

        // What the calls carried. Both publish calls embed the whole corpus,
        // as title, blank line, body; both searches embed the query.
        let corpus_batch = vec![
            json_fragment(&text_for(KEYWORD.1, KEYWORD.2)),
            json_fragment(&text_for(PARAPHRASE.1, PARAPHRASE.2)),
        ];
        let expected: Vec<Vec<String>> = vec![
            corpus_batch.clone(),
            vec![json_fragment(QUERY)],
            corpus_batch,
            vec![json_fragment(QUERY)],
        ];
        for (request, fragments) in seen.iter().zip(&expected) {
            for fragment in fragments {
                assert!(
                    request.body.contains(fragment),
                    "the request must carry exactly {fragment:?}: {request:#?}"
                );
            }
        }

        // The child's record names the model that was really sent. This is the
        // comparison the two-process shape exists for: the child wrote down what
        // its embedder claimed, and the stub saw what that embedder sent.
        assert_eq!(
            report["published_identity"]["provider"].as_str(),
            Some("openai-compatible"),
            "{report}"
        );
        assert_eq!(report["published_identity"]["dim"], serde_json::json!(DIM));
        assert_eq!(
            report["published_identity"]["model"].as_str(),
            Some(seen[0].model.as_str()),
            "the split's record must name the model that went over the wire: {report}"
        );
        assert_eq!(
            report["rebuilt_identity"]["model"].as_str(),
            Some(seen[2].model.as_str()),
            "the rebuilt split's record must name the model that went over the wire: {report}"
        );

        // The receipt: every step of the chain completed, in order. The child
        // pushes a step only after that step's assertions passed, so this is
        // what says the chain was walked rather than skipped.
        assert_eq!(
            report["steps"],
            serde_json::json!([
                "published",
                "unconfigured-search",
                "configured-search",
                "refused",
                "compacted",
                "recovered"
            ]),
            "{report}"
        );
    }

    /// A client that starts a request and then stops talking must be answered,
    /// not waited on.
    ///
    /// The stub's read deadline is what stands between a half-sent request and
    /// an accept loop parked for good, and the chain test above waits on its
    /// child with no deadline of its own — so a fixture that lost the deadline
    /// would hang a gate instead of failing it. That is the kind of guard that
    /// cannot be checked by reading the constant back: what has to be shown is
    /// that a stalled client gets a 400.
    ///
    /// The stub is given a short deadline here on purpose. The property is that
    /// the deadline exists and is enforced, and waiting [`STUB_READ_TIMEOUT`]
    /// to watch it fire would cost the everyday loop five seconds for nothing.
    #[test]
    fn a_client_that_stalls_mid_request_is_answered_not_waited_on() {
        /// Long enough that a merely loaded machine cannot deliver the request
        /// head after it, and short enough that the suite pays a second rather
        /// than the five the chain test runs with. A deadline that fires early
        /// would still answer, but with an empty request, and the assertions
        /// below ask for the head.
        const STALL_DEADLINE: Duration = Duration::from_secs(1);
        /// Above the stub's deadline, so the stub is what ends the wait; below
        /// a gate's patience, so a stub *without* a deadline fails this test
        /// instead of hanging it.
        const CLIENT_DEADLINE: Duration = Duration::from_secs(3);

        let stub = StubModel::with_read_deadline(STALL_DEADLINE);
        let mut client = TcpStream::connect(stub.address).expect("connecting to the stub");
        client
            .set_read_timeout(Some(CLIENT_DEADLINE))
            .expect("the stalled client needs a deadline of its own to fail rather than hang");

        // Headers that promise a body, and then no body at all: the accept loop
        // is inside `read_request`'s body loop when the deadline fires.
        client
            .write_all(b"POST /v1/embeddings HTTP/1.1\r\ncontent-length: 4096\r\n\r\n")
            .expect("writing the request head");
        client.flush().expect("flushing the request head");
        let mut response = [0_u8; 64];
        let read = client.read(&mut response).unwrap_or_else(|error| {
            panic!(
                "a stalled client must be answered within {CLIENT_DEADLINE:?}, not waited on: \
                 {error} (the stub's read deadline is {STALL_DEADLINE:?})"
            )
        });
        assert!(read > 0, "the stub closed the connection without answering");
        let first_line = String::from_utf8_lossy(&response[..read]).to_string();
        assert!(
            first_line.starts_with("HTTP/1.1 400 "),
            "a request cut short must be refused, not answered or dropped: {first_line}"
        );

        // The request really did reach the loop, so the 400 above is the stub's
        // answer to *this* request rather than a connection it never read.
        let seen = stub.finish();
        assert_eq!(seen.len(), 1, "the stub must have logged the short request");
        assert_eq!(
            seen[0].line, "POST /v1/embeddings HTTP/1.1",
            "the request line is read before the body it never sent"
        );
        assert!(
            seen[0].body.is_empty(),
            "the deadline fired inside the body, not after it: {:?}",
            seen[0].body
        );
    }

    /// The child half of
    /// [`a_configured_process_runs_the_whole_vector_chain_over_real_http`],
    /// for the reason that test's comment gives: the configuration has to be in
    /// the environment at spawn time.
    ///
    /// Not runnable on its own — it needs `QM_EMBEDDING_*` pointing at a stub
    /// that only the parent process runs. `QM_E2E_CHILD`, which only the parent
    /// sets, is the first thing checked, and without it this says so on stderr
    /// and returns rather than reaching out to whatever endpoint a developer's
    /// shell happens to export.
    ///
    /// It is deliberately *not* `#[ignore]`d. The ordinary suite runs it too,
    /// where the missing-premises branch reports that it was skipped, and
    /// the parent drives it by name in a child process where the premises are
    /// real. An `#[ignore]` would hide the skip in a file-level attribute
    /// instead, and would let a green `cargo test` mean nothing.
    #[tokio::test]
    async fn child_runs_the_configured_vector_chain() {
        if std::env::var("QM_E2E_CHILD").as_deref() != Ok("1") {
            eprintln!(
                "not running child_runs_the_configured_vector_chain: it is driven by \
                 a_configured_process_runs_the_whole_vector_chain_over_real_http, which supplies \
                 QM_EMBEDDING_* and a stub endpoint; on its own it would reach out to whatever \
                 endpoint this shell exports, so it is skipped here and run by that parent"
            );
            return;
        }
        let configured_model =
            std::env::var("QM_EMBEDDING_MODEL").expect("the parent sets QM_EMBEDDING_MODEL");
        let configured_key =
            std::env::var("QM_EMBEDDING_API_KEY").expect("the parent sets QM_EMBEDDING_API_KEY");
        let base_url =
            std::env::var("QM_EMBEDDING_BASE_URL").expect("the parent sets QM_EMBEDDING_BASE_URL");

        // The provider the *operator* configured: this is the value the whole
        // chain runs through, so it is only as real as the environment it was
        // handed.
        let configured = vector::embedder_from_env()
            .expect("a fully configured endpoint must build")
            .expect("QM_EMBEDDING_BASE_URL is set, so there is a provider");
        // Premise: the environment reached the provider intact. A test that
        // constructs the provider by hand cannot see this link.
        assert_eq!(configured.identity().model, configured_model);
        assert_eq!(configured.identity().dim, DIM);
        assert_eq!(configured.identity().provider, "openai-compatible");

        let mut steps = Vec::new();
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let workspace = WorkspaceId::new("acme").unwrap();
        let project_id = ProjectId::new("ai-memory").unwrap();
        let build_root = TempDir::new().unwrap();

        // 1. Publish: the corpus goes out as one batch, comes back as vectors,
        //    and lands on the split with the identity that produced them.
        publish_pages(
            &bucket,
            &workspace,
            &project_id,
            build_root.path(),
            &corpus(),
            Some(&*configured),
        )
        .await;
        assert_eq!(
            splits_in_catalog(&bucket, &workspace, &project_id).await,
            1,
            "premise: exactly one published split"
        );
        let published = recorded_identity(&bucket, &workspace, &project_id).await;
        assert_eq!(published.provider, "openai-compatible");
        assert_eq!(
            published.model, configured_model,
            "the split must record the model QM_EMBEDDING_MODEL named"
        );
        assert_eq!(published.dim, DIM);
        steps.push("published");

        // 2. Without a provider the same query has no vector stream at all.
        //    Premise first: the vector stream is the only thing that could
        //    reach the paraphrase, so its absence really is its absence.
        assert!(
            !text_for(PARAPHRASE.1, PARAPHRASE.2).contains(QUERY),
            "the paraphrase must not share the query's words"
        );
        let unconfigured =
            search_with_embedder(&bucket, &workspace, &project_id, QUERY, None).await;
        assert_eq!(
            unconfigured.stream_candidates.get("vector"),
            None,
            "an unconfigured machine must not run the vector stream: {unconfigured:?}"
        );
        assert!(
            !unconfigured.hits.iter().any(|hit| hit.path == PARAPHRASE.0),
            "only the vector stream can reach the paraphrase: {unconfigured:?}"
        );
        steps.push("unconfigured-search");

        // 3. With it, the same page is recalled *through the vector stream*.
        let recalled =
            search_with_embedder(&bucket, &workspace, &project_id, QUERY, Some(&*configured)).await;
        assert!(
            recalled
                .stream_candidates
                .get("vector")
                .is_some_and(|count| *count > 0),
            "premise: the vector stream ran and proposed candidates: {recalled:?}"
        );
        assert!(
            recalled.hits.iter().any(|hit| hit.path == PARAPHRASE.0
                && hit.streams.iter().any(|stream| stream == "vector")),
            "the configured provider must recall the paraphrase through the vector stream: {recalled:?}"
        );
        steps.push("configured-search");

        // 4. The same endpoint, the same key, one different word: a second model
        //    of the same width. The published vectors are not comparable to it,
        //    and the read path refuses rather than ranking them.
        let second =
            qm_llm::OpenAiCompatEmbedder::new(&base_url, &configured_key, SECOND_MODEL, DIM)
                .expect("the second provider");
        let error =
            search_with_embedder_result(&bucket, &workspace, &project_id, QUERY, Some(&second))
                .await
                .expect_err("a same-width model change must be refused, not ranked");
        let message = format!("{error:#}");
        assert!(
            message.contains(&format!("model '{configured_model}'")),
            "the refusal must name the model the split records: {message}"
        );
        assert!(
            message.contains(&format!("model '{SECOND_MODEL}'")),
            "the refusal must name the model this machine is configured with: {message}"
        );
        steps.push("refused");

        // 5. Compact re-embeds the live pages with the current provider and
        //    replaces the catalog, which is what `docs/ops.md` tells an operator
        //    to do after a model change.
        let outcome = compact_project(
            bucket.as_ref(),
            &reader(&bucket),
            &workspace,
            &project_id,
            &WriterId::new("compactor").unwrap(),
            &build_root.path().join("compact"),
            100,
            60_000,
            Some(&second),
        )
        .await
        .expect("compaction");
        assert!(!outcome.skipped, "{outcome:?}");
        assert_eq!(
            outcome.splits_after, 1,
            "compaction replaces the catalog rather than appending to it: {outcome:?}"
        );
        let rebuilt = recorded_identity(&bucket, &workspace, &project_id).await;
        assert_eq!(rebuilt.provider, "openai-compatible");
        assert_eq!(
            rebuilt.model, SECOND_MODEL,
            "the rebuilt split must state the provider that rebuilt it"
        );
        assert_eq!(rebuilt.dim, DIM);
        steps.push("compacted");

        // 6. The same query answers again, through the vector stream, over the
        //    same set of pages.
        let recovered =
            search_with_embedder(&bucket, &workspace, &project_id, QUERY, Some(&second)).await;
        assert!(
            recovered
                .stream_candidates
                .get("vector")
                .is_some_and(|count| *count > 0),
            "premise: the rebuilt split carries vectors: {recovered:?}"
        );
        assert!(
            recovered.hits.iter().any(|hit| hit.path == PARAPHRASE.0
                && hit.streams.iter().any(|stream| stream == "vector")),
            "the rebuilt index must recall the paraphrase again: {recovered:?}"
        );
        let sorted_paths = |outcome: &SearchOutcome| {
            let mut paths: Vec<String> = outcome.hits.iter().map(|hit| hit.path.clone()).collect();
            paths.sort();
            paths
        };
        assert_eq!(
            sorted_paths(&recovered),
            sorted_paths(&recalled),
            "re-embedding must not change which pages a search returns"
        );
        steps.push("recovered");

        // The receipt the parent compares against what its stub received.
        println!(
            "{CHILD_MARKER}{}",
            serde_json::json!({
                "published_identity": published,
                "rebuilt_identity": rebuilt,
                "steps": steps,
            })
        );
    }

    /// The stored bytes have to survive the round trip, and a value that is not
    /// a whole number of f32s is a corrupted column rather than a short vector.
    #[test]
    fn embeddings_round_trip_through_their_byte_encoding() {
        let values = vec![0.0f32, 1.5, -2.25, f32::MIN_POSITIVE];
        assert_eq!(vector::decode(&vector::encode(&values)).unwrap(), values);
        assert_eq!(vector::encode(&values).len(), values.len() * 4);
        let error = vector::decode(&[0, 1, 2]).expect_err("three bytes is not one f32");
        assert!(
            format!("{error:#}").contains("not a whole number"),
            "{error:#}"
        );
    }

    /// The workspace search fuses one list per project, so the per-project
    /// knobs have to survive that hop: `--global --no-vector` that quietly ran
    /// the vector stream anyway would be a flag that does nothing.
    #[tokio::test]
    async fn workspace_search_forwards_the_vector_switch_to_each_project() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let workspace = WorkspaceId::new("acme").unwrap();
        let project_id = ProjectId::new("ai-memory").unwrap();
        let build_root = TempDir::new().unwrap();
        let embedder = fake_for_corpus();
        publish_pages(
            &bucket,
            &workspace,
            &project_id,
            build_root.path(),
            &corpus(),
            Some(&embedder),
        )
        .await;

        async fn run(
            bucket: &Arc<dyn ObjectStore>,
            workspace: &WorkspaceId,
            project_id: &ProjectId,
            tuning: SearchTuning,
            embedder: &dyn Embedder,
        ) -> SearchOutcome {
            search_workspace_tuned(
                bucket.as_ref(),
                &reader(bucket),
                workspace,
                std::slice::from_ref(project_id),
                TempDir::new().unwrap().path(),
                QUERY,
                10,
                &tuning,
                Some(embedder),
            )
            .await
            .unwrap()
        }

        let off = run(
            &bucket,
            &workspace,
            &project_id,
            SearchTuning {
                now_ms: 0,
                vector_search: false,
                ..Default::default()
            },
            &embedder,
        )
        .await;
        assert!(
            !off.stream_candidates.contains_key("vector"),
            "the switch must reach the per-project search: {off:?}"
        );

        // Positive control: the same call with the stream on does run it.
        let on = run(
            &bucket,
            &workspace,
            &project_id,
            SearchTuning {
                now_ms: 0,
                ..Default::default()
            },
            &embedder,
        )
        .await;
        assert!(
            on.stream_candidates.contains_key("vector"),
            "the vector stream must be reachable through a workspace search: {on:?}"
        );
    }

    /// A split containing a zero-width embedding would fail every search, so
    /// the builder refuses to create one. The positive control is in the same
    /// test: the same document with a real vector builds fine.
    #[test]
    fn build_index_refuses_a_zero_width_embedding() {
        let dir = TempDir::new().unwrap();
        let mut doc = docs().remove(0);
        doc.embedding = Some(Vec::new());
        let error = build_index(dir.path(), &[doc]).expect_err("zero width must be refused");
        assert!(
            format!("{error:#}").contains("zero-width embedding"),
            "{error:#}"
        );

        let dir = TempDir::new().unwrap();
        let mut doc = docs().remove(0);
        doc.embedding = Some(vec![0.25, 0.5, 0.75]);
        build_index(dir.path(), &[doc]).expect("a real vector builds");
    }

    /// Cosine refuses the inputs it cannot answer honestly: a width mismatch, a
    /// zero vector, and a non-finite component.
    #[test]
    fn cosine_refuses_undefined_comparisons() {
        assert!(vector::cosine(&[1.0, 0.0], &[1.0]).is_err());
        assert!(vector::cosine(&[0.0, 0.0], &[1.0, 0.0]).is_err());
        assert!(vector::cosine(&[1.0, 0.0], &[0.0, 0.0]).is_err());
        assert!(vector::cosine(&[f32::NAN, 0.0], &[1.0, 0.0]).is_err());
        // Positive control: the same call with a defined pair answers 1.0.
        assert!((vector::cosine(&[2.0, 0.0], &[0.5, 0.0]).unwrap() - 1.0).abs() < 1e-6);
    }

    // ---- The authority check reads only the shards it has to ----------------

    /// The scope every test in this section reads and writes.
    const SCOPE: &str = "v1/ws/acme/proj/ai-memory";

    fn ws() -> WorkspaceId {
        WorkspaceId::new("acme").unwrap()
    }

    fn proj() -> ProjectId {
        ProjectId::new("ai-memory").unwrap()
    }

    /// A bucket that records the object keys a call actually read.
    ///
    /// The claim under test is *which* objects the authority check fetches, so
    /// the assertion has to be about the set of keys. A count would be
    /// satisfied by a reader that fetched one shard twice and never asked for
    /// another — the exact shape of the mistake this pins.
    #[derive(Debug)]
    struct RecordingStore {
        /// Shared, so a test can seed and inspect the same bucket through a raw
        /// handle and have the recording not see any of that.
        inner: Arc<InMemory>,
        reads: Mutex<Vec<String>>,
    }

    impl RecordingStore {
        fn new(inner: Arc<InMemory>) -> Arc<Self> {
            Arc::new(Self {
                inner,
                reads: Mutex::new(Vec::new()),
            })
        }

        /// Forget what has been read, so a test can time one call.
        fn reset(&self) {
            self.reads.lock().unwrap().clear();
        }

        /// The manifest objects read since the last [`Self::reset`], sorted.
        ///
        /// Sorted, so the comparison is a set comparison: the order reads come
        /// back in is the bucket's business, not the answer's.
        fn manifest_reads(&self) -> Vec<String> {
            let mut out: Vec<String> = self
                .reads
                .lock()
                .unwrap()
                .iter()
                .filter(|key| key.starts_with(SCOPE) && key.contains("/manifest"))
                .cloned()
                .collect();
            out.sort();
            out
        }
    }

    impl std::fmt::Display for RecordingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "RecordingStore({})", self.inner)
        }
    }

    impl ObjectStore for RecordingStore {
        fn put_opts<'life0, 'life1, 'async_trait>(
            &'life0 self,
            location: &'life1 ObjectPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> Pin<Box<dyn Future<Output = object_store::Result<PutResult>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async move { self.inner.put_opts(location, payload, opts).await })
        }

        fn put_multipart_opts<'life0, 'life1, 'async_trait>(
            &'life0 self,
            location: &'life1 ObjectPath,
            opts: PutMultipartOptions,
        ) -> Pin<
            Box<
                dyn Future<Output = object_store::Result<Box<dyn MultipartUpload>>>
                    + Send
                    + 'async_trait,
            >,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async move { self.inner.put_multipart_opts(location, opts).await })
        }

        fn get_opts<'life0, 'life1, 'async_trait>(
            &'life0 self,
            location: &'life1 ObjectPath,
            options: GetOptions,
        ) -> Pin<Box<dyn Future<Output = object_store::Result<GetResult>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async move {
                let out = self.inner.get_opts(location, options).await;
                self.reads
                    .lock()
                    .unwrap()
                    .push(location.as_ref().to_string());
                out
            })
        }

        fn delete<'life0, 'life1, 'async_trait>(
            &'life0 self,
            location: &'life1 ObjectPath,
        ) -> Pin<Box<dyn Future<Output = object_store::Result<()>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async move { self.inner.delete(location).await })
        }

        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_delimiter<'life0, 'life1, 'async_trait>(
            &'life0 self,
            prefix: Option<&'life1 ObjectPath>,
        ) -> Pin<Box<dyn Future<Output = object_store::Result<ListResult>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async move { self.inner.list_with_delimiter(prefix).await })
        }

        fn copy<'life0, 'life1, 'life2, 'async_trait>(
            &'life0 self,
            from: &'life1 ObjectPath,
            to: &'life2 ObjectPath,
        ) -> Pin<Box<dyn Future<Output = object_store::Result<()>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            'life2: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async move { self.inner.copy(from, to).await })
        }

        fn copy_if_not_exists<'life0, 'life1, 'life2, 'async_trait>(
            &'life0 self,
            from: &'life1 ObjectPath,
            to: &'life2 ObjectPath,
        ) -> Pin<Box<dyn Future<Output = object_store::Result<()>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            'life2: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async move { self.inner.copy_if_not_exists(from, to).await })
        }
    }

    /// The commit point the bucket holds, read through a raw handle.
    ///
    /// Expected values come from here rather than from the store's own reader,
    /// so an assertion about what a search read is about the persisted layout
    /// and not about the code under test.
    async fn persisted_root(bucket: &Arc<InMemory>) -> qm_core::ManifestRoot {
        let key = ObjectPath::from(format!("{SCOPE}/manifest.json"));
        let bytes = bucket.get(&key).await.unwrap().bytes().await.unwrap();
        serde_json::from_slice(&bytes).expect("the premise: the scope is stored sharded")
    }

    /// The key of the shard the commit point names for `path`.
    ///
    /// Not "every shard whose body says `shard: n`": a shard is immutable and
    /// content-addressed, so the bucket also keeps the shards *earlier* commit
    /// points named, and more than one object can carry the same index. The one
    /// that answers for `path` is the one the current root names.
    async fn shard_key_for(bucket: &Arc<InMemory>, path: &str) -> String {
        let root = persisted_root(bucket).await;
        let index = manifest_shard_index(path);
        root.shard(index)
            .unwrap_or_else(|| panic!("the root names no shard {index}, for {path}"))
            .key
            .clone()
    }

    /// A reader over the recording bucket, in the storage form `format` names.
    fn recording_reader(recording: &Arc<RecordingStore>, format: u32) -> ProjectStore {
        ProjectStore::new(Arc::clone(recording) as Arc<dyn ObjectStore>, "v1")
            .with_manifest_format(format)
            .with_retry(RetryPolicy {
                max_attempts: 64,
                base_delay: Duration::ZERO,
            })
    }

    /// Commit `body` at `path`, then publish a one-document split holding it.
    ///
    /// The split is what a search finds; the commit point is what decides
    /// whether what it found is still current.
    async fn commit_and_publish(
        store: &ProjectStore,
        bucket: &Arc<dyn ObjectStore>,
        writer: &WriterId,
        seq: u64,
        path: &str,
        body: &str,
        now_ms: i64,
    ) {
        let (workspace, project_id) = (ws(), proj());
        let page_path = PagePath::new(path).unwrap();
        store
            .commit_page(CommitPageRequest {
                workspace_id: workspace.clone(),
                project_id: project_id.clone(),
                path: page_path.clone(),
                title: path.to_string(),
                body: body.to_string(),
                writer_id: writer.clone(),
                now_ms,
            })
            .await
            .expect("commit");
        let page = store
            .read_page(&workspace, &project_id, &page_path)
            .await
            .expect("read")
            .expect("page");
        let doc = PageDoc::from_version(&workspace, &project_id, &page, now_ms);
        let build = TempDir::new().unwrap();
        publish_split_index(
            bucket.as_ref(),
            store,
            &workspace,
            &project_id,
            writer,
            seq,
            &[doc],
            build.path(),
            now_ms,
        )
        .await
        .expect("publish split");
    }

    /// One page per `(path, body)`, all published in a single split.
    async fn scope_with_one_split(
        raw: &Arc<InMemory>,
        format: u32,
        pages: &[(String, String)],
    ) -> ProjectStore {
        let bucket: Arc<dyn ObjectStore> = Arc::clone(raw) as Arc<dyn ObjectStore>;
        let store = ProjectStore::new(Arc::clone(&bucket), "v1").with_manifest_format(format);
        let writer = WriterId::new("mbp-a").unwrap();
        let mut docs = Vec::new();
        for (index, (path, body)) in pages.iter().enumerate() {
            let now_ms = 1_000 + index as i64;
            let page_path = PagePath::new(path.as_str()).unwrap();
            store
                .commit_page(CommitPageRequest {
                    workspace_id: ws(),
                    project_id: proj(),
                    path: page_path.clone(),
                    title: path.clone(),
                    body: body.clone(),
                    writer_id: writer.clone(),
                    now_ms,
                })
                .await
                .expect("commit");
            let page = store
                .read_page(&ws(), &proj(), &page_path)
                .await
                .expect("read")
                .expect("page");
            docs.push(PageDoc::from_version(&ws(), &proj(), &page, now_ms));
        }
        let build = TempDir::new().unwrap();
        publish_split_index(
            bucket.as_ref(),
            &store,
            &ws(),
            &proj(),
            &writer,
            1,
            &docs,
            build.path(),
            9_000,
        )
        .await
        .expect("publish split");
        store
    }

    /// A corpus of 60 paths, with `hit_token` given to three paths that land in
    /// three different shards.
    fn spread_corpus(hit_token: &str) -> Vec<(String, String)> {
        let mut pages: Vec<(String, String)> = (0..60)
            .map(|index| {
                (
                    format!("notes/page-{index:03}.md"),
                    "ordinary note".to_string(),
                )
            })
            .collect();
        // Three paths in three *different* shards, so the expected read set is
        // the root plus three shards rather than an accident of two paths
        // colliding on one.
        let mut indices = BTreeSet::new();
        let chosen: Vec<String> = pages
            .iter()
            .filter(|(path, _)| indices.insert(manifest_shard_index(path)))
            .take(3)
            .map(|(path, _)| path.clone())
            .collect();
        assert_eq!(
            chosen.len(),
            3,
            "the premise: 60 paths must reach at least 3 distinct shards"
        );
        for (path, body) in pages.iter_mut() {
            if chosen.contains(path) {
                *body = format!("{hit_token} note");
            }
        }
        pages
    }

    /// A scope with one live page, one superseded page whose old version is the
    /// only copy in its split, and one deleted page whose document is still in
    /// its split.
    ///
    /// Those last two are the candidates the authority check exists to refuse.
    async fn scope_with_stale_and_deleted(format: u32) -> (Arc<InMemory>, Arc<RecordingStore>) {
        let raw = Arc::new(InMemory::new());
        let bucket: Arc<dyn ObjectStore> = Arc::clone(&raw) as Arc<dyn ObjectStore>;
        let store = ProjectStore::new(Arc::clone(&bucket), "v1").with_manifest_format(format);

        // Three splits, one per page, each written by its own machine.
        for (index, (writer, path, body)) in [
            ("mbp-a", "notes/live.md", "zephyrquill note"),
            ("mbp-b", "notes/superseded.md", "quorumstone note"),
            ("mbp-c", "notes/removed.md", "removalmark note"),
        ]
        .into_iter()
        .enumerate()
        {
            commit_and_publish(
                &store,
                &bucket,
                &WriterId::new(writer).unwrap(),
                1,
                path,
                body,
                1_000 + index as i64,
            )
            .await;
        }
        // The superseded path's newer version is committed but never published,
        // so the split keeps proposing the version the commit point dropped.
        store
            .commit_page(CommitPageRequest {
                workspace_id: ws(),
                project_id: proj(),
                path: PagePath::new("notes/superseded.md").unwrap(),
                title: "notes/superseded.md".to_string(),
                body: "replacement note".to_string(),
                writer_id: WriterId::new("mbp-d").unwrap(),
                now_ms: 2_000,
            })
            .await
            .expect("supersede");
        // The deleted path keeps its document in the split; the commit point
        // gains a tombstone instead.
        store
            .delete_page(
                &ws(),
                &proj(),
                &PagePath::new("notes/removed.md").unwrap(),
                &WriterId::new("mbp-e").unwrap(),
                2_100,
            )
            .await
            .expect("delete");

        let recording = RecordingStore::new(Arc::clone(&raw));
        (raw, recording)
    }

    /// A search must fetch the root pointer and the shards its candidates land
    /// in — not every shard in the scope.
    #[tokio::test]
    async fn a_sharded_search_reads_only_the_shards_its_candidates_land_in() {
        const HIT_TOKEN: &str = "zephyrquill";
        let raw = Arc::new(InMemory::new());
        let pages = spread_corpus(HIT_TOKEN);
        scope_with_one_split(&raw, qm_core::MANIFEST_FORMAT_SHARDED, &pages).await;

        let recording = RecordingStore::new(Arc::clone(&raw));
        let reader = recording_reader(&recording, qm_core::MANIFEST_FORMAT_SHARDED);
        let cache = TempDir::new().unwrap();

        recording.reset();
        let outcome = search_project(
            recording.as_ref(),
            &reader,
            &ws(),
            &proj(),
            cache.path(),
            HIT_TOKEN,
            10,
        )
        .await
        .expect("search");

        // Premises: the token matches exactly the three paths this test gave it
        // to, and the authority check let all three through. Without them the
        // read-set assertion below could be about a search that found nothing.
        let mut hit_paths: Vec<String> = outcome.hits.iter().map(|hit| hit.path.clone()).collect();
        hit_paths.sort();
        let mut expected_paths: Vec<String> = pages
            .iter()
            .filter(|(_, body)| body.contains(HIT_TOKEN))
            .map(|(path, _)| path.clone())
            .collect();
        expected_paths.sort();
        assert_eq!(
            expected_paths.len(),
            3,
            "the premise: the corpus gives the token to three paths"
        );
        assert_eq!(
            hit_paths, expected_paths,
            "the premise: the token matches exactly those three paths"
        );
        assert_eq!(
            outcome.filtered_out, 0,
            "the premise: nothing in this corpus is stale"
        );

        // The answer: the root pointer and the three shards those paths hash
        // to, as a set of keys.
        let mut expected = vec![format!("{SCOPE}/manifest.json")];
        for path in &expected_paths {
            expected.push(shard_key_for(&raw, path).await);
        }
        expected.sort();
        assert_eq!(
            expected.len(),
            4,
            "the premise: three paths in three shards, plus the root: {expected:?}"
        );
        assert_eq!(
            recording.manifest_reads(),
            expected,
            "the authority check must read the root and only the shards its \
             candidates land in"
        );

        // And the scope really does commit more shards than that, so the
        // equality above is not "it read everything" in disguise.
        let root = persisted_root(&raw).await;
        assert!(
            root.shards.len() > 3,
            "the premise: 60 paths must spread over more than three shards, got {}",
            root.shards.len()
        );
    }

    /// The whole form has one commit-point object, and the check reads exactly
    /// that one — the change is about the sharded form only.
    #[tokio::test]
    async fn a_whole_search_reads_exactly_the_one_manifest_object() {
        const HIT_TOKEN: &str = "zephyrquill";
        let raw = Arc::new(InMemory::new());
        let pages = spread_corpus(HIT_TOKEN);
        scope_with_one_split(&raw, qm_core::MANIFEST_FORMAT_WHOLE, &pages).await;

        let recording = RecordingStore::new(Arc::clone(&raw));
        let reader = recording_reader(&recording, qm_core::MANIFEST_FORMAT_WHOLE);
        let cache = TempDir::new().unwrap();

        recording.reset();
        let outcome = search_project(
            recording.as_ref(),
            &reader,
            &ws(),
            &proj(),
            cache.path(),
            HIT_TOKEN,
            10,
        )
        .await
        .expect("search");

        // Premise: the search really did check three candidates against the
        // commit point, so "one object" is not "nothing to check".
        assert_eq!(outcome.hits.len(), 3, "{:?}", outcome.hits);
        assert_eq!(outcome.filtered_out, 0);
        assert_eq!(
            recording.manifest_reads(),
            vec![format!("{SCOPE}/manifest.json")],
            "the whole form is a single object, read once"
        );
    }

    /// The storage form must not change the answer: same hits, same
    /// `filtered_out`, same streams — including the two ways a candidate the
    /// split still proposes can be stale.
    #[tokio::test]
    async fn both_forms_answer_identically_including_stale_and_deleted_candidates() {
        let (whole_raw, whole_recording) =
            scope_with_stale_and_deleted(qm_core::MANIFEST_FORMAT_WHOLE).await;
        let (sharded_raw, sharded_recording) =
            scope_with_stale_and_deleted(qm_core::MANIFEST_FORMAT_SHARDED).await;
        let whole = recording_reader(&whole_recording, qm_core::MANIFEST_FORMAT_WHOLE);
        let sharded = recording_reader(&sharded_recording, qm_core::MANIFEST_FORMAT_SHARDED);

        for (query, expected_hits, expected_candidates, expected_filtered) in [
            ("zephyrquill", vec!["notes/live.md"], 1usize, 0usize),
            ("quorumstone", vec![], 1, 1),
            ("removalmark", vec![], 1, 1),
            ("note", vec!["notes/live.md"], 3, 2),
        ] {
            let from_whole = search_project(
                whole_recording.as_ref(),
                &whole,
                &ws(),
                &proj(),
                TempDir::new().unwrap().path(),
                query,
                10,
            )
            .await
            .expect("whole search");
            let from_shards = search_project(
                sharded_recording.as_ref(),
                &sharded,
                &ws(),
                &proj(),
                TempDir::new().unwrap().path(),
                query,
                10,
            )
            .await
            .expect("sharded search");

            let paths: Vec<&str> = from_whole
                .hits
                .iter()
                .map(|hit| hit.path.as_str())
                .collect();
            assert_eq!(
                paths, expected_hits,
                "the premise: what `{query}` should match and keep"
            );
            assert_eq!(
                from_whole.candidates, expected_candidates,
                "the premise: how many documents the split proposed for `{query}`"
            );
            assert_eq!(
                from_whole.filtered_out, expected_filtered,
                "the premise: how many of them the commit point refuses for `{query}`"
            );
            if expected_filtered > 0 {
                // The negative case is only real when the split actually
                // proposed something the commit point then refused.
                assert!(
                    from_whole.candidates > from_whole.hits.len(),
                    "the premise: `{query}` must reach a candidate the authority \
                     check has to refuse: {:?}",
                    from_whole
                );
            }
            assert_eq!(
                from_whole, from_shards,
                "the storage form must not change the answer to `{query}`"
            );
        }

        // The sharded side is not merely equal to a whole side that read
        // nothing: three pages, three splits, three shards of the commit point.
        assert_eq!(
            persisted_root(&sharded_raw).await.shards.len(),
            3,
            "the premise: the corpus spreads over three shards"
        );
        let shard_prefix = ObjectPath::from(format!("{SCOPE}/manifest/shards"));
        assert!(
            futures::StreamExt::next(&mut whole_raw.list(Some(&shard_prefix)))
                .await
                .is_none(),
            "the premise: the whole form stores no shard objects at all"
        );
    }

    /// A read that fetches less cannot report damage it never looked at.
    ///
    /// The authority check reads the shards its candidates land in, so a corrupt
    /// shard no candidate lands in does not fail the search — and the layer that
    /// does read everything, `verify_project`, still says so. Both halves are
    /// asserted here, because "search still answers" alone would also pass on a
    /// bucket where nothing was wrong.
    #[tokio::test]
    async fn a_corrupt_shard_no_candidate_lands_in_is_verify_s_problem_not_search_s() {
        const HIT_TOKEN: &str = "zephyrquill";
        let raw = Arc::new(InMemory::new());
        let pages = spread_corpus(HIT_TOKEN);
        scope_with_one_split(&raw, qm_core::MANIFEST_FORMAT_SHARDED, &pages).await;

        let recording = RecordingStore::new(Arc::clone(&raw));
        let reader = recording_reader(&recording, qm_core::MANIFEST_FORMAT_SHARDED);

        let before = search_project(
            recording.as_ref(),
            &reader,
            &ws(),
            &proj(),
            TempDir::new().unwrap().path(),
            HIT_TOKEN,
            10,
        )
        .await
        .expect("search");
        assert_eq!(before.hits.len(), 3, "{:?}", before.hits);
        let hit_paths: BTreeSet<String> = before.hits.iter().map(|hit| hit.path.clone()).collect();
        let hit_shards: BTreeSet<u16> = hit_paths
            .iter()
            .map(|path| manifest_shard_index(path))
            .collect();

        // A path whose shard holds no candidate: nothing the check has to
        // consult, so that shard is one it has no reason to read.
        let victim = pages
            .iter()
            .map(|(path, _)| path.clone())
            .find(|path| {
                !hit_paths.contains(path) && !hit_shards.contains(&manifest_shard_index(path))
            })
            .expect("the premise: 60 paths over ~53 shards leave a shard with no hit");
        let victim_key = shard_key_for(&raw, &victim).await;

        // Premise: that shard's own body names none of the paths the search
        // keeps, so the search has no reason to read it.
        let body = raw
            .get(&ObjectPath::from(victim_key.clone()))
            .await
            .expect("the victim shard is in the bucket")
            .bytes()
            .await
            .unwrap();
        let shard: qm_core::ManifestShard = serde_json::from_slice(&body).unwrap();
        assert!(
            !shard.pages.keys().any(|path| hit_paths.contains(path)),
            "the premise: {victim_key} holds no candidate path: {:?}",
            shard.pages.keys().collect::<Vec<_>>()
        );

        // Damage it where it lies: the same key, different bytes.
        raw.put(
            &ObjectPath::from(victim_key.clone()),
            PutPayload::from_static(b"not a manifest shard"),
        )
        .await
        .expect("overwrite the shard");

        recording.reset();
        let after = search_project(
            recording.as_ref(),
            &reader,
            &ws(),
            &proj(),
            TempDir::new().unwrap().path(),
            HIT_TOKEN,
            10,
        )
        .await
        .expect("a shard the check never reads cannot fail the search");
        assert_eq!(
            after, before,
            "the answer is the same down to the scores: the corrupt shard was not consulted"
        );
        assert!(
            !recording.manifest_reads().contains(&victim_key),
            "the premise: the search really did not read {victim_key}: {:?}",
            recording.manifest_reads()
        );

        // The layer that reads every shard reports it.
        let report = reader
            .verify_project(&ws(), &proj())
            .await
            .expect("verify reports damage as problems, not as an error");
        assert!(
            report
                .problems
                .iter()
                .any(|problem| problem.detail.contains(&victim_key)),
            "verify must name the corrupt shard: {:?}",
            report.problems
        );
    }
}
