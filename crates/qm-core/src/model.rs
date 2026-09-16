//! Authoritative data model.
//!
//! Two rules shape these types:
//!
//! 1. **Content is fully determined by identity.** A [`PageVersion`] holds only
//!    fields that feed its own id, so replaying a write reproduces
//!    byte-identical content at the same key. Time and actor live in the
//!    commit point ([`PageEntry`]), which is written exactly once because a CAS
//!    decides the winner.
//! 2. **The manifest is a commit point, not a cache.** A page is readable
//!    because the manifest names it, never because its object was uploaded.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::scrub::scrub;
use crate::{CoreError, PageId, PagePath, ProjectId, SessionId, WorkspaceId, WriterId};

/// Bound a short field such as an actor or event kind.
fn bound(value: &str, max: usize) -> String {
    let mut out = value.trim().to_string();
    if out.len() > max {
        let mut cut = max;
        while cut > 0 && !out.is_char_boundary(cut) {
            cut -= 1;
        }
        out.truncate(cut);
    }
    out
}

/// Lower-case hex SHA-256 of an object body, used for content-addressed keys.
#[must_use]
pub fn content_hash(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

/// Manifest/WAL encoding schema. Bumping it is a breaking change.
pub const MANIFEST_SCHEMA: u32 = 1;

/// Schema of an *index* (a catalog and the splits it lists): what the terms in
/// those splits were built with.
///
/// Unlike [`MANIFEST_SCHEMA`] this is not about the authority — an index is a
/// derived layer, so a reader can always rebuild it from the pages. It exists so
/// that a reader can tell "these terms came from an analyzer I no longer use"
/// apart from "there is nothing to find", and refuse instead of answering with
/// a silent zero. §6 of the design has the analyzer this identifies.
///
/// - `1`: tantivy's `default` analyzer (a CJK run is one token).
/// - `2`: the CJK analyzer (`qm-search::cjk`): CJK runs become unigrams and
///   bigrams, so a word inside a run is findable.
pub const INDEX_SCHEMA: u32 = 2;

/// Manifest storage form: every path of the scope in one object.
///
/// This is the form `quick-memory` shipped first, and the default. Its commit
/// point grows with the project, so it has a measured ceiling (see
/// `MANIFEST_MAX_BYTES` in `qm-store`).
pub const MANIFEST_FORMAT_WHOLE: u32 = 1;

/// Manifest storage form: a root pointer plus immutable content-addressed
/// shards.
///
/// The root pointer is still the only CAS target, so `seq` still counts
/// committed mutations exactly. What changes is what it holds: a layout, not
/// the paths themselves.
pub const MANIFEST_FORMAT_SHARDED: u32 = 2;

/// How many shards a scope's paths are divided into.
///
/// Fixed rather than "one per N paths" on purpose. The split has to be a
/// function of the *path alone*, because a shard is immutable and
/// content-addressed: a path that moved between shards as the project grew
/// would rewrite every shard it passed through and make the layout depend on
/// insertion order. A fixed count gives that, and bounds the root pointer's
/// size (at most this many references) instead of letting it grow without
/// limit. A scope only ever *materialises* the shards it has paths in, so this
/// is an upper bound on the split, not a cost paid per commit.
pub const MANIFEST_SHARD_COUNT: u16 = 256;

/// Lower-case hex of a SHA-256 digest.
fn hex(digest: &[u8]) -> String {
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// Derive the immutable id of a page version.
///
/// The id covers exactly the fields stored in [`PageVersion`], so the same
/// write always produces the same key: a commit retried after a CAS conflict
/// (or after a crash between uploading and committing) rewrites the same
/// object instead of accumulating duplicates.
#[must_use]
pub fn derive_page_id(
    path: &PagePath,
    title: &str,
    body: &str,
    supersedes: Option<&PageId>,
) -> PageId {
    let mut hasher = Sha256::new();
    hasher.update(b"quick-memory/page-version/v1\0");
    hasher.update(path.as_str().as_bytes());
    hasher.update(b"\0");
    hasher.update(title.as_bytes());
    hasher.update(b"\0");
    hasher.update(body.as_bytes());
    hasher.update(b"\0");
    if let Some(previous) = supersedes {
        hasher.update(previous.as_str().as_bytes());
    }
    PageId::trusted(hex(&hasher.finalize()))
}

/// Immutable page content, addressed by [`derive_page_id`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageVersion {
    /// Content-derived id, also the object key suffix.
    pub page_id: PageId,
    /// Path inside the project.
    pub path: PagePath,
    /// Page title.
    pub title: String,
    /// Markdown body.
    pub body: String,
    /// The version this one replaces, if any. This is the supersession chain.
    pub supersedes: Option<PageId>,
}

/// Commit-point entry for the current version of one path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageEntry {
    /// Current page version id.
    pub page_id: PageId,
    /// Commit sequence that made this version visible.
    pub seq: u64,
    /// Commit timestamp in milliseconds.
    pub created_at_ms: i64,
    /// Machine that committed the version.
    pub writer_id: WriterId,
    /// Title, kept here so a listing needs no object fetch.
    pub title: String,
    /// Previous version, if this write replaced one.
    pub supersedes: Option<PageId>,
}

/// One committed mutation, as stored in the write-ahead log.
///
/// Deliberately carries no commit sequence and no timestamp: a retried commit
/// must produce byte-identical content at the same key, and only the winning
/// CAS may assign a sequence (which lives in [`PageEntry`]). Global order comes
/// from the manifest; per-path order comes from the `supersedes` chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalEntry {
    /// Encoding schema.
    pub schema: u32,
    /// Machine that committed the entry.
    pub writer_id: WriterId,
    /// Page version this entry introduced.
    pub page_id: PageId,
    /// Path the version was written to.
    pub path: PagePath,
    /// Previous version, if any.
    pub supersedes: Option<PageId>,
}

impl WalEntry {
    /// Object-key suffix for this entry: the page id it introduced.
    ///
    /// Deliberately excludes `seq` and the timestamp — a retried commit must
    /// land on the same key, and only the winning CAS may assign a sequence.
    #[must_use]
    pub fn event_id(&self) -> &str {
        self.page_id.as_str()
    }
}

/// A deleted path: the tombstone that keeps it deleted.
///
/// Deletion never removes objects from the bucket — old splits and page
/// versions survive by design. What makes a delete authoritative is this
/// record, plus the search path refusing any candidate the manifest no longer
/// names. The record also carries the sequence that deleted the page, so a
/// rebuild can tell "this write happened before the delete, do not resurrect
/// it" without trusting clocks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tombstone {
    /// Commit sequence that recorded the deletion.
    pub seq: u64,
    /// Deletion timestamp in milliseconds.
    pub deleted_at_ms: i64,
    /// Machine that performed the deletion.
    pub writer_id: WriterId,
    /// Version that was current when the page was deleted, if any.
    pub last_page_id: Option<PageId>,
}

/// Per-project commit point.
///
/// Invariant: `seq` increases by exactly one per successful CAS, so it counts
/// committed mutations and orders them without trusting any clock.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// Encoding schema.
    pub schema: u32,
    /// Storage form this object is written in.
    ///
    /// Absent on the wire means [`MANIFEST_FORMAT_WHOLE`]: every manifest
    /// written before this field existed decodes as the whole form, and the
    /// whole form encodes without it, so a format-1 commit is byte-identical
    /// to what the code before the field produced. The field is what makes a
    /// *different* form detectable instead of being read as the nearest shape
    /// that happens to decode.
    #[serde(default = "whole_format", skip_serializing_if = "is_whole_format")]
    pub format: u32,
    /// Workspace this manifest belongs to.
    pub workspace_id: WorkspaceId,
    /// Project this manifest belongs to.
    pub project_id: ProjectId,
    /// Commit sequence of the last successful CAS (0 = never committed).
    pub seq: u64,
    /// Current version per page path.
    pub pages: BTreeMap<String, PageEntry>,
    /// Deleted paths, kept so a rebuild cannot resurrect them.
    pub tombstones: BTreeMap<String, Tombstone>,
    /// Timestamp of the last commit, in milliseconds.
    pub updated_at_ms: i64,
}

impl Manifest {
    /// An empty manifest for a scope that has never been committed.
    #[must_use]
    pub fn empty(workspace_id: WorkspaceId, project_id: ProjectId) -> Self {
        Self {
            schema: MANIFEST_SCHEMA,
            format: MANIFEST_FORMAT_WHOLE,
            workspace_id,
            project_id,
            seq: 0,
            pages: BTreeMap::new(),
            tombstones: BTreeMap::new(),
            updated_at_ms: 0,
        }
    }

    /// Current entry for a path, if the project has one.
    #[must_use]
    pub fn head(&self, path: &PagePath) -> Option<&PageEntry> {
        self.pages.get(path.as_str())
    }

    /// Current page id for a path, if any.
    #[must_use]
    pub fn head_page_id(&self, path: &PagePath) -> Option<PageId> {
        self.head(path).map(|entry| entry.page_id.clone())
    }

    /// Sequence the next successful commit will carry.
    #[must_use]
    pub fn next_seq(&self) -> u64 {
        self.seq + 1
    }

    /// Record a committed page version.
    ///
    /// Writing a path clears its tombstone: the newer commit is the truth.
    pub fn record(&mut self, path: &PagePath, entry: PageEntry) {
        self.seq = entry.seq;
        self.updated_at_ms = self.updated_at_ms.max(entry.created_at_ms);
        self.tombstones.remove(path.as_str());
        self.pages.insert(path.as_str().to_string(), entry);
    }

    /// Record a deletion: the path leaves `pages` and gains a tombstone.
    pub fn tombstone(&mut self, path: &PagePath, tombstone: Tombstone) {
        self.seq = tombstone.seq;
        self.updated_at_ms = self.updated_at_ms.max(tombstone.deleted_at_ms);
        self.pages.remove(path.as_str());
        self.tombstones.insert(path.as_str().to_string(), tombstone);
    }

    /// Whether the path is currently deleted.
    #[must_use]
    pub fn is_tombstoned(&self, path: &PagePath) -> bool {
        self.tombstones.contains_key(path.as_str())
    }

    /// Whether the manifest still names this exact page version as current.
    ///
    /// This is the only visibility rule the search path may use.
    #[must_use]
    pub fn is_current(&self, path: &str, page_id: &str) -> bool {
        self.pages
            .get(path)
            .is_some_and(|entry| entry.page_id.as_str() == page_id)
    }
}

/// The `format` a whole manifest defaults to when the object has no field.
const fn whole_format() -> u32 {
    MANIFEST_FORMAT_WHOLE
}

/// Whether a `format` number names the whole-object form.
#[must_use]
pub const fn is_whole_format(format: &u32) -> bool {
    *format == MANIFEST_FORMAT_WHOLE
}

/// Which shard a path belongs to.
///
/// The first byte of the SHA-256 of the path, so the split is uniform, and a
/// function of the path alone. That last part is what lets a shard be
/// immutable and content-addressed: were the index derived from anything else
/// (how many paths there are, which ones were written first), adding one page
/// would move other pages between shards and every affected shard would have
/// to be rewritten. Here a path's shard is fixed for the life of the project.
#[must_use]
pub fn manifest_shard_index(path: &str) -> u16 {
    u16::from(Sha256::digest(path.as_bytes())[0])
}

/// One immutable shard of a sharded manifest.
///
/// Holds a subset of a scope's paths — those whose [`manifest_shard_index`] is
/// `shard` — and nothing else. It is content-addressed, so it is written the
/// same way a page version is: a crash or a lost CAS race leaves an orphan,
/// never a half-written shard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestShard {
    /// Encoding schema.
    pub schema: u32,
    /// Storage form (always [`MANIFEST_FORMAT_SHARDED`]).
    pub format: u32,
    /// Workspace this shard belongs to.
    pub workspace_id: WorkspaceId,
    /// Project this shard belongs to.
    pub project_id: ProjectId,
    /// Which shard of the split this is.
    pub shard: u16,
    /// Current version per page path, for the paths in this shard.
    pub pages: BTreeMap<String, PageEntry>,
    /// Deleted paths, for the paths in this shard.
    pub tombstones: BTreeMap<String, Tombstone>,
}

impl ManifestShard {
    /// An empty shard for `index`.
    #[must_use]
    pub fn empty(workspace_id: WorkspaceId, project_id: ProjectId, index: u16) -> Self {
        Self {
            schema: MANIFEST_SCHEMA,
            format: MANIFEST_FORMAT_SHARDED,
            workspace_id,
            project_id,
            shard: index,
            pages: BTreeMap::new(),
            tombstones: BTreeMap::new(),
        }
    }
}

/// Where one shard lives, and what the root says is in it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardRef {
    /// Which shard of the split this is.
    pub shard: u16,
    /// Object key of the shard body.
    pub key: String,
    /// Content hash of the shard body: the key's suffix, and the reader's check.
    pub content_hash: String,
    /// Paths the shard carries: current versions plus tombstones.
    ///
    /// A summary, for a caller that has the root and does not want to fetch the
    /// shard to ask how big it is — not a checked invariant. It cannot drift
    /// from the shard it describes: the shard's key is the hash of its bytes, so
    /// a `path_count` that disagreed with the body would mean different bytes,
    /// which the reader refuses before it reads this field.
    pub path_count: usize,
}

/// The whole-object manifest a sharded root replaced.
///
/// Kept for two reasons that are both load-bearing: it records that a scope
/// was migrated (so a rollback can refuse to run on a scope that was never
/// migrated, rather than write a whole manifest over a scope whose history it
/// does not know), and it names the archived bytes so an operator can still
/// read the exact pre-migration object after the switch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestPredecessor {
    /// Storage form of the archived object.
    pub format: u32,
    /// Commit sequence the archived object carried.
    pub seq: u64,
    /// Object key of the archived body.
    pub key: String,
    /// Content hash of the archived body.
    pub content_hash: String,
}

/// Commit point of a sharded manifest.
///
/// This is the only mutable object of a sharded scope and the only CAS target:
/// a commit writes new immutable shards and then replaces this object once. A
/// reader that has a root has a consistent snapshot, because the shards it
/// names can never change under it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestRoot {
    /// Encoding schema.
    pub schema: u32,
    /// Storage form (always [`MANIFEST_FORMAT_SHARDED`]).
    pub format: u32,
    /// Workspace this manifest belongs to.
    pub workspace_id: WorkspaceId,
    /// Project this manifest belongs to.
    pub project_id: ProjectId,
    /// Commit sequence of the last successful CAS (0 = never committed).
    pub seq: u64,
    /// Timestamp of the last commit, in milliseconds.
    pub updated_at_ms: i64,
    /// The shards that make up the current state, ordered by shard index.
    pub shards: Vec<ShardRef>,
    /// The whole-object manifest this root was migrated from, if any.
    pub predecessor: Option<ManifestPredecessor>,
}

impl ManifestRoot {
    /// An empty root for a scope whose first commit lands sharded.
    #[must_use]
    pub fn empty(workspace_id: WorkspaceId, project_id: ProjectId) -> Self {
        Self {
            schema: MANIFEST_SCHEMA,
            format: MANIFEST_FORMAT_SHARDED,
            workspace_id,
            project_id,
            seq: 0,
            updated_at_ms: 0,
            shards: Vec::new(),
            predecessor: None,
        }
    }

    /// Sequence the next successful commit will carry.
    #[must_use]
    pub fn next_seq(&self) -> u64 {
        self.seq + 1
    }

    /// The layout entry for `index`, if the root names that shard.
    #[must_use]
    pub fn shard(&self, index: u16) -> Option<&ShardRef> {
        self.shards.iter().find(|entry| entry.shard == index)
    }

    /// Assemble the whole form from the shards this root names.
    ///
    /// Refuses anything that is not exactly the layout the root describes: a
    /// missing shard, an extra one, a shard filed under the wrong index, one
    /// that names another scope, or a path sitting in the wrong shard. A commit
    /// point that cannot be reassembled is damage, and saying so is the only
    /// safe answer — returning the shards that *did* load would hand back a
    /// project with pages silently missing.
    ///
    /// # Errors
    /// [`CoreError::Manifest`] for any of the mismatches above.
    pub fn materialize(&self, parts: &[ManifestShard]) -> Result<Manifest, CoreError> {
        if parts.len() != self.shards.len() {
            return Err(CoreError::Manifest(format!(
                "root names {} shards but {} were loaded",
                self.shards.len(),
                parts.len()
            )));
        }
        // The value handed back is the *whole* form of the state — that is what
        // a `Manifest` is — so it is tagged as the whole form. The sharded shape
        // lives in the root, and a body tagged with another form is not a whole
        // manifest at all (see `AnyManifest::decode`).
        let mut manifest = Manifest {
            schema: MANIFEST_SCHEMA,
            format: MANIFEST_FORMAT_WHOLE,
            workspace_id: self.workspace_id.clone(),
            project_id: self.project_id.clone(),
            seq: self.seq,
            pages: BTreeMap::new(),
            tombstones: BTreeMap::new(),
            updated_at_ms: self.updated_at_ms,
        };
        for reference in &self.shards {
            let part = parts
                .iter()
                .find(|part| part.shard == reference.shard)
                .ok_or_else(|| {
                    CoreError::Manifest(format!(
                        "shard {} is named but was not loaded",
                        reference.shard
                    ))
                })?;
            part.check_layout(self, reference.shard)?;
            for path in part.pages.keys().chain(part.tombstones.keys()) {
                if manifest_shard_index(path) != reference.shard {
                    return Err(CoreError::Manifest(format!(
                        "shard {} holds {path:?}, which belongs to shard {}",
                        reference.shard,
                        manifest_shard_index(path)
                    )));
                }
            }
            for (path, entry) in &part.pages {
                if let Some(previous) = manifest.pages.insert(path.clone(), entry.clone()) {
                    return Err(CoreError::Manifest(format!(
                        "shard {} and an earlier shard both name {path:?} (seq {})",
                        reference.shard, previous.seq
                    )));
                }
            }
            for (path, tombstone) in &part.tombstones {
                if manifest
                    .tombstones
                    .insert(path.clone(), tombstone.clone())
                    .is_some()
                {
                    return Err(CoreError::Manifest(format!(
                        "shard {} and an earlier shard both tombstone {path:?}",
                        reference.shard
                    )));
                }
            }
        }
        // A path cannot be both live and deleted; the whole form had one map
        // and the split must reproduce that, not merely the union.
        if let Some(path) = manifest
            .pages
            .keys()
            .find(|path| manifest.tombstones.contains_key(path.as_str()))
        {
            return Err(CoreError::Manifest(format!(
                "{path:?} is both live and tombstoned across shards"
            )));
        }
        Ok(manifest)
    }
}

impl ManifestShard {
    /// Check this shard is filed under the index the root says, for this scope.
    fn check_layout(&self, root: &ManifestRoot, index: u16) -> Result<(), CoreError> {
        if self.schema != MANIFEST_SCHEMA {
            return Err(CoreError::Manifest(format!(
                "shard {index} carries schema {} but {} was expected",
                self.schema, MANIFEST_SCHEMA
            )));
        }
        if self.format != MANIFEST_FORMAT_SHARDED {
            return Err(CoreError::Manifest(format!(
                "shard {index} carries format {} but {} was expected",
                self.format, MANIFEST_FORMAT_SHARDED
            )));
        }
        if self.shard != index {
            return Err(CoreError::Manifest(format!(
                "a shard filed under {index} declares itself shard {}",
                self.shard
            )));
        }
        if self.workspace_id != root.workspace_id || self.project_id != root.project_id {
            return Err(CoreError::Manifest(format!(
                "shard {index} belongs to {}/{}, not {}/{}",
                self.workspace_id, self.project_id, root.workspace_id, root.project_id
            )));
        }
        Ok(())
    }
}

/// A commit-point object decoded as whichever form it is written in.
///
/// The discriminant is read first and the body is then decoded as *that* form.
/// It has to be this way round: a sharded root decoded as a whole manifest
/// would report a project with no pages, and "the project is empty" is the one
/// wrong answer a reader must never give.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnyManifest {
    /// Every path in one object.
    Whole(Manifest),
    /// A root pointer plus immutable shards.
    Sharded(Box<ManifestRoot>),
}

/// What the `format` field of a commit-point body says, if it says anything.
#[derive(Debug, Deserialize)]
struct FormatProbe {
    #[serde(default = "whole_format")]
    format: u32,
}

impl AnyManifest {
    /// Decode a commit-point body, dispatching on its `format` field.
    ///
    /// # Errors
    /// [`CoreError::Manifest`] for undecodable bytes and for a format this
    /// build does not know.
    pub fn decode(bytes: &[u8]) -> Result<Self, CoreError> {
        let probe: FormatProbe = serde_json::from_slice(bytes)
            .map_err(|error| CoreError::Manifest(format!("undecodable manifest: {error}")))?;
        match probe.format {
            MANIFEST_FORMAT_WHOLE => serde_json::from_slice(bytes)
                .map(Self::Whole)
                .map_err(|error| CoreError::Manifest(format!("undecodable manifest: {error}"))),
            MANIFEST_FORMAT_SHARDED => serde_json::from_slice(bytes)
                .map(|root| Self::Sharded(Box::new(root)))
                .map_err(|error| {
                    CoreError::Manifest(format!("undecodable manifest root: {error}"))
                }),
            other => Err(CoreError::Manifest(format!(
                "manifest format {other} is not one this build knows \
                 ({MANIFEST_FORMAT_WHOLE} whole, {MANIFEST_FORMAT_SHARDED} sharded)"
            ))),
        }
    }

    /// Storage form of this object.
    #[must_use]
    pub fn format(&self) -> u32 {
        match self {
            Self::Whole(manifest) => manifest.format,
            Self::Sharded(root) => root.format,
        }
    }

    /// Commit sequence this object carries.
    #[must_use]
    pub fn seq(&self) -> u64 {
        match self {
            Self::Whole(manifest) => manifest.seq,
            Self::Sharded(root) => root.seq,
        }
    }
}

impl Manifest {
    /// Split this manifest into the shards its paths belong to.
    ///
    /// Only non-empty shards are produced, ordered by index, so a scope with a
    /// handful of paths writes a handful of objects rather than
    /// [`MANIFEST_SHARD_COUNT`].
    #[must_use]
    pub fn into_shards(self) -> Vec<ManifestShard> {
        let Manifest {
            workspace_id,
            project_id,
            pages,
            tombstones,
            ..
        } = self;
        let mut shards: BTreeMap<u16, ManifestShard> = BTreeMap::new();
        for (path, entry) in pages {
            let index = manifest_shard_index(&path);
            shards
                .entry(index)
                .or_insert_with(|| {
                    ManifestShard::empty(workspace_id.clone(), project_id.clone(), index)
                })
                .pages
                .insert(path, entry);
        }
        for (path, tombstone) in tombstones {
            let index = manifest_shard_index(&path);
            shards
                .entry(index)
                .or_insert_with(|| {
                    ManifestShard::empty(workspace_id.clone(), project_id.clone(), index)
                })
                .tombstones
                .insert(path, tombstone);
        }
        shards.into_values().collect()
    }
}

/// One published split, as recorded in the index catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SplitEntry {
    /// Machine that built and published the split.
    pub writer_id: WriterId,
    /// Monotonic per-writer sequence number.
    pub seq: u64,
    /// Object-key prefix holding the split's files.
    pub prefix: String,
    /// Number of documents in the split.
    pub doc_count: u64,
    /// Content hash over the split's files; publishing the same split twice is
    /// a no-op because the catalog deduplicates on this value.
    pub content_hash: String,
}

/// Index catalog: the set of splits a reader must open to cover a project.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexCatalog {
    /// Index schema: which analyzer the terms in these splits were built with
    /// (see [`INDEX_SCHEMA`]). Deliberately *not* [`MANIFEST_SCHEMA`]: that one
    /// describes the authority's encoding, this one describes a derived layer.
    pub schema: u32,
    /// Catalog generation; increases by one per publish.
    pub generation: u64,
    /// Splits that make up the index, oldest first.
    pub splits: Vec<SplitEntry>,
    /// Timestamp (ms) through which the catalog is known complete.
    pub covered_until_ms: i64,
}

impl IndexCatalog {
    /// A catalog with no splits.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            schema: INDEX_SCHEMA,
            generation: 0,
            splits: Vec::new(),
            covered_until_ms: 0,
        }
    }

    /// Whether a split with this content is already published.
    #[must_use]
    pub fn contains_hash(&self, content_hash: &str) -> bool {
        self.splits
            .iter()
            .any(|split| split.content_hash == content_hash)
    }

    /// Append a split.
    ///
    /// The catalog's [`schema`](Self::schema) is deliberately *not* raised here:
    /// appending a new-analyzer split to a catalog that already holds old ones
    /// leaves the catalog describing the oldest terms in it, which is what a
    /// reader needs to know before it answers. `qm compact` is what rebuilds the
    /// set and raises the value.
    pub fn push_split(&mut self, split: SplitEntry, now_ms: i64) {
        self.generation += 1;
        self.covered_until_ms = self.covered_until_ms.max(now_ms);
        self.splits.push(split);
    }
}

/// One captured event from an agent session.
///
/// Content-only, like every other immutable object here: the id is derived from
/// the bytes, so a hook that retries the same event produces the same object
/// instead of a duplicate. Capture never assigns a sequence — the session head
/// does that when the CAS wins.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Observation {
    /// Encoding schema.
    pub schema: u32,
    /// Content-derived id, also the deduplication key.
    pub observation_id: String,
    /// Session this event belongs to.
    pub session_id: SessionId,
    /// Harness or agent that emitted it.
    pub actor: String,
    /// Event kind, e.g. `tool_use` or `session_end`.
    pub kind: String,
    /// Scrubbed event text.
    pub text: String,
    /// Client-supplied timestamp in milliseconds; ordering only.
    pub created_at_ms: i64,
}

impl Observation {
    /// Scrub, bound, and re-id an observation.
    ///
    /// This is the intake boundary in type form: the id is recomputed after
    /// scrubbing so it always matches the bytes that get stored, and callers
    /// cannot accidentally persist a secret the scrubber removed.
    #[must_use]
    pub fn sanitized(mut self) -> Self {
        self.text = scrub(&self.text);
        self.actor = bound(&self.actor, 128);
        self.kind = bound(&self.kind, 64);
        self.observation_id = derive_observation_id(
            &self.session_id,
            &self.actor,
            &self.kind,
            &self.text,
            self.created_at_ms,
        );
        self
    }
}

/// Derive an observation id from everything that makes the event unique.
#[must_use]
pub fn derive_observation_id(
    session_id: &SessionId,
    actor: &str,
    kind: &str,
    text: &str,
    created_at_ms: i64,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"quick-memory/observation/v1\0");
    hasher.update(session_id.as_str().as_bytes());
    hasher.update(b"\0");
    hasher.update(actor.as_bytes());
    hasher.update(b"\0");
    hasher.update(kind.as_bytes());
    hasher.update(b"\0");
    hasher.update(text.as_bytes());
    hasher.update(b"\0");
    hasher.update(created_at_ms.to_le_bytes());
    hex(&hasher.finalize())
}

/// An immutable batch of observations, chained to its predecessor.
///
/// The chain is what makes capture visible: a segment is readable only when it
/// is reachable from the session head, so a batch that lost its CAS race is an
/// orphan rather than a phantom event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservationSegment {
    /// Encoding schema.
    pub schema: u32,
    /// Session the segment belongs to.
    pub session_id: SessionId,
    /// Id of the segment this one extends, if any.
    pub prev: Option<String>,
    /// Events captured in this batch, in the order they arrived.
    pub observations: Vec<Observation>,
}

/// Content hash of a segment, used as its object key.
#[must_use]
pub fn derive_segment_id(segment: &ObservationSegment) -> String {
    match serde_json::to_vec(segment) {
        Ok(bytes) => content_hash(&bytes),
        // Serialization of these types cannot fail; fall back to a value that
        // is obviously not a real id rather than panicking in a capture path.
        Err(_) => "unserializable-segment".to_string(),
    }
}

/// CAS head of a session's observation chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionHead {
    /// Encoding schema.
    pub schema: u32,
    /// Session this head belongs to.
    pub session_id: SessionId,
    /// Number of committed segments.
    pub generation: u64,
    /// Newest committed segment id.
    pub segment_id: String,
    /// Total observations committed across the chain.
    pub count: u64,
    /// When the head was last committed, in milliseconds.
    pub updated_at_ms: i64,
}

/// State of a handoff.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HandoffState {
    /// Waiting for someone to pick it up.
    Open,
    /// Exactly one machine took it.
    Claimed,
    /// The claimer finished it.
    Done,
}

/// A baton passed between sessions or machines.
///
/// Pages are shared; a baton is owned. The whole point is that taking one is a
/// single atomic act, so two machines racing for the same handoff cannot both
/// believe they own it — the CAS on this object is that act, and it needs no
/// second guard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Handoff {
    /// Encoding schema.
    pub schema: u32,
    /// Content-derived id.
    pub id: String,
    /// Short title.
    pub title: String,
    /// What the next session needs to know.
    pub body: String,
    /// Who left the handoff.
    pub created_by: WriterId,
    /// When it was created, in milliseconds.
    pub created_at_ms: i64,
    /// Who claimed it, if anyone.
    pub claimed_by: Option<WriterId>,
    /// When it was claimed, in milliseconds.
    pub claimed_at_ms: Option<i64>,
    /// When it was finished, in milliseconds.
    pub finished_at_ms: Option<i64>,
}

impl Handoff {
    /// Current state, derived from the timestamps so the two cannot disagree.
    #[must_use]
    pub fn state(&self) -> HandoffState {
        if self.finished_at_ms.is_some() {
            HandoffState::Done
        } else if self.claimed_by.is_some() {
            HandoffState::Claimed
        } else {
            HandoffState::Open
        }
    }
}

/// Derive a handoff id from its content, so re-opening the same note is idempotent.
#[must_use]
pub fn derive_handoff_id(title: &str, body: &str, created_at_ms: i64) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"quick-memory/handoff/v1\0");
    hasher.update(title.as_bytes());
    hasher.update(b"\0");
    hasher.update(body.as_bytes());
    hasher.update(b"\0");
    hasher.update(created_at_ms.to_le_bytes());
    hex(&hasher.finalize())
}

/// What a commit did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitKind {
    /// A page version became current.
    PageWritten,
    /// A page was tombstoned.
    PageDeleted,
}

/// Advisory record of one commit, written *after* the CAS that made it real.
///
/// This is the one place commit-time facts can live: putting them inside the
/// immutable version object would break retry idempotency (a retry would write
/// different bytes to the same content-addressed key). So the record is written
/// second, and a crash between the two steps leaves a page whose history shows
/// no timestamp — degraded, never wrong. Nothing authoritative reads this log:
/// it answers "when" and "as of", never "what is true now".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitRecord {
    /// Encoding schema.
    pub schema: u32,
    /// Commit sequence assigned by the winning CAS.
    pub seq: u64,
    /// Commit timestamp in milliseconds.
    pub at_ms: i64,
    /// Machine that committed.
    pub writer_id: WriterId,
    /// What was committed.
    pub kind: CommitKind,
    /// Page the commit concerns.
    pub path: PagePath,
    /// Version written, for `PageWritten`.
    pub page_id: Option<PageId>,
}

/// State of a proposed edit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProposalState {
    /// Waiting for a human or a policy to decide.
    Pending,
    /// Approved and applied to the target path.
    Approved,
    /// Refused; the target path is untouched.
    Rejected,
}

/// A staged edit to a page, waiting for a decision.
///
/// The borrowed design's rule is that learning edits go through an approval
/// gate: an automated curator may *propose* rewriting a page, but nothing
/// changes until something with authority says so. The proposal is the whole
/// record of that: what would change, why, who asked, and how it ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Proposal {
    /// Encoding schema.
    pub schema: u32,
    /// Content-derived id.
    pub id: String,
    /// Page the edit targets.
    pub target_path: PagePath,
    /// Title to write.
    pub title: String,
    /// Proposed body.
    pub body: String,
    /// Why the change is being proposed.
    pub rationale: String,
    /// Who proposed it.
    pub created_by: WriterId,
    /// When it was proposed, in milliseconds.
    pub created_at_ms: i64,
    /// Where it stands.
    pub state: ProposalState,
    /// Who decided, if anyone.
    pub decided_by: Option<WriterId>,
    /// When it was decided, in milliseconds.
    pub decided_at_ms: Option<i64>,
    /// Free-text reason for a rejection.
    pub decision_note: Option<String>,
    /// Page version produced by an approval, once it is known.
    ///
    /// Written after the decision (and after the page commit) so a crash in
    /// between leaves an approved proposal that says "applied, id unknown"
    /// rather than losing the decision.
    pub applied_page_id: Option<PageId>,
}

/// Derive a proposal id from its content, so re-proposing the same edit is a
/// no-op instead of a duplicate.
#[must_use]
pub fn derive_proposal_id(
    target_path: &PagePath,
    title: &str,
    body: &str,
    rationale: &str,
    created_at_ms: i64,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"quick-memory/proposal/v1\0");
    hasher.update(target_path.as_str().as_bytes());
    hasher.update(b"\0");
    hasher.update(title.as_bytes());
    hasher.update(b"\0");
    hasher.update(body.as_bytes());
    hasher.update(b"\0");
    hasher.update(rationale.as_bytes());
    hasher.update(b"\0");
    hasher.update(created_at_ms.to_le_bytes());
    hex(&hasher.finalize())
}

/// A lease on an optional, abandonable job (compaction, garbage collection).
///
/// Leases exist so that "only one machine at a time" is expressible without a
/// server. Every field is in the object, so a machine that finds an expired
/// lease can take over simply by winning the CAS; nothing has to be released
/// cleanly for the system to make progress.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    /// Encoding schema.
    pub schema: u32,
    /// Machine currently holding the lease.
    pub owner: WriterId,
    /// Monotonic takeover counter; bumped on every acquisition.
    pub epoch: u64,
    /// When the holder acquired it, in milliseconds.
    pub acquired_at_ms: i64,
    /// When the lease stops being valid, in milliseconds.
    pub expires_at_ms: i64,
}

impl Lease {
    /// Whether a takeover is allowed at `now_ms`.
    #[must_use]
    pub fn is_expired(&self, now_ms: i64) -> bool {
        now_ms >= self.expires_at_ms
    }
}

/// CAS pointer to the current index catalog.
///
/// Separate from the catalog itself so a reader can pin a generation: reading
/// the head once gives a consistent snapshot even if another machine publishes
/// while a query is running.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogHead {
    /// Encoding schema.
    pub schema: u32,
    /// Generation of the catalog this points at.
    pub generation: u64,
    /// Full object key of the immutable catalog.
    pub catalog_key: String,
    /// When the head was last swapped, in milliseconds.
    pub updated_at_ms: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path() -> PagePath {
        PagePath::new("notes/raft.md").unwrap()
    }

    #[test]
    fn page_id_is_deterministic_and_content_addressed() {
        let a = derive_page_id(&path(), "Raft", "leader election", None);
        let b = derive_page_id(&path(), "Raft", "leader election", None);
        assert_eq!(a, b);
        assert_eq!(a.as_str().len(), 64);

        let c = derive_page_id(&path(), "Raft", "leader election v2", None);
        assert_ne!(a, c);

        let d = derive_page_id(&path(), "Raft", "leader election", Some(&a));
        assert_ne!(a, d, "superseding changes identity");

        let other_path = PagePath::new("notes/quickwit.md").unwrap();
        assert_ne!(
            a,
            derive_page_id(&other_path, "Raft", "leader election", None)
        );
    }

    #[test]
    fn catalog_dedupes_by_content_hash_and_advances_generation() {
        let mut catalog = IndexCatalog::empty();
        assert!(!catalog.contains_hash("h1"));
        let split = SplitEntry {
            writer_id: WriterId::new("mbp-1").unwrap(),
            seq: 1,
            prefix: "v1/ws/acme/proj/ai-memory/index/splits/mbp-1/0000000001".into(),
            doc_count: 3,
            content_hash: "h1".into(),
        };
        catalog.push_split(split, 10);
        assert_eq!(catalog.generation, 1);
        assert!(catalog.contains_hash("h1"));
        assert_eq!(catalog.covered_until_ms, 10);
    }

    #[test]
    fn observation_ids_are_content_addressed_and_dedupe_replays() {
        let session = SessionId::new("sess-1").unwrap();
        let first = derive_observation_id(&session, "codex", "tool_use", "ran cargo t", 10);
        assert_eq!(
            first,
            derive_observation_id(&session, "codex", "tool_use", "ran cargo t", 10),
            "a retry must produce the same id"
        );
        assert_ne!(
            first,
            derive_observation_id(&session, "codex", "tool_use", "ran cargo t", 11)
        );
        assert_ne!(
            first,
            derive_observation_id(&session, "codex", "tool_use", "ran cargo t2", 10)
        );

        let segment = ObservationSegment {
            schema: MANIFEST_SCHEMA,
            session_id: session.clone(),
            prev: None,
            observations: vec![Observation {
                schema: MANIFEST_SCHEMA,
                observation_id: first,
                session_id: session,
                actor: "codex".into(),
                kind: "tool_use".into(),
                text: "ran cargo t".into(),
                created_at_ms: 10,
            }],
        };
        assert_eq!(
            derive_segment_id(&segment),
            derive_segment_id(&segment.clone())
        );
        let mut other = segment.clone();
        other.prev = Some("abc".into());
        assert_ne!(
            derive_segment_id(&segment),
            derive_segment_id(&other),
            "changing the chain position changes the segment identity"
        );
    }

    #[test]
    fn tombstoning_removes_the_page_and_refuses_to_resurrect_it() {
        let path = path();
        let mut manifest = Manifest::empty(
            WorkspaceId::new("acme").unwrap(),
            ProjectId::new("ai-memory").unwrap(),
        );
        let page_id = derive_page_id(&path, "Raft", "body", None);
        manifest.record(
            &path,
            PageEntry {
                page_id: page_id.clone(),
                seq: 1,
                created_at_ms: 10,
                writer_id: WriterId::new("mbp-1").unwrap(),
                title: "Raft".into(),
                supersedes: None,
            },
        );
        assert!(manifest.is_current(path.as_str(), page_id.as_str()));

        manifest.tombstone(
            &path,
            Tombstone {
                seq: 2,
                deleted_at_ms: 20,
                writer_id: WriterId::new("mbp-1").unwrap(),
                last_page_id: Some(page_id.clone()),
            },
        );
        assert!(manifest.is_tombstoned(&path));
        assert!(!manifest.is_current(path.as_str(), page_id.as_str()));
        assert!(manifest.head(&path).is_none());

        // Writing the path again clears the tombstone: newer wins.
        let second = derive_page_id(&path, "Raft", "body v2", None);
        manifest.record(
            &path,
            PageEntry {
                page_id: second.clone(),
                seq: 3,
                created_at_ms: 30,
                writer_id: WriterId::new("mbp-2").unwrap(),
                title: "Raft".into(),
                supersedes: None,
            },
        );
        assert!(!manifest.is_tombstoned(&path));
        assert!(manifest.is_current(path.as_str(), second.as_str()));
    }

    fn ws() -> WorkspaceId {
        WorkspaceId::new("acme").unwrap()
    }

    fn proj() -> ProjectId {
        ProjectId::new("ai-memory").unwrap()
    }

    fn entry(path: &str, seq: u64, at_ms: i64) -> PageEntry {
        let page = PagePath::new(path).unwrap();
        PageEntry {
            page_id: derive_page_id(&page, "title", "body", None),
            seq,
            created_at_ms: at_ms,
            writer_id: WriterId::new("mbp-1").unwrap(),
            title: "title".into(),
            supersedes: None,
        }
    }

    /// Build a whole manifest with `count` fresh paths.
    fn whole_with_paths(count: usize) -> Manifest {
        let mut manifest = Manifest::empty(ws(), proj());
        for index in 0..count {
            manifest.record(
                &PagePath::new(format!("notes/page-{index:05}.md")).unwrap(),
                entry(
                    &format!("notes/page-{index:05}.md"),
                    index as u64 + 1,
                    1_000,
                ),
            );
        }
        manifest.tombstones.insert(
            "notes/gone.md".to_string(),
            Tombstone {
                seq: 9_000,
                deleted_at_ms: 2_000,
                writer_id: WriterId::new("mbp-1").unwrap(),
                last_page_id: None,
            },
        );
        manifest
    }

    /// The whole form's bytes must not move when the discriminant is added.
    ///
    /// This is the compatibility promise the format switch rests on: a commit
    /// with the sharded form switched off has to produce the same object the
    /// code before the field produced, or every existing tool that reads the
    /// bytes (and every evaluation that counts them) changes behaviour for a
    /// feature nobody turned on.
    #[test]
    fn a_whole_manifest_encodes_without_the_format_field() {
        let manifest = whole_with_paths(3);
        let text = serde_json::to_string(&manifest).unwrap();
        assert!(
            !text.contains("\"format\""),
            "the whole form must not grow a field on the wire: {text}"
        );
        let back: Manifest = serde_json::from_str(&text).unwrap();
        assert_eq!(back, manifest);
        assert_eq!(back.format, MANIFEST_FORMAT_WHOLE);
    }

    /// A body written before the field existed is still a whole manifest.
    #[test]
    fn a_manifest_without_the_field_decodes_as_the_whole_form() {
        let text = r#"{"schema":1,"workspace_id":"acme","project_id":"ai-memory",
                       "seq":2,"pages":{},"tombstones":{},"updated_at_ms":5}"#;
        let decoded = AnyManifest::decode(text.as_bytes()).unwrap();
        assert_eq!(decoded.format(), MANIFEST_FORMAT_WHOLE);
        assert_eq!(decoded.seq(), 2);
        assert!(matches!(decoded, AnyManifest::Whole(_)));
    }

    /// The whole point of the discriminant: a root must not be read as an
    /// empty project just because it has no `pages` of its own.
    #[test]
    fn the_discriminant_decides_the_form_and_an_unknown_one_is_refused() {
        let root = ManifestRoot::empty(ws(), proj());
        let text = serde_json::to_string(&root).unwrap();
        let decoded = AnyManifest::decode(text.as_bytes()).unwrap();
        assert_eq!(decoded.format(), MANIFEST_FORMAT_SHARDED);
        assert!(matches!(decoded, AnyManifest::Sharded(_)));

        // The same bytes read as a whole manifest must fail, not answer "".
        let as_whole: Result<Manifest, _> = serde_json::from_str(&text);
        assert!(as_whole.is_err(), "a root is not a whole manifest");

        let unknown = br#"{"format":3,"schema":1}"#;
        let error = AnyManifest::decode(unknown).unwrap_err();
        assert!(
            error.to_string().contains("format 3"),
            "an unknown form must name itself: {error}"
        );
    }

    /// A path's shard is a function of the path, so it cannot depend on what
    /// else the scope holds — that is what keeps shards immutable.
    #[test]
    fn a_paths_shard_depends_on_the_path_alone() {
        let all: Vec<u16> = (0..2_000)
            .map(|index| manifest_shard_index(&format!("notes/page-{index:05}.md")))
            .collect();
        assert!(all.iter().all(|index| *index < MANIFEST_SHARD_COUNT));
        assert_eq!(
            all.iter().collect::<std::collections::BTreeSet<_>>().len(),
            all.len().min(usize::from(MANIFEST_SHARD_COUNT)),
            "2000 paths must spread over the fixed split"
        );
        // Same path, same shard — no matter what else is in the manifest.
        let alone = manifest_shard_index("notes/raft.md");
        let crowded = whole_with_paths(500);
        let from_split = crowded
            .into_shards()
            .into_iter()
            .find(|shard| shard.pages.contains_key("notes/raft.md"))
            .map(|shard| shard.shard);
        if let Some(index) = from_split {
            assert_eq!(index, alone);
        }
    }

    /// The split has to be reversible, byte for byte, or a sharded scope
    /// answers differently from a whole one.
    #[test]
    fn splitting_and_materializing_returns_the_same_manifest() {
        let manifest = whole_with_paths(400);
        let parts = manifest.clone().into_shards();
        assert!(parts.len() > 1, "400 paths must land in several shards");
        assert!(
            parts.iter().filter(|shard| !shard.pages.is_empty()).count() > 1,
            "the fixture must actually split, not land in one shard"
        );

        let root = ManifestRoot {
            shards: parts
                .iter()
                .map(|shard| ShardRef {
                    shard: shard.shard,
                    key: format!("k/{}", shard.shard),
                    content_hash: "hash".into(),
                    path_count: shard.pages.len() + shard.tombstones.len(),
                })
                .collect(),
            ..ManifestRoot::empty(ws(), proj())
        };
        let rebuilt = root.materialize(&parts).unwrap();
        assert_eq!(rebuilt.pages, manifest.pages);
        assert_eq!(rebuilt.tombstones, manifest.tombstones);
        // `into_shards` drops the sequence: it is the root that carries it.
        assert_eq!(rebuilt.seq, root.seq);
    }

    /// The hinge the one-shard read hangs on: if a shard could hold a path that
    /// belongs elsewhere, a reader that trusts the split would report that path
    /// missing. Refusing the layout is what makes the optimisation sound.
    #[test]
    fn a_shard_holding_a_path_from_another_shard_is_refused() {
        let manifest = whole_with_paths(50);
        let mut parts = manifest.into_shards();
        assert!(parts.len() > 1, "the fixture must split");
        let moved_path = parts[0].pages.keys().next().unwrap().clone();
        let entry = parts[0].pages.remove(&moved_path).unwrap();
        parts[1].pages.insert(moved_path.clone(), entry);

        let root = ManifestRoot {
            shards: parts
                .iter()
                .map(|shard| ShardRef {
                    shard: shard.shard,
                    key: format!("k/{}", shard.shard),
                    content_hash: "hash".into(),
                    path_count: shard.pages.len() + shard.tombstones.len(),
                })
                .collect(),
            ..ManifestRoot::empty(ws(), proj())
        };
        let error = root.materialize(&parts).unwrap_err();
        assert!(
            error.to_string().contains("belongs to shard"),
            "a misplaced path must be named, not absorbed: {error}"
        );
    }

    /// A root that names shards a reader did not get is damage, not an empty
    /// project.
    #[test]
    fn a_root_with_a_missing_shard_is_refused() {
        let parts = whole_with_paths(50).into_shards();
        let root = ManifestRoot {
            shards: parts
                .iter()
                .map(|shard| ShardRef {
                    shard: shard.shard,
                    key: format!("k/{}", shard.shard),
                    content_hash: "hash".into(),
                    path_count: shard.pages.len(),
                })
                .collect(),
            ..ManifestRoot::empty(ws(), proj())
        };
        let short = &parts[..parts.len() - 1];
        let error = root.materialize(short).unwrap_err();
        assert!(
            error.to_string().contains("were loaded"),
            "a short read must be refused: {error}"
        );
    }

    #[test]
    fn manifest_records_only_advance_the_sequence() {
        let mut manifest = Manifest::empty(
            WorkspaceId::new("acme").unwrap(),
            ProjectId::new("ai-memory").unwrap(),
        );
        assert_eq!(manifest.next_seq(), 1);
        let page_id = derive_page_id(&path(), "Raft", "body", None);
        manifest.record(
            &path(),
            PageEntry {
                page_id,
                seq: 1,
                created_at_ms: 10,
                writer_id: WriterId::new("mbp-1").unwrap(),
                title: "Raft".into(),
                supersedes: None,
            },
        );
        assert_eq!(manifest.seq, 1);
        assert_eq!(manifest.next_seq(), 2);
        assert_eq!(manifest.pages.len(), 1);
    }
}
