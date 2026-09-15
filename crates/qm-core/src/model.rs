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

use crate::{PageId, PagePath, ProjectId, WorkspaceId, WriterId};

/// Lower-case hex SHA-256 of an object body, used for content-addressed keys.
#[must_use]
pub fn content_hash(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

/// Manifest/WAL encoding schema. Bumping it is a breaking change.
pub const MANIFEST_SCHEMA: u32 = 1;

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

/// Per-project commit point.
///
/// Invariant: `seq` increases by exactly one per successful CAS, so it counts
/// committed mutations and orders them without trusting any clock.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// Encoding schema.
    pub schema: u32,
    /// Workspace this manifest belongs to.
    pub workspace_id: WorkspaceId,
    /// Project this manifest belongs to.
    pub project_id: ProjectId,
    /// Commit sequence of the last successful CAS (0 = never committed).
    pub seq: u64,
    /// Current version per page path.
    pub pages: BTreeMap<String, PageEntry>,
    /// Timestamp of the last commit, in milliseconds.
    pub updated_at_ms: i64,
}

impl Manifest {
    /// An empty manifest for a scope that has never been committed.
    #[must_use]
    pub fn empty(workspace_id: WorkspaceId, project_id: ProjectId) -> Self {
        Self {
            schema: MANIFEST_SCHEMA,
            workspace_id,
            project_id,
            seq: 0,
            pages: BTreeMap::new(),
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
    pub fn record(&mut self, path: &PagePath, entry: PageEntry) {
        self.seq = entry.seq;
        self.updated_at_ms = self.updated_at_ms.max(entry.created_at_ms);
        self.pages.insert(path.as_str().to_string(), entry);
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
    /// Encoding schema.
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
            schema: MANIFEST_SCHEMA,
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

    /// Append a split and advance the generation.
    pub fn push_split(&mut self, split: SplitEntry, now_ms: i64) {
        self.generation += 1;
        self.covered_until_ms = self.covered_until_ms.max(now_ms);
        self.splits.push(split);
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
