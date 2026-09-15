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
use crate::{PageId, PagePath, ProjectId, SessionId, WorkspaceId, WriterId};

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
