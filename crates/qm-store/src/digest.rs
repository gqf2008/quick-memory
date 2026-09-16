//! "What changed recently" — assembled from bucket objects without the index.
//!
//! A digest is a read, not a cache, and it never consults the search index: a
//! machine that has never published a split gets the same answer, and a deletion
//! shows up as a deletion. Its three parts are not equally authoritative
//! though — session heads and handoff objects are, while pages come from the
//! advisory commit log, which is appended after the CAS and whose failure is
//! ignored. The pages section can therefore be missing a record; it answers
//! "what happened recently", not "what is true now".

use qm_core::{CommitRecord, Handoff, SessionId};
use serde::{Deserialize, Serialize};

/// Recent activity in one scope, newest first within each part.
///
/// The three parts are independent: any of them may be empty, and that is a
/// normal answer rather than an error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Digest {
    /// Commits in the window, newest first. Includes deletions.
    pub pages: Vec<CommitRecord>,
    /// Sessions whose head moved inside the window, most recent first.
    pub sessions: Vec<SessionSummary>,
    /// Handoffs with any activity inside the window, most recent first.
    pub handoffs: Vec<Handoff>,
}

impl Digest {
    /// Whether the window contained nothing at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pages.is_empty() && self.sessions.is_empty() && self.handoffs.is_empty()
    }
}

/// One session's activity inside the window.
///
/// `observations` is the committed count carried by the session head, so it is
/// the total for the session, not the number that arrived inside the window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSummary {
    /// Session whose head moved.
    pub session_id: SessionId,
    /// Observations committed across the session's chain.
    pub observations: u64,
    /// When the session's head was last committed, in milliseconds.
    pub last_seen_ms: i64,
}

/// The timestamp that makes a handoff "recent": its most recent stage change.
///
/// A handoff is in the window when *any* of created/claimed/finished is, so a
/// baton opened before the window but claimed inside it still shows up — which
/// is the whole point of a baton.
#[must_use]
pub fn handoff_activity_ms(handoff: &Handoff) -> i64 {
    let mut newest = handoff.created_at_ms;
    for at in [handoff.claimed_at_ms, handoff.finished_at_ms]
        .into_iter()
        .flatten()
    {
        if at > newest {
            newest = at;
        }
    }
    newest
}
