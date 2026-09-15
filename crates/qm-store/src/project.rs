//! The per-project commit protocol.
//!
//! A page becomes part of the project when, and only when, its entry is
//! written into `manifest.json` by a successful CAS. Everything before that —
//! the immutable page object and the write-ahead record — is invisible to
//! readers, so a crash or a lost CAS race leaves orphans, never half-truths.
//!
//! Concurrency comes from retrying, not from a single writer: two machines
//! that read the same manifest both try to replace it, exactly one wins, and
//! the loser re-reads, re-derives its page id (its `supersedes` may have
//! changed) and tries again. No machine has to know another exists.

use std::cmp::Ordering;
use std::time::Duration;

use object_store::ObjectStore;
use qm_core::{
    CatalogHead, CommitKind, CommitRecord, Handoff, HandoffState, IndexCatalog, KeyLayout, Lease,
    MANIFEST_SCHEMA, Manifest, Observation, ObservationSegment, PageEntry, PagePath, PageVersion,
    ProjectId, Proposal, ProposalState, SessionHead, SessionId, SplitEntry, Tombstone, WalEntry,
    WorkspaceId, WriterId, content_hash, derive_handoff_id, derive_page_id, derive_proposal_id,
    derive_segment_id,
};

use crate::digest::{Digest, SessionSummary, handoff_activity_ms};
use crate::{CasStore, ObjectVersion, StoreError, decode, encode};

/// Upper bound on how far back `page_history` will walk a supersession chain.
const MAX_HISTORY_DEPTH: usize = 100_000;

/// How hard to fight for the commit point before giving up.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Total attempts, including the first.
    pub max_attempts: u32,
    /// Base backoff between attempts; multiplied by the attempt number.
    pub base_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 8,
            base_delay: Duration::from_millis(5),
        }
    }
}

/// A manifest plus the version it was read at.
#[derive(Debug, Clone)]
pub struct LoadedManifest {
    /// Current manifest contents (empty when the scope has never committed).
    pub manifest: Manifest,
    /// Version to pass to a CAS, or `None` when the manifest does not exist yet.
    pub version: Option<ObjectVersion>,
}

/// What a retention pass would keep.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionPlan {
    /// Observations that survive.
    pub keep: Vec<Observation>,
    /// How many would be dropped.
    pub dropped: usize,
}

/// Decide which observations survive a retention pass.
///
/// Two rules, because either alone is wrong: `keep_ms` bounds age (so a quiet
/// project still converges), and `keep_last` bounds count (so a burst of
/// activity, or a machine with a wrong clock, cannot drop everything recent).
/// Observations are assumed oldest-first, as the chain returns them.
#[must_use]
pub fn plan_retention(
    observations: &[Observation],
    keep_ms: i64,
    keep_last: usize,
    now_ms: i64,
) -> RetentionPlan {
    let cutoff = now_ms.saturating_sub(keep_ms);
    let start_of_recent = observations.len().saturating_sub(keep_last);
    let keep: Vec<Observation> = observations
        .iter()
        .enumerate()
        .filter(|(index, observation)| {
            *index >= start_of_recent || observation.created_at_ms >= cutoff
        })
        .map(|(_, observation)| observation.clone())
        .collect();
    let dropped = observations.len() - keep.len();
    RetentionPlan { keep, dropped }
}

/// What a session rewrite did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRewriteOutcome {
    /// Observations kept.
    pub kept: usize,
    /// Segments after the rewrite (always one on success).
    pub segments: u64,
    /// True when the session already looked like this.
    pub already_present: bool,
    /// Attempts consumed, including retries.
    pub attempts: u32,
}

/// One proposed edit.
#[derive(Debug, Clone)]
pub struct ProposalRequest {
    /// Workspace scope.
    pub workspace_id: WorkspaceId,
    /// Project scope.
    pub project_id: ProjectId,
    /// Page the edit targets.
    pub target_path: PagePath,
    /// Title to write.
    pub title: String,
    /// Proposed body.
    pub body: String,
    /// Why the change is proposed.
    pub rationale: String,
    /// Who proposed it.
    pub created_by: WriterId,
    /// Timestamp recorded on the proposal.
    pub now_ms: i64,
}

/// One batch of observations to ingest.
#[derive(Debug, Clone)]
pub struct IngestObservationsRequest {
    /// Workspace the session belongs to.
    pub workspace_id: WorkspaceId,
    /// Project the session belongs to.
    pub project_id: ProjectId,
    /// Session being captured.
    pub session_id: SessionId,
    /// Machine performing the capture.
    pub writer_id: WriterId,
    /// Already-scrubbed observations, in arrival order.
    pub observations: Vec<Observation>,
    /// Commit timestamp in milliseconds.
    pub now_ms: i64,
}

/// What an ingest did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestOutcome {
    /// Id of the committed segment (empty when the batch was already present).
    pub segment_id: String,
    /// Segments committed after this ingest.
    pub generation: u64,
    /// Observations committed across the chain.
    pub count: u64,
    /// Observations accepted from this batch.
    pub accepted: usize,
    /// True when this exact batch was already committed.
    pub already_present: bool,
    /// Attempts consumed, including retries.
    pub attempts: u32,
}

/// A catalog plus the head version it was read at.
#[derive(Debug, Clone)]
pub struct LoadedCatalog {
    /// Current catalog (empty when nothing has been published).
    pub catalog: IndexCatalog,
    /// Full object key of the pinned catalog, if any.
    pub catalog_key: Option<String>,
    /// Head version to pass to a CAS, or `None` when the head does not exist.
    pub head_version: Option<ObjectVersion>,
}

/// What a deletion did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteOutcome {
    /// Commit sequence after the deletion (unchanged when already deleted).
    pub manifest_seq: u64,
    /// Version that was current when it was deleted.
    pub removed: Option<qm_core::PageId>,
    /// True when the path was already tombstoned; the call was a no-op.
    pub already_deleted: bool,
    /// Attempts consumed, including retries.
    pub attempts: u32,
}

/// What a catalog replacement did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplaceCatalogOutcome {
    /// Generation of the replacement catalog.
    pub generation: u64,
    /// Key of the replacement catalog.
    pub catalog_key: String,
    /// Splits in the replacement catalog.
    pub splits: usize,
    /// Attempts consumed, including retries.
    pub attempts: u32,
}

/// A held lease, with the version needed to renew or release it.
#[derive(Debug, Clone)]
pub struct LeaseGuard {
    /// Object key of the lease.
    pub key: String,
    /// Current lease contents.
    pub lease: Lease,
    /// Version the lease was last read/written at.
    pub version: ObjectVersion,
}

impl LeaseGuard {
    /// Extend the lease. Returns false when the lease moved on (we lost it).
    ///
    /// # Errors
    /// Propagates backend failures.
    pub async fn renew(
        &mut self,
        store: &ProjectStore,
        now_ms: i64,
        ttl_ms: i64,
    ) -> Result<bool, StoreError> {
        let mut next = self.lease.clone();
        next.acquired_at_ms = now_ms;
        next.expires_at_ms = now_ms + ttl_ms;
        match store
            .cas
            .update(&self.key, encode(&next)?, &self.version)
            .await
        {
            Ok(version) => {
                self.lease = next;
                self.version = version;
                Ok(true)
            }
            Err(StoreError::Precondition | StoreError::NotFound | StoreError::AlreadyExists) => {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    /// Mark the lease free. Best effort: an unreleased lease simply expires.
    ///
    /// # Errors
    /// Propagates backend failures.
    pub async fn release(mut self, store: &ProjectStore, now_ms: i64) -> Result<(), StoreError> {
        self.lease.expires_at_ms = now_ms;
        match store
            .cas
            .update(&self.key, encode(&self.lease)?, &self.version)
            .await
        {
            Ok(_) => Ok(()),
            // Losing the release race is not an error: the lease still expires.
            Err(StoreError::Precondition | StoreError::NotFound) => Ok(()),
            Err(error) => Err(error),
        }
    }
}

/// What a split publication did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishOutcome {
    /// Catalog generation after the publish.
    pub generation: u64,
    /// Key of the catalog version describing that generation.
    pub catalog_key: String,
    /// Attempts consumed, including retries.
    pub attempts: u32,
    /// True when this split was already published (publication is idempotent).
    pub already_present: bool,
}

/// One page write to commit.
#[derive(Debug, Clone)]
pub struct CommitPageRequest {
    /// Workspace the page belongs to.
    pub workspace_id: WorkspaceId,
    /// Project the page belongs to.
    pub project_id: ProjectId,
    /// Path inside the project.
    pub path: PagePath,
    /// Page title.
    pub title: String,
    /// Markdown body.
    pub body: String,
    /// Machine performing the write.
    pub writer_id: WriterId,
    /// Caller-supplied clock, recorded for ordering only.
    pub now_ms: i64,
}

/// What a successful commit did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitOutcome {
    /// Commit sequence the winning CAS assigned.
    pub manifest_seq: u64,
    /// Id of the page version that was committed.
    pub page_id: qm_core::PageId,
    /// Version this commit superseded, if any.
    pub supersedes: Option<qm_core::PageId>,
    /// Attempts consumed, including retries.
    pub attempts: u32,
    /// False when the page object already existed (a replay).
    pub page_object_created: bool,
}

/// Reads and commits inside one object-store prefix.
pub struct ProjectStore {
    cas: CasStore,
    layout: KeyLayout,
    retry: RetryPolicy,
}

/// Ordering for [`ProjectStore::recent_pages`]: newest commit first, path
/// ascending to break ties.
///
/// The tie-breaker is defence in depth rather than the only source of order:
/// `Manifest::pages` is a `BTreeMap` and `sort_by` is stable, so equal
/// timestamps already come out path-ascending today. Stating it explicitly
/// keeps the order *total* if the container or the sort changes underneath —
/// and a total order is the thing two machines must agree on.
fn recency_order(left: &(PagePath, PageEntry), right: &(PagePath, PageEntry)) -> Ordering {
    right
        .1
        .created_at_ms
        .cmp(&left.1.created_at_ms)
        .then_with(|| left.0.as_str().cmp(right.0.as_str()))
}

impl ProjectStore {
    /// Build a store over `store`, rooted at `root` (use `v1`).
    #[must_use]
    pub fn new(store: std::sync::Arc<dyn ObjectStore>, root: impl Into<String>) -> Self {
        Self {
            cas: CasStore::new(store, ""),
            layout: KeyLayout::new(root),
            retry: RetryPolicy::default(),
        }
    }

    /// Override the commit retry policy.
    #[must_use]
    pub fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// The key layout in use.
    #[must_use]
    pub fn layout(&self) -> &KeyLayout {
        &self.layout
    }

    /// The CAS layer, for reachability walks and maintenance.
    #[must_use]
    pub(crate) fn cas(&self) -> &CasStore {
        &self.cas
    }

    /// Load the manifest, treating "absent" as an empty scope.
    ///
    /// # Errors
    /// [`StoreError::Corrupt`] when the stored manifest cannot be decoded.
    pub async fn load(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
    ) -> Result<LoadedManifest, StoreError> {
        let key = self.layout.manifest(workspace_id, project_id);
        match self.cas.read(&key).await {
            Ok((bytes, version)) => Ok(LoadedManifest {
                manifest: decode(&bytes, &key)?,
                version: Some(version),
            }),
            Err(StoreError::NotFound) => Ok(LoadedManifest {
                manifest: Manifest::empty(workspace_id.clone(), project_id.clone()),
                version: None,
            }),
            Err(error) => Err(error),
        }
    }

    /// List the live pages of a scope, newest commit first.
    ///
    /// Sorting is by `PageEntry::created_at_ms` — the *latest* commit's
    /// timestamp, so a rewritten page moves to the top — descending, with the
    /// path ascending as a tie-breaker (the private `recency_order`).
    ///
    /// A tombstoned path is absent from `manifest.pages` by construction, so
    /// deleted pages need no extra filter here.
    ///
    /// Only keys that `PagePath::new` reproduces *exactly* are listed. That
    /// filter exists because the normaliser is not idempotent: it trims and
    /// then strips one leading `/`, so `"/ ."` is accepted while the `" ."` it
    /// stores is rejected on the next pass, and `"/ notes/odd.md"` is accepted
    /// while its key re-normalises to `"notes/odd.md"`. The return type can
    /// only carry paths the normaliser accepts, so listing such a key would
    /// hand back a spelling no reader can open — an invented handle. Omitting
    /// it is deliberate; making `PagePath::new` idempotent would fix the root
    /// cause but changes its public semantics, which is out of scope here.
    ///
    /// # Errors
    /// Propagates backend and decode failures from [`Self::load`]. A path that
    /// cannot be represented is skipped, never rendered partially or allowed to
    /// make a whole project unlistable.
    pub async fn recent_pages(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
        limit: usize,
    ) -> Result<Vec<(PagePath, PageEntry)>, StoreError> {
        let loaded = self.load(workspace_id, project_id).await?;
        let mut pages = Vec::with_capacity(loaded.manifest.pages.len());
        for (raw, entry) in loaded.manifest.pages {
            // One key we cannot represent must not take the whole listing down
            // with it: skip it, the way the doc above explains.
            let Ok(path) = PagePath::new(&raw) else {
                continue;
            };
            if path.as_str() != raw {
                continue;
            }
            pages.push((path, entry));
        }
        pages.sort_by(recency_order);
        pages.truncate(limit);
        Ok(pages)
    }

    /// Commit a new page version.
    ///
    /// # Errors
    /// [`StoreError::Conflict`] when every attempt lost the CAS race, plus any
    /// backend or corruption error encountered on the way.
    pub async fn commit_page(
        &self,
        request: CommitPageRequest,
    ) -> Result<CommitOutcome, StoreError> {
        let manifest_key = self
            .layout
            .manifest(&request.workspace_id, &request.project_id);
        let mut attempt = 0u32;

        loop {
            attempt += 1;
            let loaded = self
                .load(&request.workspace_id, &request.project_id)
                .await?;
            let supersedes = loaded.manifest.head_page_id(&request.path);
            let page_id = derive_page_id(
                &request.path,
                &request.title,
                &request.body,
                supersedes.as_ref(),
            );
            let seq = loaded.manifest.next_seq();

            let page = PageVersion {
                page_id: page_id.clone(),
                path: request.path.clone(),
                title: request.title.clone(),
                body: request.body.clone(),
                supersedes: supersedes.clone(),
            };
            let page_key = self.layout.page_version(
                &request.workspace_id,
                &request.project_id,
                &request.path,
                &page_id,
            );
            let page_object_created = self.create_or_verify(&page_key, &page).await?;

            let entry = WalEntry {
                schema: MANIFEST_SCHEMA,
                writer_id: request.writer_id.clone(),
                page_id: page_id.clone(),
                path: request.path.clone(),
                supersedes: supersedes.clone(),
            };
            let wal_key =
                self.layout
                    .wal_entry(&request.workspace_id, &request.project_id, entry.event_id());
            self.create_or_verify(&wal_key, &entry).await?;

            let mut manifest = loaded.manifest;
            manifest.record(
                &request.path,
                PageEntry {
                    page_id: page_id.clone(),
                    seq,
                    created_at_ms: request.now_ms,
                    writer_id: request.writer_id.clone(),
                    title: request.title.clone(),
                    supersedes: supersedes.clone(),
                },
            );
            let bytes = encode(&manifest)?;
            let committed = match loaded.version {
                Some(version) => self.cas.update(&manifest_key, bytes, &version).await,
                None => self.cas.create(&manifest_key, bytes).await,
            };

            match committed {
                Ok(_) => {
                    // Advisory, and deliberately second: the commit is already
                    // real, so a failure here degrades history rather than
                    // correctness.
                    let _ = self
                        .write_commit_record(
                            &request.workspace_id,
                            &request.project_id,
                            CommitRecord {
                                schema: MANIFEST_SCHEMA,
                                seq,
                                at_ms: request.now_ms,
                                writer_id: request.writer_id.clone(),
                                kind: CommitKind::PageWritten,
                                path: request.path.clone(),
                                page_id: Some(page_id.clone()),
                            },
                        )
                        .await;
                    return Ok(CommitOutcome {
                        manifest_seq: seq,
                        page_id,
                        supersedes,
                        attempts: attempt,
                        page_object_created,
                    });
                }
                Err(StoreError::Precondition | StoreError::AlreadyExists) => {
                    if attempt >= self.retry.max_attempts {
                        return Err(StoreError::Conflict { attempts: attempt });
                    }
                    let delay = self.retry.base_delay * attempt;
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Leave a handoff for whoever comes next.
    ///
    /// Re-opening an identical note is a no-op: the id is content-derived, so a
    /// retry cannot leave two copies of the same baton.
    ///
    /// # Errors
    /// Propagates backend failures.
    pub async fn open_handoff(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
        title: &str,
        body: &str,
        created_by: &WriterId,
        now_ms: i64,
    ) -> Result<(Handoff, bool), StoreError> {
        let id = derive_handoff_id(title, body, now_ms);
        let key = self.layout.handoff(workspace_id, project_id, &id);
        let handoff = Handoff {
            schema: MANIFEST_SCHEMA,
            id: id.clone(),
            title: title.to_string(),
            body: body.to_string(),
            created_by: created_by.clone(),
            created_at_ms: now_ms,
            claimed_by: None,
            claimed_at_ms: None,
            finished_at_ms: None,
        };
        match self.create_or_verify(&key, &handoff).await {
            Ok(created) => Ok((handoff, created)),
            Err(error) => Err(error),
        }
    }

    /// Read one handoff.
    ///
    /// # Errors
    /// [`StoreError::NotFound`] when the id does not exist.
    pub async fn read_handoff(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
        id: &str,
    ) -> Result<(Handoff, ObjectVersion), StoreError> {
        let key = self.layout.handoff(workspace_id, project_id, id);
        let (bytes, version) = self.cas.read(&key).await?;
        let handoff: Handoff = decode(&bytes, &key)?;
        if handoff.id != id {
            return Err(StoreError::Corrupt(format!(
                "{key} holds handoff {}",
                handoff.id
            )));
        }
        Ok((handoff, version))
    }

    /// List every handoff of a project, oldest first.
    ///
    /// # Errors
    /// Propagates listing and decode failures.
    pub async fn list_handoffs(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
    ) -> Result<Vec<Handoff>, StoreError> {
        let prefix = self.layout.handoff_prefix(workspace_id, project_id);
        let listed = self.cas.list(&prefix).await?;
        let mut handoffs = Vec::with_capacity(listed.len());
        for (key, _) in listed {
            if !key.ends_with(".json") {
                continue;
            }
            let (bytes, _) = self.cas.read(&key).await?;
            handoffs.push(decode::<Handoff>(&bytes, &key)?);
        }
        handoffs.sort_by(|a, b| {
            a.created_at_ms
                .cmp(&b.created_at_ms)
                .then_with(|| a.id.cmp(&b.id))
        });
        Ok(handoffs)
    }

    /// Everything that changed in this scope since `since_ms`, newest first.
    ///
    /// This is the "what happened while I was away" read. It is assembled from
    /// authoritative objects only — the commit log, session heads and handoff
    /// objects — so it needs no index and no cache, and a machine that has just
    /// started sees the same answer as one that has been running all along.
    ///
    /// `pages` keeps deletions, as [`CommitKind::PageDeleted`]: "this page was
    /// removed" is exactly the kind of thing a new session needs to know, and
    /// dropping it would make a digest launder a deletion into silence.
    ///
    /// Each part is sorted by its own clock, newest first, and any part may be
    /// empty — an empty window is an answer, never an error.
    ///
    /// # Errors
    /// Propagates backend and decode failures.
    pub async fn digest(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
        since_ms: i64,
        limit: usize,
    ) -> Result<Digest, StoreError> {
        // The log is already newest-first by sequence, so filtering to the
        // window and keeping its head is "the latest `limit` records in range"
        // — no second sort, and no chance of the two orders disagreeing.
        let log = self
            .read_commit_log(workspace_id, project_id, usize::MAX)
            .await?;
        let pages: Vec<CommitRecord> = log
            .into_iter()
            .filter(|record| record.at_ms >= since_ms)
            .take(limit)
            .collect();

        let mut sessions = Vec::new();
        for session_id in self.list_sessions(workspace_id, project_id).await? {
            // Listed a moment ago and gone now is a normal race for a
            // snapshot read; skip it rather than failing the whole digest.
            let Some((head, _)) = self
                .load_session_head(workspace_id, project_id, &session_id)
                .await?
            else {
                continue;
            };
            if head.updated_at_ms >= since_ms {
                sessions.push(SessionSummary {
                    session_id,
                    observations: head.count,
                    last_seen_ms: head.updated_at_ms,
                });
            }
        }
        sessions.sort_by(|a, b| {
            b.last_seen_ms
                .cmp(&a.last_seen_ms)
                .then_with(|| a.session_id.cmp(&b.session_id))
        });

        let mut handoffs: Vec<Handoff> = self
            .list_handoffs(workspace_id, project_id)
            .await?
            .into_iter()
            .filter(|handoff| handoff_activity_ms(handoff) >= since_ms)
            .collect();
        handoffs.sort_by(|a, b| {
            handoff_activity_ms(b)
                .cmp(&handoff_activity_ms(a))
                .then_with(|| a.id.cmp(&b.id))
        });

        Ok(Digest {
            pages,
            sessions,
            handoffs,
        })
    }

    /// Claim a handoff. Exactly one machine can win.
    ///
    /// # Errors
    /// [`StoreError::HandoffNotOpen`] when someone already took it or finished
    /// it, plus backend failures.
    pub async fn claim_handoff(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
        id: &str,
        claimant: &WriterId,
        now_ms: i64,
    ) -> Result<Handoff, StoreError> {
        let key = self.layout.handoff(workspace_id, project_id, id);
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let (mut handoff, version) = self.read_handoff(workspace_id, project_id, id).await?;
            if handoff.state() != HandoffState::Open {
                return Err(StoreError::HandoffNotOpen {
                    id: id.to_string(),
                    state: handoff.state(),
                });
            }
            handoff.claimed_by = Some(claimant.clone());
            handoff.claimed_at_ms = Some(now_ms);
            match self.cas.update(&key, encode(&handoff)?, &version).await {
                Ok(_) => return Ok(handoff),
                Err(StoreError::Precondition | StoreError::NotFound) => {
                    // Someone else moved it; re-read and report their win.
                    if attempt >= self.retry.max_attempts {
                        return Err(StoreError::Conflict { attempts: attempt });
                    }
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Finish a handoff, but only the machine that claimed it.
    ///
    /// # Errors
    /// [`StoreError::HandoffOwnedByAnother`] when the caller is not the claimer,
    /// [`StoreError::HandoffNotOpen`] when it was never claimed.
    pub async fn finish_handoff(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
        id: &str,
        owner: &WriterId,
        now_ms: i64,
    ) -> Result<Handoff, StoreError> {
        let key = self.layout.handoff(workspace_id, project_id, id);
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let (mut handoff, version) = self.read_handoff(workspace_id, project_id, id).await?;
            match handoff.claimed_by.as_ref() {
                Some(claimer) if claimer == owner => {}
                Some(claimer) => {
                    return Err(StoreError::HandoffOwnedByAnother {
                        id: id.to_string(),
                        owner: claimer.to_string(),
                    });
                }
                None => {
                    return Err(StoreError::HandoffNotOpen {
                        id: id.to_string(),
                        state: handoff.state(),
                    });
                }
            }
            handoff.finished_at_ms = Some(now_ms);
            match self.cas.update(&key, encode(&handoff)?, &version).await {
                Ok(_) => return Ok(handoff),
                Err(StoreError::Precondition | StoreError::NotFound) => {
                    if attempt >= self.retry.max_attempts {
                        return Err(StoreError::Conflict { attempts: attempt });
                    }
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Stage a proposed edit for approval.
    ///
    /// Re-proposing identical content is a no-op: the id is derived from what
    /// would change, so a retry cannot leave two copies of the same request.
    ///
    /// # Errors
    /// Propagates backend failures.
    pub async fn propose_edit(
        &self,
        request: ProposalRequest,
    ) -> Result<(Proposal, bool), StoreError> {
        let ProposalRequest {
            workspace_id,
            project_id,
            target_path,
            title,
            body,
            rationale,
            created_by,
            now_ms,
        } = request;
        let id = derive_proposal_id(&target_path, &title, &body, &rationale, now_ms);
        let key = self.layout.proposal(&workspace_id, &project_id, &id);
        let proposal = Proposal {
            schema: MANIFEST_SCHEMA,
            id: id.clone(),
            target_path,
            title,
            body,
            rationale,
            created_by,
            created_at_ms: now_ms,
            state: ProposalState::Pending,
            decided_by: None,
            decided_at_ms: None,
            decision_note: None,
            applied_page_id: None,
        };
        let created = self.create_or_verify(&key, &proposal).await?;
        Ok((proposal, created))
    }

    /// Read one proposal.
    ///
    /// # Errors
    /// [`StoreError::NotFound`] when the id is unknown.
    pub async fn read_proposal(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
        id: &str,
    ) -> Result<(Proposal, ObjectVersion), StoreError> {
        let key = self.layout.proposal(workspace_id, project_id, id);
        let (bytes, version) = self.cas.read(&key).await?;
        let proposal: Proposal = decode(&bytes, &key)?;
        if proposal.id != id {
            return Err(StoreError::Corrupt(format!(
                "{key} holds proposal {}",
                proposal.id
            )));
        }
        Ok((proposal, version))
    }

    /// List proposals, oldest first.
    ///
    /// # Errors
    /// Propagates listing and decode failures.
    pub async fn list_proposals(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
    ) -> Result<Vec<Proposal>, StoreError> {
        let prefix = self.layout.proposal_prefix(workspace_id, project_id);
        let listed = self.cas.list(&prefix).await?;
        let mut proposals = Vec::with_capacity(listed.len());
        for (key, _) in listed {
            if !key.ends_with(".json") {
                continue;
            }
            let (bytes, _) = self.cas.read(&key).await?;
            proposals.push(decode::<Proposal>(&bytes, &key)?);
        }
        proposals.sort_by(|a, b| {
            a.created_at_ms
                .cmp(&b.created_at_ms)
                .then_with(|| a.id.cmp(&b.id))
        });
        Ok(proposals)
    }

    /// Approve a proposal and apply it.
    ///
    /// Order matters. The proposal is claimed (`Pending` → `Approved`) *before*
    /// the page is written, so two machines racing to approve cannot both apply
    /// it. The applied page id is recorded afterwards, which means a crash in
    /// between leaves an approved proposal that says "applied, id unknown" —
    /// the decision is never lost, and re-applying is a deliberate act rather
    /// than something a retry does silently.
    ///
    /// # Errors
    /// [`StoreError::HandoffNotOpen`]-style refusals are reported as
    /// [`StoreError::ProposalNotPending`], plus backend failures.
    pub async fn approve_proposal(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
        id: &str,
        decided_by: &WriterId,
        now_ms: i64,
    ) -> Result<(Proposal, u64), StoreError> {
        let key = self.layout.proposal(workspace_id, project_id, id);
        let mut attempt = 0u32;
        let claimed = loop {
            attempt += 1;
            let (mut proposal, version) = self.read_proposal(workspace_id, project_id, id).await?;
            if proposal.state != ProposalState::Pending {
                return Err(StoreError::ProposalNotPending {
                    id: id.to_string(),
                    state: proposal.state,
                });
            }
            proposal.state = ProposalState::Approved;
            proposal.decided_by = Some(decided_by.clone());
            proposal.decided_at_ms = Some(now_ms);
            match self.cas.update(&key, encode(&proposal)?, &version).await {
                Ok(_) => break proposal,
                Err(StoreError::Precondition | StoreError::NotFound) => {
                    if attempt >= self.retry.max_attempts {
                        return Err(StoreError::Conflict { attempts: attempt });
                    }
                    continue;
                }
                Err(error) => return Err(error),
            }
        };

        // Apply the edit through the ordinary page path: it supersedes the
        // current version rather than overwriting it, and it is visible to the
        // same authority checks as any other write.
        let outcome = self
            .commit_page(CommitPageRequest {
                workspace_id: workspace_id.clone(),
                project_id: project_id.clone(),
                path: claimed.target_path.clone(),
                title: claimed.title.clone(),
                body: claimed.body.clone(),
                writer_id: decided_by.clone(),
                now_ms,
            })
            .await?;

        // Best-effort annotation: losing it costs a lookup, not the decision.
        let mut annotated = claimed.clone();
        annotated.applied_page_id = Some(outcome.page_id.clone());
        let _ = match self.read_proposal(workspace_id, project_id, id).await {
            Ok((_, version)) => self.cas.update(&key, encode(&annotated)?, &version).await,
            Err(error) => Err(error),
        };
        Ok((annotated, outcome.manifest_seq))
    }

    /// Reject a proposal; the target path is untouched.
    ///
    /// # Errors
    /// [`StoreError::ProposalNotPending`] when it was already decided.
    pub async fn reject_proposal(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
        id: &str,
        decided_by: &WriterId,
        now_ms: i64,
        note: Option<String>,
    ) -> Result<Proposal, StoreError> {
        let key = self.layout.proposal(workspace_id, project_id, id);
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let (mut proposal, version) = self.read_proposal(workspace_id, project_id, id).await?;
            if proposal.state != ProposalState::Pending {
                return Err(StoreError::ProposalNotPending {
                    id: id.to_string(),
                    state: proposal.state,
                });
            }
            proposal.state = ProposalState::Rejected;
            proposal.decided_by = Some(decided_by.clone());
            proposal.decided_at_ms = Some(now_ms);
            proposal.decision_note = note.clone();
            match self.cas.update(&key, encode(&proposal)?, &version).await {
                Ok(_) => return Ok(proposal),
                Err(StoreError::Precondition | StoreError::NotFound) => {
                    if attempt >= self.retry.max_attempts {
                        return Err(StoreError::Conflict { attempts: attempt });
                    }
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Load a session's head, if the session has ever committed.
    ///
    /// # Errors
    /// [`StoreError::Corrupt`] when the stored head cannot be decoded.
    pub async fn load_session_head(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
        session_id: &SessionId,
    ) -> Result<Option<(SessionHead, ObjectVersion)>, StoreError> {
        let key = self
            .layout
            .session_head(workspace_id, project_id, session_id);
        match self.cas.read(&key).await {
            Ok((bytes, version)) => Ok(Some((decode(&bytes, &key)?, version))),
            Err(StoreError::NotFound) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Append a batch of observations to a session's chain.
    ///
    /// Sessions are their own commit domains: two machines capturing two
    /// sessions never contend, and two machines capturing the *same* session
    /// contend only on that session's head. Re-ingesting the batch the head
    /// already ends with is a no-op, which is what makes a hook that retries
    /// safe.
    ///
    /// # Errors
    /// [`StoreError::Conflict`] when every attempt lost the CAS race.
    pub async fn ingest_observations(
        &self,
        request: IngestObservationsRequest,
    ) -> Result<IngestOutcome, StoreError> {
        let head_key = self.layout.session_head(
            &request.workspace_id,
            &request.project_id,
            &request.session_id,
        );
        // Scrubbing happens here, at the only path into storage, so no caller
        // can persist an unscubbed observation by forgetting to scrub first.
        let observations: Vec<Observation> = request
            .observations
            .iter()
            .cloned()
            .map(Observation::sanitized)
            .collect();
        let mut attempt = 0u32;

        loop {
            attempt += 1;
            let current = self
                .load_session_head(
                    &request.workspace_id,
                    &request.project_id,
                    &request.session_id,
                )
                .await?;

            // Replay guard: if the chain already ends with exactly this batch,
            // there is nothing to commit.
            if let Some((head, _)) = &current {
                let last = self
                    .read_segment(
                        &request.workspace_id,
                        &request.project_id,
                        &request.session_id,
                        &head.segment_id,
                    )
                    .await?;
                if last.observations == observations {
                    return Ok(IngestOutcome {
                        segment_id: String::new(),
                        generation: head.generation,
                        count: head.count,
                        accepted: 0,
                        already_present: true,
                        attempts: attempt,
                    });
                }
            }

            let segment = ObservationSegment {
                schema: MANIFEST_SCHEMA,
                session_id: request.session_id.clone(),
                prev: current.as_ref().map(|(head, _)| head.segment_id.clone()),
                observations: observations.clone(),
            };
            let segment_id = derive_segment_id(&segment);
            let segment_key = self.layout.session_segment(
                &request.workspace_id,
                &request.project_id,
                &request.session_id,
                &segment_id,
            );
            self.create_or_verify(&segment_key, &segment).await?;

            let head = SessionHead {
                schema: MANIFEST_SCHEMA,
                session_id: request.session_id.clone(),
                generation: current.as_ref().map_or(1, |(head, _)| head.generation + 1),
                segment_id: segment_id.clone(),
                count: current.as_ref().map_or(0, |(head, _)| head.count)
                    + observations.len() as u64,
                updated_at_ms: request.now_ms,
            };
            let bytes = encode(&head)?;
            let committed = match &current {
                Some((_, version)) => self.cas.update(&head_key, bytes, version).await,
                None => self.cas.create(&head_key, bytes).await,
            };

            match committed {
                Ok(_) => {
                    return Ok(IngestOutcome {
                        segment_id,
                        generation: head.generation,
                        count: head.count,
                        accepted: observations.len(),
                        already_present: false,
                        attempts: attempt,
                    });
                }
                Err(StoreError::Precondition | StoreError::AlreadyExists) => {
                    if attempt >= self.retry.max_attempts {
                        return Err(StoreError::Conflict { attempts: attempt });
                    }
                    let delay = self.retry.base_delay * attempt;
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Replace a session's whole chain with one segment.
    ///
    /// This is how retention works without breaking the chain: a segment points
    /// at its predecessor, so removing a middle one would break the walk.
    /// Rewriting means the head starts pointing at a new root, and every
    /// superseded segment becomes unreachable — collectable by the ordinary
    /// reachability GC, with no special case.
    ///
    /// Rewriting is idempotent: the same retained set produces the same
    /// content-addressed segment, so a replay is a no-op.
    ///
    /// # Errors
    /// [`StoreError::Conflict`] when every attempt lost the CAS race, plus
    /// backend failures.
    pub async fn rewrite_session(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
        session_id: &SessionId,
        observations: Vec<Observation>,
        now_ms: i64,
    ) -> Result<SessionRewriteOutcome, StoreError> {
        let head_key = self
            .layout
            .session_head(workspace_id, project_id, session_id);
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let current = self
                .load_session_head(workspace_id, project_id, session_id)
                .await?;
            let segment = ObservationSegment {
                schema: MANIFEST_SCHEMA,
                session_id: session_id.clone(),
                prev: None,
                observations: observations.clone(),
            };
            let segment_id = derive_segment_id(&segment);
            if current.as_ref().map(|(head, _)| head.segment_id.clone()) == Some(segment_id.clone())
            {
                return Ok(SessionRewriteOutcome {
                    kept: observations.len(),
                    segments: 1,
                    already_present: true,
                    attempts: attempt,
                });
            }
            let segment_key =
                self.layout
                    .session_segment(workspace_id, project_id, session_id, &segment_id);
            self.create_or_verify(&segment_key, &segment).await?;

            let head = SessionHead {
                schema: MANIFEST_SCHEMA,
                session_id: session_id.clone(),
                generation: current.as_ref().map_or(1, |(head, _)| head.generation + 1),
                segment_id: segment_id.clone(),
                count: observations.len() as u64,
                updated_at_ms: now_ms,
            };
            let bytes = encode(&head)?;
            let committed = match &current {
                Some((_, version)) => self.cas.update(&head_key, bytes, version).await,
                None => self.cas.create(&head_key, bytes).await,
            };
            match committed {
                Ok(_) => {
                    return Ok(SessionRewriteOutcome {
                        kept: observations.len(),
                        segments: 1,
                        already_present: false,
                        attempts: attempt,
                    });
                }
                Err(StoreError::Precondition | StoreError::AlreadyExists) => {
                    if attempt >= self.retry.max_attempts {
                        return Err(StoreError::Conflict { attempts: attempt });
                    }
                    let delay = self.retry.base_delay * attempt;
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Read one segment by id.
    ///
    /// # Errors
    /// [`StoreError::Corrupt`] when the segment does not belong to the session.
    pub async fn read_segment(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
        session_id: &SessionId,
        segment_id: &str,
    ) -> Result<ObservationSegment, StoreError> {
        let key = self
            .layout
            .session_segment(workspace_id, project_id, session_id, segment_id);
        let (bytes, _) = self.cas.read(&key).await?;
        let segment: ObservationSegment = decode(&bytes, &key)?;
        if segment.session_id != *session_id {
            return Err(StoreError::Corrupt(format!(
                "{key} holds a segment for {}",
                segment.session_id
            )));
        }
        if derive_segment_id(&segment) != segment_id {
            return Err(StoreError::Corrupt(format!(
                "{key} does not hash to its key"
            )));
        }
        Ok(segment)
    }

    /// Walk a session's chain, oldest segment first.
    ///
    /// # Errors
    /// [`StoreError::Corrupt`] on a broken or cyclic chain.
    pub async fn read_session_chain(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
        session_id: &SessionId,
    ) -> Result<Vec<ObservationSegment>, StoreError> {
        let Some((head, _)) = self
            .load_session_head(workspace_id, project_id, session_id)
            .await?
        else {
            return Ok(Vec::new());
        };
        let mut newest_first = Vec::new();
        let mut cursor = Some(head.segment_id);
        while let Some(segment_id) = cursor {
            if newest_first.len() >= MAX_HISTORY_DEPTH {
                return Err(StoreError::Corrupt(format!(
                    "session {session_id} chain exceeds {MAX_HISTORY_DEPTH} segments"
                )));
            }
            let segment = self
                .read_segment(workspace_id, project_id, session_id, &segment_id)
                .await?;
            cursor = segment.prev.clone();
            newest_first.push(segment);
        }
        newest_first.reverse();
        Ok(newest_first)
    }

    /// Every observation of a session, oldest first, deduplicated by id.
    ///
    /// Deduplication is why a duplicated batch is merely wasteful rather than
    /// wrong: two segments can carry the same event, and consumers see it once.
    ///
    /// # Errors
    /// Propagates chain read failures.
    pub async fn read_session_observations(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
        session_id: &SessionId,
    ) -> Result<Vec<Observation>, StoreError> {
        let mut seen = std::collections::HashSet::new();
        let mut observations = Vec::new();
        for segment in self
            .read_session_chain(workspace_id, project_id, session_id)
            .await?
        {
            for observation in segment.observations {
                if seen.insert(observation.observation_id.clone()) {
                    observations.push(observation);
                }
            }
        }
        Ok(observations)
    }

    /// Discover the sessions of a project.
    ///
    /// Sessions are found by listing their heads: a session whose head never
    /// committed has no head object, so its orphaned segments stay invisible.
    ///
    /// # Errors
    /// Propagates listing failures.
    pub async fn list_sessions(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
    ) -> Result<Vec<SessionId>, StoreError> {
        let prefix = self.layout.session_prefix(workspace_id, project_id);
        let listed = self.cas.list(&prefix).await?;
        let mut sessions = Vec::new();
        for (key, _) in listed {
            let Some(rest) = key.strip_prefix(&format!("{prefix}/")) else {
                continue;
            };
            let Some(session) = rest.strip_suffix("/head.json") else {
                continue;
            };
            if session.contains('/') {
                continue;
            }
            sessions.push(SessionId::new(session)?);
        }
        sessions.sort();
        sessions.dedup();
        Ok(sessions)
    }

    /// Delete a page by tombstoning it.
    ///
    /// Deletion is a commit like any other: the tombstone enters the manifest
    /// by CAS, and old splits keep their copies forever without ever being
    /// able to answer from them. Already-deleted paths are a no-op.
    ///
    /// # Errors
    /// [`StoreError::Conflict`] when every attempt lost the CAS race.
    pub async fn delete_page(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
        path: &PagePath,
        writer_id: &WriterId,
        now_ms: i64,
    ) -> Result<DeleteOutcome, StoreError> {
        let manifest_key = self.layout.manifest(workspace_id, project_id);
        let mut attempt = 0u32;

        loop {
            attempt += 1;
            let loaded = self.load(workspace_id, project_id).await?;
            if loaded.manifest.is_tombstoned(path) {
                return Ok(DeleteOutcome {
                    manifest_seq: loaded.manifest.seq,
                    removed: None,
                    already_deleted: true,
                    attempts: attempt,
                });
            }

            let removed = loaded.manifest.head_page_id(path);
            let seq = loaded.manifest.next_seq();
            let mut manifest = loaded.manifest;
            manifest.tombstone(
                path,
                Tombstone {
                    seq,
                    deleted_at_ms: now_ms,
                    writer_id: writer_id.clone(),
                    last_page_id: removed.clone(),
                },
            );
            let bytes = encode(&manifest)?;
            let committed = match loaded.version {
                Some(version) => self.cas.update(&manifest_key, bytes, &version).await,
                None => self.cas.create(&manifest_key, bytes).await,
            };

            match committed {
                Ok(_) => {
                    let _ = self
                        .write_commit_record(
                            workspace_id,
                            project_id,
                            CommitRecord {
                                schema: MANIFEST_SCHEMA,
                                seq,
                                at_ms: now_ms,
                                writer_id: writer_id.clone(),
                                kind: CommitKind::PageDeleted,
                                path: path.clone(),
                                page_id: removed.clone(),
                            },
                        )
                        .await;
                    return Ok(DeleteOutcome {
                        manifest_seq: seq,
                        removed,
                        already_deleted: false,
                        attempts: attempt,
                    });
                }
                Err(StoreError::Precondition | StoreError::AlreadyExists) => {
                    if attempt >= self.retry.max_attempts {
                        return Err(StoreError::Conflict { attempts: attempt });
                    }
                    let delay = self.retry.base_delay * attempt;
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Replace the whole catalog with `splits`.
    ///
    /// Used by compaction: the replacement drops superseded and tombstoned
    /// content from the index, while the objects themselves stay in the bucket
    /// for garbage collection.
    ///
    /// # Errors
    /// [`StoreError::Conflict`] when every attempt lost the CAS race.
    pub async fn replace_catalog(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
        splits: Vec<SplitEntry>,
        now_ms: i64,
    ) -> Result<ReplaceCatalogOutcome, StoreError> {
        let head_key = self.layout.catalog_head(workspace_id, project_id);
        let mut attempt = 0u32;

        loop {
            attempt += 1;
            let loaded = self.load_catalog(workspace_id, project_id).await?;
            let catalog = IndexCatalog {
                schema: MANIFEST_SCHEMA,
                generation: loaded.catalog.generation + 1,
                splits: splits.clone(),
                covered_until_ms: loaded.catalog.covered_until_ms.max(now_ms),
            };
            let bytes = encode(&catalog)?;
            let catalog_key =
                self.layout
                    .catalog_version(workspace_id, project_id, &content_hash(&bytes));
            self.create_or_verify(&catalog_key, &catalog).await?;

            let head = CatalogHead {
                schema: MANIFEST_SCHEMA,
                generation: catalog.generation,
                catalog_key: catalog_key.clone(),
                updated_at_ms: now_ms,
            };
            let bytes = encode(&head)?;
            let swapped = match loaded.head_version {
                Some(version) => self.cas.update(&head_key, bytes, &version).await,
                None => self.cas.create(&head_key, bytes).await,
            };

            match swapped {
                Ok(_) => {
                    return Ok(ReplaceCatalogOutcome {
                        generation: catalog.generation,
                        catalog_key,
                        splits: catalog.splits.len(),
                        attempts: attempt,
                    });
                }
                Err(StoreError::Precondition | StoreError::AlreadyExists) => {
                    if attempt >= self.retry.max_attempts {
                        return Err(StoreError::Conflict { attempts: attempt });
                    }
                    let delay = self.retry.base_delay * attempt;
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Take a lease, or return `None` when another machine holds it.
    ///
    /// # Errors
    /// Propagates backend failures (a lost CAS race is not an error; it is
    /// another attempt).
    pub async fn acquire_lease(
        &self,
        scope: &str,
        owner: &WriterId,
        now_ms: i64,
        ttl_ms: i64,
    ) -> Result<Option<LeaseGuard>, StoreError> {
        let key = self.layout.lease(scope);
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let current = match self.cas.read(&key).await {
                Ok((bytes, version)) => {
                    let lease: Lease = decode(&bytes, &key)?;
                    if !lease.is_expired(now_ms) {
                        return Ok(None);
                    }
                    Some((lease, version))
                }
                Err(StoreError::NotFound) => None,
                Err(error) => return Err(error),
            };

            let next = Lease {
                schema: MANIFEST_SCHEMA,
                owner: owner.clone(),
                epoch: current.as_ref().map_or(1, |(lease, _)| lease.epoch + 1),
                acquired_at_ms: now_ms,
                expires_at_ms: now_ms + ttl_ms,
            };
            let bytes = encode(&next)?;
            let taken = match &current {
                Some((_, version)) => self.cas.update(&key, bytes, version).await,
                None => self.cas.create(&key, bytes).await,
            };
            match taken {
                Ok(version) => {
                    // Re-check after the CAS: if the deadline already passed
                    // (a suspended holder), the next acquisition takes over.
                    return Ok(Some(LeaseGuard {
                        key,
                        lease: next,
                        version,
                    }));
                }
                Err(StoreError::Precondition | StoreError::AlreadyExists) => {
                    if attempt >= self.retry.max_attempts {
                        return Ok(None);
                    }
                    let delay = self.retry.base_delay * attempt;
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Load the index catalog, treating "absent" as an empty index.
    ///
    /// # Errors
    /// [`StoreError::Corrupt`] when the head and the catalog disagree.
    pub async fn load_catalog(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
    ) -> Result<LoadedCatalog, StoreError> {
        let head_key = self.layout.catalog_head(workspace_id, project_id);
        let (head, head_version) = match self.cas.read(&head_key).await {
            Ok((bytes, version)) => (decode::<CatalogHead>(&bytes, &head_key)?, Some(version)),
            Err(StoreError::NotFound) => {
                return Ok(LoadedCatalog {
                    catalog: IndexCatalog::empty(),
                    catalog_key: None,
                    head_version: None,
                });
            }
            Err(error) => return Err(error),
        };
        let (bytes, _) = self.cas.read(&head.catalog_key).await?;
        let catalog: IndexCatalog = decode(&bytes, &head.catalog_key)?;
        if catalog.generation != head.generation {
            return Err(StoreError::Corrupt(format!(
                "head generation {} but catalog {} says {}",
                head.generation, head.catalog_key, catalog.generation
            )));
        }
        Ok(LoadedCatalog {
            catalog,
            catalog_key: Some(head.catalog_key),
            head_version,
        })
    }

    /// Publish a split into the project catalog.
    ///
    /// The split's files are expected to be uploaded already: an unpublished
    /// split is invisible, so uploading first is always safe. Publishing the
    /// same `content_hash` twice is a no-op, which makes the whole step
    /// replay-safe.
    ///
    /// # Errors
    /// [`StoreError::Conflict`] when every attempt lost the CAS race.
    pub async fn publish_split(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
        split: SplitEntry,
        now_ms: i64,
    ) -> Result<PublishOutcome, StoreError> {
        let head_key = self.layout.catalog_head(workspace_id, project_id);
        let mut attempt = 0u32;

        loop {
            attempt += 1;
            let loaded = self.load_catalog(workspace_id, project_id).await?;
            if loaded.catalog.contains_hash(&split.content_hash) {
                return Ok(PublishOutcome {
                    generation: loaded.catalog.generation,
                    catalog_key: loaded.catalog_key.unwrap_or_default(),
                    attempts: attempt,
                    already_present: true,
                });
            }

            let mut catalog = loaded.catalog;
            catalog.push_split(split.clone(), now_ms);
            let bytes = encode(&catalog)?;
            let catalog_key =
                self.layout
                    .catalog_version(workspace_id, project_id, &content_hash(&bytes));
            self.create_or_verify(&catalog_key, &catalog).await?;

            let head = CatalogHead {
                schema: MANIFEST_SCHEMA,
                generation: catalog.generation,
                catalog_key: catalog_key.clone(),
                updated_at_ms: now_ms,
            };
            let bytes = encode(&head)?;
            let swapped = match loaded.head_version {
                Some(version) => self.cas.update(&head_key, bytes, &version).await,
                None => self.cas.create(&head_key, bytes).await,
            };

            match swapped {
                Ok(_) => {
                    return Ok(PublishOutcome {
                        generation: catalog.generation,
                        catalog_key,
                        attempts: attempt,
                        already_present: false,
                    });
                }
                Err(StoreError::Precondition | StoreError::AlreadyExists) => {
                    if attempt >= self.retry.max_attempts {
                        return Err(StoreError::Conflict { attempts: attempt });
                    }
                    let delay = self.retry.base_delay * attempt;
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Read the current version of a page, if the manifest names one.
    ///
    /// # Errors
    /// Propagates backend failures and [`StoreError::Corrupt`] on bad content.
    pub async fn read_page(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
        path: &PagePath,
    ) -> Result<Option<PageVersion>, StoreError> {
        let loaded = self.load(workspace_id, project_id).await?;
        let Some(entry) = loaded.manifest.head(path) else {
            return Ok(None);
        };
        let page_id = entry.page_id.clone();
        let key = self
            .layout
            .page_version(workspace_id, project_id, path, &page_id);
        let (bytes, _) = self.cas.read(&key).await?;
        let page: PageVersion = decode(&bytes, &key)?;
        if page.page_id != page_id {
            return Err(StoreError::Corrupt(format!(
                "{key} holds {} but the manifest names {page_id}",
                page.page_id
            )));
        }
        Ok(Some(page))
    }

    /// Read a specific page version without consulting the manifest.
    ///
    /// Used by compaction, which already holds a consistent manifest snapshot
    /// and must not pay a manifest read per page.
    ///
    /// # Errors
    /// [`StoreError::NotFound`] when the version object is missing.
    pub async fn read_page_version(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
        path: &PagePath,
        page_id: &qm_core::PageId,
    ) -> Result<PageVersion, StoreError> {
        let key = self
            .layout
            .page_version(workspace_id, project_id, path, page_id);
        let (bytes, _) = self.cas.read(&key).await?;
        let page: PageVersion = decode(&bytes, &key)?;
        if page.page_id != *page_id {
            return Err(StoreError::Corrupt(format!(
                "{key} holds {} but {page_id} was requested",
                page.page_id
            )));
        }
        Ok(page)
    }

    /// Walk a page's supersession chain, oldest first.
    ///
    /// # Errors
    /// [`StoreError::Corrupt`] when a link is missing or names another version.
    pub async fn page_history(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
        path: &PagePath,
    ) -> Result<Vec<WalEntry>, StoreError> {
        let loaded = self.load(workspace_id, project_id).await?;
        let mut cursor = loaded.manifest.head_page_id(path);
        let mut newest_first = Vec::new();
        while let Some(page_id) = cursor {
            if newest_first.len() >= MAX_HISTORY_DEPTH {
                return Err(StoreError::Corrupt(format!(
                    "supersession chain for {} exceeds {MAX_HISTORY_DEPTH} links",
                    path.as_str()
                )));
            }
            let key = self
                .layout
                .wal_entry(workspace_id, project_id, page_id.as_str());
            let (bytes, _) = self.cas.read(&key).await?;
            let entry: WalEntry = decode(&bytes, &key)?;
            if entry.page_id != page_id {
                return Err(StoreError::Corrupt(format!(
                    "{key} holds {} but the chain expects {page_id}",
                    entry.page_id
                )));
            }
            cursor = entry.supersedes.clone();
            newest_first.push(entry);
        }
        newest_first.reverse();
        Ok(newest_first)
    }

    /// List the projects of a workspace that have ever committed.
    ///
    /// Projects are discovered from their commit points: a project directory
    /// whose manifest never landed is not a project yet.
    ///
    /// # Errors
    /// Propagates listing failures.
    pub async fn list_projects(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<ProjectId>, StoreError> {
        let prefix = format!("{}/ws/{workspace_id}/proj", self.layout.root_prefix());
        let listed = self.cas.list(&prefix).await?;
        let mut projects = Vec::new();
        for (key, _) in listed {
            let Some(rest) = key.strip_prefix(&format!("{prefix}/")) else {
                continue;
            };
            let Some(project) = rest.strip_suffix("/manifest.json") else {
                continue;
            };
            if project.contains('/') || project.is_empty() {
                continue;
            }
            projects.push(ProjectId::new(project)?);
        }
        projects.sort();
        projects.dedup();
        Ok(projects)
    }

    /// Write one advisory commit record. Create-if-absent, so a replay of the
    /// same sequence is a no-op rather than a mismatch.
    async fn write_commit_record(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
        record: CommitRecord,
    ) -> Result<(), StoreError> {
        let key = self
            .layout
            .commit_record(workspace_id, project_id, record.seq);
        self.create_or_verify(&key, &record).await.map(|_| ())
    }

    /// Read the commit log, newest first, up to `limit` records.
    ///
    /// Advisory metadata: entries can be missing (a crash between the commit
    /// and this write), and nothing authoritative depends on them.
    ///
    /// # Errors
    /// Propagates listing and decode failures.
    pub async fn read_commit_log(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
        limit: usize,
    ) -> Result<Vec<CommitRecord>, StoreError> {
        let prefix = self.layout.commit_prefix(workspace_id, project_id);
        let listed = self.cas.list(&prefix).await?;
        let mut records = Vec::with_capacity(listed.len());
        for (key, _) in listed {
            if !key.ends_with(".json") {
                continue;
            }
            let (bytes, _) = self.cas.read(&key).await?;
            records.push(decode::<CommitRecord>(&bytes, &key)?);
        }
        records.sort_by_key(|record| std::cmp::Reverse(record.seq));
        records.truncate(limit);
        Ok(records)
    }

    /// The version of a page that was current at `at_ms`, if any.
    ///
    /// Answers a question history alone cannot: what did this page say on that
    /// date. It reads the commit log backwards from `at_ms`, so a page that was
    /// deleted later still reports what it said before the delete.
    ///
    /// # Errors
    /// Propagates log reads and version reads.
    pub async fn version_at(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
        path: &PagePath,
        at_ms: i64,
    ) -> Result<Option<PageVersion>, StoreError> {
        let records = self
            .read_commit_log(workspace_id, project_id, usize::MAX)
            .await?;
        for record in records {
            if record.path != *path || record.at_ms > at_ms {
                continue;
            }
            return match (record.kind, record.page_id) {
                (CommitKind::PageWritten, Some(page_id)) => Ok(Some(
                    self.read_page_version(workspace_id, project_id, path, &page_id)
                        .await?,
                )),
                // Deleted as of then: the page did not exist.
                _ => Ok(None),
            };
        }
        Ok(None)
    }

    /// Every version of a page, oldest first, bodies included.
    ///
    /// The chain is the history: each version's object stays immutable and the
    /// `supersedes` links order them, so this needs no extra bookkeeping.
    ///
    /// # Errors
    /// [`StoreError::Corrupt`] on a broken chain, plus backend failures.
    pub async fn read_page_versions(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
        path: &PagePath,
    ) -> Result<Vec<PageVersion>, StoreError> {
        let mut versions = Vec::new();
        for entry in self.page_history(workspace_id, project_id, path).await? {
            versions.push(
                self.read_page_version(workspace_id, project_id, path, &entry.page_id)
                    .await?,
            );
        }
        Ok(versions)
    }

    /// Read one historical version by id.
    ///
    /// # Errors
    /// [`StoreError::NotFound`] when that version object is absent.
    pub async fn read_page_version_by_id(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
        path: &PagePath,
        page_id: &qm_core::PageId,
    ) -> Result<PageVersion, StoreError> {
        self.read_page_version(workspace_id, project_id, path, page_id)
            .await
    }

    /// Read every committed WAL record.
    ///
    /// The result is a *set*, ordered deterministically by page id for stable
    /// output: commit ordering lives in the manifest, and per-path ordering in
    /// the `supersedes` chain. Nothing is returned for an attempt that never
    /// won its CAS, because an uncommitted record is simply an unreferenced
    /// object.
    ///
    /// # Errors
    /// Propagates backend failures and [`StoreError::Corrupt`] on bad content.
    pub async fn read_wal(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
    ) -> Result<Vec<WalEntry>, StoreError> {
        let prefix = self.layout.wal_prefix(workspace_id, project_id);
        let listed = self.cas.list(&prefix).await?;
        let mut entries = Vec::with_capacity(listed.len());
        for (key, _) in listed {
            let (bytes, _) = self.cas.read(&key).await?;
            entries.push(decode::<WalEntry>(&bytes, &key)?);
        }
        entries.sort_by(|a, b| a.page_id.cmp(&b.page_id));
        Ok(entries)
    }

    /// Create an immutable object, treating a byte-identical existing object as
    /// success (a replay) and anything else as corruption.
    async fn create_or_verify<T>(&self, key: &str, value: &T) -> Result<bool, StoreError>
    where
        T: serde::Serialize + serde::de::DeserializeOwned + PartialEq,
    {
        match self.cas.create(key, encode(value)?).await {
            Ok(_) => Ok(true),
            Err(StoreError::Precondition | StoreError::AlreadyExists) => {
                let (existing, _) = self.cas.read(key).await?;
                let parsed: T = decode(&existing, key)?;
                if &parsed == value {
                    Ok(false)
                } else {
                    Err(StoreError::Corrupt(format!(
                        "{key} already exists with different content"
                    )))
                }
            }
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use object_store::memory::InMemory;

    use super::*;

    fn ws() -> WorkspaceId {
        WorkspaceId::new("acme").unwrap()
    }

    fn proj() -> ProjectId {
        ProjectId::new("ai-memory").unwrap()
    }

    /// A fresh handle on the same bucket — what another machine looks like.
    fn machine(store: &Arc<dyn ObjectStore>) -> ProjectStore {
        ProjectStore::new(Arc::clone(store), "v1").with_retry(RetryPolicy {
            max_attempts: 500,
            base_delay: Duration::ZERO,
        })
    }

    fn request(writer: &str, path: &str, body: &str, now_ms: i64) -> CommitPageRequest {
        CommitPageRequest {
            workspace_id: ws(),
            project_id: proj(),
            path: PagePath::new(path).unwrap(),
            title: path.to_string(),
            body: body.to_string(),
            writer_id: WriterId::new(writer).unwrap(),
            now_ms,
        }
    }

    #[tokio::test]
    async fn recent_pages_orders_newest_first_and_respects_limit() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = machine(&bucket);

        // Three paths, three distinct commit times, in deliberately unsorted
        // path order so a passing test cannot be explained by the key order.
        for (path, at) in [
            ("notes/middle.md", 2_000),
            ("notes/oldest.md", 1_000),
            ("notes/newest.md", 3_000),
        ] {
            store
                .commit_page(request("mbp-a", path, "body", at))
                .await
                .expect("commit");
        }

        let all = store.recent_pages(&ws(), &proj(), 10).await.unwrap();
        let observed: Vec<&str> = all.iter().map(|(path, _)| path.as_str()).collect();
        assert_eq!(
            observed,
            ["notes/newest.md", "notes/middle.md", "notes/oldest.md"],
            "newest commit must come first"
        );
        assert_eq!(all[0].1.created_at_ms, 3_000);
        assert_eq!(all[0].1.title, "notes/newest.md");

        let top_two = store.recent_pages(&ws(), &proj(), 2).await.unwrap();
        assert_eq!(top_two.len(), 2, "limit must truncate");
        assert_eq!(top_two[0].0.as_str(), "notes/newest.md");
        assert_eq!(top_two[1].0.as_str(), "notes/middle.md");

        // A limit larger than the corpus is not an error.
        assert_eq!(
            store.recent_pages(&ws(), &proj(), 99).await.unwrap().len(),
            3
        );
    }

    #[test]
    fn recency_order_is_total_for_equal_timestamps() {
        let entry = |at_ms: i64, title: &str| PageEntry {
            page_id: qm_core::PageId::new("page-1").unwrap(),
            seq: 1,
            created_at_ms: at_ms,
            writer_id: WriterId::new("mbp-a").unwrap(),
            title: title.to_string(),
            supersedes: None,
        };
        let pair = |path: &str, at_ms: i64| (PagePath::new(path).unwrap(), entry(at_ms, path));

        // Equal timestamps in both argument orders: the comparator must impose
        // a direction rather than answering `Equal` and leaving the result to
        // whatever the caller's sort happens to do with it.
        let (a, b) = (pair("notes/a.md", 5_000), pair("notes/b.md", 5_000));
        assert_eq!(recency_order(&a, &b), Ordering::Less);
        assert_eq!(recency_order(&b, &a), Ordering::Greater);
        assert_eq!(recency_order(&a, &a), Ordering::Equal);

        // A newer commit wins even when its path sorts the other way, so the
        // timestamp outranks the tie-breaker.
        let newer = pair("notes/z.md", 9_000);
        assert_eq!(recency_order(&newer, &a), Ordering::Less);
        assert_eq!(recency_order(&a, &newer), Ordering::Greater);
    }

    #[tokio::test]
    async fn recent_pages_break_ties_on_the_path() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = machine(&bucket);

        // Same millisecond, committed in deliberately unsorted path order, so
        // this pins the *direction* the tie-break has to run (a path-descending
        // tie-break fails it). The comparator test above is what fails when the
        // tie-break is dropped outright.
        for path in ["notes/b.md", "notes/a.md", "notes/c.md"] {
            store
                .commit_page(request("mbp-a", path, "body", 5_000))
                .await
                .expect("commit");
        }

        let observed: Vec<String> = store
            .recent_pages(&ws(), &proj(), 10)
            .await
            .unwrap()
            .into_iter()
            .map(|(path, _)| path.as_str().to_string())
            .collect();
        assert_eq!(observed, ["notes/a.md", "notes/b.md", "notes/c.md"]);
    }

    #[tokio::test]
    async fn recent_pages_never_resurrect_a_deleted_page() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = machine(&bucket);

        for (path, at) in [("notes/keep.md", 1_000), ("notes/drop.md", 2_000)] {
            store
                .commit_page(request("mbp-a", path, "body", at))
                .await
                .expect("commit");
        }
        assert_eq!(
            store.recent_pages(&ws(), &proj(), 10).await.unwrap().len(),
            2
        );

        store
            .delete_page(
                &ws(),
                &proj(),
                &PagePath::new("notes/drop.md").unwrap(),
                &WriterId::new("mbp-a").unwrap(),
                3_000,
            )
            .await
            .expect("delete");

        let observed: Vec<String> = store
            .recent_pages(&ws(), &proj(), 10)
            .await
            .unwrap()
            .into_iter()
            .map(|(path, _)| path.as_str().to_string())
            .collect();
        assert_eq!(
            observed,
            ["notes/keep.md"],
            "a tombstoned page must not list"
        );
    }

    #[tokio::test]
    async fn recent_pages_skip_keys_a_reader_could_never_open() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = machine(&bucket);

        // Two shapes of stored key that `PagePath::new` does not reproduce.
        // `"/ ."` commits under `" ."`, which stops parsing at all; `"/ notes/
        // odd.md"` commits under `" notes/odd.md"`, which parses to a
        // *different* string. Either way the return type cannot carry a path
        // that resolves back to the stored key, so listing it would hand back a
        // handle nobody can open.
        for (spelling, stored) in [("/ .", " ."), ("/ notes/odd.md", " notes/odd.md")] {
            let odd = PagePath::new(spelling).unwrap();
            assert_eq!(odd.as_str(), stored, "{spelling} normalises to {stored}");
            store
                .commit_page(CommitPageRequest {
                    path: odd,
                    ..request("mbp-a", "notes/filler.md", "body", 1_000)
                })
                .await
                .expect("commit");
        }
        store
            .commit_page(request("mbp-a", "notes/keep.md", "body", 2_000))
            .await
            .expect("commit");

        let listed = store.recent_pages(&ws(), &proj(), 10).await.unwrap();
        let shown: Vec<&str> = listed.iter().map(|(path, _)| path.as_str()).collect();
        assert_eq!(
            shown,
            ["notes/keep.md"],
            "an unopenable key must neither break the listing nor appear in it"
        );
    }

    #[tokio::test]
    async fn recent_pages_of_an_empty_scope_is_empty() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = machine(&bucket);

        // No manifest at all: a scope nobody has written to yet.
        assert!(
            store
                .recent_pages(&ws(), &proj(), 10)
                .await
                .unwrap()
                .is_empty()
        );
        // A committed-but-empty manifest, for good measure.
        store
            .commit_page(request("mbp-a", "notes/one.md", "body", 1_000))
            .await
            .unwrap();
        store
            .delete_page(
                &ws(),
                &proj(),
                &PagePath::new("notes/one.md").unwrap(),
                &WriterId::new("mbp-a").unwrap(),
                2_000,
            )
            .await
            .unwrap();
        assert!(
            store
                .recent_pages(&ws(), &proj(), 10)
                .await
                .unwrap()
                .is_empty(),
            "a fully deleted scope is empty, not an error"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_machines_never_lose_a_version() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let machines = [
            machine(&bucket),
            machine(&bucket),
            machine(&bucket),
            machine(&bucket),
        ];

        // Four machines, four paths each, five consecutive versions per path.
        // The shared bucket is what makes them a cluster of peers, not the
        // process: each handle re-reads and re-derives on its own.
        let mut tasks = Vec::new();
        for (machine_index, _) in machines.into_iter().enumerate() {
            for path_index in 0..4 {
                let store = machine(&bucket);
                let writer = format!("mbp-{machine_index}");
                let path = format!("notes/m{machine_index}-{path_index}.md");
                tasks.push(tokio::spawn(async move {
                    let mut ids = Vec::new();
                    for version in 0..5 {
                        let outcome = store
                            .commit_page(request(
                                &writer,
                                &path,
                                &format!("body v{version}"),
                                1_000 + version,
                            ))
                            .await
                            .expect("commit");
                        ids.push(outcome.page_id);
                    }
                    (path, ids)
                }));
            }
        }

        let mut expected_paths = BTreeSet::new();
        let mut chains = Vec::new();
        for task in tasks {
            let (path, ids) = task.await.unwrap();
            expected_paths.insert(path.clone());
            chains.push((path, ids));
        }
        // Deliberately no "a CAS conflict must have happened" assertion.
        // Whether two commits overlap is decided by the scheduler, not by the
        // code under test: on a fast in-memory backend these tasks are often
        // serialised and every commit wins on its first attempt. Such an
        // assertion fails on a fraction of runs for a reason that says nothing
        // about correctness. The guard that makes retrying safe -- a superseded
        // manifest version is refused at the commit point -- is pinned
        // deterministically by
        // `a_stale_manifest_version_is_rejected_at_the_commit_point`; the
        // retry loop itself only runs when the scheduler interleaves. What
        // stays here are the properties that hold however the runtime schedules
        // the tasks: no lost write, a complete chain per path, and a WAL with
        // exactly one record per committed version.

        let store = machine(&bucket);
        let loaded = store.load(&ws(), &proj()).await.unwrap();
        let manifest = loaded.manifest;

        // Every commit consumed exactly one sequence, so nothing was lost.
        assert_eq!(manifest.seq, 16 * 5, "commit sequence lost writes");
        assert_eq!(manifest.pages.len(), expected_paths.len());

        for (path, ids) in chains {
            let page_path = PagePath::new(&path).unwrap();
            let history = store
                .page_history(&ws(), &proj(), &page_path)
                .await
                .unwrap();
            assert_eq!(history.len(), 5, "chain length for {path}");
            // The chain links each version to its predecessor, in order.
            assert_eq!(history[0].supersedes, None);
            for window in history.windows(2) {
                assert_eq!(window[1].supersedes.as_ref(), Some(&window[0].page_id));
            }
            let observed: Vec<_> = history.iter().map(|e| e.page_id.clone()).collect();
            assert_eq!(observed, ids, "committed order for {path}");
            // The head is the newest link and stays readable.
            let head = manifest.head(&page_path).expect("head");
            assert_eq!(head.page_id, *ids.last().unwrap());
            let current = store.read_page(&ws(), &proj(), &page_path).await.unwrap();
            assert_eq!(current.unwrap().body, "body v4");
        }

        // The WAL is the complete record of the same 80 commits: every
        // committed page version appears exactly once, and the sequence the
        // CAS assigned is visible on the manifest heads.
        let wal = store.read_wal(&ws(), &proj()).await.unwrap();
        assert_eq!(wal.len(), 80, "every committed version is in the WAL");
        let unique: BTreeSet<&str> = wal.iter().map(|e| e.page_id.as_str()).collect();
        assert_eq!(unique.len(), 80, "no duplicate WAL records");
        let head_seqs: Vec<u64> = manifest.pages.values().map(|e| e.seq).collect();
        let max_seq = head_seqs.iter().copied().max().unwrap();
        assert_eq!(max_seq, 80, "the last commit is the 80th sequence");
    }

    fn split(writer: &str, seq: u64, hash: &str) -> SplitEntry {
        SplitEntry {
            writer_id: WriterId::new(writer).unwrap(),
            seq,
            prefix: format!("v1/ws/acme/proj/ai-memory/index/splits/{writer}/{seq:010}"),
            doc_count: 1,
            content_hash: hash.to_string(),
        }
    }

    #[tokio::test]
    async fn publishing_the_same_split_twice_is_a_no_op() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = machine(&bucket);
        let first = store
            .publish_split(&ws(), &proj(), split("mbp-1", 1, "h1"), 10)
            .await
            .unwrap();
        assert_eq!(first.generation, 1);
        assert!(!first.already_present);

        let again = store
            .publish_split(&ws(), &proj(), split("mbp-1", 1, "h1"), 20)
            .await
            .unwrap();
        assert!(again.already_present, "republishing must not advance");
        assert_eq!(again.generation, 1);
        assert_eq!(again.catalog_key, first.catalog_key);

        let loaded = store.load_catalog(&ws(), &proj()).await.unwrap();
        assert_eq!(loaded.catalog.splits.len(), 1);
        assert_eq!(loaded.catalog.generation, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_publishes_keep_every_split() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut tasks = Vec::new();
        for index in 0..8u64 {
            let store = machine(&bucket);
            tasks.push(tokio::spawn(async move {
                store
                    .publish_split(
                        &ws(),
                        &proj(),
                        split("mbp-1", index + 1, &format!("h{index}")),
                        index as i64,
                    )
                    .await
                    .expect("publish")
            }));
        }
        // Every publish must land, and the head must have advanced once per
        // publish. There is deliberately no assertion that a head CAS conflict
        // was observed: eight publishes are frequently serialised by the
        // scheduler, so "saw more than eight attempts" is a coin flip, not a
        // property of the code. What is pinned deterministically is the *guard*
        // that makes retrying safe -- a superseded version is refused at a
        // commit point -- by
        // `a_stale_manifest_version_is_rejected_at_the_commit_point`, which
        // exercises the manifest commit point; this test's contention is on the
        // catalog head, which uses the same CAS primitive. The retry loop
        // itself is only covered when the scheduler actually interleaves.
        for task in tasks {
            task.await.unwrap();
        }

        let store = machine(&bucket);
        let loaded = store.load_catalog(&ws(), &proj()).await.unwrap();
        assert_eq!(loaded.catalog.splits.len(), 8, "a publish was lost");
        assert_eq!(loaded.catalog.generation, 8);
        let hashes: BTreeSet<&str> = loaded
            .catalog
            .splits
            .iter()
            .map(|s| s.content_hash.as_str())
            .collect();
        assert_eq!(hashes.len(), 8);
    }

    /// The property the concurrency tests above depend on, stated without a
    /// race: a manifest version that has been superseded must not be accepted
    /// at the commit point.
    ///
    /// This is what makes retrying safe rather than merely optimistic. It is
    /// asserted directly -- read a version, let another machine move the commit
    /// point on, then CAS the stale version -- because driving the same code
    /// through concurrent commits only *sometimes* produces the overlap, and a
    /// test that proves the guard only when the scheduler cooperates is not a
    /// guard.
    #[tokio::test]
    async fn a_stale_manifest_version_is_rejected_at_the_commit_point() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let manifest_key = machine(&bucket).layout().manifest(&ws(), &proj());

        // Machine A commits, then reads the version it committed at.
        let machine_a = machine(&bucket);
        machine_a
            .commit_page(request("mbp-a", "notes/one.md", "body a", 1_000))
            .await
            .unwrap();
        let stale = machine_a
            .load(&ws(), &proj())
            .await
            .unwrap()
            .version
            .expect("a commit must leave a manifest behind");

        // Machine B commits against the same scope, moving the commit point on.
        machine(&bucket)
            .commit_page(request("mbp-b", "notes/two.md", "body b", 2_000))
            .await
            .unwrap();

        // A's version now describes the past. Offering it back to the commit
        // point must be refused, not silently accepted as a lost update.
        let refused = machine_a
            .cas()
            .update(
                &manifest_key,
                encode(&Manifest::empty(ws(), proj())).unwrap(),
                &stale,
            )
            .await;
        assert!(
            matches!(refused, Err(StoreError::Precondition)),
            "a superseded manifest version must be refused, got {refused:?}"
        );

        // The refusal left the commit point exactly where machine B put it.
        let loaded = machine_a.load(&ws(), &proj()).await.unwrap();
        assert_eq!(loaded.manifest.seq, 2, "the refused write must not commit");
        assert_eq!(
            loaded.manifest.pages.len(),
            2,
            "both committed pages survive the refused write"
        );
    }

    fn observation(session: &str, text: &str, at: i64) -> Observation {
        let session_id = SessionId::new(session).unwrap();
        Observation {
            schema: MANIFEST_SCHEMA,
            observation_id: qm_core::derive_observation_id(
                &session_id,
                "codex",
                "tool_use",
                text,
                at,
            ),
            session_id,
            actor: "codex".into(),
            kind: "tool_use".into(),
            text: text.into(),
            created_at_ms: at,
        }
    }

    fn ingest(session: &str, texts: &[&str], now_ms: i64) -> IngestObservationsRequest {
        IngestObservationsRequest {
            workspace_id: ws(),
            project_id: proj(),
            session_id: SessionId::new(session).unwrap(),
            writer_id: WriterId::new("mbp-1").unwrap(),
            observations: texts
                .iter()
                .enumerate()
                .map(|(index, text)| observation(session, text, now_ms + index as i64))
                .collect(),
            now_ms,
        }
    }

    #[tokio::test]
    async fn observation_capture_builds_a_visible_chain_and_tolerates_replays() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = machine(&bucket);
        let session = SessionId::new("sess-1").unwrap();

        for (index, texts) in [["first", "second"], ["third", "fourth"], ["fifth", "sixth"]]
            .iter()
            .enumerate()
        {
            let outcome = store
                .ingest_observations(ingest("sess-1", texts, 10 + index as i64))
                .await
                .unwrap();
            assert!(!outcome.already_present);
            assert_eq!(outcome.generation, index as u64 + 1);
            assert_eq!(outcome.count, (index as u64 + 1) * 2);
            assert_eq!(outcome.accepted, 2);
        }

        let (head, _) = store
            .load_session_head(&ws(), &proj(), &session)
            .await
            .unwrap()
            .expect("head");
        assert_eq!(head.generation, 3);
        assert_eq!(head.count, 6);

        let observations = store
            .read_session_observations(&ws(), &proj(), &session)
            .await
            .unwrap();
        let texts: Vec<&str> = observations.iter().map(|o| o.text.as_str()).collect();
        assert_eq!(
            texts,
            vec!["first", "second", "third", "fourth", "fifth", "sixth"]
        );

        // Replaying the last batch must not append anything.
        let replay = store
            .ingest_observations(ingest("sess-1", &["fifth", "sixth"], 12))
            .await
            .unwrap();
        assert!(replay.already_present, "a retried batch must be a no-op");
        assert_eq!(replay.count, 6);
        let (head, _) = store
            .load_session_head(&ws(), &proj(), &session)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(head.generation, 3, "a replay must not add a segment");
    }

    #[test]
    fn retention_keeps_recent_observations_and_always_the_newest() {
        let session = SessionId::new("sess-retain").unwrap();
        let observations: Vec<Observation> = (0..10)
            .map(|index| observation("sess-retain", &format!("event {index}"), index as i64))
            .collect();
        let _ = session;

        // Age alone: only the newest two are inside the window.
        let plan = plan_retention(&observations, 1, 0, 9);
        assert_eq!(plan.keep.len(), 2);
        assert_eq!(plan.dropped, 8);
        assert_eq!(plan.keep[0].text, "event 8");

        // Count alone: a wrong clock cannot drop everything recent.
        let plan = plan_retention(&observations, 0, 3, 1_000_000);
        assert_eq!(plan.keep.len(), 3);
        assert_eq!(plan.keep[0].text, "event 7");

        // Both rules union, never intersect.
        let plan = plan_retention(&observations, 0, 3, 9);
        assert_eq!(plan.keep.len(), 3);
    }

    #[tokio::test]
    async fn rewriting_a_session_retires_old_observations_and_frees_segments() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = machine(&bucket);
        let session = SessionId::new("sess-retain").unwrap();

        for batch in 0..3 {
            let texts: Vec<String> = (0..2)
                .map(|index| format!("event {}", batch * 2 + index))
                .collect();
            let refs: Vec<&str> = texts.iter().map(String::as_str).collect();
            store
                .ingest_observations(IngestObservationsRequest {
                    workspace_id: ws(),
                    project_id: proj(),
                    session_id: session.clone(),
                    writer_id: WriterId::new("mbp-1").unwrap(),
                    observations: refs
                        .iter()
                        .enumerate()
                        .map(|(index, text)| {
                            observation("sess-retain", text, (batch * 2 + index) as i64)
                        })
                        .collect(),
                    now_ms: batch as i64,
                })
                .await
                .unwrap();
        }
        assert_eq!(
            store
                .read_session_chain(&ws(), &proj(), &session)
                .await
                .unwrap()
                .len(),
            3
        );

        // Keep only the newest two: the rest retire.
        let observations = store
            .read_session_observations(&ws(), &proj(), &session)
            .await
            .unwrap();
        let plan = plan_retention(&observations, 0, 2, 10);
        assert_eq!(plan.dropped, 4);
        let outcome = store
            .rewrite_session(&ws(), &proj(), &session, plan.keep.clone(), 10)
            .await
            .unwrap();
        assert!(!outcome.already_present);
        assert_eq!(outcome.kept, 2);

        // The chain is now a single root segment…
        let chain = store
            .read_session_chain(&ws(), &proj(), &session)
            .await
            .unwrap();
        assert_eq!(chain.len(), 1);
        assert!(chain[0].prev.is_none());
        // …and the surviving observations read back in order.
        let kept = store
            .read_session_observations(&ws(), &proj(), &session)
            .await
            .unwrap();
        assert_eq!(kept.len(), 2);
        assert_eq!(kept[0].text, "event 4");
        assert_eq!(kept[1].text, "event 5");

        // A replay of the same rewrite is a no-op.
        let again = store
            .rewrite_session(&ws(), &proj(), &session, plan.keep, 11)
            .await
            .unwrap();
        assert!(again.already_present);

        // The retired segments are unreachable, so the ordinary GC collects
        // them — no session-specific cleanup path exists.
        let before = store
            .cas()
            .list("v1/ws/acme/proj/ai-memory/sessions")
            .await
            .unwrap()
            .len();
        let collected = store
            .gc_orphans(&ws(), &proj(), 1_000, 0, true)
            .await
            .unwrap();
        let after = store
            .cas()
            .list("v1/ws/acme/proj/ai-memory/sessions")
            .await
            .unwrap()
            .len();
        assert!(
            collected.deleted >= 2,
            "retired segments must be collectable: {collected:?}"
        );
        assert!(after < before, "the session should shrink on disk");
    }

    #[tokio::test]
    async fn two_machines_capturing_one_session_keep_both_batches() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let first = machine(&bucket);
        let second = machine(&bucket);
        let session = SessionId::new("sess-shared").unwrap();

        let a = {
            let store = machine(&bucket);
            tokio::spawn(async move {
                store
                    .ingest_observations(ingest("sess-shared", &["from-a"], 100))
                    .await
                    .unwrap()
            })
        };
        let b = {
            let store = machine(&bucket);
            tokio::spawn(async move {
                store
                    .ingest_observations(ingest("sess-shared", &["from-b"], 101))
                    .await
                    .unwrap()
            })
        };
        a.await.unwrap();
        b.await.unwrap();

        let (head, _) = second
            .load_session_head(&ws(), &proj(), &session)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(head.count, 2, "both batches must land");
        assert_eq!(head.generation, 2, "one segment per batch");
        let observations = first
            .read_session_observations(&ws(), &proj(), &session)
            .await
            .unwrap();
        assert_eq!(observations.len(), 2);
        let texts: BTreeSet<&str> = observations.iter().map(|o| o.text.as_str()).collect();
        assert_eq!(texts, BTreeSet::from(["from-a", "from-b"]));
    }

    #[tokio::test]
    async fn scrubbing_happens_inside_the_ingest_boundary() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = machine(&bucket);
        let session = SessionId::new("sess-secret").unwrap();
        let mut request = ingest(
            "sess-secret",
            &["export api_key=abcd1234 before the run"],
            5,
        );
        // Even a caller that hands over raw text cannot persist a secret.
        request.observations[0].text = "export api_key=abcd1234 before the run".into();
        store.ingest_observations(request).await.unwrap();

        let observations = store
            .read_session_observations(&ws(), &proj(), &session)
            .await
            .unwrap();
        assert_eq!(observations.len(), 1);
        assert!(
            !observations[0].text.contains("abcd1234"),
            "{}",
            observations[0].text
        );
        assert!(observations[0].text.contains("[REDACTED]"));
        // The stored id must match the stored bytes.
        assert_eq!(
            observations[0].observation_id,
            qm_core::derive_observation_id(
                &session,
                &observations[0].actor,
                &observations[0].kind,
                &observations[0].text,
                observations[0].created_at_ms,
            )
        );
    }

    #[tokio::test]
    async fn sessions_are_independent_commit_domains() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = machine(&bucket);
        for session in ["sess-a", "sess-b", "sess-c"] {
            store
                .ingest_observations(ingest(session, &["one"], 1))
                .await
                .unwrap();
        }
        let sessions = store.list_sessions(&ws(), &proj()).await.unwrap();
        let names: Vec<&str> = sessions.iter().map(|s| s.as_str()).collect();
        assert_eq!(names, vec!["sess-a", "sess-b", "sess-c"]);

        // A session that never committed a head has no visible segment.
        let orphan = store
            .cas
            .create(
                "v1/ws/acme/proj/ai-memory/sessions/sess-orphan/segments/deadbeef.json",
                bytes::Bytes::from_static(b"{}"),
            )
            .await;
        assert!(orphan.is_ok());
        let sessions = store.list_sessions(&ws(), &proj()).await.unwrap();
        assert!(
            !sessions.iter().any(|s| s.as_str() == "sess-orphan"),
            "a session without a committed head must stay invisible"
        );
    }

    #[tokio::test]
    async fn a_proposal_changes_nothing_until_it_is_approved() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = machine(&bucket);
        let path = PagePath::new("notes/raft.md").unwrap();
        let curator = WriterId::new("curator").unwrap();

        // The page exists in its original form.
        store
            .commit_page(request("mbp-a", "notes/raft.md", "original body", 1))
            .await
            .unwrap();

        let (proposal, created) = store
            .propose_edit(ProposalRequest {
                workspace_id: ws(),
                project_id: proj(),
                target_path: path.clone(),
                title: "Raft".into(),
                body: "rewritten body".into(),
                rationale: "the curator thinks this is clearer".into(),
                created_by: curator.clone(),
                now_ms: 10,
            })
            .await
            .unwrap();
        assert!(created);
        assert_eq!(proposal.state, ProposalState::Pending);

        // Proposing the same edit twice is a no-op.
        let (same, created) = store
            .propose_edit(ProposalRequest {
                workspace_id: ws(),
                project_id: proj(),
                target_path: path.clone(),
                title: "Raft".into(),
                body: "rewritten body".into(),
                rationale: "the curator thinks this is clearer".into(),
                created_by: curator.clone(),
                now_ms: 10,
            })
            .await
            .unwrap();
        assert!(!created);
        assert_eq!(same.id, proposal.id);
        assert_eq!(store.list_proposals(&ws(), &proj()).await.unwrap().len(), 1);

        // The proposal has changed nothing yet.
        let current = store
            .read_page(&ws(), &proj(), &path)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.body, "original body");

        // Two machines race to approve: exactly one applies the edit.
        let approver_a = WriterId::new("human-a").unwrap();
        let approver_b = WriterId::new("human-b").unwrap();
        let store_a = machine(&bucket);
        let store_b = machine(&bucket);
        let id = proposal.id.clone();
        let (workspace, project_id) = (ws(), proj());
        let (a, b) = tokio::join!(
            store_a.approve_proposal(&workspace, &project_id, &id, &approver_a, 20),
            store_b.approve_proposal(&workspace, &project_id, &id, &approver_b, 21),
        );
        assert_eq!(
            [a.is_ok(), b.is_ok()].iter().filter(|ok| **ok).count(),
            1,
            "a proposal must be decided exactly once: {a:?} {b:?}"
        );
        let loser = if a.is_err() { a } else { b };
        assert!(
            matches!(loser, Err(StoreError::ProposalNotPending { .. })),
            "{loser:?}"
        );

        // The edit is live, and the proposal records how it ended.
        let applied = store
            .read_page(&ws(), &proj(), &path)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(applied.body, "rewritten body");
        let (decided, _) = store.read_proposal(&ws(), &proj(), &id).await.unwrap();
        assert_eq!(decided.state, ProposalState::Approved);
        assert_eq!(decided.applied_page_id.as_ref(), Some(&applied.page_id));
        assert!(decided.decided_by.is_some());

        // A decided proposal cannot be rejected afterwards.
        let too_late = store
            .reject_proposal(&ws(), &proj(), &id, &approver_a, 30, None)
            .await;
        assert!(
            matches!(too_late, Err(StoreError::ProposalNotPending { .. })),
            "{too_late:?}"
        );
    }

    #[tokio::test]
    async fn rejecting_a_proposal_leaves_the_page_alone() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = machine(&bucket);
        let path = PagePath::new("notes/raft.md").unwrap();
        store
            .commit_page(request("mbp-a", "notes/raft.md", "original body", 1))
            .await
            .unwrap();
        let (proposal, _) = store
            .propose_edit(ProposalRequest {
                workspace_id: ws(),
                project_id: proj(),
                target_path: path.clone(),
                title: "Raft".into(),
                body: "rewritten body".into(),
                rationale: "bad idea".into(),
                created_by: WriterId::new("curator").unwrap(),
                now_ms: 10,
            })
            .await
            .unwrap();

        let rejected = store
            .reject_proposal(
                &ws(),
                &proj(),
                &proposal.id,
                &WriterId::new("human").unwrap(),
                20,
                Some("not this time".into()),
            )
            .await
            .unwrap();
        assert_eq!(rejected.state, ProposalState::Rejected);
        assert_eq!(rejected.decision_note.as_deref(), Some("not this time"));
        assert!(rejected.applied_page_id.is_none());

        let current = store
            .read_page(&ws(), &proj(), &path)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            current.body, "original body",
            "a rejected proposal must not touch the page"
        );
        let manifest = store.load(&ws(), &proj()).await.unwrap().manifest;
        assert_eq!(manifest.seq, 1, "only the original commit happened");
    }

    #[tokio::test]
    async fn a_handoff_is_claimed_exactly_once() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let opener = machine(&bucket);
        let writer = WriterId::new("mbp-a").unwrap();

        let (handoff, created) = opener
            .open_handoff(
                &ws(),
                &proj(),
                "finish the index rebuild",
                "tantivy splits are published; compaction is pending",
                &writer,
                10,
            )
            .await
            .unwrap();
        assert!(created);

        // Re-opening the same note is a no-op, not a second baton.
        let (same, created) = opener
            .open_handoff(
                &ws(),
                &proj(),
                "finish the index rebuild",
                "tantivy splits are published; compaction is pending",
                &writer,
                10,
            )
            .await
            .unwrap();
        assert!(!created);
        assert_eq!(same.id, handoff.id);
        assert_eq!(opener.list_handoffs(&ws(), &proj()).await.unwrap().len(), 1);

        // Two machines race for it: exactly one wins.
        let racer_a = machine(&bucket);
        let racer_b = machine(&bucket);
        let owner_a = WriterId::new("mbp-a").unwrap();
        let owner_b = WriterId::new("mbp-b").unwrap();
        let id = handoff.id.clone();
        let (workspace, project_id) = (ws(), proj());
        let (a, b) = tokio::join!(
            racer_a.claim_handoff(&workspace, &project_id, &id, &owner_a, 20),
            racer_b.claim_handoff(&workspace, &project_id, &id, &owner_b, 21),
        );
        let winners = [a.is_ok(), b.is_ok()].iter().filter(|ok| **ok).count();
        assert_eq!(
            winners, 1,
            "a handoff must have exactly one claimer: {a:?} {b:?}"
        );
        let loser = if a.is_err() { a } else { b };
        assert!(
            matches!(loser, Err(StoreError::HandoffNotOpen { .. })),
            "the loser must be told it is not open: {loser:?}"
        );

        // Only the claimer can finish it.
        let (claimed, _) = racer_a.read_handoff(&ws(), &proj(), &id).await.unwrap();
        let claimer = claimed.claimed_by.clone().unwrap();
        let intruder = if claimer == owner_a {
            &owner_b
        } else {
            &owner_a
        };
        let refused = racer_b
            .finish_handoff(&ws(), &proj(), &id, intruder, 30)
            .await;
        assert!(
            matches!(refused, Err(StoreError::HandoffOwnedByAnother { .. })),
            "{refused:?}"
        );

        let finished = opener
            .finish_handoff(&ws(), &proj(), &id, &claimer, 40)
            .await
            .unwrap();
        assert_eq!(finished.state(), HandoffState::Done);

        // A finished handoff cannot be claimed again.
        let too_late = opener
            .claim_handoff(&ws(), &proj(), &id, &owner_a, 50)
            .await;
        assert!(
            matches!(too_late, Err(StoreError::HandoffNotOpen { .. })),
            "{too_late:?}"
        );
    }

    #[tokio::test]
    async fn the_commit_log_answers_when_and_as_of() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = machine(&bucket);
        let path = PagePath::new("notes/raft.md").unwrap();
        let writer = WriterId::new("mbp-a").unwrap();

        for (version, body) in [(1_000, "first"), (2_000, "second"), (3_000, "third")] {
            store
                .commit_page(request("mbp-a", "notes/raft.md", body, version))
                .await
                .unwrap();
        }

        // The log is newest-first and carries the commit-time facts the
        // immutable version objects deliberately cannot hold.
        let log = store.read_commit_log(&ws(), &proj(), 10).await.unwrap();
        assert_eq!(log.len(), 3);
        assert_eq!(log[0].seq, 3);
        assert_eq!(log[0].at_ms, 3_000);
        assert_eq!(log[0].kind, CommitKind::PageWritten);
        assert!(log[0].page_id.is_some());

        // As-of reads walk back to whatever was current then.
        let at_two = store
            .version_at(&ws(), &proj(), &path, 2_000)
            .await
            .unwrap()
            .expect("a version existed then");
        assert_eq!(at_two.body, "second");
        let at_one = store
            .version_at(&ws(), &proj(), &path, 1_500)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(at_one.body, "first");
        assert!(
            store
                .version_at(&ws(), &proj(), &path, 999)
                .await
                .unwrap()
                .is_none(),
            "nothing existed before the first commit"
        );

        // A later delete does not rewrite the past.
        store
            .delete_page(&ws(), &proj(), &path, &writer, 4_000)
            .await
            .unwrap();
        assert!(
            store
                .version_at(&ws(), &proj(), &path, 5_000)
                .await
                .unwrap()
                .is_none(),
            "as of now the page is gone"
        );
        let before_delete = store
            .version_at(&ws(), &proj(), &path, 3_500)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(before_delete.body, "third");
        let log = store.read_commit_log(&ws(), &proj(), 10).await.unwrap();
        assert_eq!(log[0].kind, CommitKind::PageDeleted);
        assert_eq!(log[0].page_id, Some(before_delete.page_id));
    }

    #[tokio::test]
    async fn deleting_a_page_is_idempotent_and_refuses_to_resurrect_it() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = machine(&bucket);
        let path = PagePath::new("notes/raft.md").unwrap();
        let writer = WriterId::new("mbp-1").unwrap();

        let committed = store
            .commit_page(request("mbp-1", "notes/raft.md", "body", 1))
            .await
            .unwrap();
        let deleted = store
            .delete_page(&ws(), &proj(), &path, &writer, 2)
            .await
            .unwrap();
        assert!(!deleted.already_deleted);
        assert_eq!(deleted.removed.as_ref(), Some(&committed.page_id));
        assert_eq!(deleted.manifest_seq, 2);

        let loaded = store.load(&ws(), &proj()).await.unwrap();
        assert!(loaded.manifest.pages.is_empty());
        assert!(loaded.manifest.is_tombstoned(&path));
        assert_eq!(
            loaded
                .manifest
                .tombstones
                .get(path.as_str())
                .map(|t| t.last_page_id.clone()),
            Some(Some(committed.page_id.clone()))
        );
        assert!(
            store
                .read_page(&ws(), &proj(), &path)
                .await
                .unwrap()
                .is_none()
        );

        // Deleting again changes nothing.
        let again = store
            .delete_page(&ws(), &proj(), &path, &writer, 3)
            .await
            .unwrap();
        assert!(again.already_deleted);
        assert_eq!(again.manifest_seq, 2, "a no-op must not consume a sequence");

        // Writing the path again is the newer truth and clears the tombstone.
        let rewritten = store
            .commit_page(request("mbp-1", "notes/raft.md", "body v2", 4))
            .await
            .unwrap();
        assert_eq!(rewritten.manifest_seq, 3);
        let loaded = store.load(&ws(), &proj()).await.unwrap();
        assert!(!loaded.manifest.is_tombstoned(&path));
        assert!(
            loaded
                .manifest
                .is_current(path.as_str(), rewritten.page_id.as_str())
        );
    }

    #[tokio::test]
    async fn only_one_machine_holds_a_lease_at_a_time() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let first = machine(&bucket);
        let second = machine(&bucket);
        let owner_a = WriterId::new("mbp-a").unwrap();
        let owner_b = WriterId::new("mbp-b").unwrap();
        let scope = "compact/acme/ai-memory";

        let mut held = first
            .acquire_lease(scope, &owner_a, 1_000, 60_000)
            .await
            .unwrap()
            .expect("first acquisition wins");
        assert_eq!(held.lease.epoch, 1);
        assert_eq!(held.lease.owner, owner_a);

        // Still inside the TTL: the second machine must back off.
        assert!(
            second
                .acquire_lease(scope, &owner_b, 2_000, 60_000)
                .await
                .unwrap()
                .is_none(),
            "a live lease must not be taken over"
        );

        // The holder is gone past its deadline: takeover bumps the epoch.
        let taken = second
            .acquire_lease(scope, &owner_b, 100_000, 60_000)
            .await
            .unwrap()
            .expect("an expired lease is free");
        assert_eq!(taken.lease.epoch, 2);
        assert_eq!(taken.lease.owner, owner_b);

        // The old holder's renewal must fail: its version is stale.
        assert!(
            !held.renew(&first, 100_001, 60_000).await.unwrap(),
            "the previous holder must not be able to renew"
        );

        // Releasing hands the lease over immediately.
        taken.release(&second, 100_002).await.unwrap();
        let reacquired = first
            .acquire_lease(scope, &owner_a, 100_003, 60_000)
            .await
            .unwrap()
            .expect("a released lease is free");
        assert_eq!(reacquired.lease.epoch, 3);
    }

    #[tokio::test]
    async fn a_machine_that_never_saw_the_state_can_continue() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let first = machine(&bucket);
        for version in 0..3 {
            first
                .commit_page(request(
                    "mbp-1",
                    "notes/raft.md",
                    &format!("body v{version}"),
                    version,
                ))
                .await
                .unwrap();
        }
        drop(first); // the machine goes away; no coordination, nothing to release

        let second = machine(&bucket);
        let loaded = second.load(&ws(), &proj()).await.unwrap();
        assert_eq!(loaded.manifest.seq, 3);
        let current = second
            .read_page(&ws(), &proj(), &PagePath::new("notes/raft.md").unwrap())
            .await
            .unwrap()
            .expect("page readable by a machine that never wrote it");
        assert_eq!(current.body, "body v2");

        let outcome = second
            .commit_page(request("mbp-2", "notes/raft.md", "body v3", 100))
            .await
            .unwrap();
        assert_eq!(outcome.manifest_seq, 4);
        assert_eq!(outcome.supersedes.as_ref(), Some(&current.page_id));

        let history = second
            .page_history(&ws(), &proj(), &PagePath::new("notes/raft.md").unwrap())
            .await
            .unwrap();
        assert_eq!(history.len(), 4);
        assert_eq!(history[0].supersedes, None);
        for window in history.windows(2) {
            assert_eq!(window[1].supersedes.as_ref(), Some(&window[0].page_id));
        }
    }

    #[tokio::test]
    async fn a_replayed_commit_reuses_its_object_instead_of_failing() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = machine(&bucket);
        let path = PagePath::new("notes/raft.md").unwrap();

        // Simulate a crash between uploading the page object and committing:
        // the object is on the bucket, the manifest does not know about it.
        let page_id = derive_page_id(&path, "notes/raft.md", "body", None);
        let orphan = PageVersion {
            page_id: page_id.clone(),
            path: path.clone(),
            title: "notes/raft.md".into(),
            body: "body".into(),
            supersedes: None,
        };
        let key = store.layout().page_version(&ws(), &proj(), &path, &page_id);
        store
            .cas
            .create(&key, encode(&orphan).unwrap())
            .await
            .unwrap();

        let outcome = store
            .commit_page(request("mbp-1", "notes/raft.md", "body", 1))
            .await
            .unwrap();
        assert_eq!(outcome.page_id, page_id);
        assert!(
            !outcome.page_object_created,
            "the replay must reuse the orphan object"
        );
        assert_eq!(outcome.manifest_seq, 1);
    }

    #[tokio::test]
    async fn a_divergent_object_at_the_same_key_is_reported() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = machine(&bucket);
        let path = PagePath::new("notes/raft.md").unwrap();
        let page_id = derive_page_id(&path, "notes/raft.md", "body", None);
        let key = store.layout().page_version(&ws(), &proj(), &path, &page_id);
        let divergent = PageVersion {
            page_id: page_id.clone(),
            path: path.clone(),
            title: "notes/raft.md".into(),
            body: "something else".into(),
            supersedes: None,
        };
        store
            .cas
            .create(&key, encode(&divergent).unwrap())
            .await
            .unwrap();

        let error = store
            .commit_page(request("mbp-1", "notes/raft.md", "body", 1))
            .await
            .expect_err("content addressing must fail closed");
        assert!(matches!(error, StoreError::Corrupt(_)), "{error:?}");
    }

    /// A digest answers "what changed recently", so the window and the cap are
    /// its whole contract: everything older than `since_ms` is out, and at most
    /// `limit` of the survivors come back, newest first.
    #[tokio::test]
    async fn digest_windows_and_limits_the_commit_log() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = machine(&bucket);
        for (index, at) in [1_000_i64, 2_000, 3_000, 4_000].into_iter().enumerate() {
            store
                .commit_page(request("mbp-1", &format!("notes/n{index}.md"), "body", at))
                .await
                .unwrap();
        }

        // The window keeps only what happened at or after `since_ms`; the two
        // older commits are not merely truncated, they are absent.
        let windowed = store.digest(&ws(), &proj(), 3_000, 20).await.unwrap();
        let paths: Vec<&str> = windowed
            .pages
            .iter()
            .map(|record| record.path.as_str())
            .collect();
        assert_eq!(paths, vec!["notes/n3.md", "notes/n2.md"]);
        assert!(
            windowed.pages.iter().all(|record| record.at_ms >= 3_000),
            "a commit from outside the window leaked in: {:?}",
            windowed.pages
        );

        // The limit caps the window and keeps the *newest* end of it: a cap
        // that returned the oldest commits would be useless for a digest.
        let capped = store.digest(&ws(), &proj(), 0, 2).await.unwrap();
        let capped_paths: Vec<&str> = capped
            .pages
            .iter()
            .map(|record| record.path.as_str())
            .collect();
        assert_eq!(capped_paths, vec!["notes/n3.md", "notes/n2.md"]);

        // A cap of zero is a legal request, not an error.
        let none = store.digest(&ws(), &proj(), 0, 0).await.unwrap();
        assert!(none.pages.is_empty());
    }

    /// A deleted page must show up as a deletion. Dropping it would let a
    /// digest quietly launder "this was removed" into "nothing happened".
    #[tokio::test]
    async fn digest_keeps_a_deletion_instead_of_dropping_it() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = machine(&bucket);
        store
            .commit_page(request("mbp-1", "notes/keep.md", "kept", 1_000))
            .await
            .unwrap();
        store
            .commit_page(request("mbp-1", "notes/gone.md", "doomed", 2_000))
            .await
            .unwrap();
        let doomed = PagePath::new("notes/gone.md").unwrap();
        let deleted = store
            .delete_page(
                &ws(),
                &proj(),
                &doomed,
                &WriterId::new("mbp-1").unwrap(),
                3_000,
            )
            .await
            .unwrap();
        assert!(!deleted.already_deleted);

        let digest = store.digest(&ws(), &proj(), 0, 20).await.unwrap();
        let kinds: Vec<(&str, &CommitKind)> = digest
            .pages
            .iter()
            .map(|record| (record.path.as_str(), &record.kind))
            .collect();
        assert_eq!(
            kinds,
            vec![
                ("notes/gone.md", &CommitKind::PageDeleted),
                ("notes/gone.md", &CommitKind::PageWritten),
                ("notes/keep.md", &CommitKind::PageWritten),
            ],
            "the deletion must be present, and the newest commit first"
        );
        // The manifest no longer knows the path; the digest still does.
        let loaded = store.load(&ws(), &proj()).await.unwrap();
        assert!(loaded.manifest.is_tombstoned(&doomed));
    }

    /// Sessions and handoffs are windows too, and a handoff counts as recent
    /// when *any* of its stages lands inside the window -- a baton opened
    /// before it was claimed is exactly the one a new session must see.
    #[tokio::test]
    async fn digest_reports_sessions_and_handoffs_in_the_window() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = machine(&bucket);
        store
            .ingest_observations(ingest("sess-old", &["a", "b"], 1_000))
            .await
            .unwrap();
        store
            .ingest_observations(ingest("sess-new", &["c"], 5_000))
            .await
            .unwrap();

        let writer = WriterId::new("mbp-1").unwrap();
        let (handoff, _) = store
            .open_handoff(
                &ws(),
                &proj(),
                "finish the rebuild",
                "pending",
                &writer,
                2_000,
            )
            .await
            .unwrap();
        store
            .claim_handoff(&ws(), &proj(), &handoff.id, &writer, 6_000)
            .await
            .unwrap();

        let digest = store.digest(&ws(), &proj(), 4_000, 20).await.unwrap();

        let sessions: Vec<(&str, u64)> = digest
            .sessions
            .iter()
            .map(|summary| (summary.session_id.as_str(), summary.observations))
            .collect();
        assert_eq!(sessions, vec![("sess-new", 1)]);

        assert_eq!(digest.handoffs.len(), 1, "claimed inside the window");
        assert_eq!(digest.handoffs[0].id, handoff.id);

        // The claim was the only in-window activity; before it the handoff was
        // not open yet, and after the window opens both sessions are gone.
        let earlier = store.digest(&ws(), &proj(), 7000, 20).await.unwrap();
        assert!(earlier.is_empty(), "{earlier:?}");
    }

    /// A window with nothing in it is an empty answer, never an error.
    #[tokio::test]
    async fn digest_of_a_quiet_window_is_empty() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = machine(&bucket);
        store
            .commit_page(request("mbp-1", "notes/old.md", "body", 1_000))
            .await
            .unwrap();

        let digest = store.digest(&ws(), &proj(), 10_000, 20).await.unwrap();
        assert!(digest.pages.is_empty());
        assert!(digest.sessions.is_empty());
        assert!(digest.handoffs.is_empty());
        assert!(digest.is_empty());

        // A scope that has never been touched behaves the same way: a fresh
        // bucket, so there is genuinely nothing to find.
        let untouched: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let virgin = machine(&untouched);
        let digest = virgin.digest(&ws(), &proj(), 0, 20).await.unwrap();
        assert!(digest.is_empty(), "{digest:?}");
    }
}
