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

use std::time::Duration;

use object_store::ObjectStore;
use qm_core::{
    CatalogHead, Handoff, HandoffState, IndexCatalog, KeyLayout, Lease, MANIFEST_SCHEMA, Manifest,
    Observation, ObservationSegment, PageEntry, PagePath, PageVersion, ProjectId, SessionHead,
    SessionId, SplitEntry, Tombstone, WalEntry, WorkspaceId, WriterId, content_hash,
    derive_handoff_id, derive_page_id, derive_segment_id,
};

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
                    let mut outcomes = 0u32;
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
                        outcomes += outcome.attempts;
                        ids.push(outcome.page_id);
                    }
                    (path, ids, outcomes)
                }));
            }
        }

        let mut expected_paths = BTreeSet::new();
        let mut chains = Vec::new();
        let mut total_attempts = 0u32;
        let mut total_commits = 0u32;
        for task in tasks {
            let (path, ids, attempts) = task.await.unwrap();
            expected_paths.insert(path.clone());
            chains.push((path, ids));
            total_attempts += attempts;
            total_commits += 5;
        }
        // Contention control: if every commit had won on the first attempt the
        // run would prove nothing about the retry path.
        assert!(
            total_attempts > total_commits,
            "expected at least one CAS conflict and retry, got {total_attempts} attempts for {total_commits} commits"
        );

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
        let mut attempts = 0u32;
        for task in tasks {
            attempts += task.await.unwrap().attempts;
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
        assert!(
            attempts > 8,
            "expected at least one head CAS conflict, saw {attempts} attempts"
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
}
