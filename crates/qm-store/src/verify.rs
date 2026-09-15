//! Integrity self-check for one project scope.
//!
//! There is no server to ask "is this bucket healthy?", so any machine has to
//! be able to answer it from the objects alone. The check walks what the
//! authoritative state points at and reports every inconsistency it finds
//! rather than stopping at the first: an operator wants the shape of the
//! damage, not one symptom.
//!
//! It is deliberately additive: nothing here repairs anything, and a report of
//! zero problems means "everything the manifest and catalog name is present and
//! self-consistent", not "the data is what you meant to write".

use serde::{Deserialize, Serialize};

use qm_core::{PagePath, ProjectId, WorkspaceId};

use crate::{ProjectStore, StoreError};

/// One thing that is wrong.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Problem {
    /// What kind of check failed, e.g. `page_version` or `session_segment`.
    pub kind: String,
    /// Object key or path the problem concerns.
    pub subject: String,
    /// Human-readable detail.
    pub detail: String,
}

/// Result of one integrity pass.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifyReport {
    /// Manifest sequence observed.
    pub manifest_seq: u64,
    /// Live pages checked.
    pub pages: usize,
    /// Sessions checked.
    pub sessions: usize,
    /// Splits referenced by the catalog.
    pub splits: usize,
    /// Shards the commit point names (0 for the whole-object form).
    pub shards: usize,
    /// Everything that is inconsistent.
    pub problems: Vec<Problem>,
}

impl VerifyReport {
    /// Whether the scope is internally consistent.
    #[must_use]
    pub fn ok(&self) -> bool {
        self.problems.is_empty()
    }
}

impl ProjectStore {
    /// Check that everything the manifest and catalog name actually exists and
    /// hashes to what its key claims.
    ///
    /// # Errors
    /// Propagates backend failures. Missing or inconsistent objects are
    /// reported in [`VerifyReport::problems`], not returned as errors.
    pub async fn verify_project(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
    ) -> Result<VerifyReport, StoreError> {
        let mut problems = Vec::new();
        let loaded = match self.load(workspace_id, project_id).await {
            Ok(loaded) => loaded,
            Err(error) => {
                problems.push(Problem {
                    kind: "manifest".into(),
                    subject: self.layout().manifest(workspace_id, project_id),
                    detail: error.to_string(),
                });
                return Ok(VerifyReport {
                    manifest_seq: 0,
                    pages: 0,
                    sessions: 0,
                    splits: 0,
                    shards: 0,
                    problems,
                });
            }
        };
        let shards = loaded.root.as_ref().map_or(0, |root| root.shards.len());
        let root = loaded.root;
        let manifest = loaded.manifest;

        // The sharded layout is checked here rather than left to the read path,
        // because these are the two ways a root can advertise a state it does
        // not actually have: a shard filed under a key that is not its content
        // hash (so the hash check the reader makes is checking the wrong
        // thing), and a `predecessor` whose archive is gone (so the recovery
        // point the root claims does not exist).
        if let Some(root) = &root {
            for reference in &root.shards {
                let canonical =
                    self.layout()
                        .manifest_shard(workspace_id, project_id, &reference.content_hash);
                if reference.key != canonical {
                    problems.push(Problem {
                        kind: "manifest_shard_key".into(),
                        subject: reference.key.clone(),
                        detail: format!(
                            "shard {} is filed under a key that is not its content hash \
                             (expected {canonical})",
                            reference.shard
                        ),
                    });
                }
            }
            if let Some(predecessor) = &root.predecessor {
                match self.cas().read(&predecessor.key).await {
                    Ok((bytes, _)) => {
                        let actual = qm_core::content_hash(&bytes);
                        if actual != predecessor.content_hash {
                            problems.push(Problem {
                                kind: "manifest_predecessor".into(),
                                subject: predecessor.key.clone(),
                                detail: format!(
                                    "the archived whole manifest hashes to {actual}, but the \
                                     root records {}",
                                    predecessor.content_hash
                                ),
                            });
                        }
                    }
                    Err(error) => problems.push(Problem {
                        kind: "manifest_predecessor".into(),
                        subject: predecessor.key.clone(),
                        detail: format!(
                            "the root names a predecessor at seq {} that cannot be read: {error}",
                            predecessor.seq
                        ),
                    }),
                }
            }
        }

        // Pages: every head must resolve, and each chain must walk cleanly.
        for (path, entry) in &manifest.pages {
            let page_path = match PagePath::new(path) {
                Ok(page_path) => page_path,
                Err(error) => {
                    problems.push(Problem {
                        kind: "page_path".into(),
                        subject: path.clone(),
                        detail: error.to_string(),
                    });
                    continue;
                }
            };
            match self
                .page_history_from_head(
                    workspace_id,
                    project_id,
                    &page_path,
                    Some(entry.page_id.clone()),
                )
                .await
            {
                Ok(history) => {
                    if history.last().map(|wal| &wal.page_id) != Some(&entry.page_id) {
                        problems.push(Problem {
                            kind: "page_chain".into(),
                            subject: path.clone(),
                            detail: "the chain does not end at the manifest head".into(),
                        });
                    }
                }
                Err(error) => problems.push(Problem {
                    kind: "page_chain".into(),
                    subject: path.clone(),
                    detail: error.to_string(),
                }),
            }
            match self
                .read_page_version(workspace_id, project_id, &page_path, &entry.page_id)
                .await
            {
                Ok(_) => {}
                Err(error) => problems.push(Problem {
                    kind: "page_version".into(),
                    subject: format!("{path} ({})", entry.page_id),
                    detail: error.to_string(),
                }),
            }
        }

        // Sessions: heads must resolve and segments must hash to their keys.
        let sessions = match self.list_sessions(workspace_id, project_id).await {
            Ok(sessions) => sessions,
            Err(error) => {
                problems.push(Problem {
                    kind: "sessions".into(),
                    subject: self.layout().session_prefix(workspace_id, project_id),
                    detail: error.to_string(),
                });
                Vec::new()
            }
        };
        for session in &sessions {
            match self
                .read_session_chain(workspace_id, project_id, session)
                .await
            {
                Ok(_) => {}
                Err(error) => problems.push(Problem {
                    kind: "session_chain".into(),
                    subject: session.to_string(),
                    detail: error.to_string(),
                }),
            }
        }

        // Catalog: the head must point at a catalog that names splits that exist.
        let mut splits_checked = 0usize;
        match self.load_catalog(workspace_id, project_id).await {
            Ok(catalog) => {
                for split in &catalog.catalog.splits {
                    splits_checked += 1;
                    match self.cas().list(&split.prefix).await {
                        Ok(objects) if objects.is_empty() => problems.push(Problem {
                            kind: "split_missing".into(),
                            subject: split.prefix.clone(),
                            detail: "the catalog references a split with no objects".into(),
                        }),
                        Ok(_) => {}
                        Err(error) => problems.push(Problem {
                            kind: "split_unreadable".into(),
                            subject: split.prefix.clone(),
                            detail: error.to_string(),
                        }),
                    }
                }
            }
            Err(error) => problems.push(Problem {
                kind: "catalog".into(),
                subject: self.layout().catalog_head(workspace_id, project_id),
                detail: error.to_string(),
            }),
        }

        Ok(VerifyReport {
            manifest_seq: manifest.seq,
            shards,
            pages: manifest.pages.len(),
            sessions: sessions.len(),
            splits: splits_checked,
            problems,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use object_store::memory::InMemory;
    use qm_core::{MANIFEST_FORMAT_SHARDED, WriterId, content_hash};

    use super::*;
    use crate::{CommitPageRequest, RetryPolicy};

    fn store(bucket: &Arc<dyn object_store::ObjectStore>) -> ProjectStore {
        ProjectStore::new(Arc::clone(bucket), "v1").with_retry(RetryPolicy {
            max_attempts: 32,
            base_delay: std::time::Duration::ZERO,
        })
    }

    fn ws() -> WorkspaceId {
        WorkspaceId::new("acme").unwrap()
    }

    fn proj() -> ProjectId {
        ProjectId::new("ai-memory").unwrap()
    }

    async fn commit(store: &ProjectStore, path: &str, body: &str, now_ms: i64) -> qm_core::PageId {
        store
            .commit_page(CommitPageRequest {
                workspace_id: ws(),
                project_id: proj(),
                path: PagePath::new(path).unwrap(),
                title: path.to_string(),
                body: body.to_string(),
                writer_id: WriterId::new("mbp-a").unwrap(),
                now_ms,
            })
            .await
            .unwrap()
            .page_id
    }

    #[tokio::test]
    async fn a_healthy_scope_reports_nothing() {
        let bucket: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let store = store(&bucket);
        commit(&store, "notes/raft.md", "first", 1).await;
        commit(&store, "notes/raft.md", "second", 2).await;
        commit(&store, "notes/other.md", "third", 3).await;

        let report = store.verify_project(&ws(), &proj()).await.unwrap();
        assert!(report.ok(), "{:?}", report.problems);
        assert_eq!(report.pages, 2);
        assert_eq!(report.manifest_seq, 3);
    }

    #[tokio::test]
    async fn a_missing_page_version_is_reported_by_name() {
        let bucket: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let store = store(&bucket);
        let page_id = commit(&store, "notes/raft.md", "first", 1).await;

        // Delete the object the manifest points at, the way a botched cleanup
        // or a partial restore would.
        let key = store.layout().page_version(
            &ws(),
            &proj(),
            &PagePath::new("notes/raft.md").unwrap(),
            &page_id,
        );
        store.cas().delete(&key).await.unwrap();

        let report = store.verify_project(&ws(), &proj()).await.unwrap();
        assert!(!report.ok());
        assert!(
            report
                .problems
                .iter()
                .any(|problem| problem.kind == "page_version"
                    && problem.subject.contains(page_id.as_str())),
            "{:?}",
            report.problems
        );
        assert!(
            report
                .problems
                .iter()
                .any(|problem| problem.detail.contains("not found")
                    || problem.detail.contains("NotFound")),
            "the detail should say what is wrong: {:?}",
            report.problems
        );
        let _ = content_hash(b"unused");
    }

    #[tokio::test]
    async fn a_tampered_session_segment_is_reported() {
        use crate::IngestObservationsRequest;
        use qm_core::{MANIFEST_SCHEMA, Observation, SessionId, derive_observation_id};

        let bucket: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let store = store(&bucket);
        let session = SessionId::new("sess-1").unwrap();
        let text = "an observation";
        let observation = Observation {
            schema: MANIFEST_SCHEMA,
            observation_id: derive_observation_id(&session, "codex", "tool_use", text, 1),
            session_id: session.clone(),
            actor: "codex".into(),
            kind: "tool_use".into(),
            text: text.into(),
            created_at_ms: 1,
        };
        store
            .ingest_observations(IngestObservationsRequest {
                workspace_id: ws(),
                project_id: proj(),
                session_id: session.clone(),
                writer_id: WriterId::new("mbp-a").unwrap(),
                observations: vec![observation],
                now_ms: 1,
            })
            .await
            .unwrap();

        // Overwrite the segment with content that no longer hashes to its key.
        let head = store
            .load_session_head(&ws(), &proj(), &session)
            .await
            .unwrap()
            .unwrap()
            .0;
        let key = store
            .layout()
            .session_segment(&ws(), &proj(), &session, &head.segment_id);
        let forged = qm_core::ObservationSegment {
            schema: MANIFEST_SCHEMA,
            session_id: session.clone(),
            prev: None,
            observations: Vec::new(),
        };
        store.cas().delete(&key).await.unwrap();
        store
            .cas()
            .create(&key, crate::encode(&forged).unwrap())
            .await
            .unwrap();

        let report = store.verify_project(&ws(), &proj()).await.unwrap();
        assert!(
            report
                .problems
                .iter()
                .any(|problem| problem.kind == "session_chain"),
            "{:?}",
            report.problems
        );
    }

    /// A shard filed under a key that is not its content hash is reported —
    /// even though every read of the scope still works.
    ///
    /// This is the check that keeps the layout *canonical*: a shard body lives
    /// at the key its own content produced. It cannot be caught by reading,
    /// because the reader takes whatever key the root names and checks the bytes
    /// against the recorded hash — which is exactly why a scope like this
    /// answers perfectly and why `verify` is the only place that can say so.
    ///
    /// The comment on the check says a non-canonical key "would break the rest
    /// of the tooling": de-duplication, the reachability walk and the local
    /// object cache all address a shard by its content hash, so a body living
    /// somewhere else is unreachable by anything except this root.
    #[tokio::test]
    async fn a_shard_filed_under_a_non_canonical_key_is_reported() {
        let bucket: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let store = store(&bucket).with_manifest_format(MANIFEST_FORMAT_SHARDED);
        commit(&store, "notes/raft.md", "first", 1).await;

        let loaded = store.load(&ws(), &proj()).await.unwrap();
        let root = loaded
            .root
            .clone()
            .expect("the premise: the scope is sharded");
        assert_eq!(root.shards.len(), 1, "the premise: one shard was written");
        let original = root.shards[0].clone();
        let canonical = store
            .layout()
            .manifest_shard(&ws(), &proj(), &original.content_hash);
        assert_eq!(
            original.key, canonical,
            "the premise: it starts out filed where its content says"
        );

        // Same bytes, another legal key, and a root that points at the copy.
        let renamed = format!("{}-copy.json", canonical.trim_end_matches(".json"));
        let (body, _) = store.cas().read(&canonical).await.unwrap();
        store.cas().create(&renamed, body).await.unwrap();
        let mut renamed_root = (*root).clone();
        renamed_root.shards[0].key = renamed.clone();
        let manifest_key = store.layout().manifest(&ws(), &proj());
        let (_, version) = store.cas().read(&manifest_key).await.unwrap();
        store
            .cas()
            .update(
                &manifest_key,
                Bytes::from(serde_json::to_vec(&renamed_root).unwrap()),
                &version,
            )
            .await
            .unwrap();

        // Premise for the check existing at all: the scope still reads, so this
        // is not a "missing object" problem the reader would have caught.
        assert!(
            store
                .read_page(&ws(), &proj(), &PagePath::new("notes/raft.md").unwrap())
                .await
                .unwrap()
                .is_some(),
            "the premise: a non-canonical key still answers"
        );

        let report = store.verify_project(&ws(), &proj()).await.unwrap();
        let problem = report
            .problems
            .iter()
            .find(|problem| problem.kind == "manifest_shard_key")
            .unwrap_or_else(|| {
                panic!(
                    "a shard filed under a non-canonical key must be reported: {:?}",
                    report.problems
                )
            });
        assert_eq!(problem.subject, renamed);
        assert!(
            problem.detail.contains(&canonical),
            "the report has to name the key it should have been at: {problem:?}"
        );
        assert!(
            report
                .problems
                .iter()
                .all(|problem| problem.kind != "manifest_shard"),
            "the bytes are fine, so this must not read as a broken shard: {:?}",
            report.problems
        );
    }
}
