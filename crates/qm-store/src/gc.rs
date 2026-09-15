//! Reachability-based garbage collection.
//!
//! Every object this system writes is immutable, so "deleting" something is
//! really "stopping to reference it" — and the only safe way to reclaim space
//! is to compute what is still reachable and remove the rest.
//!
//! What is reachable:
//!
//! * the manifest, and for every live page the **whole supersession chain**
//!   (history is a feature: it is what `read_page` and restore rely on);
//! * for every tombstone, the version it deleted, so a delete stays reversible;
//! * the catalog the head points at, and every split that catalog names;
//! * every session head, and every segment reachable from it.
//!
//! Everything else — an orphaned segment from a lost CAS race, a split whose
//! publish failed, a catalog superseded by a later generation — is collectable,
//! but only after a grace window. That window is what stops GC from deleting an
//! object a slower machine is still uploading or a reader has just pinned.

use std::collections::BTreeSet;

use qm_core::{PagePath, ProjectId, WorkspaceId};
use serde::{Deserialize, Serialize};

use crate::{ProjectStore, StoreError};

/// What one collection pass did (or would do).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcOutcome {
    /// Objects under the scope prefix.
    pub scanned: usize,
    /// Objects the reachability walk kept.
    pub live: usize,
    /// Unreachable objects old enough to collect.
    pub collectable: usize,
    /// Unreachable objects still inside the grace window.
    pub kept_recent: usize,
    /// Objects actually deleted (zero on a dry run).
    pub deleted: usize,
    /// Keys that were deleted.
    pub deleted_keys: Vec<String>,
    /// True when the pass only reported what it would do.
    pub dry_run: bool,
}

impl ProjectStore {
    /// Compute the live set of one project scope.
    ///
    /// # Errors
    /// Propagates listing and chain-read failures.
    pub async fn live_keys(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
    ) -> Result<(BTreeSet<String>, BTreeSet<String>), StoreError> {
        let layout = self.layout();
        let mut keys = BTreeSet::new();
        let mut prefixes = BTreeSet::new();

        keys.insert(layout.manifest(workspace_id, project_id));
        let loaded = self.load(workspace_id, project_id).await?;

        // The sharded commit point names its shards, and they are live for
        // exactly as long as it does. Missing one here would collect a shard
        // the manifest still points at, which is the worst thing a reclaimer
        // can do; the archive a migration wrote is live too, because it is what
        // `predecessor` names and the only copy of the pre-migration body.
        if let Some(root) = &loaded.root {
            for reference in &root.shards {
                keys.insert(reference.key.clone());
            }
            if let Some(predecessor) = &root.predecessor {
                keys.insert(predecessor.key.clone());
            }
        }

        for (path, entry) in &loaded.manifest.pages {
            let page_path = PagePath::new(path)?;
            keys.insert(layout.page_version(workspace_id, project_id, &page_path, &entry.page_id));
            // The head is already in hand from the manifest read above, so the
            // chain walk skips a commit-point read per path — which in the
            // sharded form would be a root read plus a shard read, each.
            for wal in self
                .page_history_from_head(
                    workspace_id,
                    project_id,
                    &page_path,
                    Some(entry.page_id.clone()),
                )
                .await?
            {
                keys.insert(layout.wal_entry(workspace_id, project_id, wal.event_id()));
                keys.insert(layout.page_version(
                    workspace_id,
                    project_id,
                    &page_path,
                    &wal.page_id,
                ));
            }
        }

        for (path, tombstone) in &loaded.manifest.tombstones {
            if let Some(page_id) = &tombstone.last_page_id {
                let page_path = PagePath::new(path)?;
                keys.insert(layout.page_version(workspace_id, project_id, &page_path, page_id));
                keys.insert(layout.wal_entry(workspace_id, project_id, page_id.as_str()));
            }
        }

        // Commit records are advisory for correctness but they *are* the
        // timeline, so they stay live: reclaiming them would silently erase
        // "when" from every page.
        let commit_prefix = layout.commit_prefix(workspace_id, project_id);
        for (key, _) in self.cas().list(&commit_prefix).await? {
            keys.insert(key);
        }

        let catalog = self.load_catalog(workspace_id, project_id).await?;
        keys.insert(layout.catalog_head(workspace_id, project_id));
        if let Some(catalog_key) = catalog.catalog_key {
            keys.insert(catalog_key);
            for split in &catalog.catalog.splits {
                prefixes.insert(split.prefix.clone());
            }
        }

        for session in self.list_sessions(workspace_id, project_id).await? {
            keys.insert(layout.session_head(workspace_id, project_id, &session));
            for segment in self
                .read_session_chain(workspace_id, project_id, &session)
                .await?
            {
                let segment_id = qm_core::derive_segment_id(&segment);
                keys.insert(layout.session_segment(
                    workspace_id,
                    project_id,
                    &session,
                    &segment_id,
                ));
            }
        }

        Ok((keys, prefixes))
    }

    /// Delete everything the scope no longer references.
    ///
    /// `grace_ms` protects recent objects: anything last modified less than
    /// that long ago is left alone even when it is unreachable. Passing `0`
    /// means no grace at all — collect immediately — which is only safe in
    /// tests and for a scope nobody else is writing to.
    ///
    /// # Errors
    /// Propagates listing, reachability, and delete failures.
    pub async fn gc_orphans(
        &self,
        workspace_id: &WorkspaceId,
        project_id: &ProjectId,
        now_ms: i64,
        grace_ms: i64,
        apply: bool,
    ) -> Result<GcOutcome, StoreError> {
        let (keys, prefixes) = self.live_keys(workspace_id, project_id).await?;
        let scope = self.layout().scope_prefix(workspace_id, project_id);
        let objects = self.cas().list(&scope).await?;

        let mut outcome = GcOutcome {
            scanned: objects.len(),
            live: 0,
            collectable: 0,
            kept_recent: 0,
            deleted: 0,
            deleted_keys: Vec::new(),
            dry_run: !apply,
        };

        for (key, _) in objects {
            let is_live = keys.contains(&key)
                || prefixes
                    .iter()
                    .any(|prefix| key == *prefix || key.starts_with(&format!("{prefix}/")));
            if is_live {
                outcome.live += 1;
                continue;
            }
            let Some(last_modified_ms) = self.cas().last_modified_ms(&key).await? else {
                // It vanished between the listing and the check: another
                // machine collected it, which is a fine outcome.
                continue;
            };
            // A zero grace means "no grace": collect immediately, whatever the
            // clocks say. Otherwise the object must be at least `grace_ms` old.
            let within_grace = grace_ms > 0 && now_ms - last_modified_ms < grace_ms;
            if within_grace {
                outcome.kept_recent += 1;
                continue;
            }
            outcome.collectable += 1;
            if apply {
                self.cas().delete(&key).await?;
                outcome.deleted += 1;
                outcome.deleted_keys.push(key);
            }
        }

        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use qm_core::{PagePath, WriterId};

    use super::*;
    use crate::{CommitPageRequest, RetryPolicy};

    fn store(bucket: &Arc<dyn object_store::ObjectStore>) -> ProjectStore {
        ProjectStore::new(Arc::clone(bucket), "v1").with_retry(RetryPolicy {
            max_attempts: 64,
            base_delay: std::time::Duration::ZERO,
        })
    }

    fn ws() -> WorkspaceId {
        WorkspaceId::new("acme").unwrap()
    }

    fn proj() -> ProjectId {
        ProjectId::new("ai-memory").unwrap()
    }

    #[tokio::test]
    async fn gc_collects_only_unreferenced_objects() {
        let bucket: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let store = store(&bucket);

        // A live page with two versions and a chain.
        let path = PagePath::new("notes/raft.md").unwrap();
        for (version, body) in [(1, "leader election"), (2, "leader election and snapshots")] {
            store
                .commit_page(CommitPageRequest {
                    workspace_id: ws(),
                    project_id: proj(),
                    path: path.clone(),
                    title: "Raft".into(),
                    body: body.into(),
                    writer_id: WriterId::new("mbp-a").unwrap(),
                    now_ms: version,
                })
                .await
                .unwrap();
        }

        // Two orphans: a segment whose CAS never landed, and a stray catalog.
        let orphan_segment = "v1/ws/acme/proj/ai-memory/sessions/sess-ghost/segments/deadbeef.json";
        store
            .cas()
            .create(orphan_segment, bytes::Bytes::from_static(b"{}"))
            .await
            .unwrap();
        let orphan_catalog = "v1/ws/acme/proj/ai-memory/index/catalog/feedface.json";
        store
            .cas()
            .create(orphan_catalog, bytes::Bytes::from_static(b"{}"))
            .await
            .unwrap();

        // A dry run reports without deleting.
        let plan = store
            .gc_orphans(&ws(), &proj(), 1_000, 0, false)
            .await
            .unwrap();
        assert!(plan.dry_run);
        assert_eq!(plan.deleted, 0);
        assert!(
            plan.deleted_keys.is_empty(),
            "a dry run must not name deletions it did not make"
        );
        assert!(plan.collectable >= 2, "{plan:?}");
        assert!(
            store.cas().head(orphan_catalog).await.is_ok(),
            "dry run must leave orphans in place"
        );

        // An apply pass removes them and keeps everything reachable.
        let applied = store
            .gc_orphans(&ws(), &proj(), 1_000, 0, true)
            .await
            .unwrap();
        assert!(applied.deleted >= 2, "{applied:?}");
        assert!(applied.deleted_keys.iter().any(|k| k == orphan_catalog));
        assert!(applied.deleted_keys.iter().any(|k| k == orphan_segment));
        assert!(matches!(
            store.cas().head(orphan_catalog).await,
            Err(StoreError::NotFound)
        ));

        // Commit records must survive too: they carry the timeline.
        let log = store.read_commit_log(&ws(), &proj(), 10).await.unwrap();
        // (two page commits were made above)
        assert_eq!(log.len(), 2, "the commit log must not be collected");

        // The live project still reads and its history survives.
        let page = store
            .read_page(&ws(), &proj(), &path)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(page.body, "leader election and snapshots");
        assert_eq!(
            store
                .page_history(&ws(), &proj(), &path)
                .await
                .unwrap()
                .len(),
            2,
            "history is a feature, not garbage"
        );
    }

    #[tokio::test]
    async fn gc_respects_the_grace_window() {
        let bucket: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let store = store(&bucket);
        let orphan = "v1/ws/acme/proj/ai-memory/index/catalog/feedface.json";
        store
            .cas()
            .create(orphan, bytes::Bytes::from_static(b"{}"))
            .await
            .unwrap();

        // A generous grace window keeps a fresh orphan: a slower machine may
        // still be about to reference it.
        let outcome = store
            .gc_orphans(&ws(), &proj(), 1_000, i64::MAX, true)
            .await
            .unwrap();
        assert_eq!(outcome.deleted, 0);
        assert!(outcome.kept_recent >= 1);
        assert!(store.cas().head(orphan).await.is_ok());
    }
}
