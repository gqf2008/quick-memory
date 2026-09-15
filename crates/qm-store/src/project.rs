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
    KeyLayout, MANIFEST_SCHEMA, Manifest, PageEntry, PagePath, PageVersion, ProjectId, WalEntry,
    WorkspaceId, WriterId, derive_page_id,
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
