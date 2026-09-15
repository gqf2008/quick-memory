//! Shared plumbing for the S0 probes.
//!
//! The probes refuse to report success without a real backend: a missing
//! credential is an error, not a skip. A conformance check that silently
//! passes when it never ran is worse than no check at all.

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;

/// S3-compatible connection settings, resolved from the environment.
#[derive(Debug, Clone)]
pub struct S3Config {
    /// Endpoint URL, e.g. `https://<account>.r2.cloudflarestorage.com`.
    pub endpoint: String,
    /// Bucket name.
    pub bucket: String,
    /// Region; R2 wants `auto`.
    pub region: String,
    /// Access key id.
    pub access_key_id: String,
    /// Secret access key.
    pub secret_access_key: String,
    /// Path-style requests. R2 needs `true`.
    pub force_path_style: bool,
    /// Key prefix every probe object lives under.
    pub prefix: String,
}

fn env_first(names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| {
        std::env::var(name)
            .ok()
            .filter(|value| !value.trim().is_empty())
    })
}

fn require(names: &[&str], what: &str) -> Result<String> {
    env_first(names).ok_or_else(|| {
        anyhow::anyhow!(
            "missing {what}; set one of {names:?} (a probe that cannot reach the \
             backend must fail, not skip)"
        )
    })
}

impl S3Config {
    /// Resolve configuration from `QM_S3_*` (falling back to `R2_*`).
    ///
    /// # Errors
    /// Fails when endpoint, bucket, or credentials are absent.
    pub fn from_env() -> Result<Self> {
        let endpoint = require(&["QM_S3_ENDPOINT", "R2_ENDPOINT"], "S3 endpoint")?;
        let bucket = require(&["QM_S3_BUCKET", "R2_BUCKET"], "bucket name")?;
        let access_key_id = require(
            &["QM_S3_ACCESS_KEY_ID", "R2_ACCESS_KEY_ID"],
            "access key id",
        )?;
        let secret_access_key = require(
            &["QM_S3_SECRET_ACCESS_KEY", "R2_SECRET_ACCESS_KEY"],
            "secret access key",
        )?;
        let region = env_first(&["QM_S3_REGION"]).unwrap_or_else(|| "auto".to_string());
        let force_path_style = env_first(&["QM_S3_FORCE_PATH_STYLE"])
            .map(|value| !matches!(value.as_str(), "0" | "false" | "no"))
            .unwrap_or(true);
        let prefix = env_first(&["QM_S3_PREFIX"]).unwrap_or_else(|| "qm-probe".to_string());
        Ok(Self {
            endpoint,
            bucket,
            region,
            access_key_id,
            secret_access_key,
            force_path_style,
            prefix,
        })
    }

    /// Build an object store client for this configuration.
    ///
    /// # Errors
    /// Fails when the client cannot be constructed.
    pub fn build_store(&self) -> Result<Arc<dyn ObjectStore>> {
        if !self.endpoint.starts_with("http") {
            bail!("endpoint must be an http(s) URL, got {:?}", self.endpoint);
        }
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&self.bucket)
            .with_region(&self.region)
            .with_virtual_hosted_style_request(!self.force_path_style)
            .with_access_key_id(&self.access_key_id)
            .with_secret_access_key(&self.secret_access_key)
            .with_endpoint(&self.endpoint);
        if self.endpoint.starts_with("http://") {
            builder = builder.with_allow_http(true);
        }
        let store = builder.build().context("building S3 client")?;
        Ok(Arc::new(store))
    }
}

/// Initialize stdout tracing for the probe binaries.
pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();
}

/// Parameters for the multi-machine commit scenario.
#[derive(Debug, Clone)]
pub struct ManifestScenarioParams {
    /// Number of simulated machines writing concurrently.
    pub machines: usize,
    /// Versions each machine writes to its own path.
    pub writes: usize,
    /// Commit attempts before a machine gives up on the CAS race.
    pub max_attempts: u32,
    /// Suffix making the probe scope unique; defaults to the wall clock.
    pub scope_suffix: Option<String>,
}

/// What the scenario observed, after verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestScenarioReport {
    /// Workspace id the run used.
    pub workspace: String,
    /// Project id the run used.
    pub project: String,
    /// Commit sequence reached on the manifest.
    pub commits: u64,
    /// Total CAS attempts across all machines.
    pub attempts: u32,
    /// Pages in the manifest.
    pub pages: usize,
    /// WAL records readable afterwards.
    pub wal_records: usize,
    /// Verification failures; empty means the run proved the invariants.
    pub failures: Vec<String>,
}

/// Run the multi-machine commit scenario against `bucket` and verify it.
///
/// The scenario is what S1 must prove: independent writers, no coordination,
/// no lost versions, intact supersession chains, and a complete WAL.
///
/// # Errors
/// Fails on backend errors or a panicking task; verification failures are
/// reported in [`ManifestScenarioReport::failures`] so the caller can decide
/// how loudly to shout.
pub async fn run_manifest_scenario(
    bucket: &std::sync::Arc<dyn ObjectStore>,
    params: &ManifestScenarioParams,
) -> Result<ManifestScenarioReport> {
    use std::time::Duration;

    use qm_core::{PagePath, ProjectId, WorkspaceId, WriterId};
    use qm_store::{CommitPageRequest, ProjectStore, RetryPolicy};

    if params.machines == 0 || params.writes == 0 {
        bail!("machines and writes must both be greater than zero");
    }
    let run = params.scope_suffix.clone().unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos().to_string())
            .unwrap_or_else(|_| "0".to_string())
    });
    let workspace = WorkspaceId::new(format!("probe-ws-{run}"))?;
    let project = ProjectId::new(format!("probe-proj-{run}"))?;

    let mut tasks = Vec::new();
    for machine_index in 0..params.machines {
        let bucket: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::clone(bucket);
        let workspace = workspace.clone();
        let project = project.clone();
        let writer = WriterId::new(format!("probe-{machine_index}"))?;
        let path = PagePath::new(format!("notes/probe/m{machine_index}.md"))?;
        let writes = params.writes;
        let max_attempts = params.max_attempts;
        tasks.push(tokio::spawn(async move {
            let store = ProjectStore::new(bucket, "v1").with_retry(RetryPolicy {
                max_attempts,
                base_delay: Duration::from_millis(10),
            });
            let mut ids = Vec::new();
            let mut attempts = 0u32;
            for version in 0..writes {
                let outcome = store
                    .commit_page(CommitPageRequest {
                        workspace_id: workspace.clone(),
                        project_id: project.clone(),
                        path: path.clone(),
                        title: format!("probe m{machine_index}"),
                        body: format!("body v{version}"),
                        writer_id: writer.clone(),
                        now_ms: version as i64,
                    })
                    .await
                    .with_context(|| format!("machine {machine_index} commit {version}"))?;
                attempts += outcome.attempts;
                ids.push(outcome.page_id);
            }
            anyhow::Ok((path, ids, attempts))
        }));
    }

    let mut chains = Vec::new();
    let mut attempts = 0u32;
    for task in tasks {
        let (path, ids, machine_attempts) = task.await.context("machine task panicked")??;
        chains.push((path, ids));
        attempts += machine_attempts;
    }

    // Re-read through a fresh handle: what "another machine" sees.
    let reader = ProjectStore::new(std::sync::Arc::clone(bucket), "v1");
    let manifest = reader.load(&workspace, &project).await?.manifest;
    let expected = (params.machines * params.writes) as u64;
    let mut failures = Vec::new();

    if manifest.seq != expected {
        failures.push(format!(
            "commit sequence {} != {expected} expected commits",
            manifest.seq
        ));
    }
    if manifest.pages.len() != params.machines {
        failures.push(format!(
            "manifest has {} pages, expected {}",
            manifest.pages.len(),
            params.machines
        ));
    }
    for (path, ids) in &chains {
        let history = reader.page_history(&workspace, &project, path).await?;
        if history.len() != params.writes {
            failures.push(format!("{}: chain length {}", path.as_str(), history.len()));
            continue;
        }
        if history[0].supersedes.is_some() {
            failures.push(format!(
                "{}: first version supersedes something",
                path.as_str()
            ));
        }
        for window in history.windows(2) {
            if window[1].supersedes.as_ref() != Some(&window[0].page_id) {
                failures.push(format!("{}: broken supersession link", path.as_str()));
            }
        }
        if history
            .iter()
            .map(|e| e.page_id.clone())
            .collect::<Vec<_>>()
            != *ids
        {
            failures.push(format!(
                "{}: committed order differs from chain",
                path.as_str()
            ));
        }
        match reader.read_page(&workspace, &project, path).await? {
            Some(page) if page.body == format!("body v{}", params.writes - 1) => {}
            Some(page) => failures.push(format!("{}: head body is {}", path.as_str(), page.body)),
            None => failures.push(format!("{}: head not readable", path.as_str())),
        }
    }
    let wal = reader.read_wal(&workspace, &project).await?;
    if wal.len() != expected as usize {
        failures.push(format!(
            "WAL has {} records, expected {expected}",
            wal.len()
        ));
    }

    Ok(ManifestScenarioReport {
        workspace: workspace.to_string(),
        project: project.to_string(),
        commits: manifest.seq,
        attempts,
        pages: manifest.pages.len(),
        wal_records: wal.len(),
        failures,
    })
}

/// Delete every object under a workspace prefix.
///
/// # Errors
/// Propagates listing and delete failures.
pub async fn delete_scope(
    bucket: &std::sync::Arc<dyn ObjectStore>,
    workspace: &str,
) -> Result<usize> {
    let cas = qm_store::CasStore::new(std::sync::Arc::clone(bucket), "");
    let prefix = format!("v1/ws/{workspace}");
    let objects = cas.list(&prefix).await?;
    let count = objects.len();
    for (key, _) in objects {
        cas.delete(&key).await?;
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn search_scenario_detects_stale_copies_and_offline_machines() {
        let bucket: std::sync::Arc<dyn ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        let report = run_search_scenario(&bucket, Some("unit-search".to_string()))
            .await
            .unwrap();
        assert!(report.failures.is_empty(), "{:?}", report.failures);
        assert_eq!(report.splits, 3);
        assert_eq!(report.hits, 1);
        assert!(report.filtered_out >= 1);
    }

    #[tokio::test]
    async fn session_scenario_captures_compiles_and_finds_a_session() {
        let bucket: std::sync::Arc<dyn ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        let report = run_session_scenario(&bucket, Some("unit-session".to_string()))
            .await
            .unwrap();
        assert!(report.failures.is_empty(), "{:?}", report.failures);
        assert_eq!(report.observations, 3);
        assert_eq!(report.segments, 2);
        assert_eq!(report.hits, 1);
    }

    #[tokio::test]
    async fn scenario_verifies_a_backend_with_working_cas() {
        let bucket: std::sync::Arc<dyn ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());
        let report = run_manifest_scenario(
            &bucket,
            &ManifestScenarioParams {
                machines: 3,
                writes: 4,
                max_attempts: 64,
                scope_suffix: Some("unit".to_string()),
            },
        )
        .await
        .unwrap();
        assert!(report.failures.is_empty(), "{:?}", report.failures);
        assert_eq!(report.commits, 12);
        assert_eq!(report.pages, 3);
        assert_eq!(report.wal_records, 12);
        // A lower bound, not evidence of contention: every commit costs at
        // least one attempt, so this can never fail. The probe prints whether a
        // conflict actually happened instead of asserting it, because that is
        // decided by the scheduler and the network.
        assert!(report.attempts >= 12);
        // 12 page versions + 12 WAL records + 12 commit records + the manifest.
        assert_eq!(
            delete_scope(&bucket, &report.workspace).await.unwrap(),
            12 + 12 + 12 + 1
        );
    }
}

/// What the search scenario observed, after verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchScenarioReport {
    /// Workspace id the run used.
    pub workspace: String,
    /// Project id the run used.
    pub project: String,
    /// Splits published into the catalog.
    pub splits: usize,
    /// Hits for the query that matches both a stale and a current version.
    pub hits: usize,
    /// Candidates the manifest rejected as stale.
    pub filtered_out: usize,
    /// Verification failures; empty means the run proved the invariants.
    pub failures: Vec<String>,
    /// Splits before compaction.
    pub splits_before: usize,
    /// Splits after compaction.
    pub splits_after: usize,
}

/// Run the S2/S3 acceptance scenario against `bucket` and verify it.
///
/// Three machines publish splits; one of them supersedes a page another had
/// published and then never participates again. A fresh reader must return the
/// current version exactly once, refuse the superseded copy, and still find
/// the content of the machine that went away.
///
/// # Errors
/// Fails on backend, build, or search errors; verification results are
/// reported in [`SearchScenarioReport::failures`].
pub async fn run_search_scenario(
    bucket: &std::sync::Arc<dyn ObjectStore>,
    scope_suffix: Option<String>,
) -> Result<SearchScenarioReport> {
    use std::path::Path;

    use qm_core::{PagePath, ProjectId, WorkspaceId, WriterId};
    use qm_search::{PageDoc, publish_split_index, search_project};
    use qm_store::{CommitPageRequest, ProjectStore, RetryPolicy};

    let run = scope_suffix.unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos().to_string())
            .unwrap_or_else(|_| "0".to_string())
    });
    let workspace = WorkspaceId::new(format!("probe-ws-{run}"))?;
    let project = ProjectId::new(format!("probe-proj-{run}"))?;
    let project_store =
        ProjectStore::new(std::sync::Arc::clone(bucket), "v1").with_retry(RetryPolicy {
            max_attempts: 32,
            base_delay: std::time::Duration::from_millis(10),
        });
    let build_root = tempfile::TempDir::new().context("creating build dir")?;

    struct Ctx<'a> {
        bucket: &'a std::sync::Arc<dyn ObjectStore>,
        store: &'a ProjectStore,
        workspace: &'a WorkspaceId,
        project: &'a ProjectId,
        build_root: &'a Path,
    }

    impl Ctx<'_> {
        async fn write_and_publish(
            &self,
            writer: &str,
            path: &str,
            body: &str,
            now_ms: i64,
        ) -> Result<qm_core::PageVersion> {
            let writer_id = WriterId::new(writer)?;
            let page_path = PagePath::new(path)?;
            self.store
                .commit_page(CommitPageRequest {
                    workspace_id: self.workspace.clone(),
                    project_id: self.project.clone(),
                    path: page_path.clone(),
                    title: path.to_string(),
                    body: body.to_string(),
                    writer_id: writer_id.clone(),
                    now_ms,
                })
                .await?;
            let page = self
                .store
                .read_page(self.workspace, self.project, &page_path)
                .await?
                .context("committed page not readable")?;
            let doc = PageDoc::from_version(self.workspace, self.project, &page, now_ms);
            let build_dir = self.build_root.join(writer);
            publish_split_index(
                self.bucket.as_ref(),
                self.store,
                self.workspace,
                self.project,
                &writer_id,
                1,
                &[doc],
                &build_dir,
                now_ms,
            )
            .await?;
            Ok(page)
        }
    }

    let ctx = Ctx {
        bucket,
        store: &project_store,
        workspace: &workspace,
        project: &project,
        build_root: build_root.path(),
    };

    ctx.write_and_publish(
        "probe-a",
        "shared/raft.md",
        "leader election and joint consensus",
        10,
    )
    .await?;
    // This machine publishes and is never heard from again.
    ctx.write_and_publish(
        "probe-b",
        "notes/quickwit.md",
        "tantivy splits in object storage",
        20,
    )
    .await?;
    let latest = ctx
        .write_and_publish(
            "probe-c",
            "shared/raft.md",
            "leader election and snapshots",
            30,
        )
        .await?;

    let reader = ProjectStore::new(std::sync::Arc::clone(bucket), "v1");
    let cache = tempfile::TempDir::new().context("creating cache dir")?;
    let mut failures = Vec::new();

    let both = search_project(
        bucket.as_ref(),
        &reader,
        &workspace,
        &project,
        cache.path(),
        "election",
        10,
    )
    .await?;
    if both.hits.len() != 1 {
        failures.push(format!(
            "expected exactly one live hit, got {}",
            both.hits.len()
        ));
    } else if both.hits[0].page_id != latest.page_id.as_str() {
        failures.push("the hit is not the current version".to_string());
    }
    if both.filtered_out == 0 {
        failures.push("the superseded copy was not filtered".to_string());
    }

    let stale_only = search_project(
        bucket.as_ref(),
        &reader,
        &workspace,
        &project,
        cache.path(),
        "consensus",
        10,
    )
    .await?;
    if !stale_only.hits.is_empty() {
        failures.push(format!(
            "a superseded body still answered with {} hit(s)",
            stale_only.hits.len()
        ));
    }

    let offline = search_project(
        bucket.as_ref(),
        &reader,
        &workspace,
        &project,
        cache.path(),
        "tantivy",
        10,
    )
    .await?;
    if offline.hits.len() != 1 || offline.hits[0].path != "notes/quickwit.md" {
        failures.push("the offline machine's content is not searchable".to_string());
    }

    // S3: delete a page, then compact, and prove nothing changed for readers
    // while the split set shrank.
    let deleted_path = PagePath::new("notes/quickwit.md")?;
    project_store
        .delete_page(
            &workspace,
            &project,
            &deleted_path,
            &WriterId::new("probe-c")?,
            40,
        )
        .await?;
    let after_delete = search_project(
        bucket.as_ref(),
        &reader,
        &workspace,
        &project,
        cache.path(),
        "tantivy",
        10,
    )
    .await?;
    if !after_delete.hits.is_empty() {
        failures.push(format!(
            "a deleted page still answered with {} hit(s)",
            after_delete.hits.len()
        ));
    }

    let splits_before = reader
        .load_catalog(&workspace, &project)
        .await?
        .catalog
        .splits
        .len();
    let compact = qm_search::compact_project(
        bucket.as_ref(),
        &reader,
        &workspace,
        &project,
        &WriterId::new("probe-compactor")?,
        &build_root.path().join("compact"),
        50,
        60_000,
        // The probe runs without an embedding provider, so it exercises the
        // keyword-only compaction path.
        None,
    )
    .await?;
    if compact.skipped {
        failures.push("compaction unexpectedly found the lease held".to_string());
    }
    if compact.splits_after >= splits_before {
        failures.push(format!(
            "compaction did not shrink the catalog: {} -> {}",
            splits_before, compact.splits_after
        ));
    }
    let after_compact = search_project(
        bucket.as_ref(),
        &reader,
        &workspace,
        &project,
        tempfile::TempDir::new().context("cache")?.path(),
        "election",
        10,
    )
    .await?;
    if after_compact.hits != both.hits {
        failures.push("compaction changed the search results".to_string());
    }
    let resurrected = search_project(
        bucket.as_ref(),
        &reader,
        &workspace,
        &project,
        tempfile::TempDir::new().context("cache")?.path(),
        "tantivy",
        10,
    )
    .await?;
    if !resurrected.hits.is_empty() {
        failures.push("compaction resurrected a deleted page".to_string());
    }

    Ok(SearchScenarioReport {
        workspace: workspace.to_string(),
        project: project.to_string(),
        splits: both.splits_searched,
        hits: both.hits.len(),
        filtered_out: both.filtered_out,
        failures,
        splits_before,
        splits_after: compact.splits_after,
    })
}

/// What the session scenario observed, after verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionScenarioReport {
    /// Workspace id the run used.
    pub workspace: String,
    /// Project id the run used.
    pub project: String,
    /// Observations captured into the session chain.
    pub observations: usize,
    /// Segments the chain was made of.
    pub segments: usize,
    /// Splits published for the compiled page.
    pub splits: usize,
    /// Hits for a term that only appears in a captured observation.
    pub hits: usize,
    /// Verification failures; empty means the run proved the invariants.
    pub failures: Vec<String>,
}

/// Run the S4 acceptance scenario against `bucket` and verify it.
///
/// Capture a session, compile it into a page, publish that page as a split, and
/// search for it from a fresh reader — then compile again and require that
/// nothing new was written.
///
/// # Errors
/// Fails on backend, ingest, compile, publish, or search errors; verification
/// results are reported in [`SessionScenarioReport::failures`].
pub async fn run_session_scenario(
    bucket: &std::sync::Arc<dyn ObjectStore>,
    scope_suffix: Option<String>,
) -> Result<SessionScenarioReport> {
    use qm_core::{MANIFEST_SCHEMA, Observation, ProjectId, SessionId, WorkspaceId, WriterId};
    use qm_search::{PageDoc, consolidate::consolidate_session, publish_split_index};
    use qm_store::{IngestObservationsRequest, ProjectStore, RetryPolicy};

    let run = scope_suffix.unwrap_or_else(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos().to_string())
            .unwrap_or_else(|_| "0".to_string())
    });
    let workspace = WorkspaceId::new(format!("probe-ws-{run}"))?;
    let project = ProjectId::new(format!("probe-proj-{run}"))?;
    let session = SessionId::new(format!("probe-sess-{run}"))?;
    let writer = WriterId::new("probe-session")?;
    let store = ProjectStore::new(std::sync::Arc::clone(bucket), "v1").with_retry(RetryPolicy {
        max_attempts: 32,
        base_delay: std::time::Duration::from_millis(10),
    });

    let mut failures = Vec::new();
    for (batch, texts) in [
        vec!["switched the index to tantivy splits", "ran the full suite"],
        vec!["published the catalog"],
    ]
    .iter()
    .enumerate()
    {
        let observations: Vec<Observation> = texts
            .iter()
            .enumerate()
            .map(|(index, text)| {
                let created_at_ms = (batch * 100 + index) as i64;
                Observation {
                    schema: MANIFEST_SCHEMA,
                    observation_id: qm_core::derive_observation_id(
                        &session,
                        "probe",
                        "tool_use",
                        text,
                        created_at_ms,
                    ),
                    session_id: session.clone(),
                    actor: "probe".into(),
                    kind: "tool_use".into(),
                    text: (*text).to_string(),
                    created_at_ms,
                }
            })
            .collect();
        store
            .ingest_observations(IngestObservationsRequest {
                workspace_id: workspace.clone(),
                project_id: project.clone(),
                session_id: session.clone(),
                writer_id: writer.clone(),
                observations,
                now_ms: batch as i64,
            })
            .await
            .map_err(|error| anyhow::anyhow!("{error}"))?;
    }

    let compiled =
        consolidate_session(&store, &workspace, &project, &session, &writer, 10, 60_000).await?;
    if compiled.skipped || compiled.already_up_to_date {
        failures.push("consolidation did not compile a fresh page".to_string());
    }
    let page_path = qm_core::PagePath::new(format!("sessions/{session}.md"))?;
    let page = store
        .read_page(&workspace, &project, &page_path)
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?
        .context("compiled page missing")?;
    if !page.body.contains("tantivy splits") {
        failures.push("compiled page is missing captured text".to_string());
    }

    let build_root = tempfile::TempDir::new().context("build dir")?;
    let doc = PageDoc::from_version(&workspace, &project, &page, 10);
    let published = publish_split_index(
        bucket.as_ref(),
        &store,
        &workspace,
        &project,
        &writer,
        1,
        &[doc],
        &build_root.path().join("session"),
        10,
    )
    .await?;

    let reader = ProjectStore::new(std::sync::Arc::clone(bucket), "v1");
    let cache = tempfile::TempDir::new().context("cache dir")?;
    let found = qm_search::search_project(
        bucket.as_ref(),
        &reader,
        &workspace,
        &project,
        cache.path(),
        "tantivy",
        10,
    )
    .await?;
    if found.hits.len() != 1 || found.hits[0].path != format!("sessions/{session}.md") {
        failures.push(format!("compiled page is not searchable: {:?}", found.hits));
    }

    let again = consolidate_session(
        &reader,
        &workspace,
        &project,
        &session,
        &WriterId::new("probe-other")?,
        20,
        60_000,
    )
    .await?;
    if !again.already_up_to_date {
        failures.push("recompiling an unchanged chain wrote a new version".to_string());
    }

    let chain = reader
        .read_session_chain(&workspace, &project, &session)
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    Ok(SessionScenarioReport {
        workspace: workspace.to_string(),
        project: project.to_string(),
        observations: compiled.observations,
        segments: chain.len(),
        splits: published.generation as usize,
        hits: found.hits.len(),
        failures,
    })
}
