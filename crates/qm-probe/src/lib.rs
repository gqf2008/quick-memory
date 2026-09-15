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
        assert!(report.attempts >= 12);
        assert_eq!(
            delete_scope(&bucket, &report.workspace).await.unwrap(),
            12 + 12 + 1
        );
    }
}
