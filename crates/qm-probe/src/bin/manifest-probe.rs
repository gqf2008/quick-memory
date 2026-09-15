//! Multi-machine commit probe against a real S3/R2 backend.
//!
//! Runs the S1 acceptance scenario for real: several independent clients, each
//! with its own handle on the same bucket, commit versions concurrently into
//! one project. Then it re-reads the result as a fresh machine and checks that
//! no write was lost, every supersession chain is intact, and the WAL is
//! complete.
//!
//! A backend that cannot do conditional writes is rejected up front: the probe
//! first runs the CAS conformance check, so a local filesystem (which does not
//! implement `If-Match`) fails immediately and clearly instead of somewhere
//! in the middle of the scenario.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use clap::Parser;
use object_store::ObjectStore;
use qm_probe::{
    ManifestScenarioParams, S3Config, delete_scope, init_tracing, run_manifest_scenario,
};
use qm_store::{CasStore, verify_conditional_writes};

#[derive(Debug, Parser)]
#[command(about = "Verify concurrent multi-machine commits against a real backend")]
struct Args {
    /// Number of simulated machines writing concurrently.
    #[arg(long, default_value_t = 3)]
    machines: usize,
    /// Versions each machine writes to its own path.
    #[arg(long, default_value_t = 5)]
    writes: usize,
    /// Commit attempts before a machine gives up on the CAS race.
    #[arg(long, default_value_t = 32)]
    max_attempts: u32,
    /// Leave the probe scope in the bucket instead of deleting it.
    #[arg(long, default_value_t = false)]
    keep: bool,
    /// Run against a local directory instead of S3/R2.
    ///
    /// For smoke-testing the probe itself. Expect this to fail: a local
    /// filesystem does not implement conditional writes, which is exactly why
    /// it is not a supported backend.
    #[arg(long)]
    local: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let args = Args::parse();
    let (bucket, endpoint): (Arc<dyn ObjectStore>, String) = match &args.local {
        Some(dir) => {
            std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
            let store = object_store::local::LocalFileSystem::new_with_prefix(dir)
                .with_context(|| format!("opening {}", dir.display()))?;
            (Arc::new(store), format!("local:{}", dir.display()))
        }
        None => {
            let config = S3Config::from_env()?;
            (config.build_store()?, config.endpoint.clone())
        }
    };
    if args.local.is_some() {
        println!("WARNING: local backend — this does not verify any real object store");
    }
    println!(
        "probing endpoint={endpoint} machines={} writes={}",
        args.machines, args.writes
    );

    // Pre-flight: the scenario is meaningless without conditional writes.
    let cas = CasStore::new(Arc::clone(&bucket), "");
    let probe_key = "qm-probe/cas-preflight";
    match verify_conditional_writes(&cas, probe_key).await {
        Ok(report) => {
            println!("{}", report.render());
        }
        Err(error) => {
            bail!("backend does not implement the CAS contract this design requires:\n{error}");
        }
    }
    let _ = cas.delete(probe_key).await;

    let report = run_manifest_scenario(
        &bucket,
        &ManifestScenarioParams {
            machines: args.machines,
            writes: args.writes,
            max_attempts: args.max_attempts,
            scope_suffix: None,
        },
    )
    .await?;

    for failure in &report.failures {
        eprintln!("FAIL {failure}");
    }
    println!(
        "commits={} attempts={} pages={} wal={}",
        report.commits, report.attempts, report.pages, report.wal_records
    );
    if report.attempts == report.commits as u32 {
        println!("note: no CAS conflict occurred in this run");
    }

    let mut result = Ok(());
    if !report.failures.is_empty() {
        result = Err(anyhow::anyhow!("{} checks failed", report.failures.len()));
    } else {
        println!("all checks passed");
    }

    if !args.keep {
        let deleted = delete_scope(&bucket, &report.workspace).await?;
        println!("cleaned up {deleted} objects");
    }
    result
}
