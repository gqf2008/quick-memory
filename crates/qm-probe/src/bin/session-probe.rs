//! Capture, compile, and search a session against a real backend.
//!
//! Runs the S4 scenario end to end: observations are ingested into a session
//! chain, compiled into a page, published as a split, and found by a fresh
//! reader. Then it recompiles and requires that nothing new was written.

use anyhow::Result;
use clap::Parser;
use qm_probe::{S3Config, delete_scope, init_tracing, run_session_scenario};

#[derive(Debug, Parser)]
#[command(about = "Verify capture → compile → search against a real backend")]
struct Args {
    /// Leave the probe scope in the bucket instead of deleting it.
    #[arg(long, default_value_t = false)]
    keep: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let args = Args::parse();
    let config = S3Config::from_env()?;
    let store = config.build_store()?;

    let report = run_session_scenario(&store, None).await?;
    for failure in &report.failures {
        eprintln!("FAIL {failure}");
    }
    println!(
        "observations={} segments={} splits={} hits={}",
        report.observations, report.segments, report.splits, report.hits
    );
    let mut result = Ok(());
    if report.failures.is_empty() {
        println!("all checks passed");
    } else {
        result = Err(anyhow::anyhow!("{} checks failed", report.failures.len()));
    }
    if !args.keep {
        let deleted = delete_scope(&store, &report.workspace).await?;
        println!("cleaned up {deleted} objects");
    }
    result
}
