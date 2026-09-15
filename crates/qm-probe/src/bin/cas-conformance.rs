//! Probe a real S3/R2 backend for ETag-based conditional writes.
//!
//! Exit codes: 0 = every step passed, 1 = a step failed, 2 = not configured.

use anyhow::Result;
use clap::Parser;
use qm_probe::{S3Config, init_tracing};
use qm_store::{CasStore, verify_conditional_writes};

#[derive(Debug, Parser)]
#[command(about = "Verify create-if-absent and update-if-match against a real backend")]
struct Args {
    /// Key suffix for the probe object (defaults to a timestamp).
    #[arg(long)]
    key_suffix: Option<String>,
    /// Leave the probe object in place instead of deleting it.
    #[arg(long, default_value_t = false)]
    keep: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let args = Args::parse();
    let config = S3Config::from_env()?;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let suffix = args.key_suffix.unwrap_or_else(|| nanos.to_string());
    let key = format!("{}/cas-probe-{suffix}", config.prefix.trim_matches('/'));

    println!(
        "probing endpoint={} bucket={} prefix={}",
        config.endpoint, config.bucket, config.prefix
    );
    let store = CasStore::new(config.build_store()?, "");
    let report = verify_conditional_writes(&store, &key).await?;
    println!("{}", report.render());
    println!("all steps passed");

    if !args.keep {
        store.delete(&key).await?;
        println!("cleaned up {key}");
    }
    Ok(())
}
