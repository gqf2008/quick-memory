//! `qm` — the agent-facing entry point.
//!
//! Credentials and scope come from the environment (`QM_S3_*`, `R2_*` for the
//! bucket; `QM_WORKSPACE` / `QM_PROJECT` / `QM_WRITER` for scope). Nothing is
//! cached between commands: every invocation re-reads the authoritative state,
//! which is what lets several machines share one bucket without coordination.

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use clap::Parser;
use qm_cli::{Cli, Context as CommandContext, build_bucket_from_env, execute};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0);
    let cache_dir = cli
        .cache_dir
        .clone()
        .unwrap_or_else(|| std::env::temp_dir().join(format!("qm-cache-{}", cli.writer)));
    let bucket = build_bucket_from_env()?;
    let ctx = CommandContext::new(
        bucket,
        &cli.workspace,
        &cli.project,
        &cli.writer,
        cache_dir,
        now_ms,
        cli.json,
    )?;
    let output = execute(&cli, ctx).await?;
    println!("{output}");
    Ok(())
}
