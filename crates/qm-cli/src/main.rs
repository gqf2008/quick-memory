//! `qm` — the agent-facing entry point.
//!
//! Credentials and scope come from the environment (`QM_S3_*`, `R2_*` for the
//! bucket; `QM_WORKSPACE` / `QM_PROJECT` / `QM_WRITER` for scope), or from the
//! machine-local config file when the environment leaves them empty — see
//! [`qm_cli::config`]. Nothing is cached between commands: every invocation
//! re-reads the authoritative state, which is what lets several machines share
//! one bucket without coordination.

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Result, anyhow};
use clap::parser::ValueSource;
use clap::{CommandFactory as _, FromArgMatches as _};
use qm_cli::{
    Cli, Context as CommandContext, MaintainFailure, bucket_identity_from_env,
    build_bucket_from_env, config, execute,
};

#[tokio::main]
async fn main() -> Result<()> {
    // The config file has to be read before anything consults a setting, and
    // its values have to be merged into the settings clap resolved on its own:
    // `--workspace` and friends carry `env = ...`, so clap has already filled
    // them from the environment or from their default, without saying which.
    config::init()?;
    let matches = Cli::command().get_matches();
    let mut cli = Cli::from_arg_matches(&matches)?;
    let typed = |id: &str| matches!(matches.value_source(id), Some(ValueSource::CommandLine));
    cli.workspace = config::merge_flagged(
        config::var,
        typed("workspace"),
        cli.workspace,
        "QM_WORKSPACE",
        "default",
    );
    cli.project = config::merge_flagged(
        config::var,
        typed("project"),
        cli.project,
        "QM_PROJECT",
        "default",
    );
    cli.writer = config::merge_flagged(
        config::var,
        typed("writer"),
        cli.writer,
        "QM_WRITER",
        "machine",
    );
    cli.cache_dir = config::merge_optional_flagged(
        config::var,
        typed("cache_dir"),
        cli.cache_dir,
        "QM_CACHE_DIR",
    );
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0);
    let cache_dir = cli
        .cache_dir
        .clone()
        .unwrap_or_else(|| std::env::temp_dir().join(format!("qm-cache-{}", cli.writer)));
    let bucket = build_bucket_from_env()?;
    // Local caches record facts about a bucket, so their keys have to name it:
    // the same cache directory is otherwise reused across buckets, and a
    // watermark that describes one bucket silently suppresses a publish into
    // another.
    let ctx = CommandContext::new(
        bucket,
        &cli.workspace,
        &cli.project,
        &cli.writer,
        cache_dir,
        now_ms,
        cli.json,
    )?
    .with_bucket_identity(bucket_identity_from_env());
    let output = match execute(&cli, ctx).await {
        Ok(output) => output,
        Err(error) => {
            // `maintain` returns a complete rendered summary with its failure.
            // Keep that summary on stdout (so `--json` remains machine-readable)
            // and still give schedulers a non-zero process status.
            if let Some(failure) = error.downcast_ref::<MaintainFailure>() {
                println!("{}", failure.report());
                return Err(anyhow!("maintain completed with failures"));
            }
            return Err(error);
        }
    };
    println!("{output}");
    Ok(())
}
