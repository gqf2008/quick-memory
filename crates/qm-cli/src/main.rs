//! `qm` — the agent-facing entry point.
//!
//! Credentials and scope come from the environment (`QM_S3_*`, `R2_*` for the
//! bucket; `QM_WORKSPACE` / `QM_PROJECT` / `QM_WRITER` for scope). Nothing is
//! cached between commands: every invocation re-reads the authoritative state,
//! which is what lets several machines share one bucket without coordination.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, Result};
use clap::Parser;
use object_store::aws::AmazonS3Builder;
use qm_cli::{Cli, Context as CommandContext, execute};

fn env_first(names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| {
        std::env::var(name)
            .ok()
            .filter(|value| !value.trim().is_empty())
    })
}

fn require(names: &[&str], what: &str) -> Result<String> {
    env_first(names).ok_or_else(|| {
        anyhow::anyhow!("missing {what}; set one of {names:?} (a command that cannot reach the bucket must fail, not guess)")
    })
}

fn build_bucket() -> Result<Arc<dyn object_store::ObjectStore>> {
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

    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(&bucket)
        .with_region(&region)
        .with_virtual_hosted_style_request(!force_path_style)
        .with_access_key_id(&access_key_id)
        .with_secret_access_key(&secret_access_key)
        .with_endpoint(&endpoint);
    if endpoint.starts_with("http://") {
        builder = builder.with_allow_http(true);
    }
    Ok(Arc::new(builder.build().context("building S3 client")?))
}

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
    let bucket = build_bucket()?;
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
