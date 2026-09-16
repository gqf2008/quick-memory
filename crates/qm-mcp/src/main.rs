//! `qm-mcp` — the MCP stdio server.
//!
//! Scope and credentials come from the environment, exactly as for the `qm`
//! command line (`QM_WORKSPACE` / `QM_PROJECT` / `QM_WRITER`, `QM_S3_*` or
//! `R2_*`, optional `QM_CACHE_DIR`), and fall back to the machine-local config
//! file when the environment leaves them empty — see [`qm_cli::config`]. Both
//! surfaces read that file through the same resolver, so a server started from
//! an MCP client that sets nothing sees what the command line sees.

use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use qm_cli::{bucket_identity_from_env, build_bucket_from_env, config, manifest_format_from_env};
use qm_mcp::MemoryServer;
use rmcp::ServiceExt;
use rmcp::transport::io::stdio;

/// Command line options.
#[derive(Debug, Parser)]
#[command(name = "qm-mcp", about = "MCP server for quick-memory")]
struct Args {
    /// Serve an in-process, non-durable bucket.
    ///
    /// Only for protocol tests and local exploration: nothing is persisted and
    /// no such store can provide the CAS semantics a real deployment needs.
    #[arg(long)]
    synthetic_bucket: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    // Load the config file before reading any setting: an MCP client that
    // passes no environment at all is a normal way to run this, and the file is
    // then the only thing standing between it and a working bucket.
    config::init()?;
    let setting = |name: &str| config::var(name).filter(|value| !value.trim().is_empty());
    let workspace = setting("QM_WORKSPACE").unwrap_or_else(|| "default".to_string());
    let project = setting("QM_PROJECT").unwrap_or_else(|| "default".to_string());
    let writer = setting("QM_WRITER").unwrap_or_else(|| "machine".to_string());
    let cache_dir = setting("QM_CACHE_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join(format!("qm-cache-{writer}")));

    let bucket: Arc<dyn object_store::ObjectStore> = if args.synthetic_bucket {
        eprintln!(
            "WARNING: --synthetic-bucket serves a non-durable in-memory store; \
             this does not verify any real backend"
        );
        Arc::new(object_store::memory::InMemory::new())
    } else {
        build_bucket_from_env()?
    };
    // Cache keys such as the publish watermark are facts about one bucket.
    // Naming the bucket keeps the same cache directory safe when an operator
    // changes the endpoint or bucket in the MCP configuration.
    let bucket_identity = bucket_identity_from_env();
    let server = MemoryServer::new(bucket, workspace, project, writer, cache_dir)
        .with_bucket_identity(bucket_identity)
        // Resolved through the same parser the `qm` binary uses, so the two
        // surfaces cannot disagree about what the setting means.
        .with_manifest_format(manifest_format_from_env()?);
    let service = server.serve(stdio()).await?;
    service.waiting().await?;
    Ok(())
}
