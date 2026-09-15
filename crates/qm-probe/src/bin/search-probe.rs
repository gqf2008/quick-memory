//! Publish or query an index held in object storage.
//!
//! `build`/`query` prove the S0 question (another machine can search a
//! published split). `project` proves the S2 one: splits published by several
//! machines are searched together, and every hit is checked against the
//! authoritative manifest before it is returned.

use std::io::BufRead;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use qm_probe::{S3Config, delete_scope, init_tracing, run_search_scenario};
use qm_search::{PageDoc, build_index, materialize, search, upload_dir};

#[derive(Debug, Parser)]
#[command(about = "Publish or query a tantivy index held in object storage")]
struct Args {
    /// Where the index files live, relative to the configured prefix.
    /// Required for `build` and `query`.
    #[arg(long)]
    split_prefix: Option<String>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Build an index from a JSONL file of PageDoc and upload it.
    Build {
        /// JSONL file with one PageDoc per line.
        docs: PathBuf,
    },
    /// Materialise the index into a local cache and query it.
    Query {
        /// Query text.
        query: String,
        /// Maximum hits to return.
        #[arg(long, default_value_t = 10)]
        limit: usize,
    },
    /// Run the multi-machine publish-and-search scenario against the backend.
    Project {
        /// Leave the probe scope in the bucket instead of deleting it.
        #[arg(long, default_value_t = false)]
        keep: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let args = Args::parse();
    let config = S3Config::from_env()?;
    let store = config.build_store()?;
    // Resolve the split prefix before the command is moved out of `args`.
    let prefix = match &args.command {
        Command::Build { .. } | Command::Query { .. } => Some(split_prefix(&args, &config)?),
        Command::Project { .. } => None,
    };

    match args.command {
        Command::Build { docs } => {
            let prefix = prefix.expect("resolved above");
            let file = std::fs::File::open(&docs)
                .with_context(|| format!("opening {}", docs.display()))?;
            let mut pages = Vec::new();
            for (line_no, line) in std::io::BufReader::new(file).lines().enumerate() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                let page: PageDoc = serde_json::from_str(&line)
                    .with_context(|| format!("parsing {}:{}", docs.display(), line_no + 1))?;
                pages.push(page);
            }
            if pages.is_empty() {
                bail!("no pages in {}", docs.display());
            }
            let local = tempfile::TempDir::new().context("creating build dir")?;
            build_index(local.path(), &pages)?;
            let keys = upload_dir(store.as_ref(), &prefix, local.path()).await?;
            println!(
                "uploaded {} files ({} pages) to {prefix}",
                keys.len(),
                pages.len()
            );
        }
        Command::Query { query, limit } => {
            let prefix = prefix.expect("resolved above");
            let cache = tempfile::TempDir::new().context("creating cache dir")?;
            let files = materialize(store.as_ref(), &prefix, cache.path()).await?;
            let hits = search(cache.path(), &query, limit)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "materialized_files": files,
                    "split_prefix": prefix,
                    "query": query,
                    "hits": hits,
                }))?
            );
        }
        Command::Project { keep } => {
            let report = run_search_scenario(&store, None).await?;
            for failure in &report.failures {
                eprintln!("FAIL {failure}");
            }
            println!(
                "splits={} hits={} filtered_out={}",
                report.splits, report.hits, report.filtered_out
            );
            let mut result = Ok(());
            if report.failures.is_empty() {
                println!("all checks passed");
            } else {
                result = Err(anyhow::anyhow!("{} checks failed", report.failures.len()));
            }
            if !keep {
                let deleted = delete_scope(&store, &report.workspace).await?;
                println!("cleaned up {deleted} objects");
            }
            result?;
        }
    }
    Ok(())
}

fn split_prefix(args: &Args, config: &S3Config) -> Result<String> {
    let relative = args
        .split_prefix
        .as_deref()
        .context("--split-prefix is required for build and query")?;
    Ok(format!(
        "{}/{}",
        config.prefix.trim_matches('/'),
        relative.trim_matches('/')
    ))
}
