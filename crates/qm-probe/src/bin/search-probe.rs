//! Build an index into object storage, or search one published by someone else.
//!
//! This is the S0 proof for "any machine can search the full corpus": the
//! `query` side shares nothing with the `build` side except the bucket.

use std::io::BufRead;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use qm_probe::{S3Config, init_tracing};
use qm_search::{PageDoc, build_index, materialize, search, upload_dir};

#[derive(Debug, Parser)]
#[command(about = "Publish or query a tantivy index held in object storage")]
struct Args {
    /// Where the index files live, relative to the configured prefix.
    #[arg(long)]
    split_prefix: String,
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
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let args = Args::parse();
    let config = S3Config::from_env()?;
    let store = config.build_store()?;
    let prefix = format!(
        "{}/{}",
        config.prefix.trim_matches('/'),
        args.split_prefix.trim_matches('/')
    );

    match args.command {
        Command::Build { docs } => {
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
    }
    Ok(())
}
