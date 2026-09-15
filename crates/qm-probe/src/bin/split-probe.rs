//! Read a Quickwit `.split` container with quick-memory's own parser.
//!
//! This answers the question the design hinges on: can a machine with no
//! Quickwit cluster open an index Quickwit produced? The probe unpacks the
//! container into a temp directory and searches it with tantivy, exactly as the
//! read path does for a published split.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use qm_probe::init_tracing;
use qm_search::quickwit_split::{extract_split, is_split};
use qm_search::search;

#[derive(Debug, Parser)]
#[command(about = "Unpack a Quickwit .split and search it with tantivy")]
struct Args {
    /// The `.split` file to read.
    #[arg(long)]
    file: PathBuf,
    /// Query text.
    #[arg(long)]
    query: String,
    /// Maximum hits.
    #[arg(long, default_value_t = 5)]
    limit: usize,
    /// Keep the extracted files here instead of a temp directory.
    #[arg(long)]
    extract_to: Option<PathBuf>,
}

fn main() -> Result<()> {
    init_tracing();
    let args = Args::parse();
    let bytes =
        std::fs::read(&args.file).with_context(|| format!("reading {}", args.file.display()))?;
    println!(
        "split={} bytes={} recognised={}",
        args.file.display(),
        bytes.len(),
        is_split(&bytes)
    );

    let temp;
    let destination = match &args.extract_to {
        Some(path) => path.clone(),
        None => {
            temp = tempfile::TempDir::new().context("creating temp dir")?;
            temp.path().to_path_buf()
        }
    };
    let report = extract_split(&bytes, &destination).context("extracting the split")?;
    println!(
        "extracted {} file(s), hotcache {} bytes",
        report.files.len(),
        report.hotcache_bytes
    );
    for file in &report.files {
        println!("  {file}");
    }

    let hits = search(&destination, &args.query, args.limit).context("searching the split")?;
    println!("hits={}", hits.len());
    for hit in &hits {
        println!(
            "  {:.4}\t{}\t{}\t[{}]",
            hit.score,
            hit.path,
            hit.title,
            hit.streams.join(",")
        );
    }
    if hits.is_empty() {
        anyhow::bail!("no hits: the split was readable but did not answer the query");
    }
    Ok(())
}
