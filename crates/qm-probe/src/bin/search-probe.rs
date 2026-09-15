//! Publish or query an index held in object storage.
//!
//! `build`/`query` prove the S0 question (another machine can search a
//! published split). `project` proves the S2 one: splits published by several
//! machines are searched together, and every hit is checked against the
//! authoritative manifest before it is returned.

use std::io::BufRead;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use qm_core::{PagePath, PageVersion, ProjectId, WorkspaceId, WriterId};
use qm_probe::{S3Config, delete_scope, init_tracing, run_search_scenario};
use qm_search::{
    PageDoc, SearchTuning, attach_embeddings, build_index, materialize, publish_split_index,
    search, search_project_tuned, upload_dir,
};
use qm_store::{CommitPageRequest, ProjectStore};

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
    /// Commit vector-bearing pages to the authority and publish their split.
    VectorPublish {
        /// JSONL file with one `{path,title,body}` object per line.
        docs: PathBuf,
        /// Workspace the pages belong to.
        #[arg(long)]
        workspace: String,
        /// Project the pages belong to.
        #[arg(long)]
        project: String,
        /// Writer id recorded on the commit.
        #[arg(long, default_value = "probe-vector-machine-a")]
        writer: String,
        /// First commit timestamp, in milliseconds.
        #[arg(long, default_value_t = 1)]
        now_ms: i64,
    },
    /// Materialise the index into a local cache and query it.
    Query {
        /// Query text.
        query: String,
        /// Maximum hits to return.
        #[arg(long, default_value_t = 10)]
        limit: usize,
    },
    /// Query a project's authoritative catalog with a configured embedder.
    VectorQuery {
        /// Workspace the project belongs to.
        #[arg(long)]
        workspace: String,
        /// Project to query.
        #[arg(long)]
        project: String,
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
        Command::Project { .. } | Command::VectorPublish { .. } | Command::VectorQuery { .. } => {
            None
        }
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
        Command::VectorPublish {
            docs,
            workspace,
            project,
            writer,
            now_ms,
        } => {
            let pages = read_source_pages(&docs)?;
            if pages.is_empty() {
                bail!("no pages in {}", docs.display());
            }
            let workspace_id = WorkspaceId::new(&workspace)?;
            let project_id = ProjectId::new(&project)?;
            let writer_id = WriterId::new(&writer)?;
            let project_store = ProjectStore::new(Arc::clone(&store), "v1");
            let embedder = qm_search::vector::embedder_from_env()?.context(
                "vector-publish requires an embedding provider; refusing to publish a split without vectors",
            )?;

            let mut committed = Vec::with_capacity(pages.len());
            for (index, page) in pages.iter().enumerate() {
                let path = PagePath::new(&page.path)?;
                let timestamp = now_ms + index as i64;
                let outcome = project_store
                    .commit_page(CommitPageRequest {
                        workspace_id: workspace_id.clone(),
                        project_id: project_id.clone(),
                        path: path.clone(),
                        title: page.title.clone(),
                        body: page.body.clone(),
                        writer_id: writer_id.clone(),
                        now_ms: timestamp,
                    })
                    .await?;
                let version = PageVersion {
                    page_id: outcome.page_id,
                    path,
                    title: page.title.clone(),
                    body: page.body.clone(),
                    supersedes: outcome.supersedes,
                };
                committed.push(PageDoc::from_version(
                    &workspace_id,
                    &project_id,
                    &version,
                    timestamp,
                ));
            }
            let pages = attach_embeddings(committed, Some(embedder.as_ref())).await?;
            let build_dir = tempfile::TempDir::new().context("creating build dir")?;
            let outcome = publish_split_index(
                store.as_ref(),
                &project_store,
                &workspace_id,
                &project_id,
                &writer_id,
                1,
                &pages,
                build_dir.path(),
                now_ms,
            )
            .await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "workspace_id": workspace_id,
                    "project_id": project_id,
                    "pages": pages.len(),
                    "generation": outcome.generation,
                    "catalog_key": outcome.catalog_key,
                }))?
            );
        }
        Command::VectorQuery {
            workspace,
            project,
            query,
            limit,
        } => {
            let workspace_id = WorkspaceId::new(&workspace)?;
            let project_id = ProjectId::new(&project)?;
            let project_store = ProjectStore::new(Arc::clone(&store), "v1");
            let embedder = qm_search::vector::embedder_from_env()?.context(
                "vector-query requires QM_EMBEDDING_BASE_URL; refusing to fall back to keyword-only search",
            )?;
            let tuning = SearchTuning {
                neighbor_expansion: false,
                now_ms: 0,
                ..SearchTuning::default()
            };
            let cache = tempfile::TempDir::new().context("creating cache dir")?;
            let outcome = search_project_tuned(
                store.as_ref(),
                &project_store,
                &workspace_id,
                &project_id,
                cache.path(),
                &query,
                limit,
                &tuning,
                Some(embedder.as_ref()),
            )
            .await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "workspace_id": workspace_id,
                    "project_id": project_id,
                    "query": query,
                    "hits": outcome.hits,
                    "splits_searched": outcome.splits_searched,
                    "candidates": outcome.candidates,
                    "filtered_out": outcome.filtered_out,
                    "streams_active": outcome.streams_active,
                    "stream_candidates": outcome.stream_candidates,
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

#[derive(Debug)]
struct SourcePage {
    /// Page path inside the project.
    path: String,
    /// Page title.
    title: String,
    /// Page body.
    body: String,
}

fn read_source_pages(path: &std::path::Path) -> Result<Vec<SourcePage>> {
    let file = std::fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut pages = Vec::new();
    for (line_no, line) in std::io::BufReader::new(file).lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let page: serde_json::Value = serde_json::from_str(&line)
            .with_context(|| format!("parsing {}:{}", path.display(), line_no + 1))?;
        pages.push(SourcePage {
            path: page
                .get("path")
                .and_then(serde_json::Value::as_str)
                .with_context(|| {
                    format!("{}:{} has no string `path`", path.display(), line_no + 1)
                })?
                .to_string(),
            title: page
                .get("title")
                .and_then(serde_json::Value::as_str)
                .with_context(|| {
                    format!("{}:{} has no string `title`", path.display(), line_no + 1)
                })?
                .to_string(),
            body: page
                .get("body")
                .and_then(serde_json::Value::as_str)
                .with_context(|| {
                    format!("{}:{} has no string `body`", path.display(), line_no + 1)
                })?
                .to_string(),
        });
    }
    Ok(pages)
}
