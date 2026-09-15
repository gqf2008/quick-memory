//! The `qm` command surface.
//!
//! Every command is a thin wrapper over the library calls, so the CLI adds no
//! protocol of its own: whatever it can do, the MCP server can do, and the
//! tests exercise the same dispatch the binary uses (against an in-memory
//! bucket, which is why they need no credentials).
//!
//! Scope rules that hold everywhere: the workspace and project come from the
//! caller, the machine identity is explicit, and the intake boundary (scrub +
//! bound) is applied by the store, not by a flag.

use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context as _, Result, bail};
use clap::{Parser, Subcommand};
use object_store::ObjectStore;
use qm_core::{
    MANIFEST_SCHEMA, Observation, PagePath, ProjectId, SessionId, WorkspaceId, WriterId,
};
use qm_search::consolidate::consolidate_session;
use qm_search::{PageDoc, compact_project, publish_split_index, search_project};
use qm_store::{CommitPageRequest, IngestObservationsRequest, ProjectStore};

/// Command line interface.
#[derive(Debug, Parser)]
#[command(name = "qm", about = "Shared agent memory on object storage")]
pub struct Cli {
    /// Workspace to operate in.
    #[arg(long, env = "QM_WORKSPACE", default_value = "default", global = true)]
    pub workspace: String,
    /// Project to operate in.
    #[arg(long, env = "QM_PROJECT", default_value = "default", global = true)]
    pub project: String,
    /// Identity of this machine, recorded on every write.
    #[arg(long, env = "QM_WRITER", default_value = "machine", global = true)]
    pub writer: String,
    /// Directory for materialised index splits.
    #[arg(long, env = "QM_CACHE_DIR", global = true)]
    pub cache_dir: Option<PathBuf>,
    /// Emit JSON instead of text.
    #[arg(long, global = true)]
    pub json: bool,
    #[command(subcommand)]
    pub command: Command,
}

/// Every command the surface exposes.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Capture one observation into a session.
    Capture {
        /// Session id.
        #[arg(long)]
        session: String,
        /// Event kind, e.g. `tool_use`.
        #[arg(long, default_value = "observation")]
        kind: String,
        /// Actor that emitted the event.
        #[arg(long, default_value = "qm")]
        actor: String,
        /// Event text; reads stdin when omitted.
        #[arg(long)]
        text: Option<String>,
        /// Client timestamp in milliseconds.
        #[arg(long)]
        at: Option<i64>,
    },
    /// Compile a session's observations into its page.
    Consolidate {
        /// Session id.
        #[arg(long)]
        session: String,
    },
    /// List the sessions of the project.
    Sessions,
    /// Search the project.
    Search {
        /// Query text.
        query: String,
        /// Maximum hits.
        #[arg(long, default_value_t = 10)]
        limit: usize,
    },
    /// Rebuild this machine's split from the current pages and publish it.
    Publish,
    /// Rebuild the whole index from authoritative pages (lease-guarded).
    Compact,
    /// Show what the project currently contains.
    Status,
    /// Commit a page.
    WritePage {
        /// Path inside the project.
        #[arg(long)]
        path: String,
        /// Page title; defaults to the path.
        #[arg(long)]
        title: Option<String>,
        /// Body text; reads stdin when omitted.
        #[arg(long)]
        body: Option<String>,
    },
    /// Print a page.
    ReadPage {
        /// Path inside the project.
        #[arg(long)]
        path: String,
    },
    /// Tombstone a page.
    DeletePage {
        /// Path inside the project.
        #[arg(long)]
        path: String,
    },
}

/// Everything a command needs, already resolved.
pub struct Context {
    /// The object store backing the bucket.
    pub bucket: Arc<dyn ObjectStore>,
    /// Project-scoped commit protocol.
    pub project: ProjectStore,
    /// Workspace scope.
    pub workspace: WorkspaceId,
    /// Project scope.
    pub project_id: ProjectId,
    /// Machine identity.
    pub writer: WriterId,
    /// Cache directory for materialised splits.
    pub cache_dir: PathBuf,
    /// Timestamp recorded on this command's writes.
    pub now_ms: i64,
    /// Whether to render JSON.
    pub json: bool,
}

impl Context {
    /// Build a context from resolved scope and a bucket.
    ///
    /// # Errors
    /// Fails when the scope identifiers are not valid object-key segments.
    pub fn new(
        bucket: Arc<dyn ObjectStore>,
        workspace: &str,
        project: &str,
        writer: &str,
        cache_dir: PathBuf,
        now_ms: i64,
        json: bool,
    ) -> Result<Self> {
        Ok(Self {
            project: ProjectStore::new(Arc::clone(&bucket), "v1"),
            bucket,
            workspace: WorkspaceId::new(workspace)?,
            project_id: ProjectId::new(project)?,
            writer: WriterId::new(writer)?,
            cache_dir,
            now_ms,
            json,
        })
    }

    fn page(&self, path: &str) -> Result<PagePath> {
        Ok(PagePath::new(path)?)
    }
}

/// Run one command, returning the text to print.
///
/// # Errors
/// Propagates storage, index, and parse failures; a caller-facing message is
/// included so the binary can print it verbatim.
pub async fn execute(cli: &Cli, mut ctx: Context) -> Result<String> {
    ctx.json = cli.json;
    match &cli.command {
        Command::Capture {
            session,
            kind,
            actor,
            text,
            at,
        } => {
            let session_id = SessionId::new(session)?;
            let text = match text {
                Some(text) => text.clone(),
                None => read_stdin()?,
            };
            let created_at_ms = at.unwrap_or(ctx.now_ms);
            let observation = Observation {
                schema: MANIFEST_SCHEMA,
                observation_id: qm_core::derive_observation_id(
                    &session_id,
                    actor,
                    kind,
                    &text,
                    created_at_ms,
                ),
                session_id: session_id.clone(),
                actor: actor.clone(),
                kind: kind.clone(),
                text,
                created_at_ms,
            };
            let outcome = ctx
                .project
                .ingest_observations(IngestObservationsRequest {
                    workspace_id: ctx.workspace.clone(),
                    project_id: ctx.project_id.clone(),
                    session_id,
                    writer_id: ctx.writer.clone(),
                    observations: vec![observation],
                    now_ms: ctx.now_ms,
                })
                .await
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            Ok(if ctx.json {
                serde_json::json!({
                    "already_present": outcome.already_present,
                    "segments": outcome.generation,
                    "observations": outcome.count,
                })
                .to_string()
            } else if outcome.already_present {
                "already captured (identical event)".to_string()
            } else {
                format!(
                    "captured: {} observation(s) in session, {} total",
                    outcome.accepted, outcome.count
                )
            })
        }
        Command::Consolidate { session } => {
            let session_id = SessionId::new(session)?;
            let outcome = consolidate_session(
                &ctx.project,
                &ctx.workspace,
                &ctx.project_id,
                &session_id,
                &ctx.writer,
                ctx.now_ms,
                60_000,
            )
            .await?;
            Ok(if ctx.json {
                serde_json::json!({
                    "skipped": outcome.skipped,
                    "already_up_to_date": outcome.already_up_to_date,
                    "page_id": outcome.page_id,
                    "observations": outcome.observations,
                    "segments": outcome.segments,
                })
                .to_string()
            } else if outcome.skipped {
                "nothing to compile (or another machine holds the lease)".to_string()
            } else if outcome.already_up_to_date {
                format!(
                    "page already current ({} observations)",
                    outcome.observations
                )
            } else {
                format!(
                    "compiled {} observation(s) from {} segment(s)",
                    outcome.observations, outcome.segments
                )
            })
        }
        Command::Sessions => {
            let sessions = ctx
                .project
                .list_sessions(&ctx.workspace, &ctx.project_id)
                .await
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            Ok(if ctx.json {
                serde_json::to_string(&sessions)?
            } else if sessions.is_empty() {
                "no sessions".to_string()
            } else {
                sessions
                    .iter()
                    .map(|session| session.to_string())
                    .collect::<Vec<_>>()
                    .join("\n")
            })
        }
        Command::Search { query, limit } => {
            let outcome = search_project(
                ctx.bucket.as_ref(),
                &ctx.project,
                &ctx.workspace,
                &ctx.project_id,
                &ctx.cache_dir,
                query,
                *limit,
            )
            .await?;
            Ok(if ctx.json {
                serde_json::json!({
                    "hits": outcome.hits,
                    "splits_searched": outcome.splits_searched,
                    "filtered_out": outcome.filtered_out,
                })
                .to_string()
            } else if outcome.hits.is_empty() {
                format!(
                    "no hits ({} split(s) searched, {} filtered)",
                    outcome.splits_searched, outcome.filtered_out
                )
            } else {
                outcome
                    .hits
                    .iter()
                    .map(|hit| {
                        format!(
                            "{:.4}\t{}\t{}",
                            hit.score,
                            hit.path,
                            hit.title.replace('\n', " ")
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            })
        }
        Command::Publish => {
            let loaded = ctx
                .project
                .load(&ctx.workspace, &ctx.project_id)
                .await
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            let mut docs = Vec::with_capacity(loaded.manifest.pages.len());
            for (path, entry) in &loaded.manifest.pages {
                let page_path = PagePath::new(path)?;
                let page = ctx
                    .project
                    .read_page_version(&ctx.workspace, &ctx.project_id, &page_path, &entry.page_id)
                    .await
                    .map_err(|error| anyhow::anyhow!("{error}"))?;
                docs.push(PageDoc::from_version(
                    &ctx.workspace,
                    &ctx.project_id,
                    &page,
                    entry.created_at_ms,
                ));
            }
            if docs.is_empty() {
                bail!("nothing to publish: the project has no live pages");
            }
            let seq = loaded.manifest.seq + 1;
            let build_dir = ctx.cache_dir.join(format!("build-{seq}"));
            let outcome = publish_split_index(
                ctx.bucket.as_ref(),
                &ctx.project,
                &ctx.workspace,
                &ctx.project_id,
                &ctx.writer,
                seq,
                &docs,
                &build_dir,
                ctx.now_ms,
            )
            .await?;
            Ok(if ctx.json {
                serde_json::json!({
                    "generation": outcome.generation,
                    "already_present": outcome.already_present,
                    "pages": docs.len(),
                })
                .to_string()
            } else if outcome.already_present {
                format!("split already published ({} pages)", docs.len())
            } else {
                format!(
                    "published {} page(s) as one split (generation {})",
                    docs.len(),
                    outcome.generation
                )
            })
        }
        Command::Compact => {
            let build_dir = ctx.cache_dir.join("compact");
            let outcome = compact_project(
                ctx.bucket.as_ref(),
                &ctx.project,
                &ctx.workspace,
                &ctx.project_id,
                &ctx.writer,
                &build_dir,
                ctx.now_ms,
                60_000,
            )
            .await?;
            Ok(if ctx.json {
                serde_json::json!({
                    "skipped": outcome.skipped,
                    "pages": outcome.pages,
                    "splits_before": outcome.splits_before,
                    "splits_after": outcome.splits_after,
                })
                .to_string()
            } else if outcome.skipped {
                "another machine holds the compaction lease".to_string()
            } else {
                format!(
                    "compacted {} page(s): {} split(s) -> {}",
                    outcome.pages, outcome.splits_before, outcome.splits_after
                )
            })
        }
        Command::Status => {
            let loaded = ctx
                .project
                .load(&ctx.workspace, &ctx.project_id)
                .await
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            let catalog = ctx
                .project
                .load_catalog(&ctx.workspace, &ctx.project_id)
                .await
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            let sessions = ctx
                .project
                .list_sessions(&ctx.workspace, &ctx.project_id)
                .await
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            Ok(if ctx.json {
                serde_json::json!({
                    "workspace": ctx.workspace.to_string(),
                    "project": ctx.project_id.to_string(),
                    "manifest_seq": loaded.manifest.seq,
                    "pages": loaded.manifest.pages.len(),
                    "tombstones": loaded.manifest.tombstones.len(),
                    "catalog_generation": catalog.catalog.generation,
                    "splits": catalog.catalog.splits.len(),
                    "sessions": sessions.len(),
                })
                .to_string()
            } else {
                format!(
                    "{} / {}\n  pages: {}\n  tombstones: {}\n  manifest seq: {}\n  splits: {} (generation {})\n  sessions: {}",
                    ctx.workspace,
                    ctx.project_id,
                    loaded.manifest.pages.len(),
                    loaded.manifest.tombstones.len(),
                    loaded.manifest.seq,
                    catalog.catalog.splits.len(),
                    catalog.catalog.generation,
                    sessions.len()
                )
            })
        }
        Command::WritePage { path, title, body } => {
            let page_path = ctx.page(path)?;
            let body = match body {
                Some(body) => body.clone(),
                None => read_stdin()?,
            };
            let outcome = ctx
                .project
                .commit_page(CommitPageRequest {
                    workspace_id: ctx.workspace.clone(),
                    project_id: ctx.project_id.clone(),
                    path: page_path.clone(),
                    title: title.clone().unwrap_or_else(|| path.clone()),
                    body,
                    writer_id: ctx.writer.clone(),
                    now_ms: ctx.now_ms,
                })
                .await
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            Ok(if ctx.json {
                serde_json::json!({
                    "page_id": outcome.page_id.as_str(),
                    "manifest_seq": outcome.manifest_seq,
                    "supersedes": outcome.supersedes.map(|id| id.as_str().to_string()),
                })
                .to_string()
            } else {
                format!(
                    "committed {} at seq {} (page {})",
                    page_path.as_str(),
                    outcome.manifest_seq,
                    &outcome.page_id.as_str()[..12.min(outcome.page_id.as_str().len())]
                )
            })
        }
        Command::ReadPage { path } => {
            let page_path = ctx.page(path)?;
            let page = ctx
                .project
                .read_page(&ctx.workspace, &ctx.project_id, &page_path)
                .await
                .map_err(|error| anyhow::anyhow!("{error}"))?
                .with_context(|| format!("no page at {}", page_path.as_str()))?;
            Ok(if ctx.json {
                serde_json::json!({
                    "path": page.path.as_str(),
                    "page_id": page.page_id.as_str(),
                    "title": page.title,
                    "body": page.body,
                })
                .to_string()
            } else {
                page.body
            })
        }
        Command::DeletePage { path } => {
            let page_path = ctx.page(path)?;
            let outcome = ctx
                .project
                .delete_page(
                    &ctx.workspace,
                    &ctx.project_id,
                    &page_path,
                    &ctx.writer,
                    ctx.now_ms,
                )
                .await
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            Ok(if ctx.json {
                serde_json::json!({
                    "already_deleted": outcome.already_deleted,
                    "manifest_seq": outcome.manifest_seq,
                })
                .to_string()
            } else if outcome.already_deleted {
                format!("{} was already deleted", page_path.as_str())
            } else {
                format!(
                    "deleted {} at seq {}",
                    page_path.as_str(),
                    outcome.manifest_seq
                )
            })
        }
    }
}

fn read_stdin() -> Result<String> {
    let mut buffer = String::new();
    std::io::stdin()
        .read_to_string(&mut buffer)
        .context("reading stdin")?;
    let trimmed = buffer.trim_end_matches('\n').to_string();
    if trimmed.is_empty() {
        bail!("no input: pass --text/--body or pipe text on stdin");
    }
    Ok(trimmed)
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;
    use tempfile::TempDir;

    use super::*;

    fn context(bucket: Arc<dyn ObjectStore>, cache: &TempDir) -> Context {
        Context::new(
            bucket,
            "acme",
            "ai-memory",
            "mbp-a",
            cache.path().to_path_buf(),
            1_000,
            false,
        )
        .unwrap()
    }

    fn cli(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("qm").chain(args.iter().copied())).unwrap()
    }

    #[tokio::test]
    async fn capture_consolidate_and_search_round_trip() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = TempDir::new().unwrap();
        let ctx = context(Arc::clone(&bucket), &cache);

        let out = execute(
            &cli(&[
                "capture",
                "--session",
                "sess-1",
                "--kind",
                "tool_use",
                "--text",
                "switched the index to tantivy splits",
            ]),
            ctx,
        )
        .await
        .unwrap();
        assert!(out.contains("captured"), "{out}");

        let mut ctx = context(Arc::clone(&bucket), &cache);
        let out = execute(
            &cli(&["consolidate", "--session", "sess-1"]),
            Context {
                now_ms: 2_000,
                ..context(Arc::clone(&bucket), &cache)
            },
        )
        .await
        .unwrap();
        assert!(out.contains("compiled 1 observation"), "{out}");

        ctx.now_ms = 3_000;
        let out = execute(&cli(&["publish"]), ctx).await.unwrap();
        assert!(out.contains("published 1 page"), "{out}");

        let out = execute(&cli(&["search", "tantivy", "--json"]), {
            let mut ctx = context(Arc::clone(&bucket), &cache);
            ctx.cache_dir = TempDir::new().unwrap().path().to_path_buf();
            ctx
        })
        .await
        .unwrap();
        assert!(out.contains("sessions/sess-1.md"), "{out}");
    }

    #[tokio::test]
    async fn page_commands_commit_read_and_delete() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = TempDir::new().unwrap();

        let out = execute(
            &cli(&[
                "write-page",
                "--path",
                "notes/raft.md",
                "--title",
                "Raft",
                "--body",
                "leader election",
            ]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        assert!(out.contains("committed notes/raft.md"), "{out}");

        let mut ctx = context(Arc::clone(&bucket), &cache);
        ctx.now_ms = 2_000;
        let out = execute(&cli(&["read-page", "--path", "notes/raft.md"]), ctx)
            .await
            .unwrap();
        assert_eq!(out, "leader election");

        let mut ctx = context(Arc::clone(&bucket), &cache);
        ctx.now_ms = 3_000;
        let out = execute(&cli(&["delete-page", "--path", "notes/raft.md"]), ctx)
            .await
            .unwrap();
        assert!(out.contains("deleted"), "{out}");

        let mut ctx = context(Arc::clone(&bucket), &cache);
        ctx.now_ms = 4_000;
        let error = execute(&cli(&["read-page", "--path", "notes/raft.md"]), ctx)
            .await
            .expect_err("a deleted page must not be readable");
        assert!(error.to_string().contains("no page"), "{error}");
    }

    #[tokio::test]
    async fn status_reports_scope_counts_and_sessions_lists_them() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = TempDir::new().unwrap();

        execute(
            &cli(&["capture", "--session", "sess-a", "--text", "one"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();

        let out = execute(&cli(&["sessions"]), context(Arc::clone(&bucket), &cache))
            .await
            .unwrap();
        assert_eq!(out, "sess-a");

        let out = execute(
            &cli(&["status", "--json"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        assert!(out.contains("\"sessions\":1"), "{out}");
    }

    #[test]
    fn scope_defaults_are_explicit_and_overridable() {
        let parsed = cli(&["search", "hello"]);
        assert_eq!(parsed.workspace, "default");
        assert_eq!(parsed.project, "default");
        assert_eq!(parsed.writer, "machine");

        let parsed = cli(&[
            "--workspace",
            "acme",
            "--project",
            "ai-memory",
            "--writer",
            "mbp-1",
            "search",
            "hello",
        ]);
        assert_eq!(parsed.workspace, "acme");
        assert_eq!(parsed.project, "ai-memory");
        assert_eq!(parsed.writer, "mbp-1");
    }
}
