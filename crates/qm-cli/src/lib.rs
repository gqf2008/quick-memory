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
    HandoffState, MANIFEST_SCHEMA, Observation, PagePath, ProjectId, SessionId, WorkspaceId,
    WriterId,
};
use qm_search::consolidate::{CompilerChoice, consolidate_session_with};
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

/// Handoff operations.
#[derive(Debug, Subcommand)]
pub enum HandoffAction {
    /// Leave a baton for whoever comes next.
    Open {
        /// Short title.
        #[arg(long)]
        title: String,
        /// What the next session needs to know; reads stdin when omitted.
        #[arg(long)]
        body: Option<String>,
    },
    /// List handoffs (default: only the open ones).
    List {
        /// Filter: `all`, `open`, `claimed`, or `done`.
        #[arg(long, default_value = "open")]
        state: String,
    },
    /// Claim a handoff; exactly one machine can win.
    Claim {
        /// Handoff id.
        #[arg(long)]
        id: String,
    },
    /// Finish a handoff you claimed.
    Done {
        /// Handoff id.
        #[arg(long)]
        id: String,
    },
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
        /// Compiler: `auto` (LLM when QM_LLM_BASE_URL is set, else rules),
        /// `rules`, or `llm` (which falls back to rules on failure).
        #[arg(long, default_value = "auto")]
        compiler: String,
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
    /// Capture an event from stdin, the way an agent lifecycle hook does.
    Hook {
        /// Event kind; overrides the kind found in the payload.
        #[arg(long)]
        event: Option<String>,
        /// Session id; overrides the payload and `QM_SESSION`.
        #[arg(long)]
        session: Option<String>,
        /// Actor recorded on the observation.
        #[arg(long)]
        actor: Option<String>,
        /// How long to wait for the bucket before spooling instead.
        #[arg(long, default_value_t = 200)]
        timeout_ms: u64,
    },
    /// Replay events that were spooled because the bucket was unreachable.
    HookDrain {
        /// Maximum number of spooled events to attempt.
        #[arg(long, default_value_t = 100)]
        limit: usize,
    },
    /// Leave, list, claim, or finish a handoff.
    Handoff {
        #[command(subcommand)]
        action: HandoffAction,
    },
    /// Reclaim unreachable objects (dry run unless `--apply`).
    Gc {
        /// Actually delete instead of reporting.
        #[arg(long, default_value_t = false)]
        apply: bool,
        /// Leave anything modified more recently than this (default: one hour).
        #[arg(long, default_value_t = 3_600_000)]
        grace_ms: i64,
    },
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
    /// List a page's versions, oldest first.
    History {
        /// Path inside the project.
        #[arg(long)]
        path: String,
    },
    /// Restore an older version as a new one.
    Restore {
        /// Path inside the project.
        #[arg(long)]
        path: String,
        /// Version id to restore (from `qm history`).
        #[arg(long)]
        version: String,
    },
    /// Print a page, optionally as it was at a point in time.
    ReadPage {
        /// Path inside the project.
        #[arg(long)]
        path: String,
        /// Show the version that was current at this Unix time (ms).
        #[arg(long)]
        as_of: Option<i64>,
    },
    /// Show the most recent commits.
    Log {
        /// Maximum entries.
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    /// Tombstone a page.
    DeletePage {
        /// Path inside the project.
        #[arg(long)]
        path: String,
    },
}

/// Build the bucket client from the environment.
///
/// `QM_S3_*` (falling back to `R2_*`) must be present: a surface that cannot
/// reach the bucket fails loudly rather than degrading to a local store, which
/// would silently lose the CAS semantics every write depends on.
///
/// # Errors
/// Fails when endpoint, bucket, or credentials are missing, or the client
/// cannot be constructed.
pub fn build_bucket_from_env() -> Result<Arc<dyn ObjectStore>> {
    use object_store::aws::AmazonS3Builder;

    fn env_first(names: &[&str]) -> Option<String> {
        names.iter().find_map(|name| {
            std::env::var(name)
                .ok()
                .filter(|value| !value.trim().is_empty())
        })
    }
    fn require(names: &[&str], what: &str) -> Result<String> {
        env_first(names).ok_or_else(|| {
            anyhow::anyhow!(
                "missing {what}; set one of {names:?} (a command that cannot reach the bucket must fail, not guess)"
            )
        })
    }

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
    /// Where hook events are spooled when the bucket cannot be reached.
    pub spool_dir: PathBuf,
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
        let spool_dir = std::env::var("QM_SPOOL_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir().join("qm-spool"));
        Ok(Self {
            project: ProjectStore::new(Arc::clone(&bucket), "v1"),
            bucket,
            spool_dir,
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
        Command::Consolidate { session, compiler } => {
            let session_id = SessionId::new(session)?;
            let choice = match compiler.as_str() {
                "auto" => {
                    if std::env::var("QM_LLM_BASE_URL")
                        .map(|value| !value.trim().is_empty())
                        .unwrap_or(false)
                    {
                        CompilerChoice::Llm
                    } else {
                        CompilerChoice::Rules
                    }
                }
                "rules" => CompilerChoice::Rules,
                "llm" => CompilerChoice::Llm,
                other => bail!("unknown compiler {other:?}: use auto, rules or llm"),
            };
            let outcome = consolidate_session_with(
                choice,
                &ctx.project,
                qm_search::consolidate::ConsolidationRequest {
                    workspace_id: &ctx.workspace,
                    project_id: &ctx.project_id,
                    session_id: &session_id,
                    consolidator: &ctx.writer,
                    now_ms: ctx.now_ms,
                    lease_ttl_ms: 60_000,
                },
            )
            .await?;
            Ok(if ctx.json {
                serde_json::json!({
                    "skipped": outcome.skipped,
                    "already_up_to_date": outcome.already_up_to_date,
                    "page_id": outcome.page_id,
                    "observations": outcome.observations,
                    "segments": outcome.segments,
                    "compiler": outcome.compiler,
                    "used_fallback": outcome.used_fallback,
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
                    "compiled {} observation(s) from {} segment(s) with {}{}",
                    outcome.observations,
                    outcome.segments,
                    outcome.compiler,
                    if outcome.used_fallback {
                        " (fell back from the llm)"
                    } else {
                        ""
                    }
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
                    "streams_active": outcome.streams_active,
                    "stream_candidates": outcome.stream_candidates,
                })
                .to_string()
            } else if outcome.hits.is_empty() {
                format!(
                    "no hits ({} split(s) searched, {} filtered, streams: {})",
                    outcome.splits_searched,
                    outcome.filtered_out,
                    if outcome.streams_active.is_empty() {
                        "none".to_string()
                    } else {
                        outcome.streams_active.join(", ")
                    }
                )
            } else {
                outcome
                    .hits
                    .iter()
                    .map(|hit| {
                        format!(
                            "{:.4}\t{}\t{}\t[{}]",
                            hit.score,
                            hit.path,
                            hit.title.replace('\n', " "),
                            hit.streams.join(",")
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
            let watermark_path = publish_watermark_path(&ctx);
            let watermark = read_watermark(&watermark_path);
            let mut docs = Vec::new();
            for (path, entry) in &loaded.manifest.pages {
                // Only what changed since this machine last published. Without
                // the watermark (a fresh cache) everything is republished,
                // which is wasteful but never wrong.
                if entry.seq <= watermark {
                    continue;
                }
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
                let reason = if loaded.manifest.pages.is_empty() {
                    "nothing to publish: the project has no live pages".to_string()
                } else {
                    format!(
                        "nothing to publish: this machine is up to date through seq {}",
                        watermark
                    )
                };
                return Ok(reason);
            }
            let seq = loaded.manifest.seq + 1;
            // One directory per invocation: a leftover build directory from an
            // earlier run (same seq, different process) must never be reused,
            // because building an index into a non-empty directory fails.
            let build_dir =
                ctx.cache_dir
                    .join(format!("build-{seq}-{}-{}", std::process::id(), ctx.now_ms));
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
            write_watermark(&watermark_path, loaded.manifest.seq)?;
            Ok(if ctx.json {
                serde_json::json!({
                    "generation": outcome.generation,
                    "already_present": outcome.already_present,
                    "pages": docs.len(),
                    "manifest_seq": loaded.manifest.seq,
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
            let build_dir =
                ctx.cache_dir
                    .join(format!("compact-{}-{}", std::process::id(), ctx.now_ms));
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
        Command::Hook {
            event,
            session,
            actor,
            timeout_ms,
        } => {
            let raw = read_stdin_bounded(MAX_HOOK_INPUT_BYTES)?;
            capture_hook_event(
                &ctx,
                session.as_deref(),
                event.as_deref(),
                actor.as_deref(),
                &raw,
                *timeout_ms,
            )
            .await
        }
        Command::HookDrain { limit } => {
            let (drained, kept) = drain_spool(&ctx, *limit).await?;
            Ok(format!("drained {drained} spooled event(s), kept {kept}"))
        }
        Command::Handoff { action } => match action {
            HandoffAction::Open { title, body } => {
                let body = match body {
                    Some(body) => body.clone(),
                    None => read_stdin()?,
                };
                let (handoff, created) = ctx
                    .project
                    .open_handoff(
                        &ctx.workspace,
                        &ctx.project_id,
                        title,
                        &body,
                        &ctx.writer,
                        ctx.now_ms,
                    )
                    .await
                    .map_err(|error| anyhow::anyhow!("{error}"))?;
                Ok(if ctx.json {
                    serde_json::to_string(&handoff)?
                } else {
                    format!(
                        "{} handoff {}: {}",
                        if created { "opened" } else { "already open" },
                        &handoff.id[..12.min(handoff.id.len())],
                        handoff.title
                    )
                })
            }
            HandoffAction::List { state } => {
                let wanted = match state.as_str() {
                    "all" => None,
                    "open" => Some(HandoffState::Open),
                    "claimed" => Some(HandoffState::Claimed),
                    "done" => Some(HandoffState::Done),
                    other => bail!("unknown state {other:?}: use all, open, claimed or done"),
                };
                let handoffs = ctx
                    .project
                    .list_handoffs(&ctx.workspace, &ctx.project_id)
                    .await
                    .map_err(|error| anyhow::anyhow!("{error}"))?;
                let filtered: Vec<&qm_core::Handoff> = handoffs
                    .iter()
                    .filter(|handoff| wanted.as_ref().is_none_or(|want| handoff.state() == *want))
                    .collect();
                Ok(if ctx.json {
                    serde_json::to_string(&filtered)?
                } else if filtered.is_empty() {
                    "no handoffs".to_string()
                } else {
                    filtered
                        .iter()
                        .map(|handoff| {
                            format!("{:?}\t{}\t{}", handoff.state(), handoff.id, handoff.title)
                        })
                        .collect::<Vec<_>>()
                        .join("\n")
                })
            }
            HandoffAction::Claim { id } => {
                let handoff = ctx
                    .project
                    .claim_handoff(&ctx.workspace, &ctx.project_id, id, &ctx.writer, ctx.now_ms)
                    .await
                    .map_err(|error| anyhow::anyhow!("{error}"))?;
                Ok(if ctx.json {
                    serde_json::to_string(&handoff)?
                } else {
                    format!("claimed {}", handoff.id)
                })
            }
            HandoffAction::Done { id } => {
                let handoff = ctx
                    .project
                    .finish_handoff(&ctx.workspace, &ctx.project_id, id, &ctx.writer, ctx.now_ms)
                    .await
                    .map_err(|error| anyhow::anyhow!("{error}"))?;
                Ok(if ctx.json {
                    serde_json::to_string(&handoff)?
                } else {
                    format!("finished {}", handoff.id)
                })
            }
        },
        Command::Gc { apply, grace_ms } => {
            let outcome = ctx
                .project
                .gc_orphans(
                    &ctx.workspace,
                    &ctx.project_id,
                    ctx.now_ms,
                    *grace_ms,
                    *apply,
                )
                .await
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            Ok(if ctx.json {
                serde_json::to_string(&outcome)?
            } else {
                format!(
                    "{}: scanned {}, live {}, collectable {}, kept within grace {}, deleted {}",
                    if outcome.dry_run {
                        "dry run"
                    } else {
                        "applied"
                    },
                    outcome.scanned,
                    outcome.live,
                    outcome.collectable,
                    outcome.kept_recent,
                    outcome.deleted
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
        Command::ReadPage { path, as_of } => {
            let page_path = ctx.page(path)?;
            let page = match as_of {
                Some(at_ms) => ctx
                    .project
                    .version_at(&ctx.workspace, &ctx.project_id, &page_path, *at_ms)
                    .await
                    .map_err(|error| anyhow::anyhow!("{error}"))?
                    .with_context(|| {
                        format!("no version of {} existed at {at_ms}", page_path.as_str())
                    })?,
                None => ctx
                    .project
                    .read_page(&ctx.workspace, &ctx.project_id, &page_path)
                    .await
                    .map_err(|error| anyhow::anyhow!("{error}"))?
                    .with_context(|| format!("no page at {}", page_path.as_str()))?,
            };
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
        Command::History { path } => {
            let page_path = ctx.page(path)?;
            let versions = ctx
                .project
                .read_page_versions(&ctx.workspace, &ctx.project_id, &page_path)
                .await
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            // Timestamps come from the advisory commit log, which may be
            // missing an entry (a crash between commit and log write); the
            // version order itself is authoritative.
            let log = ctx
                .project
                .read_commit_log(&ctx.workspace, &ctx.project_id, usize::MAX)
                .await
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            let committed_at = |page_id: &qm_core::PageId| {
                log.iter()
                    .find(|record| record.page_id.as_ref() == Some(page_id))
                    .map(|record| record.at_ms)
            };
            Ok(if ctx.json {
                let described: Vec<serde_json::Value> = versions
                    .iter()
                    .map(|version| {
                        // A superset of the version object: enough to restore
                        // from, plus when it was committed when the log knows.
                        serde_json::json!({
                            "page_id": version.page_id,
                            "path": version.path,
                            "title": version.title,
                            "body": version.body,
                            "supersedes": version.supersedes,
                            "committed_at_ms": committed_at(&version.page_id),
                        })
                    })
                    .collect();
                serde_json::to_string(&described)?
            } else if versions.is_empty() {
                format!("no versions at {}", page_path.as_str())
            } else {
                versions
                    .iter()
                    .enumerate()
                    .map(|(index, version)| {
                        format!(
                            "{}\t{}\tcommitted_at={}\tsupersedes={}",
                            index + 1,
                            version.page_id,
                            committed_at(&version.page_id)
                                .map_or("-".to_string(), |at| at.to_string()),
                            version
                                .supersedes
                                .as_ref()
                                .map_or("-".to_string(), ToString::to_string)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            })
        }
        Command::Restore { path, version } => {
            let page_path = ctx.page(path)?;
            let page_id = qm_core::PageId::new(version)?;
            let historical = ctx
                .project
                .read_page_version_by_id(&ctx.workspace, &ctx.project_id, &page_path, &page_id)
                .await
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            // Restoring is a normal write: it supersedes the current version
            // and leaves every later object untouched, so a restore is itself
            // reversible.
            let outcome = ctx
                .project
                .commit_page(CommitPageRequest {
                    workspace_id: ctx.workspace.clone(),
                    project_id: ctx.project_id.clone(),
                    path: page_path.clone(),
                    title: historical.title.clone(),
                    body: historical.body.clone(),
                    writer_id: ctx.writer.clone(),
                    now_ms: ctx.now_ms,
                })
                .await
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            Ok(if ctx.json {
                serde_json::json!({
                    "restored_from": historical.page_id.as_str(),
                    "page_id": outcome.page_id.as_str(),
                    "manifest_seq": outcome.manifest_seq,
                })
                .to_string()
            } else {
                format!(
                    "restored {} from {} as a new version at seq {}",
                    page_path.as_str(),
                    historical.page_id,
                    outcome.manifest_seq
                )
            })
        }
        Command::Log { limit } => {
            let records = ctx
                .project
                .read_commit_log(&ctx.workspace, &ctx.project_id, *limit)
                .await
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            Ok(if ctx.json {
                serde_json::to_string(&records)?
            } else if records.is_empty() {
                "no commits".to_string()
            } else {
                records
                    .iter()
                    .map(|record| {
                        format!(
                            "{:>6}\t{}\t{:?}\t{}",
                            record.seq,
                            record.at_ms,
                            record.kind,
                            record.path.as_str()
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
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

/// Local record of how far this machine has published.
///
/// Deliberately local: each machine publishes its own splits, so its publish
/// position is its own business. Losing the file costs one redundant full
/// publish, never correctness.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PublishWatermark {
    manifest_seq: u64,
}

fn publish_watermark_path(ctx: &Context) -> PathBuf {
    ctx.cache_dir.join(format!(
        "publish-{}-{}-{}.json",
        ctx.workspace, ctx.project_id, ctx.writer
    ))
}

fn read_watermark(path: &PathBuf) -> u64 {
    std::fs::read(path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<PublishWatermark>(&bytes).ok())
        .map(|watermark| watermark.manifest_seq)
        .unwrap_or(0)
}

fn write_watermark(path: &PathBuf, manifest_seq: u64) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let bytes = serde_json::to_vec(&PublishWatermark { manifest_seq })?;
    std::fs::write(path, bytes).with_context(|| format!("writing {}", path.display()))
}

/// Hard cap on hook input, applied before any parsing.
pub const MAX_HOOK_INPUT_BYTES: usize = 256 * 1024;

/// Read stdin, refusing to buffer more than `limit` bytes.
fn read_stdin_bounded(limit: usize) -> Result<String> {
    use std::io::Read as _;

    let mut buffer = Vec::new();
    std::io::stdin()
        .take(limit as u64)
        .read_to_end(&mut buffer)
        .context("reading stdin")?;
    Ok(String::from_utf8_lossy(&buffer).to_string())
}

/// Map an arbitrary harness session id onto a valid key segment.
///
/// Hook payloads carry session ids in every shape imaginable (UUIDs, paths,
/// `<harness>:<uuid>`). Rather than reject the event, normalise it: anything
/// outside the safe alphabet becomes `-`, and an empty result falls back to
/// `unattributed` so the event is still captured somewhere findable.
#[must_use]
pub fn normalize_session(raw: &str) -> String {
    let mut out: String = raw
        .trim()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '-'
            }
        })
        .collect();
    out.truncate(128);
    if out.is_empty() || out == "." || out == ".." {
        out = "unattributed".to_string();
    }
    out
}

/// Extract `(session, kind, text)` from a hook payload.
///
/// Understands a JSON object with any of the common key spellings and falls
/// back to treating the whole input as the event text. The scrubber still runs
/// later, so nothing here decides what is safe to store.
#[must_use]
pub fn extract_hook_event(input: &str, default_session: Option<&str>) -> (String, String, String) {
    let fallback_session = || default_session.unwrap_or("unattributed").to_string();
    let Ok(serde_json::Value::Object(map)) = serde_json::from_str::<serde_json::Value>(input)
    else {
        let text = input.trim().to_string();
        return (fallback_session(), "message".to_string(), text);
    };
    let string_at = |keys: &[&str]| -> Option<String> {
        keys.iter()
            .find_map(|key| map.get(*key))
            .and_then(|value| match value {
                serde_json::Value::String(text) => Some(text.clone()),
                serde_json::Value::Number(number) => Some(number.to_string()),
                serde_json::Value::Bool(flag) => Some(flag.to_string()),
                _ => None,
            })
    };
    let session = string_at(&["session_id", "session", "conversation_id"])
        .or_else(|| default_session.map(ToString::to_string))
        .unwrap_or_else(|| "unattributed".to_string());
    let kind = string_at(&["kind", "event", "hook_event_name"])
        .unwrap_or_else(|| "observation".to_string());
    let text = string_at(&[
        "text",
        "message",
        "prompt",
        "summary",
        "tool_response",
        "tool_input",
    ])
    .unwrap_or_else(|| input.trim().to_string());
    (session, kind, text)
}

/// Capture one hook event, spooling instead of failing.
///
/// This is the fire-and-forget contract in code: the event goes to the bucket
/// when the bucket answers inside `timeout_ms`, and to the local spool
/// otherwise. Either way the caller gets `Ok`, because an agent lifecycle hook
/// must never be blocked or failed by memory being unreachable.
///
/// # Errors
/// Only fails when the event cannot be recorded *anywhere* (invalid scope, or
/// an unwritable spool directory).
pub async fn capture_hook_event(
    ctx: &Context,
    session: Option<&str>,
    kind: Option<&str>,
    actor: Option<&str>,
    raw: &str,
    timeout_ms: u64,
) -> Result<String> {
    let default_session = session.map(ToString::to_string).or_else(|| {
        std::env::var("QM_SESSION")
            .ok()
            .filter(|value| !value.trim().is_empty())
    });
    let (session_id, payload_kind, text) = extract_hook_event(raw, default_session.as_deref());
    let session_id = SessionId::new(normalize_session(&session_id))?;
    let kind = kind.unwrap_or(&payload_kind).to_string();
    let actor = actor.unwrap_or("hook").to_string();
    let observation = Observation {
        schema: MANIFEST_SCHEMA,
        observation_id: qm_core::derive_observation_id(
            &session_id,
            &actor,
            &kind,
            &text,
            ctx.now_ms,
        ),
        session_id: session_id.clone(),
        actor,
        kind,
        text,
        created_at_ms: ctx.now_ms,
    };
    let request = IngestObservationsRequest {
        workspace_id: ctx.workspace.clone(),
        project_id: ctx.project_id.clone(),
        session_id,
        writer_id: ctx.writer.clone(),
        observations: vec![observation.clone()],
        now_ms: ctx.now_ms,
    };
    let attempt = tokio::time::timeout(
        std::time::Duration::from_millis(timeout_ms),
        ctx.project.ingest_observations(request),
    )
    .await;
    match attempt {
        Ok(Ok(outcome)) if !outcome.already_present => Ok(format!(
            "captured {} observation(s), {} total",
            outcome.accepted, outcome.count
        )),
        Ok(Ok(_)) => Ok("already captured".to_string()),
        outcome => {
            let spooled = spool_observation(ctx, &observation)?;
            let reason = match outcome {
                Ok(Err(error)) => error.to_string(),
                _ => "timed out".to_string(),
            };
            Ok(format!("spooled ({reason}): {}", spooled.display()))
        }
    }
}

/// One spooled hook event, scoped so a drain never writes into the wrong project.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SpoolEntry {
    workspace: String,
    project: String,
    observation: Observation,
}

/// Persist an event locally so a failed capture is retried later.
fn spool_observation(ctx: &Context, observation: &Observation) -> Result<PathBuf> {
    std::fs::create_dir_all(&ctx.spool_dir)
        .with_context(|| format!("creating spool dir {}", ctx.spool_dir.display()))?;
    let entry = SpoolEntry {
        workspace: ctx.workspace.to_string(),
        project: ctx.project_id.to_string(),
        observation: observation.clone(),
    };
    let name = format!(
        "{}-{}-{}.json",
        ctx.now_ms,
        std::process::id(),
        observation
            .observation_id
            .chars()
            .take(16)
            .collect::<String>()
    );
    let target = ctx.spool_dir.join(name);
    let tmp = ctx.spool_dir.join(format!(
        ".tmp-{}",
        target.file_name().unwrap_or_default().to_string_lossy()
    ));
    std::fs::write(&tmp, serde_json::to_vec(&entry)?).context("writing spool file")?;
    std::fs::rename(&tmp, &target).context("publishing spool file")?;
    Ok(target)
}

/// Replay spooled events, deleting each one only after it is stored.
async fn drain_spool(ctx: &Context, limit: usize) -> Result<(usize, usize)> {
    let Ok(entries) = std::fs::read_dir(&ctx.spool_dir) else {
        return Ok((0, 0));
    };
    let mut files: Vec<PathBuf> = entries
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .collect();
    files.sort();

    let mut drained = 0usize;
    let mut kept = 0usize;
    for path in files.into_iter().take(limit) {
        let bytes = std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
        let entry: SpoolEntry = match serde_json::from_slice(&bytes) {
            Ok(entry) => entry,
            Err(_) => {
                // An unreadable spool file must not block the queue forever.
                kept += 1;
                continue;
            }
        };
        if entry.workspace != ctx.workspace.to_string()
            || entry.project != ctx.project_id.to_string()
        {
            kept += 1;
            continue;
        }
        let request = IngestObservationsRequest {
            workspace_id: ctx.workspace.clone(),
            project_id: ctx.project_id.clone(),
            session_id: entry.observation.session_id.clone(),
            writer_id: ctx.writer.clone(),
            observations: vec![entry.observation.clone()],
            now_ms: ctx.now_ms,
        };
        match ctx.project.ingest_observations(request).await {
            Ok(_) => {
                std::fs::remove_file(&path)
                    .with_context(|| format!("removing {}", path.display()))?;
                drained += 1;
            }
            Err(_) => kept += 1,
        }
    }
    Ok((drained, kept))
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

    #[tokio::test]
    async fn history_lists_versions_and_restore_creates_a_new_one() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = TempDir::new().unwrap();

        for (index, body) in ["first", "second", "third"].iter().enumerate() {
            execute(
                &cli(&["write-page", "--path", "notes/raft.md", "--body", body]),
                Context {
                    now_ms: (index as i64 + 1) * 1_000,
                    ..context(Arc::clone(&bucket), &cache)
                },
            )
            .await
            .unwrap();
        }

        let history = execute(
            &cli(&["history", "--path", "notes/raft.md", "--json"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        let versions: Vec<qm_core::PageVersion> = serde_json::from_str(&history).unwrap();
        assert_eq!(versions.len(), 3);
        assert_eq!(versions[0].body, "first");
        assert_eq!(versions[2].body, "third");
        assert!(versions[2].supersedes.is_some());
        let oldest = versions[0].page_id.as_str().to_string();

        // Restoring the first version writes a *new* version…
        let mut ctx = context(Arc::clone(&bucket), &cache);
        ctx.now_ms = 10_000;
        let restored = execute(
            &cli(&["restore", "--path", "notes/raft.md", "--version", &oldest]),
            ctx,
        )
        .await
        .unwrap();
        assert!(restored.contains("as a new version"), "{restored}");

        let mut ctx = context(Arc::clone(&bucket), &cache);
        ctx.now_ms = 11_000;
        let current = execute(&cli(&["read-page", "--path", "notes/raft.md"]), ctx)
            .await
            .unwrap();
        assert_eq!(current, "first");

        // …and the version it restored from is still readable, as is the third.
        let history = execute(
            &cli(&["history", "--path", "notes/raft.md", "--json"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        let versions: Vec<qm_core::PageVersion> = serde_json::from_str(&history).unwrap();
        assert_eq!(versions.len(), 4, "a restore appends, it never rewrites");
        assert!(versions.iter().any(|v| v.body == "third"));

        // An unknown version id is refused by name.
        let mut ctx = context(Arc::clone(&bucket), &cache);
        ctx.now_ms = 12_000;
        let missing = execute(
            &cli(&[
                "restore",
                "--path",
                "notes/raft.md",
                "--version",
                &"0".repeat(64),
            ]),
            ctx,
        )
        .await
        .expect_err("restoring a version that does not exist must fail");
        assert!(missing.to_string().contains("not found"), "{missing}");
    }

    #[tokio::test]
    async fn as_of_reading_and_the_commit_log_answer_when() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = TempDir::new().unwrap();

        for (index, body) in ["first", "second", "third"].iter().enumerate() {
            execute(
                &cli(&["write-page", "--path", "notes/raft.md", "--body", body]),
                Context {
                    now_ms: (index as i64 + 1) * 1_000,
                    ..context(Arc::clone(&bucket), &cache)
                },
            )
            .await
            .unwrap();
        }

        // Read the page as it was at each point in its life.
        for (at, expected) in [(1_000, "first"), (2_000, "second"), (3_000, "third")] {
            let mut ctx = context(Arc::clone(&bucket), &cache);
            ctx.now_ms = at;
            let body = execute(
                &cli(&[
                    "read-page",
                    "--path",
                    "notes/raft.md",
                    "--as-of",
                    &at.to_string(),
                ]),
                ctx,
            )
            .await
            .unwrap();
            assert_eq!(body, expected, "as of {at}");
        }

        // Before the first commit there is nothing to show.
        let mut ctx = context(Arc::clone(&bucket), &cache);
        ctx.now_ms = 500;
        let missing = execute(
            &cli(&["read-page", "--path", "notes/raft.md", "--as-of", "500"]),
            ctx,
        )
        .await
        .expect_err("nothing existed that early");
        assert!(missing.to_string().contains("no version"), "{missing}");

        // The log lists commits newest first, and history reports timestamps.
        let log = execute(
            &cli(&["log", "--json"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        let records: Vec<qm_core::CommitRecord> = serde_json::from_str(&log).unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].seq, 3);
        assert_eq!(records[0].at_ms, 3_000);

        let history = execute(
            &cli(&["history", "--path", "notes/raft.md", "--json"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        let described: Vec<serde_json::Value> = serde_json::from_str(&history).unwrap();
        assert_eq!(described.len(), 3);
        assert_eq!(described[0]["committed_at_ms"], 1_000);
        assert_eq!(described[2]["committed_at_ms"], 3_000);
    }

    #[tokio::test]
    async fn publishing_is_incremental_per_machine() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = TempDir::new().unwrap();

        execute(
            &cli(&[
                "write-page",
                "--path",
                "notes/one.md",
                "--body",
                "first page",
            ]),
            Context {
                now_ms: 1_000,
                ..context(Arc::clone(&bucket), &cache)
            },
        )
        .await
        .unwrap();

        let mut ctx = context(Arc::clone(&bucket), &cache);
        ctx.now_ms = 2_000;
        let first = execute(&cli(&["publish"]), ctx).await.unwrap();
        assert!(first.contains("published 1 page(s)"), "{first}");

        // Nothing changed: a second publish must not append another split.
        let mut ctx = context(Arc::clone(&bucket), &cache);
        ctx.now_ms = 3_000;
        let second = execute(&cli(&["publish"]), ctx).await.unwrap();
        assert!(second.contains("nothing to publish"), "{second}");
        let catalog = {
            let ctx = context(Arc::clone(&bucket), &cache);
            ctx.project
                .load_catalog(&ctx.workspace, &ctx.project_id)
                .await
                .unwrap()
        };
        assert_eq!(catalog.catalog.splits.len(), 1);

        // One new page: only that page is republished.
        execute(
            &cli(&[
                "write-page",
                "--path",
                "notes/two.md",
                "--body",
                "second page",
            ]),
            Context {
                now_ms: 4_000,
                ..context(Arc::clone(&bucket), &cache)
            },
        )
        .await
        .unwrap();
        let mut ctx = context(Arc::clone(&bucket), &cache);
        ctx.now_ms = 5_000;
        let third = execute(&cli(&["publish"]), ctx).await.unwrap();
        assert!(third.contains("published 1 page(s)"), "{third}");
        let catalog = {
            let ctx = context(Arc::clone(&bucket), &cache);
            ctx.project
                .load_catalog(&ctx.workspace, &ctx.project_id)
                .await
                .unwrap()
        };
        assert_eq!(catalog.catalog.splits.len(), 2);
    }

    #[tokio::test]
    async fn gc_defaults_to_a_dry_run_and_never_touches_live_objects() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = TempDir::new().unwrap();
        execute(
            &cli(&[
                "write-page",
                "--path",
                "notes/raft.md",
                "--body",
                "leader election",
            ]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();

        let out = execute(
            &cli(&["gc", "--json"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        assert!(out.contains("\"dry_run\":true"), "{out}");
        assert!(out.contains("\"deleted\":0"), "{out}");

        // Applying it still keeps the live page readable.
        execute(
            &cli(&["gc", "--apply", "--grace-ms", "0"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        let mut ctx = context(Arc::clone(&bucket), &cache);
        ctx.now_ms = 5_000;
        let page = execute(&cli(&["read-page", "--path", "notes/raft.md"]), ctx)
            .await
            .unwrap();
        assert_eq!(page, "leader election");
    }

    #[test]
    fn hook_payloads_are_extracted_and_session_ids_normalized() {
        let payload = r#"{"session_id":"codex:01J/2","hook_event_name":"PostToolUse","tool_response":"cargo t passed"}"#;
        let (session, kind, text) = extract_hook_event(payload, None);
        assert_eq!(session, "codex:01J/2");
        assert_eq!(kind, "PostToolUse");
        assert_eq!(text, "cargo t passed");

        // Plain text is the whole event, and the default session applies.
        let (session, kind, text) = extract_hook_event("ran the suite", Some("sess-9"));
        assert_eq!(session, "sess-9");
        assert_eq!(kind, "message");
        assert_eq!(text, "ran the suite");

        // Unknown JSON shape still gets captured somewhere findable.
        let (session, kind, _) = extract_hook_event(r#"{"unexpected":true}"#, None);
        assert_eq!(session, "unattributed");
        assert_eq!(kind, "observation");
        assert_eq!(normalize_session("codex:01J/2"), "codex-01J-2");
        assert_eq!(normalize_session("  "), "unattributed");
        assert_eq!(normalize_session(".."), "unattributed");
    }

    #[tokio::test]
    async fn a_hook_spools_when_the_bucket_is_unreachable_and_drains_later() {
        // A store pointed at a closed port: every write fails, quickly.
        let unreachable: Arc<dyn ObjectStore> = Arc::new(
            object_store::aws::AmazonS3Builder::new()
                .with_bucket_name("unreachable")
                .with_region("auto")
                .with_endpoint("http://127.0.0.1:1")
                .with_allow_http(true)
                .with_access_key_id("test")
                .with_secret_access_key("test")
                .with_retry(object_store::RetryConfig {
                    max_retries: 0,
                    ..Default::default()
                })
                .build()
                .unwrap(),
        );
        let spool = TempDir::new().unwrap();
        let mut ctx = context(unreachable, &TempDir::new().unwrap());
        ctx.spool_dir = spool.path().to_path_buf();

        let outcome = capture_hook_event(
            &ctx,
            Some("sess-hook"),
            None,
            Some("codex"),
            r#"{"session_id":"sess-hook","hook_event_name":"PostToolUse","tool_response":"cargo t passed"}"#,
            1_000,
        )
        .await
        .unwrap();
        assert!(outcome.starts_with("spooled"), "{outcome}");
        assert_eq!(
            std::fs::read_dir(spool.path()).unwrap().count(),
            1,
            "the event must be persisted locally"
        );

        // The same machine, now able to reach a working bucket, drains it.
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut live = context(Arc::clone(&bucket), &TempDir::new().unwrap());
        live.spool_dir = spool.path().to_path_buf();
        let outcome = execute(&cli(&["hook-drain"]), live).await.unwrap();
        assert!(outcome.contains("drained 1"), "{outcome}");
        assert_eq!(
            std::fs::read_dir(spool.path()).unwrap().count(),
            0,
            "a drained event must leave the spool"
        );

        let session = SessionId::new("sess-hook").unwrap();
        let observed = {
            let ctx = context(Arc::clone(&bucket), &TempDir::new().unwrap());
            ctx.project
                .read_session_observations(&ctx.workspace, &ctx.project_id, &session)
                .await
                .unwrap()
        };
        assert_eq!(observed.len(), 1);
        assert!(observed[0].text.contains("cargo t passed"));
        assert_eq!(observed[0].actor, "codex");
    }

    #[tokio::test]
    async fn handoffs_flow_open_claim_done_through_the_cli() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = TempDir::new().unwrap();

        let out = execute(
            &cli(&[
                "handoff",
                "open",
                "--title",
                "finish the rebuild",
                "--body",
                "compaction is pending",
            ]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        assert!(out.contains("opened handoff"), "{out}");

        let listed = execute(
            &cli(&["handoff", "list", "--json"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        let parsed: Vec<qm_core::Handoff> = serde_json::from_str(&listed).unwrap();
        assert_eq!(parsed.len(), 1);
        let id = parsed[0].id.clone();

        // A different machine claims it; the first can no longer do so.
        let mut other = context(Arc::clone(&bucket), &cache);
        other.writer = WriterId::new("mbp-b").unwrap();
        other.now_ms = 2_000;
        let claimed = execute(&cli(&["handoff", "claim", "--id", &id]), other)
            .await
            .unwrap();
        assert!(claimed.contains("claimed"), "{claimed}");

        let mut third = context(Arc::clone(&bucket), &cache);
        third.writer = WriterId::new("mbp-c").unwrap();
        third.now_ms = 3_000;
        let refused = execute(&cli(&["handoff", "claim", "--id", &id]), third)
            .await
            .expect_err("a claimed handoff must not be claimable again");
        assert!(refused.to_string().contains("not open"), "{refused}");

        // Only the claimer can finish it.
        let mut owner = context(Arc::clone(&bucket), &cache);
        owner.writer = WriterId::new("mbp-b").unwrap();
        owner.now_ms = 4_000;
        let done = execute(&cli(&["handoff", "done", "--id", &id]), owner)
            .await
            .unwrap();
        assert!(done.contains("finished"), "{done}");

        let open_now = execute(
            &cli(&["handoff", "list"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        assert_eq!(open_now, "no handoffs", "a finished handoff is not open");
        let all = execute(
            &cli(&["handoff", "list", "--state", "all", "--json"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        let parsed: Vec<qm_core::Handoff> = serde_json::from_str(&all).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].state(), HandoffState::Done);
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
