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
    CommitKind, HandoffState, MANIFEST_SCHEMA, Observation, PagePath, ProjectId, SessionId,
    WorkspaceId, WriterId,
};
use qm_search::consolidate::{CompilerChoice, consolidate_session_with};
use qm_search::{
    PageDoc, attach_embeddings, compact_project, publish_split_index, search_project_tuned,
    search_workspace_tuned,
};
use qm_store::{CommitPageRequest, Digest, IngestObservationsRequest, ProjectStore};

/// Default page count for `recent` / `memory_recent`.
///
/// One constant rather than a literal per surface: the CLI default and the MCP
/// default are the same promise, so they must not be able to drift apart.
pub const RECENT_DEFAULT_LIMIT: usize = 10;

/// Default look-back window for `digest` / `memory_digest`, in hours.
///
/// One constant rather than a literal per surface: the CLI default and the MCP
/// default are the same promise, so they must not be able to drift apart.
pub const DIGEST_DEFAULT_HOURS: i64 = 24;

/// Default per-section entry cap for `digest` / `memory_digest`.
pub const DIGEST_DEFAULT_LIMIT: usize = 20;

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
    /// Retire old observations from a session's chain (dry run unless `--apply`).
    CompactSession {
        /// Session id.
        #[arg(long)]
        session: String,
        /// Keep observations newer than this many milliseconds.
        #[arg(long, default_value_t = 2_592_000_000)]
        keep_ms: i64,
        /// Always keep at least this many of the newest observations.
        #[arg(long, default_value_t = 50)]
        keep_last: usize,
        /// Actually rewrite the chain instead of reporting.
        #[arg(long, default_value_t = false)]
        apply: bool,
    },
    /// Search the project (or every project in the workspace with `--global`).
    Search {
        /// Query text.
        query: String,
        /// Maximum hits.
        #[arg(long, default_value_t = 10)]
        limit: usize,
        /// Search every project in the workspace, not just the current one.
        #[arg(long, default_value_t = false)]
        global: bool,
        /// Ignore how recently a page changed when ranking.
        #[arg(long, default_value_t = false)]
        no_recency: bool,
        /// Do not expand results with pages that link to the matches.
        #[arg(long, default_value_t = false)]
        no_neighbors: bool,
        /// Disable the vector stream even when an embedding provider is configured.
        #[arg(long, default_value_t = false)]
        no_vector: bool,
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
    /// Stage a proposed edit for approval.
    Propose {
        /// Target page path.
        #[arg(long)]
        path: String,
        /// Title to write.
        #[arg(long)]
        title: String,
        /// Proposed body; reads stdin when omitted.
        #[arg(long)]
        body: Option<String>,
        /// Why the change is proposed.
        #[arg(long, default_value = "")]
        rationale: String,
    },
    /// List proposals (default: the pending ones).
    Proposals {
        /// Filter: `all`, `pending`, `approved`, or `rejected`.
        #[arg(long, default_value = "pending")]
        state: String,
    },
    /// Approve a proposal and apply it.
    Approve {
        /// Proposal id.
        #[arg(long)]
        id: String,
    },
    /// Reject a proposal.
    Reject {
        /// Proposal id.
        #[arg(long)]
        id: String,
        /// Why it was rejected.
        #[arg(long)]
        note: Option<String>,
    },
    /// Leave, list, claim, or finish a handoff.
    Handoff {
        #[command(subcommand)]
        action: HandoffAction,
    },
    /// Write every live page (and session) to a directory.
    Export {
        /// Destination directory.
        #[arg(long)]
        to: PathBuf,
    },
    /// Commit pages and sessions from an exported directory.
    Import {
        /// Source directory.
        #[arg(long)]
        from: PathBuf,
    },
    /// Check that the bucket's authoritative state is internally consistent.
    Verify {
        /// Check every project in the workspace.
        #[arg(long, default_value_t = false)]
        global: bool,
        /// Exit non-zero when anything is inconsistent.
        #[arg(long, default_value_t = false)]
        strict: bool,
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
    /// List pages by when they last changed, newest first.
    Recent {
        /// Maximum pages.
        #[arg(long, default_value_t = RECENT_DEFAULT_LIMIT)]
        limit: usize,
    },
    /// Summarise everything that changed recently.
    Digest {
        /// Look back this many milliseconds.
        #[arg(long)]
        since_ms: Option<i64>,
        /// Look back this many hours (default: 24).
        #[arg(long, conflicts_with = "since_ms")]
        hours: Option<i64>,
        /// Maximum entries per section.
        #[arg(long, default_value_t = DIGEST_DEFAULT_LIMIT)]
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

/// Flatten every control character to a space.
///
/// A single record is a single line in this CLI's output, and titles, paths and
/// handoff text are not validated: a bare newline, a CRLF, a tab or an escape
/// sequence would each break some half of that contract.
fn one_line(text: &str) -> String {
    text.chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect()
}

/// Render a digest as three labelled sections: pages, sessions, handoffs.
///
/// The sections are always all present when there is anything at all to show,
/// because "no handoffs" and "the handoff section never rendered" are different
/// facts and a reader should not have to guess which one they are looking at.
fn render_digest(digest: &Digest) -> String {
    if digest.is_empty() {
        return "no recent activity".to_string();
    }
    let mut out = String::new();

    out.push_str(&format!("pages ({})\n", digest.pages.len()));
    for record in &digest.pages {
        let kind = match record.kind {
            CommitKind::PageWritten => "written",
            CommitKind::PageDeleted => "deleted",
        };
        out.push_str(&format!(
            "  {}\t{}\t{}\n",
            record.at_ms,
            kind,
            one_line(record.path.as_str())
        ));
    }

    out.push_str(&format!("sessions ({})\n", digest.sessions.len()));
    for session in &digest.sessions {
        out.push_str(&format!(
            "  {}\t{}\t{} observations\n",
            session.last_seen_ms,
            one_line(session.session_id.as_str()),
            session.observations
        ));
    }

    out.push_str(&format!("handoffs ({})\n", digest.handoffs.len()));
    for handoff in &digest.handoffs {
        let state = match handoff.state() {
            HandoffState::Open => "open",
            HandoffState::Claimed => "claimed",
            HandoffState::Done => "done",
        };
        out.push_str(&format!(
            "  {}\t{}\t{}\t{}\n",
            qm_store::handoff_activity_ms(handoff),
            state,
            handoff.id,
            one_line(&handoff.title)
        ));
    }

    out
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
        Command::CompactSession {
            session,
            keep_ms,
            keep_last,
            apply,
        } => {
            let session_id = SessionId::new(session)?;
            let observations = ctx
                .project
                .read_session_observations(&ctx.workspace, &ctx.project_id, &session_id)
                .await
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            let plan = qm_store::plan_retention(&observations, *keep_ms, *keep_last, ctx.now_ms);
            if plan.dropped == 0 {
                return Ok(if ctx.json {
                    serde_json::json!({
                        "kept": plan.keep.len(),
                        "dropped": 0,
                        "applied": false,
                    })
                    .to_string()
                } else {
                    format!(
                        "nothing to retire ({} observation(s) kept)",
                        plan.keep.len()
                    )
                });
            }
            if !apply {
                return Ok(if ctx.json {
                    serde_json::json!({
                        "kept": plan.keep.len(),
                        "dropped": plan.dropped,
                        "applied": false,
                    })
                    .to_string()
                } else {
                    format!(
                        "dry run: would keep {} and retire {} observation(s)",
                        plan.keep.len(),
                        plan.dropped
                    )
                });
            }
            let outcome = ctx
                .project
                .rewrite_session(
                    &ctx.workspace,
                    &ctx.project_id,
                    &session_id,
                    plan.keep.clone(),
                    ctx.now_ms,
                )
                .await
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            Ok(if ctx.json {
                serde_json::json!({
                    "kept": outcome.kept,
                    "retired": plan.dropped,
                    "segments": outcome.segments,
                    "already_present": outcome.already_present,
                })
                .to_string()
            } else {
                format!(
                    "retired {} observation(s); the session is now one segment of {}",
                    plan.dropped, outcome.kept
                )
            })
        }
        Command::Search {
            query,
            limit,
            global,
            no_recency,
            no_neighbors,
            no_vector,
        } => {
            // Freshness helps but must not dominate relevance: the boost is
            // bounded, and --no-recency turns it off entirely.
            let tuning = qm_search::SearchTuning {
                now_ms: if *no_recency { 0 } else { ctx.now_ms },
                neighbor_expansion: !*no_neighbors,
                vector_search: !*no_vector,
                ..Default::default()
            };
            // An unconfigured provider is not an error, it simply means this
            // search has no vector stream.
            let embedder = if *no_vector {
                None
            } else {
                qm_search::vector::embedder_from_env()?
            };
            let embedder = embedder.as_deref();
            let outcome = if *global {
                let projects = ctx
                    .project
                    .list_projects(&ctx.workspace)
                    .await
                    .map_err(|error| anyhow::anyhow!("{error}"))?;
                search_workspace_tuned(
                    ctx.bucket.as_ref(),
                    &ctx.project,
                    &ctx.workspace,
                    &projects,
                    &ctx.cache_dir,
                    query,
                    *limit,
                    &tuning,
                    embedder,
                )
                .await?
            } else {
                search_project_tuned(
                    ctx.bucket.as_ref(),
                    &ctx.project,
                    &ctx.workspace,
                    &ctx.project_id,
                    &ctx.cache_dir,
                    query,
                    *limit,
                    &tuning,
                    embedder,
                )
                .await?
            };
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
                        let scope = if *global {
                            format!("{}/{}:", hit.workspace_id, hit.project_id)
                        } else {
                            String::new()
                        };
                        format!(
                            "{:.4}\t{scope}{}\t{}\t[{}]",
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
            let embedder = qm_search::vector::embedder_from_env()?;
            let embedded = embedder.is_some();
            let watermark = read_watermark(&watermark_path);
            // Enabling a provider has to re-embed pages that were published
            // without one, and their sequence numbers have not moved. Without
            // this the vector stream would stay empty forever on a machine
            // that had already caught up.
            let watermark = if embedded && !watermark.embedded {
                0
            } else {
                watermark.manifest_seq
            };
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
            let docs = attach_embeddings(docs, embedder.as_deref()).await?;
            let seq = loaded.manifest.seq + 1;
            // One directory per invocation: building an index into a directory
            // that already holds one fails, and seq+timestamp is not unique
            // enough (two projects can publish in the same millisecond).
            let build_dir = unique_build_dir(&ctx, "build");
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
            write_watermark(&watermark_path, loaded.manifest.seq, embedded)?;
            Ok(if ctx.json {
                serde_json::json!({
                    "generation": outcome.generation,
                    "already_present": outcome.already_present,
                    "pages": docs.len(),
                    "manifest_seq": loaded.manifest.seq,
                    "embedded": embedded,
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
            let build_dir = unique_build_dir(&ctx, "compact");
            let embedder = qm_search::vector::embedder_from_env()?;
            let outcome = compact_project(
                ctx.bucket.as_ref(),
                &ctx.project,
                &ctx.workspace,
                &ctx.project_id,
                &ctx.writer,
                &build_dir,
                ctx.now_ms,
                60_000,
                embedder.as_deref(),
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
        Command::Propose {
            path,
            title,
            body,
            rationale,
        } => {
            let target = ctx.page(path)?;
            let body = match body {
                Some(body) => body.clone(),
                None => read_stdin()?,
            };
            let (proposal, created) = ctx
                .project
                .propose_edit(qm_store::ProposalRequest {
                    workspace_id: ctx.workspace.clone(),
                    project_id: ctx.project_id.clone(),
                    target_path: target.clone(),
                    title: title.clone(),
                    body: body.clone(),
                    rationale: rationale.clone(),
                    created_by: ctx.writer.clone(),
                    now_ms: ctx.now_ms,
                })
                .await
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            Ok(if ctx.json {
                serde_json::to_string(&proposal)?
            } else {
                format!(
                    "{} proposal {} for {}",
                    if created { "staged" } else { "already staged" },
                    &proposal.id[..12.min(proposal.id.len())],
                    target.as_str()
                )
            })
        }
        Command::Proposals { state } => {
            let wanted = match state.as_str() {
                "all" => None,
                "pending" => Some(qm_core::ProposalState::Pending),
                "approved" => Some(qm_core::ProposalState::Approved),
                "rejected" => Some(qm_core::ProposalState::Rejected),
                other => bail!("unknown state {other:?}: use all, pending, approved or rejected"),
            };
            let proposals = ctx
                .project
                .list_proposals(&ctx.workspace, &ctx.project_id)
                .await
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            let filtered: Vec<&qm_core::Proposal> = proposals
                .iter()
                .filter(|proposal| wanted.as_ref().is_none_or(|want| proposal.state == *want))
                .collect();
            Ok(if ctx.json {
                serde_json::to_string(&filtered)?
            } else if filtered.is_empty() {
                "no proposals".to_string()
            } else {
                filtered
                    .iter()
                    .map(|proposal| {
                        format!(
                            "{:?}\t{}\t{}\t{}",
                            proposal.state,
                            proposal.id,
                            proposal.target_path.as_str(),
                            proposal.rationale
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            })
        }
        Command::Approve { id } => {
            let (proposal, seq) = ctx
                .project
                .approve_proposal(&ctx.workspace, &ctx.project_id, id, &ctx.writer, ctx.now_ms)
                .await
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            Ok(if ctx.json {
                serde_json::to_string(&proposal)?
            } else {
                format!(
                    "approved {} and applied it to {} at seq {seq}",
                    &proposal.id[..12.min(proposal.id.len())],
                    proposal.target_path.as_str()
                )
            })
        }
        Command::Reject { id, note } => {
            let proposal = ctx
                .project
                .reject_proposal(
                    &ctx.workspace,
                    &ctx.project_id,
                    id,
                    &ctx.writer,
                    ctx.now_ms,
                    note.clone(),
                )
                .await
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            Ok(if ctx.json {
                serde_json::to_string(&proposal)?
            } else {
                format!(
                    "rejected {} ({})",
                    &proposal.id[..12.min(proposal.id.len())],
                    proposal.target_path.as_str()
                )
            })
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
        Command::Export { to } => {
            std::fs::create_dir_all(to).with_context(|| format!("creating {}", to.display()))?;
            let loaded = ctx
                .project
                .load(&ctx.workspace, &ctx.project_id)
                .await
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            let mut index = Vec::new();
            for (path, entry) in &loaded.manifest.pages {
                let page_path = PagePath::new(path)?;
                let page = ctx
                    .project
                    .read_page_version(&ctx.workspace, &ctx.project_id, &page_path, &entry.page_id)
                    .await
                    .map_err(|error| anyhow::anyhow!("{error}"))?;
                let target = to.join(path);
                if let Some(parent) = target.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                // The markdown stays exactly what was stored: no frontmatter is
                // invented, so the export is editable in any editor.
                std::fs::write(&target, &page.body)
                    .with_context(|| format!("writing {}", target.display()))?;
                index.push(serde_json::json!({
                    "path": path,
                    "title": page.title,
                    "page_id": page.page_id.as_str(),
                }));
            }
            // Sessions travel as their raw observations so a rebuild can
            // recompile them rather than trusting a rendered page.
            let mut sessions = 0usize;
            for session in ctx
                .project
                .list_sessions(&ctx.workspace, &ctx.project_id)
                .await
                .map_err(|error| anyhow::anyhow!("{error}"))?
            {
                let observations = ctx
                    .project
                    .read_session_observations(&ctx.workspace, &ctx.project_id, &session)
                    .await
                    .map_err(|error| anyhow::anyhow!("{error}"))?;
                if observations.is_empty() {
                    continue;
                }
                let dir = to.join("_sessions");
                std::fs::create_dir_all(&dir)?;
                let mut body = String::new();
                for observation in &observations {
                    body.push_str(&serde_json::to_string(observation)?);
                    body.push('\n');
                }
                std::fs::write(dir.join(format!("{session}.jsonl")), body)?;
                sessions += 1;
            }
            std::fs::write(
                to.join("_export.json"),
                serde_json::to_vec_pretty(&serde_json::json!({
                    "schema": 1,
                    "workspace": ctx.workspace.to_string(),
                    "project": ctx.project_id.to_string(),
                    "manifest_seq": loaded.manifest.seq,
                    "pages": index,
                }))?,
            )?;
            let pages = index.len();
            Ok(if ctx.json {
                serde_json::json!({ "pages": pages, "sessions": sessions, "to": to.display().to_string() })
                    .to_string()
            } else {
                format!(
                    "exported {pages} page(s) and {sessions} session(s) to {}",
                    to.display()
                )
            })
        }
        Command::Import { from } => {
            let manifest_path = from.join("_export.json");
            let titles: std::collections::BTreeMap<String, String> = std::fs::read(&manifest_path)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
                .and_then(|value| value.get("pages").cloned())
                .and_then(|pages| pages.as_array().cloned())
                .map(|pages| {
                    pages
                        .iter()
                        .filter_map(|page| {
                            Some((
                                page.get("path")?.as_str()?.to_string(),
                                page.get("title")?.as_str()?.to_string(),
                            ))
                        })
                        .collect()
                })
                .unwrap_or_default();

            let mut imported = 0usize;
            let mut skipped = 0usize;
            let mut sessions = 0usize;
            for entry in walkdir::WalkDir::new(from).sort_by_file_name() {
                let entry = entry?;
                if !entry.file_type().is_file()
                    || entry.path().extension().is_none_or(|ext| ext != "md")
                {
                    continue;
                }
                let relative = entry
                    .path()
                    .strip_prefix(from)?
                    .to_string_lossy()
                    .replace('\\', "/");
                let page_path = PagePath::new(&relative)?;
                let body = std::fs::read_to_string(entry.path())?;
                // Re-importing an unchanged file must not append a version.
                if let Some(current) = ctx
                    .project
                    .read_page(&ctx.workspace, &ctx.project_id, &page_path)
                    .await
                    .map_err(|error| anyhow::anyhow!("{error}"))?
                    && current.body == body
                {
                    skipped += 1;
                    continue;
                }
                ctx.project
                    .commit_page(CommitPageRequest {
                        workspace_id: ctx.workspace.clone(),
                        project_id: ctx.project_id.clone(),
                        path: page_path,
                        title: titles
                            .get(&relative)
                            .cloned()
                            .unwrap_or_else(|| relative.clone()),
                        body,
                        writer_id: ctx.writer.clone(),
                        now_ms: ctx.now_ms,
                    })
                    .await
                    .map_err(|error| anyhow::anyhow!("{error}"))?;
                imported += 1;
            }

            let sessions_dir = from.join("_sessions");
            if sessions_dir.is_dir() {
                for entry in walkdir::WalkDir::new(&sessions_dir).sort_by_file_name() {
                    let entry = entry?;
                    if !entry.file_type().is_file()
                        || entry.path().extension().is_none_or(|ext| ext != "jsonl")
                    {
                        continue;
                    }
                    let session_id = SessionId::new(
                        entry
                            .path()
                            .file_stem()
                            .unwrap_or_default()
                            .to_string_lossy(),
                    )?;
                    let mut observations = Vec::new();
                    for line in std::fs::read_to_string(entry.path())?.lines() {
                        if line.trim().is_empty() {
                            continue;
                        }
                        observations.push(serde_json::from_str::<Observation>(line)?);
                    }
                    if observations.is_empty() {
                        continue;
                    }
                    ctx.project
                        .ingest_observations(IngestObservationsRequest {
                            workspace_id: ctx.workspace.clone(),
                            project_id: ctx.project_id.clone(),
                            session_id,
                            writer_id: ctx.writer.clone(),
                            observations,
                            now_ms: ctx.now_ms,
                        })
                        .await
                        .map_err(|error| anyhow::anyhow!("{error}"))?;
                    sessions += 1;
                }
            }

            Ok(if ctx.json {
                serde_json::json!({ "imported": imported, "unchanged": skipped, "sessions": sessions })
                    .to_string()
            } else {
                format!(
                    "imported {imported} page(s) ({skipped} unchanged) and {sessions} session(s)"
                )
            })
        }
        Command::Verify { global, strict } => {
            // First prove the backend itself honours the contract everything
            // else rests on: a store without conditional writes is not
            // "unhealthy", it is unusable, and that is worth knowing up front.
            let mut problems = Vec::new();
            let cas = qm_store::CasStore::new(std::sync::Arc::clone(&ctx.bucket), "");
            let probe_key = format!("qm-verify/{}-{}", ctx.workspace, ctx.writer);
            match qm_store::verify_conditional_writes(&cas, &probe_key).await {
                Ok(report) => {
                    let _ = cas.delete(&probe_key).await;
                    if !report.all_passed() {
                        problems.push(qm_store::Problem {
                            kind: "cas".into(),
                            subject: probe_key.clone(),
                            detail: report.render(),
                        });
                    }
                }
                Err(error) => problems.push(qm_store::Problem {
                    kind: "cas".into(),
                    subject: probe_key.clone(),
                    detail: error.to_string(),
                }),
            }

            let projects: Vec<ProjectId> = if *global {
                ctx.project
                    .list_projects(&ctx.workspace)
                    .await
                    .map_err(|error| anyhow::anyhow!("{error}"))?
            } else {
                vec![ctx.project_id.clone()]
            };
            let mut reports = Vec::new();
            let mut pages = 0usize;
            let mut sessions = 0usize;
            let mut splits = 0usize;
            for project_id in &projects {
                let report = ctx
                    .project
                    .verify_project(&ctx.workspace, project_id)
                    .await
                    .map_err(|error| anyhow::anyhow!("{error}"))?;
                pages += report.pages;
                sessions += report.sessions;
                splits += report.splits;
                for problem in &report.problems {
                    problems.push(qm_store::Problem {
                        subject: format!("{project_id}/{}", problem.subject),
                        ..problem.clone()
                    });
                }
                reports.push((project_id.clone(), report.manifest_seq));
            }

            let text = if ctx.json {
                serde_json::json!({
                    "projects": reports
                        .iter()
                        .map(|(project, seq)| serde_json::json!({
                            "project": project.to_string(),
                            "manifest_seq": seq,
                        }))
                        .collect::<Vec<_>>(),
                    "pages": pages,
                    "sessions": sessions,
                    "splits": splits,
                    "problems": problems,
                })
                .to_string()
            } else if problems.is_empty() {
                format!(
                    "verified {} project(s): {} page(s), {} session(s), {} split(s), no problems",
                    projects.len(),
                    pages,
                    sessions,
                    splits
                )
            } else {
                let mut out = format!(
                    "{} problem(s) across {} project(s):",
                    problems.len(),
                    projects.len()
                );
                for problem in &problems {
                    out.push_str(&format!(
                        "\n  [{}] {}: {}",
                        problem.kind, problem.subject, problem.detail
                    ));
                }
                out
            };
            if *strict && !problems.is_empty() {
                bail!("{text}");
            }
            Ok(text)
        }
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
        Command::Recent { limit } => {
            let pages = ctx
                .project
                .recent_pages(&ctx.workspace, &ctx.project_id, *limit)
                .await
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            Ok(if ctx.json {
                let described: Vec<serde_json::Value> = pages
                    .iter()
                    .map(|(path, entry)| {
                        serde_json::json!({
                            "path": path,
                            "page_id": entry.page_id,
                            "title": entry.title,
                            "created_at_ms": entry.created_at_ms,
                            "seq": entry.seq,
                            "writer_id": entry.writer_id,
                        })
                    })
                    .collect();
                serde_json::to_string(&described)?
            } else if pages.is_empty() {
                // Asking for nothing is not the same as finding nothing.
                if *limit == 0 {
                    String::new()
                } else {
                    "no pages".to_string()
                }
            } else {
                pages
                    .iter()
                    .map(|(path, entry)| {
                        format!(
                            "{}\t{}\t{}",
                            entry.created_at_ms,
                            path.as_str(),
                            // One line per page, three tab-separated fields
                            // per line: that is this output's whole contract,
                            // and titles are not validated. A bare newline,
                            // a CRLF, a tab or an escape sequence would each
                            // break some half of it, so every control
                            // character becomes a space.
                            entry
                                .title
                                .chars()
                                .map(|c| if c.is_control() { ' ' } else { c })
                                .collect::<String>()
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            })
        }
        Command::Digest {
            since_ms,
            hours,
            limit,
        } => {
            // `--since-ms` wins outright; otherwise the window is relative to
            // this command's clock, so a digest and the writes it reports on
            // agree about what "now" means.
            let since = match *since_ms {
                Some(ms) => ms,
                None => {
                    let hours = hours.unwrap_or(DIGEST_DEFAULT_HOURS);
                    ctx.now_ms.saturating_sub(hours.saturating_mul(3_600_000))
                }
            };
            let digest = ctx
                .project
                .digest(&ctx.workspace, &ctx.project_id, since, *limit)
                .await
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            Ok(if ctx.json {
                serde_json::to_string(&digest)?
            } else {
                render_digest(&digest)
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

/// A build directory that no other invocation can be using.
///
/// Tantivy refuses to create an index in a directory that already has one, so
/// this must be unique across projects, processes and time — a millisecond
/// timestamps is not enough.
fn unique_build_dir(ctx: &Context, label: &str) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0);
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    ctx.cache_dir
        .join(format!("{label}-{}-{nanos}-{counter}", std::process::id()))
}

/// Local record of how far this machine has published.
///
/// Deliberately local: each machine publishes its own splits, so its publish
/// position is its own business. Losing the file costs one redundant full
/// publish, never correctness.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PublishWatermark {
    manifest_seq: u64,
    /// Whether the split published at that position carried embeddings.
    ///
    /// `serde(default)` reads a file written before the vector stream existed
    /// as `false`, which is exactly what it was — and that is what makes the
    /// first publish after a provider is configured re-embed everything.
    #[serde(default)]
    embedded: bool,
}

fn publish_watermark_path(ctx: &Context) -> PathBuf {
    ctx.cache_dir.join(format!(
        "publish-{}-{}-{}.json",
        ctx.workspace, ctx.project_id, ctx.writer
    ))
}

fn read_watermark(path: &PathBuf) -> PublishWatermark {
    std::fs::read(path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<PublishWatermark>(&bytes).ok())
        .unwrap_or(PublishWatermark {
            manifest_seq: 0,
            embedded: false,
        })
}

fn write_watermark(path: &PathBuf, manifest_seq: u64, embedded: bool) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let bytes = serde_json::to_vec(&PublishWatermark {
        manifest_seq,
        embedded,
    })?;
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
    async fn recent_lists_the_last_pages_to_change() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = TempDir::new().unwrap();

        for (path, title, body, now_ms) in [
            ("notes/first.md", "First", "one", 1_000),
            ("notes/second.md", "Second", "two", 2_000),
        ] {
            execute(
                &cli(&[
                    "write-page",
                    "--path",
                    path,
                    "--title",
                    title,
                    "--body",
                    body,
                ]),
                Context {
                    now_ms,
                    ..context(Arc::clone(&bucket), &cache)
                },
            )
            .await
            .unwrap();
        }

        let out = execute(
            &cli(&["recent", "--json"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&out).unwrap();
        let paths: Vec<&str> = parsed
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["path"].as_str().unwrap())
            .collect();
        assert_eq!(paths, ["notes/second.md", "notes/first.md"]);

        // The human shape is what a person reads at the top of a session.
        let text = execute(&cli(&["recent"]), context(Arc::clone(&bucket), &cache))
            .await
            .unwrap();
        assert_eq!(
            text,
            "2000\tnotes/second.md\tSecond\n1000\tnotes/first.md\tFirst"
        );
        assert_eq!(text.lines().count(), 2, "one page per line");

        // `--limit` narrows it; a two-page corpus cannot tell the flag apart
        // from the default, so ask for exactly one and assert the default
        // separately — a default nothing checks is a default free to drift.
        let limited = execute(
            &cli(&["recent", "--limit", "1"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        assert_eq!(limited, "2000\tnotes/second.md\tSecond");
        assert!(matches!(
            cli(&["recent"]).command,
            Command::Recent {
                limit: RECENT_DEFAULT_LIMIT
            }
        ));

        // One line per page, three tab-separated fields per line. Titles are
        // not validated, so the realistic CRLF (and the tab that would eat a
        // field boundary) must not be able to break either half of it.
        execute(
            &cli(&[
                "write-page",
                "--path",
                "notes/multiline.md",
                "--title",
                "two\r\nlines\there",
                "--body",
                "body",
            ]),
            Context {
                now_ms: 3_000,
                ..context(Arc::clone(&bucket), &cache)
            },
        )
        .await
        .unwrap();
        let tricky = execute(&cli(&["recent"]), context(Arc::clone(&bucket), &cache))
            .await
            .unwrap();
        assert_eq!(tricky.lines().count(), 3, "one page per line: {tricky}");
        assert!(tricky.contains("two  lines here"), "{tricky}");
        for line in tricky.lines() {
            assert_eq!(
                line.matches('\t').count(),
                2,
                "three tab-separated fields per line: {line:?}"
            );
        }

        // Asking for nothing prints nothing; it must not claim the project is
        // empty.
        let none = execute(
            &cli(&["recent", "--limit", "0"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        assert_eq!(none, "");
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
    async fn export_and_import_move_a_project_between_buckets() {
        let source: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = TempDir::new().unwrap();

        // A project with two pages and a captured session.
        for (index, (path, body)) in [
            ("notes/raft.md", "leader election"),
            ("notes/quickwit.md", "`tantivy` splits"),
        ]
        .iter()
        .enumerate()
        {
            execute(
                &cli(&[
                    "write-page",
                    "--path",
                    path,
                    "--title",
                    path,
                    "--body",
                    body,
                ]),
                Context {
                    now_ms: (index as i64 + 1) * 1_000,
                    ..context(Arc::clone(&source), &cache)
                },
            )
            .await
            .unwrap();
        }
        execute(
            &cli(&["capture", "--session", "sess-1", "--text", "an observation"]),
            Context {
                now_ms: 5_000,
                ..context(Arc::clone(&source), &cache)
            },
        )
        .await
        .unwrap();

        let exported = TempDir::new().unwrap();
        let out = execute(
            &cli(&["export", "--to", &exported.path().to_string_lossy()]),
            context(Arc::clone(&source), &cache),
        )
        .await
        .unwrap();
        assert!(out.contains("2 page(s)"), "{out}");
        assert!(exported.path().join("notes/raft.md").is_file());
        assert!(exported.path().join("_export.json").is_file());
        assert!(exported.path().join("_sessions/sess-1.jsonl").is_file());
        assert_eq!(
            std::fs::read_to_string(exported.path().join("notes/raft.md")).unwrap(),
            "leader election",
            "exported markdown must be exactly what was stored"
        );

        // A different bucket imports it.
        let target: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let target_cache = TempDir::new().unwrap();
        let imported = execute(
            &cli(&["import", "--from", &exported.path().to_string_lossy()]),
            Context {
                now_ms: 9_000,
                ..context(Arc::clone(&target), &target_cache)
            },
        )
        .await
        .unwrap();
        assert!(imported.contains("imported 2 page(s)"), "{imported}");

        let ctx = context(Arc::clone(&target), &target_cache);
        let page = ctx
            .project
            .read_page(
                &ctx.workspace,
                &ctx.project_id,
                &PagePath::new("notes/quickwit.md").unwrap(),
            )
            .await
            .unwrap()
            .expect("imported page");
        assert_eq!(page.body, "`tantivy` splits");
        assert_eq!(
            page.title, "notes/quickwit.md",
            "titles travel with the export"
        );
        let sessions = ctx
            .project
            .list_sessions(&ctx.workspace, &ctx.project_id)
            .await
            .unwrap();
        assert_eq!(sessions.len(), 1, "sessions travel as raw observations");

        // Re-importing an unchanged directory adds no versions.
        let mut again = context(Arc::clone(&target), &target_cache);
        again.now_ms = 10_000;
        let again_out = execute(
            &cli(&["import", "--from", &exported.path().to_string_lossy()]),
            again,
        )
        .await
        .unwrap();
        assert!(again_out.contains("0 page(s)"), "{again_out}");
        assert!(again_out.contains("2 unchanged"), "{again_out}");
        let ctx = context(Arc::clone(&target), &target_cache);
        assert_eq!(
            ctx.project
                .load(&ctx.workspace, &ctx.project_id)
                .await
                .unwrap()
                .manifest
                .seq,
            2,
            "a repeated import must not append versions"
        );
    }

    #[tokio::test]
    async fn verify_reports_a_healthy_scope_and_a_missing_object() {
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
            Context {
                now_ms: 1_000,
                ..context(Arc::clone(&bucket), &cache)
            },
        )
        .await
        .unwrap();

        let mut ctx = context(Arc::clone(&bucket), &cache);
        ctx.now_ms = 2_000;
        let healthy = execute(&cli(&["verify"]), ctx).await.unwrap();
        assert!(healthy.contains("no problems"), "{healthy}");

        // Remove the object the manifest points at.
        let key = {
            let ctx = context(Arc::clone(&bucket), &cache);
            let path = PagePath::new("notes/raft.md").unwrap();
            let entry = ctx
                .project
                .load(&ctx.workspace, &ctx.project_id)
                .await
                .unwrap()
                .manifest
                .head(&path)
                .unwrap()
                .page_id
                .clone();
            ctx.project
                .layout()
                .page_version(&ctx.workspace, &ctx.project_id, &path, &entry)
        };
        {
            let cas = qm_store::CasStore::new(Arc::clone(&bucket), "");
            cas.delete(&key).await.unwrap();
        }

        let mut ctx = context(Arc::clone(&bucket), &cache);
        ctx.now_ms = 3_000;
        let broken = execute(&cli(&["verify"]), ctx).await.unwrap();
        assert!(broken.contains("page_version"), "{broken}");

        // Strict mode turns the same finding into a non-zero exit.
        let mut ctx = context(Arc::clone(&bucket), &cache);
        ctx.now_ms = 4_000;
        let strict = execute(&cli(&["verify", "--strict"]), ctx)
            .await
            .expect_err("strict verification of a broken scope must fail");
        assert!(strict.to_string().contains("page_version"), "{strict}");
    }

    #[tokio::test]
    async fn global_search_spans_projects_and_reports_provenance() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = TempDir::new().unwrap();

        // Two projects, each publishing a page that answers the same query.
        for (project, body) in [
            ("alpha", "we run `tantivy` in alpha"),
            ("beta", "tantivy powers beta too"),
        ] {
            let ctx = Context::new(
                Arc::clone(&bucket),
                "acme",
                project,
                "mbp-a",
                cache.path().to_path_buf(),
                1_000,
                false,
            )
            .unwrap();
            execute(
                &cli(&["write-page", "--path", "notes/engine.md", "--body", body]),
                Context {
                    now_ms: 1_000,
                    ..ctx
                },
            )
            .await
            .unwrap();
            let mut ctx = Context::new(
                Arc::clone(&bucket),
                "acme",
                project,
                "mbp-a",
                cache.path().to_path_buf(),
                2_000,
                false,
            )
            .unwrap();
            ctx.now_ms = 2_000;
            execute(&cli(&["publish"]), ctx).await.unwrap();
        }

        // Scoped search sees only the project it is scoped to.
        let mut scoped = Context::new(
            Arc::clone(&bucket),
            "acme",
            "alpha",
            "mbp-a",
            cache.path().to_path_buf(),
            3_000,
            false,
        )
        .unwrap();
        scoped.now_ms = 3_000;
        let scoped_out = execute(&cli(&["search", "tantivy", "--json"]), scoped)
            .await
            .unwrap();
        let scoped_json: serde_json::Value = serde_json::from_str(&scoped_out).unwrap();
        let scoped_hits = scoped_json["hits"].as_array().unwrap();
        assert_eq!(scoped_hits.len(), 1);
        assert_eq!(scoped_hits[0]["project_id"], "alpha");

        // Global search sees both, with provenance on every hit.
        let mut global = Context::new(
            Arc::clone(&bucket),
            "acme",
            "alpha",
            "mbp-a",
            cache.path().to_path_buf(),
            3_000,
            false,
        )
        .unwrap();
        global.now_ms = 3_000;
        let global_out = execute(&cli(&["search", "tantivy", "--global", "--json"]), global)
            .await
            .unwrap();
        let global_json: serde_json::Value = serde_json::from_str(&global_out).unwrap();
        let hits = global_json["hits"].as_array().unwrap();
        assert_eq!(hits.len(), 2, "{global_json}");
        let projects: std::collections::BTreeSet<&str> = hits
            .iter()
            .map(|hit| hit["project_id"].as_str().unwrap())
            .collect();
        assert_eq!(
            projects,
            std::collections::BTreeSet::from(["alpha", "beta"])
        );
        for hit in hits {
            assert_eq!(hit["workspace_id"], "acme");
            assert_eq!(hit["path"], "notes/engine.md");
        }
    }

    #[tokio::test]
    async fn proposals_gate_every_edit_through_an_approval() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = TempDir::new().unwrap();

        execute(
            &cli(&[
                "write-page",
                "--path",
                "notes/raft.md",
                "--body",
                "original body",
            ]),
            Context {
                now_ms: 1_000,
                ..context(Arc::clone(&bucket), &cache)
            },
        )
        .await
        .unwrap();

        // A curator stages an edit; nothing changes yet.
        let staged = execute(
            &cli(&[
                "propose",
                "--path",
                "notes/raft.md",
                "--title",
                "Raft",
                "--body",
                "rewritten body",
                "--rationale",
                "clearer",
            ]),
            Context {
                now_ms: 2_000,
                ..context(Arc::clone(&bucket), &cache)
            },
        )
        .await
        .unwrap();
        assert!(staged.contains("staged proposal"), "{staged}");
        let listed = execute(
            &cli(&["proposals", "--json"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        let proposals: Vec<qm_core::Proposal> = serde_json::from_str(&listed).unwrap();
        assert_eq!(proposals.len(), 1);
        assert_eq!(proposals[0].state, qm_core::ProposalState::Pending);

        let unchanged = execute(
            &cli(&["read-page", "--path", "notes/raft.md"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        assert_eq!(unchanged, "original body", "a proposal must change nothing");

        // Approval applies it, once.
        let id = proposals[0].id.clone();
        let mut ctx = context(Arc::clone(&bucket), &cache);
        ctx.now_ms = 3_000;
        let approved = execute(&cli(&["approve", "--id", &id]), ctx).await.unwrap();
        assert!(approved.contains("applied"), "{approved}");
        let applied = execute(
            &cli(&["read-page", "--path", "notes/raft.md"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        assert_eq!(applied, "rewritten body");

        let mut ctx = context(Arc::clone(&bucket), &cache);
        ctx.now_ms = 4_000;
        let again = execute(&cli(&["approve", "--id", &id]), ctx)
            .await
            .expect_err("a decided proposal cannot be approved twice");
        assert!(again.to_string().contains("not pending"), "{again}");

        // And with nothing pending, the default listing is empty.
        let pending = execute(&cli(&["proposals"]), context(Arc::clone(&bucket), &cache))
            .await
            .unwrap();
        assert_eq!(pending, "no proposals");
    }

    #[tokio::test]
    async fn session_retention_is_a_dry_run_until_applied() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = TempDir::new().unwrap();

        for index in 0..5 {
            execute(
                &cli(&[
                    "capture",
                    "--session",
                    "sess-old",
                    "--text",
                    &format!("event {index}"),
                    "--at",
                    &(index * 1_000).to_string(),
                ]),
                Context {
                    now_ms: 10_000,
                    ..context(Arc::clone(&bucket), &cache)
                },
            )
            .await
            .unwrap();
        }

        // Dry run: keeps the newest two, writes nothing.
        let mut ctx = context(Arc::clone(&bucket), &cache);
        ctx.now_ms = 10_000;
        let plan = execute(
            &cli(&[
                "compact-session",
                "--session",
                "sess-old",
                "--keep-last",
                "2",
                "--keep-ms",
                "0",
            ]),
            ctx,
        )
        .await
        .unwrap();
        assert!(plan.contains("dry run"), "{plan}");
        let observations = {
            let ctx = context(Arc::clone(&bucket), &cache);
            ctx.project
                .read_session_observations(
                    &ctx.workspace,
                    &ctx.project_id,
                    &SessionId::new("sess-old").unwrap(),
                )
                .await
                .unwrap()
        };
        assert_eq!(observations.len(), 5, "a dry run must not retire anything");

        // Apply: the chain becomes one segment holding the two survivors.
        let mut ctx = context(Arc::clone(&bucket), &cache);
        ctx.now_ms = 10_000;
        let applied = execute(
            &cli(&[
                "compact-session",
                "--session",
                "sess-old",
                "--keep-last",
                "2",
                "--keep-ms",
                "0",
                "--apply",
            ]),
            ctx,
        )
        .await
        .unwrap();
        assert!(applied.contains("retired 3"), "{applied}");

        let ctx = context(Arc::clone(&bucket), &cache);
        let chain = ctx
            .project
            .read_session_chain(
                &ctx.workspace,
                &ctx.project_id,
                &SessionId::new("sess-old").unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(chain.len(), 1);
        let observations = ctx
            .project
            .read_session_observations(
                &ctx.workspace,
                &ctx.project_id,
                &SessionId::new("sess-old").unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(observations.len(), 2);
        assert_eq!(observations[0].text, "event 3");
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
    /// The vector stream is optional, so both of its absent forms have to work:
    /// a bucket with no provider, and a caller that explicitly turns it off.
    /// `--no-vector` is asserted unconditionally because a flag that only
    /// behaves when a provider happens to be configured is not a flag.
    #[tokio::test]
    async fn search_succeeds_with_the_vector_stream_off() {
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
                "leader election and log replication",
            ]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        assert!(out.contains("committed notes/raft.md"), "{out}");

        let mut publisher = context(Arc::clone(&bucket), &cache);
        publisher.now_ms = 2_000;
        let out = execute(&cli(&["publish"]), publisher).await.unwrap();
        assert!(out.contains("published 1 page"), "{out}");

        let mut searcher = context(Arc::clone(&bucket), &cache);
        searcher.now_ms = 3_000;
        let out = execute(
            &cli(&["search", "leader", "--no-vector", "--json"]),
            searcher,
        )
        .await
        .unwrap();
        let payload: serde_json::Value = serde_json::from_str(&out).unwrap();
        let streams = payload["streams_active"].to_string();
        assert!(streams.contains("body"), "{out}");
        assert!(
            !streams.contains("vector"),
            "--no-vector must keep the vector stream out of the outcome: {out}"
        );
        assert_eq!(payload["hits"].as_array().unwrap().len(), 1, "{out}");
    }

    /// A watermark written before the vector stream existed has to read as
    /// "not embedded". That is what makes the first publish after a provider is
    /// configured re-embed pages whose sequence numbers have not moved — the
    /// alternative is a vector stream that stays empty forever.
    #[test]
    fn a_watermark_from_before_the_vector_stream_reads_as_not_embedded() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("watermark.json");
        std::fs::write(&path, br#"{"manifest_seq":7}"#).unwrap();

        let watermark = read_watermark(&path);
        assert_eq!(watermark.manifest_seq, 7);
        assert!(!watermark.embedded, "an older file embedded nothing");

        write_watermark(&path, 7, true).unwrap();
        let watermark = read_watermark(&path);
        assert_eq!(watermark.manifest_seq, 7);
        assert!(watermark.embedded);

        // A missing file is the same shape as "nothing published yet".
        assert_eq!(
            read_watermark(&dir.path().join("absent.json")).manifest_seq,
            0
        );
    }

    /// End to end: two pages written, one deleted, and the digest reports all
    /// three commits -- including the deletion, which the manifest can no
    /// longer answer for.
    #[tokio::test]
    async fn digest_reports_writes_and_deletions_end_to_end() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = TempDir::new().unwrap();

        for (path, at) in [("notes/a.md", 1_000_i64), ("notes/b.md", 2_000)] {
            execute(
                &cli(&[
                    "write-page",
                    "--path",
                    path,
                    "--title",
                    path,
                    "--body",
                    "body of the page",
                ]),
                Context {
                    now_ms: at,
                    ..context(Arc::clone(&bucket), &cache)
                },
            )
            .await
            .unwrap();
        }
        let deleted = execute(
            &cli(&["delete-page", "--path", "notes/b.md"]),
            Context {
                now_ms: 3_000,
                ..context(Arc::clone(&bucket), &cache)
            },
        )
        .await
        .unwrap();
        assert!(deleted.contains("deleted"), "{deleted}");

        let out = execute(
            &cli(&["digest", "--json"]),
            Context {
                now_ms: 4_000,
                ..context(Arc::clone(&bucket), &cache)
            },
        )
        .await
        .unwrap();
        let digest: qm_store::Digest = serde_json::from_str(&out).unwrap();

        let written = digest
            .pages
            .iter()
            .filter(|record| record.kind == qm_core::CommitKind::PageWritten)
            .count();
        let removed = digest
            .pages
            .iter()
            .filter(|record| record.kind == qm_core::CommitKind::PageDeleted)
            .count();
        assert_eq!((written, removed), (2, 1), "{out}");
        // Newest first, and the deletion is the newest commit of all.
        assert_eq!(digest.pages[0].kind, qm_core::CommitKind::PageDeleted);
        assert_eq!(digest.pages[0].path.as_str(), "notes/b.md");
    }

    /// The human output is three labelled sections, and a window with nothing
    /// in it says so instead of printing three bare headers.
    #[tokio::test]
    async fn digest_renders_three_sections_or_says_there_is_nothing() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = TempDir::new().unwrap();

        execute(
            &cli(&["capture", "--session", "sess-1", "--text", "did a thing"]),
            Context {
                now_ms: 1_000,
                ..context(Arc::clone(&bucket), &cache)
            },
        )
        .await
        .unwrap();
        execute(
            &cli(&["write-page", "--path", "notes/a.md", "--body", "body"]),
            Context {
                now_ms: 2_000,
                ..context(Arc::clone(&bucket), &cache)
            },
        )
        .await
        .unwrap();

        let out = execute(
            &cli(&["digest"]),
            Context {
                now_ms: 3_000,
                ..context(Arc::clone(&bucket), &cache)
            },
        )
        .await
        .unwrap();
        assert!(out.contains("pages (1)"), "{out}");
        assert!(out.contains("sessions (1)"), "{out}");
        assert!(out.contains("handoffs (0)"), "{out}");

        // A window with nothing in it gets one plain sentence rather than
        // three headers that all say nothing.
        let empty = execute(
            &cli(&["digest", "--since-ms", "9999999"]),
            Context {
                now_ms: 3_000,
                ..context(Arc::clone(&bucket), &cache)
            },
        )
        .await
        .unwrap();
        assert_eq!(empty, "no recent activity");
    }

    /// `--since-ms` and `--hours` both narrow the window, and the default is a
    /// day, not "everything ever".
    #[tokio::test]
    async fn digest_window_flags_exclude_older_commits() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = TempDir::new().unwrap();

        let day_ms = 86_400_000_i64;
        execute(
            &cli(&["write-page", "--path", "notes/old.md", "--body", "old"]),
            Context {
                now_ms: 1_000,
                ..context(Arc::clone(&bucket), &cache)
            },
        )
        .await
        .unwrap();
        execute(
            &cli(&["write-page", "--path", "notes/new.md", "--body", "new"]),
            Context {
                now_ms: 1_000 + day_ms * 2,
                ..context(Arc::clone(&bucket), &cache)
            },
        )
        .await
        .unwrap();

        // Default window: a day, so the older commit is out.
        let out = execute(
            &cli(&["digest", "--json"]),
            Context {
                now_ms: 1_000 + day_ms * 2 + 1_000,
                ..context(Arc::clone(&bucket), &cache)
            },
        )
        .await
        .unwrap();
        let digest: qm_store::Digest = serde_json::from_str(&out).unwrap();
        let paths: Vec<&str> = digest
            .pages
            .iter()
            .map(|record| record.path.as_str())
            .collect();
        assert_eq!(paths, vec!["notes/new.md"], "{out}");

        // `--hours` widens it enough to reach both.
        let out = execute(
            &cli(&["digest", "--hours", "72", "--json"]),
            Context {
                now_ms: 1_000 + day_ms * 2 + 1_000,
                ..context(Arc::clone(&bucket), &cache)
            },
        )
        .await
        .unwrap();
        let digest: qm_store::Digest = serde_json::from_str(&out).unwrap();
        assert_eq!(digest.pages.len(), 2, "{out}");

        // The two flags are mutually exclusive rather than silently ranked.
        assert!(Cli::try_parse_from(["qm", "digest", "--hours", "1", "--since-ms", "0"]).is_err());
    }
}
