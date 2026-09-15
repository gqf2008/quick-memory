//! MCP surface for quick-memory.
//!
//! Every tool delegates to the same `qm_cli::execute` dispatch the command line
//! uses, so the two surfaces cannot drift: a tool is a typed argument list and
//! a call, never a second implementation of the protocol.
//!
//! Tools are named `memory_*` after the ai-memory convention so an agent's
//! existing habits transfer.

use std::path::PathBuf;
use std::sync::Arc;

use object_store::ObjectStore;
use qm_cli::{Cli, Command, Context as CommandContext, HandoffAction, MaintainFailure, execute};
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::tool::ToolCallContext;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, Content, ServerCapabilities, ServerInfo};
use rmcp::service::RequestContext;
use rmcp::{ErrorData as McpError, RoleServer, ServerHandler, tool, tool_handler, tool_router};
use serde::Deserialize;

/// Arguments for `memory_capture`.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct CaptureArgs {
    /// Session to capture into.
    pub session: String,
    /// Event text, already written by the agent.
    pub text: String,
    /// Event kind, e.g. `tool_use`.
    #[serde(default)]
    pub kind: Option<String>,
    /// Actor that emitted the event.
    #[serde(default)]
    pub actor: Option<String>,
    /// Client timestamp in milliseconds.
    #[serde(default)]
    pub at: Option<i64>,
}

/// Arguments for `memory_consolidate`.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SessionArgs {
    /// Session id.
    pub session: String,
}

/// Arguments for `memory_search`.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct SearchArgs {
    /// Query text.
    pub query: String,
    /// Maximum hits to return.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Search every project in the workspace instead of just this one.
    #[serde(default)]
    pub global: Option<bool>,
    /// Disable the vector stream for this search.
    ///
    /// Only useful to force keyword-only recall; leaving it unset runs the
    /// vector stream whenever an embedding provider is configured, and means
    /// nothing when none is.
    #[serde(default)]
    pub no_vector: Option<bool>,
}

/// Arguments for `memory_write_page`.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct WritePageArgs {
    /// Path inside the project, e.g. `notes/raft.md`.
    pub path: String,
    /// Markdown body.
    pub body: String,
    /// Page title; defaults to the path.
    #[serde(default)]
    pub title: Option<String>,
}

/// Arguments for `memory_handoff_open`.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct HandoffOpenArgs {
    /// Short title.
    pub title: String,
    /// What the next session needs to know.
    pub body: String,
}

/// Arguments for `memory_handoff_list`.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct HandoffListArgs {
    /// Filter: `all`, `open`, `claimed`, or `done`.
    #[serde(default)]
    pub state: Option<String>,
}

/// Arguments for handoff-addressed tools.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct HandoffIdArgs {
    /// Handoff id.
    pub id: String,
}

/// Arguments for `memory_restore`.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RestoreArgs {
    /// Path inside the project.
    pub path: String,
    /// Version id to restore, from `memory_history`.
    pub version: String,
}

/// Arguments for `memory_read_page`.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ReadPageArgs {
    /// Path inside the project.
    pub path: String,
    /// Read the version that was current at this Unix time (ms).
    #[serde(default)]
    pub as_of: Option<i64>,
}

/// Arguments for `memory_compact_session`.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct CompactSessionArgs {
    /// Session id.
    pub session: String,
    /// Keep observations newer than this many milliseconds.
    #[serde(default)]
    pub keep_ms: Option<i64>,
    /// Always keep at least this many of the newest observations.
    #[serde(default)]
    pub keep_last: Option<usize>,
    /// Actually rewrite the chain. Omitted or false is a dry run.
    #[serde(default)]
    pub apply: Option<bool>,
}

/// Arguments for `memory_propose`.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ProposeArgs {
    /// Target page path.
    pub path: String,
    /// Title to write.
    pub title: String,
    /// Proposed body.
    pub body: String,
    /// Why the change is proposed.
    #[serde(default)]
    pub rationale: String,
}

/// Arguments for `memory_proposals`.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ProposalsArgs {
    /// Filter: `all`, `pending`, `approved`, or `rejected`.
    #[serde(default)]
    pub state: Option<String>,
}

/// Arguments for `memory_reject`.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RejectArgs {
    /// Proposal id.
    pub id: String,
    /// Why it was rejected.
    #[serde(default)]
    pub note: Option<String>,
}

/// Arguments for `memory_verify`.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct VerifyArgs {
    /// Check every project in the workspace.
    #[serde(default)]
    pub global: Option<bool>,
}

/// Arguments for `memory_log`.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct LogArgs {
    /// Maximum entries.
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Arguments for `memory_recent`.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RecentArgs {
    /// Maximum pages.
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Arguments for `memory_digest`.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DigestArgs {
    /// Look back this many hours (default: 24).
    #[serde(default)]
    pub since_hours: Option<i64>,
    /// Maximum entries per section (default: 20).
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Arguments for `memory_maintain`.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct MaintainArgs {
    /// Compiler: `auto` (LLM when QM_LLM_BASE_URL is set, else rules),
    /// `rules`, or `llm` (which falls back to rules on failure).
    #[serde(default)]
    pub compiler: Option<String>,
    /// Maximum current-scope spooled events to attempt before consolidating.
    #[serde(default)]
    pub drain_limit: Option<usize>,
}

/// Arguments for page-addressed tools.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct PageArgs {
    /// Path inside the project.
    pub path: String,
}

/// The MCP server.
#[derive(Clone)]
pub struct MemoryServer {
    bucket: Arc<dyn ObjectStore>,
    bucket_identity: String,
    /// Storage form new commits are written in, from `QM_MANIFEST_FORMAT`.
    /// Reads do not use it: they dispatch on what the bucket actually holds.
    manifest_format: u32,
    workspace: String,
    project: String,
    writer: String,
    cache_dir: PathBuf,
    clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    tool_router: ToolRouter<Self>,
}

/// Resolve the wall clock, used for capture and commit timestamps.
fn wall_clock_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
}

impl MemoryServer {
    /// Build a server for one workspace/project scope.
    #[must_use]
    pub fn new(
        bucket: Arc<dyn ObjectStore>,
        workspace: String,
        project: String,
        writer: String,
        cache_dir: PathBuf,
    ) -> Self {
        Self {
            bucket,
            bucket_identity: String::new(),
            manifest_format: qm_core::MANIFEST_FORMAT_WHOLE,
            workspace,
            project,
            writer,
            cache_dir,
            clock: Arc::new(wall_clock_ms),
            tool_router: Self::tool_router(),
        }
    }

    /// Name the bucket the server's object store points at.
    ///
    /// The value is used as the scope for local facts about the bucket, such
    /// as the publish watermark. Without it, one cache directory shared by
    /// two buckets can suppress the first publish into the second.
    #[must_use]
    pub fn with_bucket_identity(mut self, identity: impl Into<String>) -> Self {
        self.bucket_identity = identity.into();
        self
    }

    /// Select the storage form this server's writes use.
    ///
    /// The value is resolved once from the process environment by the binary
    /// that starts the server, and threaded through here so every tool call
    /// writes the same form. A server that fell back to the default while the
    /// operator had asked for shards would write a whole manifest into a
    /// sharded scope, which the store refuses — loudly, but only after the
    /// misconfiguration reached a write.
    #[must_use]
    pub fn with_manifest_format(mut self, format: u32) -> Self {
        self.manifest_format = format;
        self
    }

    /// Replace the wall clock with a deterministic one for regression tests.
    #[cfg(test)]
    fn with_clock(mut self, clock: Arc<dyn Fn() -> i64 + Send + Sync>) -> Self {
        self.clock = clock;
        self
    }

    /// Resolve the current wall clock for one tool call.
    fn clock_ms(&self) -> i64 {
        (self.clock)()
    }

    /// Build the shared CLI/context pair for one tool call at `now_ms`.
    fn command_context(
        &self,
        command: Command,
        now_ms: i64,
    ) -> Result<(Cli, CommandContext), McpError> {
        let cli = Cli {
            workspace: self.workspace.clone(),
            project: self.project.clone(),
            writer: self.writer.clone(),
            cache_dir: Some(self.cache_dir.clone()),
            json: true,
            command,
        };
        let context = CommandContext::new(
            Arc::clone(&self.bucket),
            &cli.workspace,
            &cli.project,
            &cli.writer,
            self.cache_dir.clone(),
            now_ms,
            true,
        )
        .map_err(|error| McpError::invalid_params(error.to_string(), None))?
        .with_bucket_identity(self.bucket_identity.clone())
        .with_manifest_format(self.manifest_format);
        Ok((cli, context))
    }

    /// Run one command through the shared dispatch at an explicit clock.
    async fn dispatch_at(&self, command: Command, now_ms: i64) -> Result<CallToolResult, McpError> {
        let (cli, context) = self.command_context(command, now_ms)?;
        let output = execute(&cli, context)
            .await
            .map_err(|error| McpError::internal_error(error.to_string(), None))?;
        Ok(CallToolResult::success(vec![Content::text(output)]))
    }

    /// Run one command through the shared dispatch and return its JSON.
    async fn dispatch(&self, command: Command) -> Result<CallToolResult, McpError> {
        self.dispatch_at(command, self.clock_ms()).await
    }

    /// Run `maintain`, preserving its partial-failure report as tool content.
    ///
    /// `execute` intentionally returns `MaintainFailure` when the pass did
    /// some work but one or more sessions or the publish step failed. MCP
    /// callers need the same complete report the CLI prints, so that case is a
    /// tool-level error carrying the report rather than an opaque internal
    /// error. The call-time clock is captured once and threaded into the
    /// context so leases and commits cannot use a stale server-start time.
    async fn dispatch_maintain_at(
        &self,
        command: Command,
        now_ms: i64,
    ) -> Result<CallToolResult, McpError> {
        let (cli, context) = self.command_context(command, now_ms)?;
        match execute(&cli, context).await {
            Ok(output) => Ok(CallToolResult::success(vec![Content::text(output)])),
            Err(error) => {
                if let Some(failure) = error.downcast_ref::<MaintainFailure>() {
                    Ok(CallToolResult::error(vec![Content::text(
                        failure.report().to_string(),
                    )]))
                } else {
                    Err(McpError::internal_error(error.to_string(), None))
                }
            }
        }
    }

    /// Run `maintain` with the clock sampled at the call site.
    async fn dispatch_maintain(&self, command: Command) -> Result<CallToolResult, McpError> {
        self.dispatch_maintain_at(command, self.clock_ms()).await
    }
}

#[tool_router]
impl MemoryServer {
    /// Capture one observation into a session.
    #[tool(
        description = "Capture one event into a session's chain. Call this after \
                       finishing a meaningful step so the memory can be compiled later. \
                       Text is scrubbed and bounded on ingest; retrying the same event \
                       is a no-op."
    )]
    async fn memory_capture(
        &self,
        Parameters(args): Parameters<CaptureArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.dispatch(Command::Capture {
            session: args.session,
            kind: args.kind.unwrap_or_else(|| "observation".to_string()),
            actor: args.actor.unwrap_or_else(|| "qm-mcp".to_string()),
            text: Some(args.text),
            at: args.at,
        })
        .await
    }

    /// Compile a session's observations into its page.
    #[tool(description = "Compile a session's captured observations into \
                       sessions/<id>.md. Deterministic and idempotent: re-running \
                       over an unchanged chain writes nothing.")]
    async fn memory_consolidate(
        &self,
        Parameters(args): Parameters<SessionArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.dispatch(Command::Consolidate {
            session: args.session,
            // `auto` picks the LLM when QM_LLM_* is configured and falls back
            // to the rule renderer otherwise; the tool never fails for lack of
            // a model.
            compiler: "auto".to_string(),
        })
        .await
    }

    /// Search the project's memory.
    #[tool(
        description = "Search the project's memory. Call this BEFORE answering \
                       questions about prior work, decisions, or conventions. \
                       Returns compiled pages with a fused rank score; every hit \
                       has been checked against the authoritative manifest, so \
                       superseded and deleted content never appears. Pass
                       global=true to search every project in the workspace;
                       hits then carry their workspace and project. When an
                       embedding provider is configured (QM_EMBEDDING_*), a
                       semantic vector stream also runs, so pages that mean the
                       same thing without sharing words are still found."
    )]
    async fn memory_search(
        &self,
        Parameters(args): Parameters<SearchArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.dispatch(Command::Search {
            query: args.query,
            limit: args.limit.unwrap_or(10),
            global: args.global.unwrap_or(false),
            // Agents get the same freshness tie-breaker and neighbour expansion
            // the CLI does; both are bounded and reported per hit.
            no_recency: false,
            no_neighbors: false,
            no_vector: args.no_vector.unwrap_or(false),
        })
        .await
    }

    /// Commit a page version.
    #[tool(description = "Commit a page. Creates a new immutable version and \
                       supersedes the previous one; old versions stay readable. \
                       Run memory_publish afterwards to make it searchable.")]
    async fn memory_write_page(
        &self,
        Parameters(args): Parameters<WritePageArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.dispatch(Command::WritePage {
            path: args.path,
            title: args.title,
            body: Some(args.body),
        })
        .await
    }

    /// Read the current version of a page.
    #[tool(description = "Read a page. Pass as_of (Unix ms) to read the version \
                       that was current then — useful when asking what a page \
                       said at some earlier point.")]
    async fn memory_read_page(
        &self,
        Parameters(args): Parameters<ReadPageArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.dispatch(Command::ReadPage {
            path: args.path,
            as_of: args.as_of,
        })
        .await
    }

    /// Retire old observations from a session.
    #[tool(
        description = "Retire old observations from a session's chain. Dry run \
                       unless apply=true. Rewrites the chain as one segment, so \
                       retired segments become unreachable and are reclaimed by \
                       memory_compact/gc; keep_last always protects the newest \
                       observations even if a clock is wrong."
    )]
    async fn memory_compact_session(
        &self,
        Parameters(args): Parameters<CompactSessionArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.dispatch(Command::CompactSession {
            session: args.session,
            keep_ms: args.keep_ms.unwrap_or(2_592_000_000),
            keep_last: args.keep_last.unwrap_or(50),
            apply: args.apply.unwrap_or(false),
        })
        .await
    }

    /// Show the most recent commits.
    #[tool(
        description = "Show the most recent commits (newest first) with their \
                       sequence, wall-clock time, kind and path. Call this at the \
                       start of a session to see what has been happening."
    )]
    async fn memory_log(
        &self,
        Parameters(args): Parameters<LogArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.dispatch(Command::Log {
            limit: args.limit.unwrap_or(20),
        })
        .await
    }

    /// List pages by when they last changed.
    #[tool(
        description = "List pages by when they last changed, newest first, with \
                       each page's timestamp, path and title. Call this at the \
                       start of a session to answer \"what was the last session \
                       working on?\" before reaching for a full search."
    )]
    async fn memory_recent(
        &self,
        Parameters(args): Parameters<RecentArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.dispatch(Command::Recent {
            limit: args.limit.unwrap_or(qm_cli::RECENT_DEFAULT_LIMIT),
        })
        .await
    }

    /// Summarise what changed recently.
    #[tool(
        description = "Summarise what changed recently in three sections: pages \
                       (commits, including deletions), sessions (whose heads \
                       moved) and handoffs (created, claimed or finished). \
                       Assembled from authoritative objects, not from the search \
                       index. Call this FIRST at the start of a session to find \
                       out what happened while you were away."
    )]
    async fn memory_digest(
        &self,
        Parameters(args): Parameters<DigestArgs>,
    ) -> Result<CallToolResult, McpError> {
        // A digest is always relative to *now*, and this server is long-lived:
        // resolving the window against its startup clock would freeze the
        // look-back. So resolve against the wall clock at call time and hand
        // the dispatch an absolute instant.
        let hours = args
            .since_hours
            .unwrap_or(qm_cli::DIGEST_DEFAULT_HOURS)
            .max(0);
        // Resolve the window and the context clock from the same call-time
        // sample: a digest assembled from one instant must not rank against a
        // frozen server-start instant.
        let now_ms = self.clock_ms();
        let since_ms = now_ms.saturating_sub(hours.saturating_mul(3_600_000));
        self.dispatch_at(
            Command::Digest {
                since_ms: Some(since_ms),
                hours: None,
                limit: args.limit.unwrap_or(qm_cli::DIGEST_DEFAULT_LIMIT),
            },
            now_ms,
        )
        .await
    }

    /// List a page's versions, oldest first.
    #[tool(
        description = "List a page's versions, oldest first, with each version's \
                       body. History is never destroyed: superseded versions stay \
                       readable, which is what makes memory_history and \
                       memory_restore possible."
    )]
    async fn memory_history(
        &self,
        Parameters(args): Parameters<PageArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.dispatch(Command::History { path: args.path }).await
    }

    /// Restore an older version as a new one.
    #[tool(
        description = "Restore an older version of a page. The restore is itself \
                       a new version that supersedes the current one, so nothing \
                       is lost and a restore can be undone by restoring again."
    )]
    async fn memory_restore(
        &self,
        Parameters(args): Parameters<RestoreArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.dispatch(Command::Restore {
            path: args.path,
            version: args.version,
        })
        .await
    }

    /// Tombstone a page.
    #[tool(description = "Delete a page by tombstoning it. The deletion is \
                       authoritative immediately, and old copies in the index are \
                       filtered out of search results from then on.")]
    async fn memory_delete_page(
        &self,
        Parameters(args): Parameters<PageArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.dispatch(Command::DeletePage { path: args.path }).await
    }

    /// List the project's sessions.
    #[tool(description = "List the sessions this project has captured.")]
    async fn memory_sessions(&self) -> Result<CallToolResult, McpError> {
        self.dispatch(Command::Sessions).await
    }

    /// Publish this machine's split so the pages become searchable.
    #[tool(
        description = "Build one index split from the current pages and publish \
                       it. Readers see the new pages as soon as the catalog CAS \
                       lands; publishing the same content twice is a no-op."
    )]
    async fn memory_publish(&self) -> Result<CallToolResult, McpError> {
        self.dispatch(Command::Publish).await
    }

    /// Rebuild the index from authoritative pages.
    #[tool(
        description = "Rebuild the whole index from authoritative pages under a \
                       lease. Optional and abandonable: if another machine holds \
                       the lease this returns immediately."
    )]
    async fn memory_compact(&self) -> Result<CallToolResult, McpError> {
        self.dispatch(Command::Compact).await
    }

    /// Drain the spool, consolidate every session, and publish if needed.
    #[tool(
        description = "Run one maintenance pass: drain the current scope's hook \
                       spool, compile every session, and publish changed pages. \
                       This is the one-shot equivalent of running the CLI \
                       `qm maintain`; it is not a daemon and does not enter the \
                       hook fast path. A partial failure is returned as a tool \
                       error whose content is the complete JSON report."
    )]
    async fn memory_maintain(
        &self,
        Parameters(args): Parameters<MaintainArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.dispatch_maintain(Command::Maintain {
            compiler: args.compiler.unwrap_or_else(|| "auto".to_string()),
            drain_limit: args.drain_limit.unwrap_or(qm_cli::HOOK_DRAIN_DEFAULT_LIMIT),
        })
        .await
    }

    /// Leave a handoff for whoever comes next.
    #[tool(
        description = "Leave a handoff (a baton) for the next session or another \
                       machine. Use it to hand over work that is in flight, with \
                       enough context that the next session can continue without \
                       re-reading the whole history."
    )]
    async fn memory_handoff_open(
        &self,
        Parameters(args): Parameters<HandoffOpenArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.dispatch(Command::Handoff {
            action: HandoffAction::Open {
                title: args.title,
                body: Some(args.body),
            },
        })
        .await
    }

    /// List handoffs.
    #[tool(description = "List handoffs. Defaults to the open ones; pass state \
                       \"all\" to see claimed and finished handoffs too. Call this \
                       at the START of a session to pick up in-flight work.")]
    async fn memory_handoff_list(
        &self,
        Parameters(args): Parameters<HandoffListArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.dispatch(Command::Handoff {
            action: HandoffAction::List {
                state: args.state.unwrap_or_else(|| "open".to_string()),
            },
        })
        .await
    }

    /// Claim a handoff, exactly once.
    #[tool(
        description = "Claim a handoff. Exactly one machine can win: a handoff \
                       already claimed (or finished) returns an error, so two \
                       agents racing for the same baton cannot both proceed."
    )]
    async fn memory_handoff_claim(
        &self,
        Parameters(args): Parameters<HandoffIdArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.dispatch(Command::Handoff {
            action: HandoffAction::Claim { id: args.id },
        })
        .await
    }

    /// Finish a handoff you claimed.
    #[tool(
        description = "Finish a handoff. Only the machine that claimed it can \
                       finish it; anyone else gets an error."
    )]
    async fn memory_handoff_done(
        &self,
        Parameters(args): Parameters<HandoffIdArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.dispatch(Command::Handoff {
            action: HandoffAction::Done { id: args.id },
        })
        .await
    }

    /// Stage a proposed edit instead of writing it.
    #[tool(
        description = "Stage an edit to a page as a proposal instead of writing \
                       it. Use this for learning-driven rewrites: nothing changes \
                       until memory_approve applies it, and memory_reject leaves \
                       the page untouched. Re-proposing identical content is a \
                       no-op."
    )]
    async fn memory_propose(
        &self,
        Parameters(args): Parameters<ProposeArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.dispatch(Command::Propose {
            path: args.path,
            title: args.title,
            body: Some(args.body),
            rationale: args.rationale,
        })
        .await
    }

    /// List proposals (default: pending).
    #[tool(
        description = "List staged proposals. Defaults to the pending ones; pass \
                       state \"all\" to include approved and rejected."
    )]
    async fn memory_proposals(
        &self,
        Parameters(args): Parameters<ProposalsArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.dispatch(Command::Proposals {
            state: args.state.unwrap_or_else(|| "pending".to_string()),
        })
        .await
    }

    /// Approve a proposal and apply it.
    #[tool(
        description = "Approve a staged proposal and apply it to its target page. \
                       The decision is claimed before the page is written, so two \
                       approvers cannot both apply it; the applied version is \
                       recorded on the proposal afterwards."
    )]
    async fn memory_approve(
        &self,
        Parameters(args): Parameters<HandoffIdArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.dispatch(Command::Approve { id: args.id }).await
    }

    /// Reject a proposal.
    #[tool(
        description = "Reject a staged proposal. The target page is not touched, \
                       and the decision (with an optional note) is recorded."
    )]
    async fn memory_reject(
        &self,
        Parameters(args): Parameters<RejectArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.dispatch(Command::Reject {
            id: args.id,
            note: args.note,
        })
        .await
    }

    /// Check the bucket's integrity.
    #[tool(
        description = "Check that the bucket's authoritative state is internally \
                       consistent: the backend honours conditional writes, every \
                       page version the manifest names exists and hashes \
                       correctly, every session chain walks, and every split the \
                       catalog names is present. Read-only; reports problems."
    )]
    async fn memory_verify(
        &self,
        Parameters(args): Parameters<VerifyArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.dispatch(Command::Verify {
            global: args.global.unwrap_or(false),
            strict: false,
        })
        .await
    }

    /// Report what the project currently contains.
    #[tool(description = "Report the scope's current state: pages, tombstones, \
                       manifest sequence, splits, and sessions.")]
    async fn memory_status(&self) -> Result<CallToolResult, McpError> {
        self.dispatch(Command::Status).await
    }
}

#[tool_handler]
impl ServerHandler for MemoryServer {
    /// Declared manually so `#[tool_handler]` keeps the generated dispatch and
    /// this one choke point stays the single place a tool call is routed.
    async fn call_tool(
        &self,
        request: rmcp::model::CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let tcc = ToolCallContext::new(self, request, context);
        self.tool_router.call(tcc).await
    }

    fn get_info(&self) -> ServerInfo {
        let mut implementation = rmcp::model::Implementation::from_build_env();
        implementation.name = "quick-memory".into();
        implementation.version = env!("CARGO_PKG_VERSION").into();
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(implementation)
            .with_instructions(
                "Shared long-term memory for coding agents, stored in S3/R2. \
                 Capture notable events with memory_capture, compile them with \
                 memory_consolidate and memory_publish, run the one-shot \
                 drain/compile/publish loop with memory_maintain, and search with \
                 memory_search before answering questions about prior work."
                    .to_string(),
            )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjectPath;
    use qm_core::{KeyLayout, Lease, ProjectId, SessionId, WorkspaceId};
    use std::sync::atomic::{AtomicI64, Ordering};

    #[tokio::test]
    async fn a_long_lived_server_uses_call_time_for_writes_and_leases() {
        let start_ms = 1_000_000;
        let call_ms = start_ms + 180_000;
        let clock = Arc::new(AtomicI64::new(start_ms));
        let clock_for_server = Arc::clone(&clock);
        let bucket = Arc::new(InMemory::new());
        let server = MemoryServer::new(
            Arc::clone(&bucket) as Arc<dyn ObjectStore>,
            "long-ws".to_string(),
            "long-project".to_string(),
            "long-writer".to_string(),
            std::env::temp_dir().join(format!("qm-mcp-long-lived-{}", std::process::id())),
        )
        .with_clock(Arc::new(move || clock_for_server.load(Ordering::SeqCst)));

        let captured = server
            .memory_capture(Parameters(CaptureArgs {
                session: "sess-long-lived".to_string(),
                text: "capture at the old time".to_string(),
                kind: None,
                actor: None,
                at: None,
            }))
            .await
            .expect("capture");
        assert_eq!(captured.is_error, Some(false), "{captured:?}");

        clock.store(call_ms, Ordering::SeqCst);
        let maintained = server
            .memory_maintain(Parameters(MaintainArgs {
                compiler: Some("rules".to_string()),
                drain_limit: Some(10),
            }))
            .await
            .expect("maintain");
        assert_eq!(maintained.is_error, Some(false), "{maintained:?}");

        let recent = server
            .memory_recent(Parameters(RecentArgs { limit: Some(10) }))
            .await
            .expect("recent");
        let recent_text = recent
            .content
            .first()
            .and_then(|content| content.as_text())
            .map(|content| content.text.as_str())
            .expect("recent text");
        let recent_json: serde_json::Value =
            serde_json::from_str(recent_text).expect("recent JSON");
        let page = recent_json
            .as_array()
            .expect("recent array")
            .iter()
            .find(|page| page["path"] == "sessions/sess-long-lived.md")
            .expect("the page written by maintain");
        assert_eq!(page["created_at_ms"], call_ms, "{page}");

        let lease_key =
            KeyLayout::new("v1").lease("consolidate/long-ws/long-project/sess-long-lived");
        let lease_bytes = bucket
            .get(&ObjectPath::from(lease_key))
            .await
            .expect("lease object")
            .bytes()
            .await
            .expect("lease bytes");
        let lease: Lease = serde_json::from_slice(&lease_bytes).expect("lease JSON");
        assert_eq!(
            lease.acquired_at_ms, call_ms,
            "lease acquisition must use call time"
        );
    }

    #[tokio::test]
    async fn a_long_lived_server_resolves_the_digest_window_at_call_time() {
        let start_ms = 1_000_000;
        let page_ms = start_ms + 1_000_000;
        let call_ms = start_ms + 5_000_000;
        let clock = Arc::new(AtomicI64::new(start_ms));
        let clock_for_server = Arc::clone(&clock);
        let server = MemoryServer::new(
            Arc::new(InMemory::new()),
            "digest-ws".to_string(),
            "digest-project".to_string(),
            "digest-writer".to_string(),
            std::env::temp_dir().join(format!("qm-mcp-digest-lived-{}", std::process::id())),
        )
        .with_clock(Arc::new(move || clock_for_server.load(Ordering::SeqCst)));

        clock.store(page_ms, Ordering::SeqCst);
        let written = server
            .memory_write_page(Parameters(WritePageArgs {
                path: "notes/long-lived.md".to_string(),
                body: "written between the startup and call windows".to_string(),
                title: None,
            }))
            .await
            .expect("write page");
        assert_eq!(written.is_error, Some(false), "{written:?}");

        clock.store(call_ms, Ordering::SeqCst);
        let digest = server
            .memory_digest(Parameters(DigestArgs {
                since_hours: Some(1),
                limit: Some(20),
            }))
            .await
            .expect("digest");
        let digest_text = digest
            .content
            .first()
            .and_then(|content| content.as_text())
            .map(|content| content.text.as_str())
            .expect("digest text");
        let digest_json: serde_json::Value =
            serde_json::from_str(digest_text).expect("digest JSON");
        assert!(
            !digest_json["pages"].as_array().is_some_and(|pages| pages
                .iter()
                .any(|page| page["path"] == "notes/long-lived.md")),
            "the digest window must be resolved against call time: {digest_json}"
        );
    }

    #[tokio::test]
    async fn memory_maintain_returns_a_complete_report_as_a_tool_error() {
        let bucket = Arc::new(InMemory::new());
        let workspace = WorkspaceId::new("unit-ws").expect("workspace");
        let project = ProjectId::new("unit-project").expect("project");
        let session = SessionId::new("sess-broken").expect("session");
        let key = KeyLayout::new("v1").session_head(&workspace, &project, &session);
        bucket
            .put(&ObjectPath::from(key), b"not-json".to_vec().into())
            .await
            .expect("seed corrupt session head");

        let server = MemoryServer::new(
            bucket,
            workspace.to_string(),
            project.to_string(),
            "unit-writer".to_string(),
            std::env::temp_dir().join(format!("qm-mcp-unit-{}", std::process::id())),
        );
        let result = server
            .memory_maintain(Parameters(MaintainArgs {
                compiler: Some("rules".to_string()),
                drain_limit: Some(10),
            }))
            .await
            .expect("partial failure must be a tool-level result, not a protocol error");

        assert_eq!(result.is_error, Some(true), "{result:?}");
        let text = result
            .content
            .first()
            .and_then(|content| content.as_text())
            .map(|content| content.text.as_str())
            .expect("tool error carries report text");
        let report: serde_json::Value = serde_json::from_str(text)
            .unwrap_or_else(|error| panic!("report must be JSON: {error}: {text}"));
        assert_eq!(report["failed"], 1, "{report}");
        assert_eq!(report["failures"][0]["session"], "sess-broken", "{report}");
        assert!(
            report["failures"][0]["error"]
                .as_str()
                .is_some_and(|error| !error.is_empty()),
            "{report}"
        );
        assert!(
            report
                .as_object()
                .is_some_and(|report| report.contains_key("publish_error")),
            "{report}"
        );
    }
}
