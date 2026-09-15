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
use qm_cli::{Cli, Command, Context as CommandContext, HandoffAction, execute};
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
    workspace: String,
    project: String,
    writer: String,
    cache_dir: PathBuf,
    now_ms: i64,
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
            workspace,
            project,
            writer,
            cache_dir,
            now_ms: wall_clock_ms(),
            tool_router: Self::tool_router(),
        }
    }

    /// Run one command through the shared dispatch and return its JSON.
    async fn dispatch(&self, command: Command) -> Result<CallToolResult, McpError> {
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
            self.now_ms,
            true,
        )
        .map_err(|error| McpError::invalid_params(error.to_string(), None))?;
        let output = execute(&cli, context)
            .await
            .map_err(|error| McpError::internal_error(error.to_string(), None))?;
        Ok(CallToolResult::success(vec![Content::text(output)]))
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
                       hits then carry their workspace and project."
    )]
    async fn memory_search(
        &self,
        Parameters(args): Parameters<SearchArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.dispatch(Command::Search {
            query: args.query,
            limit: args.limit.unwrap_or(10),
            global: args.global.unwrap_or(false),
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
                 memory_consolidate and memory_publish, and search with \
                 memory_search before answering questions about prior work."
                    .to_string(),
            )
    }
}
