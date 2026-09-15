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
use qm_cli::{Cli, Command, Context as CommandContext, execute};
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
        })
        .await
    }

    /// Search the project's memory.
    #[tool(
        description = "Search the project's memory. Call this BEFORE answering \
                       questions about prior work, decisions, or conventions. \
                       Returns compiled pages with a fused rank score; every hit \
                       has been checked against the authoritative manifest, so \
                       superseded and deleted content never appears."
    )]
    async fn memory_search(
        &self,
        Parameters(args): Parameters<SearchArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.dispatch(Command::Search {
            query: args.query,
            limit: args.limit.unwrap_or(10),
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
    #[tool(description = "Read the current version of a page.")]
    async fn memory_read_page(
        &self,
        Parameters(args): Parameters<PageArgs>,
    ) -> Result<CallToolResult, McpError> {
        self.dispatch(Command::ReadPage { path: args.path }).await
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
