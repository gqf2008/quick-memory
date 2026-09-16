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
    CommitKind, HandoffState, MANIFEST_FORMAT_SHARDED, MANIFEST_FORMAT_WHOLE, MANIFEST_SCHEMA,
    Observation, PagePath, ProjectId, SessionId, WorkspaceId, WriterId,
};
use qm_search::consolidate::{CompilerChoice, consolidate_session_with};
use qm_search::{
    PageDoc, attach_embeddings, compact_project, publish_split_index, search_project_tuned,
    search_workspace_tuned,
};
use qm_store::{
    CommitPageRequest, Digest, IngestObservationsRequest, MigrateOutcome, ProjectStore,
};

pub mod config;

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

/// Default number of spooled hook events one drain attempt processes.
///
/// `hook-drain` and `maintain` share this value so the one-shot maintenance
/// path cannot quietly drain a different queue than the explicit repair
/// command.
pub const HOOK_DRAIN_DEFAULT_LIMIT: usize = 100;

/// Command line interface.
#[derive(Debug, Parser)]
#[command(name = "qm", version, about = "Shared agent memory on object storage")]
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
    /// Drain hook spool, consolidate every session, and publish if needed.
    Maintain {
        /// Compiler: `auto` (LLM when QM_LLM_BASE_URL is set, else rules),
        /// `rules`, or `llm` (which falls back to rules on failure).
        #[arg(long, default_value = "auto")]
        compiler: String,
        /// Maximum current-scope spooled events to attempt before consolidating.
        #[arg(long, default_value_t = HOOK_DRAIN_DEFAULT_LIMIT)]
        drain_limit: usize,
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
        #[arg(long, default_value_t = HOOK_DRAIN_DEFAULT_LIMIT)]
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
    /// Convert this project's manifest between the whole and sharded forms.
    MigrateManifest {
        /// Storage form to convert to: 1 (one object) or 2 (root + shards).
        #[arg(long, default_value_t = MANIFEST_FORMAT_SHARDED)]
        to: u32,
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

/// Names the storage form new commits are written in.
///
/// One name, read in exactly two places that share this constant: the CLI's
/// context and the MCP server's. A second spelling would let the surface that
/// is supposed to be the same protocol write a different form.
pub const MANIFEST_FORMAT_ENV: &str = "QM_MANIFEST_FORMAT";

const S3_ENDPOINT_ENV_NAMES: &[&str] = &["QM_S3_ENDPOINT", "R2_ENDPOINT"];
const S3_BUCKET_ENV_NAMES: &[&str] = &["QM_S3_BUCKET", "R2_BUCKET"];
const S3_ACCESS_KEY_ENV_NAMES: &[&str] = &["QM_S3_ACCESS_KEY_ID", "R2_ACCESS_KEY_ID"];
const S3_SECRET_KEY_ENV_NAMES: &[&str] = &["QM_S3_SECRET_ACCESS_KEY", "R2_SECRET_ACCESS_KEY"];

/// Read one of several aliases out of the process environment or the config
/// file.
///
/// The lookup is per **alias group**, not per name, because the precedence
/// rule is "environment before file" and a per-name lookup cannot express it:
/// asking for `QM_S3_BUCKET` (answerable from the file) before `R2_BUCKET`
/// (answerable from the environment) would let the file win. [`config::first_of`]
/// walks the whole environment first and only then the file.
///
/// `QM_SPOOL_DIR`, `QM_SESSION` and the scope settings have no aliases; they go
/// through [`config::var`] at their own call sites.
/// [`build_bucket_from`] and [`bucket_identity_from_env`] both take this as
/// their lookup, so the client and the watermark identity cannot end up
/// consulting different name tables.
fn env_lookup() -> impl FnMut(&[&str]) -> Option<String> {
    config::first_of
}

/// Parse the write-form setting out of an injected alias -> value lookup.
///
/// Unset means [`MANIFEST_FORMAT_WHOLE`], which is what every version before
/// format 2 wrote. A value that is not a form this build knows is an error and
/// not a fallback: quietly writing the whole form because someone typed `3`
/// would make the setting look like it took effect.
///
/// # Errors
/// Fails on anything but `1` or `2`.
pub fn manifest_format_from(mut get: impl FnMut(&[&str]) -> Option<String>) -> Result<u32> {
    let Some(raw) = setting_from(&[MANIFEST_FORMAT_ENV], &mut get) else {
        return Ok(MANIFEST_FORMAT_WHOLE);
    };
    match raw.trim() {
        "1" => Ok(MANIFEST_FORMAT_WHOLE),
        "2" => Ok(MANIFEST_FORMAT_SHARDED),
        other => bail!(
            "{MANIFEST_FORMAT_ENV}={other:?} is not a manifest form this build knows: \
             set 1 (whole manifest) or 2 (root pointer plus content-addressed shards)"
        ),
    }
}

/// Parse the write-form setting out of the environment.
///
/// # Errors
/// Fails on anything but `1` or `2`.
pub fn manifest_format_from_env() -> Result<u32> {
    manifest_format_from(env_lookup())
}

fn setting_from(names: &[&str], get: &mut impl FnMut(&[&str]) -> Option<String>) -> Option<String> {
    get(names).filter(|value| !value.trim().is_empty())
}

fn bucket_identity_from(mut get: impl FnMut(&[&str]) -> Option<String>) -> String {
    let endpoint = setting_from(S3_ENDPOINT_ENV_NAMES, &mut get).unwrap_or_default();
    let bucket = setting_from(S3_BUCKET_ENV_NAMES, &mut get).unwrap_or_default();
    if endpoint.is_empty() && bucket.is_empty() {
        return String::new();
    }
    format!("{endpoint}\u{0}{bucket}")
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
    build_bucket_from(env_lookup())
}

/// Build the bucket client from an injected alias -> value lookup.
///
/// Production reaches this only through [`build_bucket_from_env`]; the seam
/// exists so a test can record *which* alias groups the client is built from
/// and compare them with [`bucket_identity_from`]'s, without touching the
/// process environment (undefined behaviour under Rust 2024, and forbidden
/// textually by this workspace's `unsafe_code` lint).
///
/// # Errors
/// Fails when endpoint, bucket, or credentials are missing, or the client
/// cannot be constructed.
fn build_bucket_from(
    mut get: impl FnMut(&[&str]) -> Option<String>,
) -> Result<Arc<dyn ObjectStore>> {
    use object_store::aws::AmazonS3Builder;

    fn require(
        get: &mut impl FnMut(&[&str]) -> Option<String>,
        names: &[&str],
        what: &str,
    ) -> Result<String> {
        setting_from(names, get).ok_or_else(|| {
            anyhow::anyhow!(
                "missing {what}; set one of {names:?} (a command that cannot reach the bucket must fail, not guess)"
            )
        })
    }

    let endpoint = require(&mut get, S3_ENDPOINT_ENV_NAMES, "S3 endpoint")?;
    let bucket = require(&mut get, S3_BUCKET_ENV_NAMES, "bucket name")?;
    let access_key_id = require(&mut get, S3_ACCESS_KEY_ENV_NAMES, "access key id")?;
    let secret_access_key = require(&mut get, S3_SECRET_KEY_ENV_NAMES, "secret access key")?;
    let region = setting_from(&["QM_S3_REGION"], &mut get).unwrap_or_else(|| "auto".to_string());
    let force_path_style = setting_from(&["QM_S3_FORCE_PATH_STYLE"], &mut get)
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

/// Identity of the bucket named by the environment.
///
/// Derived from the same endpoint and bucket name [`build_bucket_from_env`]
/// builds its client from, so a cache key can never describe a different
/// bucket than the one being written to. Returns an empty string when nothing
/// names a bucket, which callers read as "unknown"; calling this after
/// `build_bucket_from_env` succeeded means that cannot happen.
#[must_use]
pub fn bucket_identity_from_env() -> String {
    bucket_identity_from(env_lookup())
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

/// Render what a storage-form change did.
///
/// Reports the form it started in and the form it is in now rather than only
/// the one that was asked for: a migration that found the scope already in the
/// target form changed nothing, and an operator reading the output has to be
/// able to tell that apart from a migration that moved objects.
fn render_migrate(outcome: &MigrateOutcome, json: bool) -> String {
    if json {
        return serde_json::json!({
            "from": outcome.from,
            "to": outcome.to,
            "shards": outcome.shards,
            "already_there": outcome.already_there,
            "archive": outcome.archive,
            "manifest_seq": outcome.manifest_seq,
            "attempts": outcome.attempts,
        })
        .to_string();
    }
    let mut report = if outcome.already_there {
        format!(
            "manifest form: already {} (no change), {} shard(s)",
            outcome.to, outcome.shards
        )
    } else {
        format!(
            "manifest form: {} -> {}, {} shard(s)",
            outcome.from, outcome.to, outcome.shards
        )
    };
    report.push_str(&format!("\n  manifest seq: {}", outcome.manifest_seq));
    match &outcome.archive {
        Some(key) => report.push_str(&format!(
            "\n  archived whole manifest: {key} (kept: `--to 1` goes back)"
        )),
        None if outcome.to == MANIFEST_FORMAT_SHARDED => {
            report.push_str("\n  no archive: this project had no whole manifest to keep")
        }
        None => {}
    }
    report
}

/// Render a digest as three labelled sections: pages, sessions, handoffs.
///
/// The sections are always all present when there is anything at all to show,
/// because "no handoffs" and "the handoff section never rendered" are different
/// facts and a reader should not have to guess which one they are looking at.
///
/// `limit == 0` returns the truncation notice before the empty-window check:
/// every section is capped to nothing, so the report is empty *by request*, and
/// answering "no recent activity" would report an empty window instead.
fn render_digest(digest: &Digest, limit: usize) -> String {
    if limit == 0 {
        return "no entries shown: --limit 0 caps every digest section".to_string();
    }
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
    /// Which bucket this context points at, for cache keys that must not be
    /// shared across buckets.
    ///
    /// Every cache key that records a fact *about a bucket* has to name that
    /// bucket, or the same cache pointed at a second bucket will answer for
    /// the first one. An empty value means "unknown": cache keys then fall
    /// back to naming the scope alone, which is only correct for a machine
    /// that never talks to more than one bucket.
    pub bucket_identity: String,
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
        let spool_dir = config::var("QM_SPOOL_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir().join("qm-spool"));
        Ok(Self {
            project: ProjectStore::new(Arc::clone(&bucket), "v1")
                .with_manifest_format(manifest_format_from_env()?),
            bucket,
            bucket_identity: String::new(),
            spool_dir,
            workspace: WorkspaceId::new(workspace)?,
            project_id: ProjectId::new(project)?,
            writer: WriterId::new(writer)?,
            cache_dir,
            now_ms,
            json,
        })
    }

    /// Declare which bucket this context points at.
    ///
    /// Callers that know the endpoint and bucket name should set this; the
    /// default is "unknown", which keeps cache keys scoped to the project and
    /// machine only. Any string is accepted — it is hashed into the filename —
    /// but it must distinguish two buckets that share a cache directory.
    #[must_use]
    pub fn with_bucket_identity(mut self, identity: impl Into<String>) -> Self {
        self.bucket_identity = identity.into();
        self
    }

    /// Select the storage form this context's writes use.
    ///
    /// [`Context::new`] resolves it from `QM_MANIFEST_FORMAT` already; this is
    /// for a surface that has to override it after construction — the MCP
    /// server keeps the setting on the server so every tool call shares it.
    #[must_use]
    pub fn with_manifest_format(mut self, format: u32) -> Self {
        self.project = self.project.with_manifest_format(format);
        self
    }

    fn page(&self, path: &str) -> Result<PagePath> {
        Ok(PagePath::new(path)?)
    }
}

/// Resolve the compiler spelling shared by `consolidate` and `maintain`.
fn compiler_choice_from_name(name: &str) -> Result<CompilerChoice> {
    match name {
        "auto" => {
            if config::var("QM_LLM_BASE_URL")
                .map(|value| !value.trim().is_empty())
                .unwrap_or(false)
            {
                Ok(CompilerChoice::Llm)
            } else {
                Ok(CompilerChoice::Rules)
            }
        }
        "rules" => Ok(CompilerChoice::Rules),
        "llm" => Ok(CompilerChoice::Llm),
        other => bail!("unknown compiler {other:?}: use auto, rules or llm"),
    }
}

/// What one publish attempt did.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PublishReport {
    /// True only when this attempt committed a new split.
    committed: bool,
    reason: Option<String>,
    already_present: bool,
    pages: usize,
    manifest_seq: u64,
    splits: usize,
    generation: u64,
    embedded: bool,
    full_publish: bool,
}

impl PublishReport {
    fn published(&self) -> bool {
        self.committed
    }

    fn render(&self, json: bool) -> String {
        if let Some(reason) = &self.reason {
            return reason.clone();
        }
        let because = if self.full_publish {
            "; the bucket held no splits, so this was a full publish"
        } else {
            ""
        };
        if json {
            serde_json::json!({
                "generation": self.generation,
                "already_present": self.already_present,
                "pages": self.pages,
                "manifest_seq": self.manifest_seq,
                "embedded": self.embedded,
                "full_publish": self.full_publish,
            })
            .to_string()
        } else if self.already_present {
            format!("split already published ({} pages){because}", self.pages)
        } else {
            format!(
                "published {} page(s) as one split (generation {}){because}",
                self.pages, self.generation
            )
        }
    }
}

/// Publish the pages this machine has not indexed yet.
async fn publish_project(ctx: &Context) -> Result<PublishReport> {
    let loaded = ctx
        .project
        .load(&ctx.workspace, &ctx.project_id)
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    let watermark_path = publish_watermark_path(ctx);
    let embedder = qm_search::vector::embedder_from_env()?;
    let embedded = embedder.is_some();
    let watermark = read_watermark(&watermark_path);
    // Enabling a provider has to re-embed pages that were published without
    // one, and their sequence numbers have not moved. Without this the vector
    // stream would stay empty forever on a machine that had already caught up.
    let watermark = if embedded && !watermark.embedded {
        0
    } else {
        watermark.manifest_seq
    };
    // A watermark is only meaningful for a bucket that actually holds what
    // this machine published. Point the same cache at a bucket nobody has
    // built here and the sequence numbers claim "done" for an index that does
    // not exist; an empty catalog therefore wins over the watermark, and the
    // publish is full.
    let catalog = ctx
        .project
        .load_catalog(&ctx.workspace, &ctx.project_id)
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    let bucket_has_no_splits = catalog.catalog.splits.is_empty();
    let since = if bucket_has_no_splits { 0 } else { watermark };
    let mut docs = Vec::new();
    for (path, entry) in &loaded.manifest.pages {
        // Only what changed since this machine last published. Without the
        // watermark (a fresh cache) everything is republished, which is
        // wasteful but never wrong.
        if entry.seq <= since {
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
        return Ok(PublishReport {
            committed: false,
            reason: Some(reason),
            already_present: false,
            pages: 0,
            manifest_seq: loaded.manifest.seq,
            splits: catalog.catalog.splits.len(),
            generation: catalog.catalog.generation,
            embedded,
            full_publish: bucket_has_no_splits,
        });
    }
    let docs = attach_embeddings(docs, embedder.as_deref()).await?;
    let seq = loaded.manifest.seq + 1;
    // One directory per invocation: building an index into a directory that
    // already holds one fails, and seq+timestamp is not unique enough (two
    // projects can publish in the same millisecond).
    let build_dir = unique_build_dir(ctx, "build");
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
    // Keep the old `qm publish` report contract: the manifest sequence is the
    // one this invocation loaded before publishing. Re-reading after the
    // watermark write would add a failure point after the publish already
    // committed. The split count is a best-effort snapshot derived from the
    // pre-publish catalog plus this publish outcome; a concurrent publisher
    // can make it conservative, but it cannot make the publish look failed.
    let splits = catalog.catalog.splits.len() + usize::from(!outcome.already_present);
    Ok(PublishReport {
        committed: !outcome.already_present,
        reason: None,
        already_present: outcome.already_present,
        pages: docs.len(),
        manifest_seq: loaded.manifest.seq,
        splits,
        generation: outcome.generation,
        embedded,
        full_publish: bucket_has_no_splits,
    })
}

/// One session that could not be consolidated during a maintenance pass.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MaintainSessionFailure {
    /// Session whose consolidation failed.
    pub session: String,
    /// Error reported by the consolidation path.
    pub error: String,
}

/// Result of one `qm maintain` pass.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MaintainReport {
    /// Spool entries successfully ingested.
    pub drained: usize,
    /// All spool entries not successfully processed: other scopes, entries
    /// beyond the current-scope limit, invalid files, and failed replays.
    pub spool_kept: usize,
    /// Sessions listed after the drain.
    pub sessions: usize,
    /// Sessions whose consolidation wrote a new page version.
    pub consolidated: usize,
    /// Sessions whose page already matched the current chain.
    pub already_up_to_date: usize,
    /// Sessions skipped because another machine held the consolidation lease.
    pub skipped_locked: usize,
    /// Sessions with no observations to compile.
    pub skipped_empty: usize,
    /// Sessions whose consolidation failed.
    pub failed: usize,
    /// Details for each failed session.
    pub failures: Vec<MaintainSessionFailure>,
    /// Whether this pass created a new split and the publish step did not fail.
    pub published: bool,
    /// Number of pages in the split considered by the publish step.
    pub published_pages: usize,
    /// Whether the split content was already present in the catalog.
    pub publish_already_present: bool,
    /// A failure from the publish step, if any.
    pub publish_error: Option<String>,
    /// Manifest sequence after the pass, when readable.
    pub manifest_seq: u64,
    /// Catalog generation after the pass, when readable.
    pub generation: u64,
    /// Best-effort split count derived from the pre-publish catalog and the
    /// publish outcome; a concurrent publisher can make this conservative.
    pub splits: usize,
}

impl MaintainReport {
    /// Whether this pass must make the CLI exit non-zero.
    #[must_use]
    pub fn needs_nonzero_exit(&self) -> bool {
        self.failed > 0 || self.publish_error.is_some()
    }

    fn render(&self) -> String {
        let mut out = format!(
            "drained {} spooled event(s), kept {}\n\
             sessions {}: consolidated {}, already up to date {}, skipped locked {}, skipped empty {}, failed {}\n\
             published: {} ({} page(s), generation {}, already present {})\n\
             manifest: seq {}, {} split(s)",
            self.drained,
            self.spool_kept,
            self.sessions,
            self.consolidated,
            self.already_up_to_date,
            self.skipped_locked,
            self.skipped_empty,
            self.failed,
            if self.published { "yes" } else { "no" },
            self.published_pages,
            self.generation,
            self.publish_already_present,
            self.manifest_seq,
            self.splits
        );
        for failure in &self.failures {
            out.push_str(&format!("\nsession {}: {}", failure.session, failure.error));
        }
        if let Some(error) = &self.publish_error {
            out.push_str(&format!("\npublish failed: {error}"));
        }
        out
    }
}

/// A maintain pass that completed some work but must exit non-zero.
///
/// Keeping the rendered report inside the error lets `main` print the complete
/// summary to stdout (including valid JSON in `--json` mode) before returning a
/// failure status.
#[derive(Debug)]
pub struct MaintainFailure {
    report: String,
}

impl MaintainFailure {
    /// The already-rendered report to print before exiting non-zero.
    #[must_use]
    pub fn report(&self) -> &str {
        &self.report
    }
}

impl std::fmt::Display for MaintainFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.report)
    }
}

impl std::error::Error for MaintainFailure {}

/// Drain, consolidate every session, and publish the resulting pages.
///
/// A failure in one session is recorded and the pass continues with the rest,
/// because later sessions are independent and one corrupt chain must not make
/// every other session unmaintainable. The caller turns a report with failures
/// into [`MaintainFailure`] after rendering it, so automation still gets a
/// non-zero status and the complete summary.
async fn maintain(
    ctx: &Context,
    compiler: CompilerChoice,
    drain_limit: usize,
) -> Result<MaintainReport> {
    let (drained, spool_kept) = drain_spool(ctx, drain_limit).await?;
    let session_ids = ctx
        .project
        .list_sessions(&ctx.workspace, &ctx.project_id)
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?;

    let mut consolidated = 0usize;
    let mut already_up_to_date = 0usize;
    let mut skipped_locked = 0usize;
    let mut skipped_empty = 0usize;
    let mut failures = Vec::new();
    for session_id in &session_ids {
        let result = consolidate_session_with(
            compiler,
            &ctx.project,
            qm_search::consolidate::ConsolidationRequest {
                workspace_id: &ctx.workspace,
                project_id: &ctx.project_id,
                session_id,
                consolidator: &ctx.writer,
                now_ms: ctx.now_ms,
                lease_ttl_ms: 60_000,
            },
        )
        .await;
        match result {
            Ok(outcome) if outcome.lease_held => skipped_locked += 1,
            Ok(outcome) if outcome.nothing_to_compile => skipped_empty += 1,
            Ok(outcome) if outcome.already_up_to_date => already_up_to_date += 1,
            Ok(_) => consolidated += 1,
            Err(error) => failures.push(MaintainSessionFailure {
                session: session_id.to_string(),
                error: error.to_string(),
            }),
        }
    }

    // Publish unconditionally: the explicit publish path is a no-op when this
    // machine has no new pages, and calling it here also repairs a previous
    // run that committed a page but crashed before its publish step.
    let (publish, publish_error) = match publish_project(ctx).await {
        Ok(report) => (report, None),
        Err(error) => {
            let state = current_publish_state(ctx).await;
            (
                PublishReport {
                    committed: false,
                    reason: None,
                    already_present: false,
                    pages: 0,
                    manifest_seq: state.0,
                    splits: state.2,
                    generation: state.1,
                    embedded: false,
                    full_publish: false,
                },
                Some(error.to_string()),
            )
        }
    };

    Ok(MaintainReport {
        drained,
        spool_kept,
        sessions: session_ids.len(),
        consolidated,
        already_up_to_date,
        skipped_locked,
        skipped_empty,
        failed: failures.len(),
        failures,
        published: publish_error.is_none() && publish.published(),
        published_pages: publish.pages,
        publish_already_present: publish.already_present,
        publish_error,
        manifest_seq: publish.manifest_seq,
        generation: publish.generation,
        splits: publish.splits,
    })
}

/// Best-effort state for the report when publishing itself failed.
async fn current_publish_state(ctx: &Context) -> (u64, u64, usize) {
    let manifest_seq = ctx
        .project
        .load(&ctx.workspace, &ctx.project_id)
        .await
        .map(|loaded| loaded.manifest.seq)
        .unwrap_or(0);
    ctx.project
        .load_catalog(&ctx.workspace, &ctx.project_id)
        .await
        .map(|loaded| {
            (
                manifest_seq,
                loaded.catalog.generation,
                loaded.catalog.splits.len(),
            )
        })
        .unwrap_or((manifest_seq, 0, 0))
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
            let choice = compiler_choice_from_name(compiler)?;
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
                    "lease_held": outcome.lease_held,
                    "nothing_to_compile": outcome.nothing_to_compile,
                    "already_up_to_date": outcome.already_up_to_date,
                    "page_id": outcome.page_id,
                    "observations": outcome.observations,
                    "segments": outcome.segments,
                    "compiler": outcome.compiler,
                    "used_fallback": outcome.used_fallback,
                })
                .to_string()
            } else if outcome.lease_held {
                "another machine holds the consolidation lease".to_string()
            } else if outcome.nothing_to_compile {
                "nothing to compile (the session has no observations)".to_string()
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
        Command::Maintain {
            compiler,
            drain_limit,
        } => {
            let choice = compiler_choice_from_name(compiler)?;
            let report = maintain(&ctx, choice, *drain_limit).await?;
            let rendered = if ctx.json {
                serde_json::to_string(&report)?
            } else {
                report.render()
            };
            if report.needs_nonzero_exit() {
                return Err(MaintainFailure { report: rendered }.into());
            }
            Ok(rendered)
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
            let report = publish_project(&ctx).await?;
            Ok(report.render(ctx.json))
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
                    "manifest_format": loaded.storage_format(),
                    "manifest_shards": loaded.root.as_ref().map_or(0, |root| root.shards.len()),
                    "pages": loaded.manifest.pages.len(),
                    "tombstones": loaded.manifest.tombstones.len(),
                    "catalog_generation": catalog.catalog.generation,
                    "index_schema": catalog.catalog.schema,
                    "splits": catalog.catalog.splits.len(),
                    "sessions": sessions.len(),
                })
                .to_string()
            } else {
                format!(
                    "{} / {}\n  pages: {}\n  tombstones: {}\n  manifest: seq {} format {} ({} shard(s); `qm migrate-manifest` changes it)\n  index: schema {} (`qm compact` rebuilds it when the analyzer changes)\n  splits: {} (generation {})\n  sessions: {}",
                    ctx.workspace,
                    ctx.project_id,
                    loaded.manifest.pages.len(),
                    loaded.manifest.tombstones.len(),
                    loaded.manifest.seq,
                    loaded.storage_format(),
                    loaded.root.as_ref().map_or(0, |root| root.shards.len()),
                    catalog.catalog.schema,
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
        Command::MigrateManifest { to } => {
            let outcome = match *to {
                MANIFEST_FORMAT_SHARDED => {
                    ctx.project
                        .migrate_manifest_to_sharded(&ctx.workspace, &ctx.project_id)
                        .await
                }
                MANIFEST_FORMAT_WHOLE => {
                    ctx.project
                        .rollback_manifest_to_whole(&ctx.workspace, &ctx.project_id)
                        .await
                }
                other => bail!(
                    "--to {other} is not a manifest form this build knows: \
                     set 1 (whole manifest) or 2 (root pointer plus content-addressed shards)"
                ),
            }
            .map_err(|error| anyhow::anyhow!("{error}"))?;
            Ok(render_migrate(&outcome, ctx.json))
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
                    "committed {} at seq {} (page {}); not searchable until `qm publish`",
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
                render_digest(&digest, *limit)
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

/// Reduce a bucket identity to a filename-safe tag.
///
/// Endpoints are URLs and bucket names carry dots, so the identity is hashed
/// instead of embedded: it has to survive as a path segment on every
/// filesystem. FNV-1a is enough for a cache key — this is not a security
/// boundary — and it costs no dependency for one 8-byte digest.
fn bucket_identity_tag(identity: &str) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in identity.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

fn publish_watermark_path(ctx: &Context) -> PathBuf {
    let scope = format!(
        "publish-{}-{}-{}",
        ctx.workspace, ctx.project_id, ctx.writer
    );
    if ctx.bucket_identity.is_empty() {
        // Unknown bucket: keep the pre-existing scope-only name so a cache
        // written before bucket identities existed is still found.
        return ctx.cache_dir.join(format!("{scope}.json"));
    }
    ctx.cache_dir.join(format!(
        "{scope}-{}.json",
        bucket_identity_tag(&ctx.bucket_identity)
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
    let default_session = session
        .map(ToString::to_string)
        .or_else(|| config::var("QM_SESSION").filter(|value| !value.trim().is_empty()));
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
///
/// The limit is applied after filtering to the caller's scope. Entries for
/// other scopes and current-scope entries beyond the limit are all returned in
/// the `kept` count.
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

    let mut kept = 0usize;
    let mut current_scope = Vec::new();
    for path in files {
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
        current_scope.push((path, entry));
    }

    // The limit is a budget for this scope, not a scan window over the whole
    // shared spool directory. Filter first so entries for another scope cannot
    // hide current-scope work behind the limit.
    let attempted = current_scope.len().min(limit);
    kept += current_scope.len() - attempted;
    let mut drained = 0usize;
    for (path, entry) in current_scope.into_iter().take(attempted) {
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

    /// A context pinned to one bucket identity.
    ///
    /// `context` keeps the "unknown bucket" default, which is the behaviour a
    /// single-bucket machine has always had; tests that model two buckets have
    /// to name them.
    fn context_on(
        bucket: Arc<dyn ObjectStore>,
        cache: &TempDir,
        writer: &str,
        bucket_identity: &str,
    ) -> Context {
        Context::new(
            bucket,
            "acme",
            "ai-memory",
            writer,
            cache.path().to_path_buf(),
            1_000,
            false,
        )
        .unwrap()
        .with_bucket_identity(bucket_identity)
    }

    fn cli(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("qm").chain(args.iter().copied())).unwrap()
    }

    enum TestMaintainResult {
        Success(MaintainReport),
        Failure(MaintainReport),
    }

    /// Run `qm maintain --json` and decode its report from either the normal
    /// success path or the typed failure path `main` prints before exiting.
    async fn run_maintain(ctx: Context) -> TestMaintainResult {
        let command = cli(&["maintain", "--compiler", "rules", "--json"]);
        match execute(&command, ctx).await {
            Ok(output) => TestMaintainResult::Success(serde_json::from_str(&output).unwrap()),
            Err(error) => {
                let failure = error
                    .downcast_ref::<MaintainFailure>()
                    .unwrap_or_else(|| panic!("maintain failed outside its report: {error}"));
                TestMaintainResult::Failure(serde_json::from_str(failure.report()).unwrap())
            }
        }
    }

    fn write_spool_entry(
        path: &std::path::Path,
        workspace: &str,
        project: &str,
        session: &str,
        text: &str,
        at_ms: i64,
    ) {
        let session_id = SessionId::new(session).unwrap();
        let observation = Observation {
            schema: MANIFEST_SCHEMA,
            observation_id: qm_core::derive_observation_id(
                &session_id,
                "test",
                "message",
                text,
                at_ms,
            ),
            session_id,
            actor: "test".to_string(),
            kind: "message".to_string(),
            text: text.to_string(),
            created_at_ms: at_ms,
        };
        let entry = SpoolEntry {
            workspace: workspace.to_string(),
            project: project.to_string(),
            observation,
        };
        std::fs::write(path, serde_json::to_vec(&entry).unwrap()).unwrap();
    }

    /// A store with deterministic publish-stage faults.
    ///
    /// It can either refuse object reads after the catalog head commits (to
    /// prove a successful publish does not add a status-read failure point) or
    /// refuse the catalog-head commit itself (to exercise maintain's publish
    /// failure report).
    #[derive(Debug)]
    struct PublishFaultStore {
        inner: Arc<dyn ObjectStore>,
        fail_reads: Arc<std::sync::atomic::AtomicBool>,
        post_publish_reads: Arc<std::sync::atomic::AtomicUsize>,
        fail_catalog_head_put: bool,
    }

    impl PublishFaultStore {
        fn new(inner: Arc<dyn ObjectStore>) -> Self {
            Self {
                inner,
                fail_reads: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                post_publish_reads: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                fail_catalog_head_put: false,
            }
        }

        fn failing_catalog_head_put(inner: Arc<dyn ObjectStore>) -> Self {
            Self {
                fail_catalog_head_put: true,
                ..Self::new(inner)
            }
        }

        fn post_publish_reads(&self) -> usize {
            self.post_publish_reads
                .load(std::sync::atomic::Ordering::SeqCst)
        }

        fn saw_catalog_commit(&self) -> bool {
            self.fail_reads.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn refused_read() -> object_store::Error {
            object_store::Error::Generic {
                store: "fail-reads-after-publish-test",
                source: Box::new(std::io::Error::other("post-publish read refused")),
            }
        }

        fn refused_catalog_put() -> object_store::Error {
            object_store::Error::Generic {
                store: "fail-catalog-put-test",
                source: Box::new(std::io::Error::other("catalog head put refused")),
            }
        }
    }

    impl std::fmt::Display for PublishFaultStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("PublishFaultStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for PublishFaultStore {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            options: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            let is_catalog_head = location.as_ref().ends_with("/index/head.json");
            if is_catalog_head && self.fail_catalog_head_put {
                return Err(Self::refused_catalog_put());
            }
            let result = self.inner.put_opts(location, payload, options).await;
            if is_catalog_head && result.is_ok() {
                self.fail_reads
                    .store(true, std::sync::atomic::Ordering::SeqCst);
            }
            result
        }

        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            if self.fail_reads.load(std::sync::atomic::Ordering::SeqCst) {
                self.post_publish_reads
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                return Err(Self::refused_read());
            }
            self.inner.get_opts(location, options).await
        }

        async fn delete(&self, location: &object_store::path::Path) -> object_store::Result<()> {
            self.inner.delete(location).await
        }

        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
        ) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
        ) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    /// The form a scope is stored in, read from the layout rather than guessed
    /// from what the command printed.
    async fn stored_form(ctx: &Context) -> u32 {
        ctx.project
            .load(&ctx.workspace, &ctx.project_id)
            .await
            .unwrap()
            .storage_format()
    }

    /// How many shards the stored commit point names.
    async fn stored_shards(ctx: &Context) -> usize {
        ctx.project
            .load(&ctx.workspace, &ctx.project_id)
            .await
            .unwrap()
            .root
            .map_or(0, |root| root.shards.len())
    }

    /// Run `qm verify --strict`, which reports a problem rather than panicking,
    /// and return its output.
    async fn verify_strict(ctx: Context) -> Result<String> {
        execute(&cli(&["verify", "--strict"]), ctx).await
    }

    /// The switch reads one name, and an unrecognised value is an error rather
    /// than a silent fallback to the default.
    #[test]
    fn the_write_form_is_read_from_one_name_and_refuses_what_it_does_not_know() {
        // Unset, and set-but-blank, both mean the form every earlier version
        // wrote.
        assert_eq!(
            manifest_format_from(|_| None).unwrap(),
            MANIFEST_FORMAT_WHOLE
        );
        assert_eq!(
            manifest_format_from(|names| {
                assert_eq!(names, &[MANIFEST_FORMAT_ENV]);
                Some("   ".to_string())
            })
            .unwrap(),
            MANIFEST_FORMAT_WHOLE
        );
        for (value, expected) in [
            ("1", MANIFEST_FORMAT_WHOLE),
            ("2", MANIFEST_FORMAT_SHARDED),
            (" 2 ", MANIFEST_FORMAT_SHARDED),
        ] {
            assert_eq!(
                manifest_format_from(|_| Some(value.to_string())).unwrap(),
                expected,
                "{value:?} must select form {expected}"
            );
        }
        let error = manifest_format_from(|_| Some("3".to_string())).unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains("QM_MANIFEST_FORMAT") && message.contains("\"3\""),
            "the refusal has to name the setting and the value: {message}"
        );
    }

    /// The switch reaches the store from the CLI surface, and the default keeps
    /// writing the objects it always wrote.
    #[tokio::test]
    async fn the_manifest_form_switch_reaches_the_store_and_the_default_does_not() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = TempDir::new().unwrap();
        execute(
            &cli(&["write-page", "--path", "notes/plain.md", "--body", "v1"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        let plain = context(Arc::clone(&bucket), &cache);
        assert_eq!(
            stored_form(&plain).await,
            MANIFEST_FORMAT_WHOLE,
            "the default must still write the single commit point"
        );
        assert_eq!(stored_shards(&plain).await, 0);
        assert!(verify_strict(plain).await.is_ok());

        let sharded_bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let sharded = context(Arc::clone(&sharded_bucket), &cache)
            .with_manifest_format(MANIFEST_FORMAT_SHARDED);
        execute(
            &cli(&["write-page", "--path", "notes/split.md", "--body", "v1"]),
            sharded,
        )
        .await
        .unwrap();
        let sharded = context(Arc::clone(&sharded_bucket), &cache)
            .with_manifest_format(MANIFEST_FORMAT_SHARDED);
        assert_eq!(
            stored_form(&sharded).await,
            MANIFEST_FORMAT_SHARDED,
            "the switch has to reach the store's writes"
        );
        assert_eq!(stored_shards(&sharded).await, 1);
        assert!(verify_strict(sharded).await.is_ok());

        // And `status` reports the form the scope is *stored* in, which is the
        // thing an operator needs before deciding to migrate.
        let status = execute(
            &cli(&["status", "--json"]),
            context(Arc::clone(&sharded_bucket), &cache)
                .with_manifest_format(MANIFEST_FORMAT_SHARDED),
        )
        .await
        .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&status).unwrap();
        assert_eq!(parsed["manifest_format"], MANIFEST_FORMAT_SHARDED);
        assert_eq!(parsed["manifest_shards"], 1);
        let status = execute(
            &cli(&["status", "--json"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&status).unwrap();
        assert_eq!(parsed["manifest_format"], MANIFEST_FORMAT_WHOLE);
        assert_eq!(parsed["manifest_shards"], 0);
    }

    /// The operator's round trip: migrate, migrate again, roll back.
    #[tokio::test]
    async fn migrate_manifest_converts_and_rolls_back() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = TempDir::new().unwrap();
        for (path, body) in [
            ("notes/a.md", "first"),
            ("notes/b.md", "second"),
            ("notes/c.md", "third"),
        ] {
            execute(
                &cli(&["write-page", "--path", path, "--body", body]),
                context(Arc::clone(&bucket), &cache),
            )
            .await
            .unwrap();
        }

        let out = execute(
            &cli(&["migrate-manifest", "--json"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(parsed["from"], MANIFEST_FORMAT_WHOLE);
        assert_eq!(parsed["to"], MANIFEST_FORMAT_SHARDED);
        assert_eq!(parsed["already_there"], false);
        assert_eq!(parsed["manifest_seq"], 3);
        let archive = parsed["archive"]
            .as_str()
            .expect("the old body is archived");
        assert!(
            archive.contains("/manifest/archive/"),
            "the archive is the key the root will name: {archive}"
        );
        // `verify` reads the archive the root names and fails if it is missing
        // or hashes differently, so a green pass is evidence the pre-migration
        // body survived the switch.
        assert!(
            verify_strict(context(Arc::clone(&bucket), &cache))
                .await
                .is_ok()
        );
        assert_eq!(
            stored_form(&context(Arc::clone(&bucket), &cache)).await,
            MANIFEST_FORMAT_SHARDED
        );
        assert!(stored_shards(&context(Arc::clone(&bucket), &cache)).await >= 1);

        // The scope still reads, through the default form, with the same pages.
        let read = execute(
            &cli(&["read-page", "--path", "notes/b.md"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        assert!(read.contains("second"), "{read}");

        let out = execute(
            &cli(&["migrate-manifest", "--json"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(parsed["already_there"], true, "a second run is a no-op");

        let out = execute(
            &cli(&["migrate-manifest", "--to", "1", "--json"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(parsed["from"], MANIFEST_FORMAT_SHARDED);
        assert_eq!(parsed["to"], MANIFEST_FORMAT_WHOLE);
        assert_eq!(parsed["manifest_seq"], 3);
        let read = execute(
            &cli(&["read-page", "--path", "notes/c.md"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        assert!(read.contains("third"), "{read}");

        let rolled_back = context(Arc::clone(&bucket), &cache);
        assert_eq!(
            stored_form(&rolled_back).await,
            MANIFEST_FORMAT_WHOLE,
            "the scope is whole again after the rollback"
        );
        assert!(verify_strict(rolled_back).await.is_ok());

        let error = execute(
            &cli(&["migrate-manifest", "--to", "9"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("not a manifest form this build knows"),
            "{error}"
        );
    }

    /// The MCP surface resolves the same setting through the same parser.
    #[test]
    fn the_mcp_surface_reads_the_same_environment_name() {
        // The name is a constant shared by both binaries, so the only thing
        // this has to pin is that it is the documented one and that the MCP
        // crate's entry point uses this parser (it calls
        // `manifest_format_from_env`, see `qm-mcp/src/main.rs`).
        assert_eq!(MANIFEST_FORMAT_ENV, "QM_MANIFEST_FORMAT");
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

    /// The CLI surface of "a deleted page still has a history".
    ///
    /// Reported from a real bucket: `qm history --path <deleted>` printed `[]`
    /// while `read-page --as-of` still answered. The store walked its chain
    /// from the manifest head, which a deletion replaces with a tombstone.
    #[tokio::test]
    async fn history_of_a_deleted_page_lists_its_versions() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = TempDir::new().unwrap();

        for (index, body) in ["first", "second", "third"].iter().enumerate() {
            execute(
                &cli(&["write-page", "--path", "notes/doomed.md", "--body", body]),
                Context {
                    now_ms: (index as i64 + 1) * 1_000,
                    ..context(Arc::clone(&bucket), &cache)
                },
            )
            .await
            .unwrap();
        }
        execute(
            &cli(&["delete-page", "--path", "notes/doomed.md"]),
            Context {
                now_ms: 4_000,
                ..context(Arc::clone(&bucket), &cache)
            },
        )
        .await
        .unwrap();

        // Premise: the page is really gone, so an empty history could not be
        // explained by "there was nothing there".
        let missing = execute(
            &cli(&["read-page", "--path", "notes/doomed.md"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await;
        assert!(missing.is_err(), "the page must actually be deleted");

        let history = execute(
            &cli(&["history", "--path", "notes/doomed.md", "--json"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        let versions: Vec<qm_core::PageVersion> = serde_json::from_str(&history).unwrap();
        assert_eq!(
            versions.iter().map(|v| v.body.as_str()).collect::<Vec<_>>(),
            vec!["first", "second", "third"],
            "a deleted page still has a history"
        );
    }

    /// `qm status` has to say which analyzer generation the index is on: that is
    /// what tells an operator "run `qm compact`" *before* a search does.
    /// Publishing is what makes a page searchable, and the two families of reads
    /// disagree on purpose until then.
    ///
    /// The design used to claim a local tail ("read your writes"); it does not
    /// exist, so this pins the real behaviour instead: an unpublished page is
    /// visible to `recent` (authority) and invisible to `search` (index). The
    /// second half is the sharper case — a *new version* of a published page
    /// hides the path from search entirely until it is published, because the
    /// index still holds the old version and the authority rejects it.
    #[tokio::test]
    async fn an_unpublished_page_is_visible_to_recent_but_not_to_search() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = TempDir::new().unwrap();

        execute(
            &cli(&[
                "write-page",
                "--path",
                "notes/ryw.md",
                "--body",
                "first body",
            ]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();

        // Authority readers see it immediately…
        let recent = execute(
            &cli(&["recent", "--limit", "5", "--json"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        assert!(recent.contains("notes/ryw.md"), "{recent}");

        // …the index does not, because nothing has been published.
        let searched = execute(
            &cli(&["search", "first body", "--json"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        assert!(
            !searched.contains("notes/ryw.md"),
            "an unpublished page must not be searchable: {searched}"
        );

        // Publishing is the step that closes the gap.
        execute(
            &cli(&["publish", "--json"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        let searched = execute(
            &cli(&["search", "first body", "--json"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        assert!(
            searched.contains("notes/ryw.md"),
            "a published page has to be searchable: {searched}"
        );

        // A new version hides the path again until it is published: the index
        // still holds the first version, and the authority rejects it.
        execute(
            &cli(&[
                "write-page",
                "--path",
                "notes/ryw.md",
                "--body",
                "second body",
            ]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        for query in ["first body", "second body"] {
            let searched = execute(
                &cli(&["search", query, "--json"]),
                context(Arc::clone(&bucket), &cache),
            )
            .await
            .unwrap();
            assert!(
                !searched.contains("notes/ryw.md"),
                "neither version answers before the new one is published ({query}): {searched}"
            );
        }

        // And publishing the new version restores it.
        execute(
            &cli(&["publish", "--json"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        let searched = execute(
            &cli(&["search", "second body", "--json"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        assert!(searched.contains("notes/ryw.md"), "{searched}");
    }

    #[tokio::test]
    async fn status_reports_the_index_schema() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = TempDir::new().unwrap();
        execute(
            &cli(&["write-page", "--path", "notes/a.md", "--body", "a body"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        execute(
            &cli(&["publish", "--json"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();

        let json = execute(
            &cli(&["status", "--json"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        let status: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(
            status["index_schema"],
            serde_json::json!(qm_core::INDEX_SCHEMA),
            "status must name the index generation: {status}"
        );

        let text = execute(&cli(&["status"]), context(bucket.clone(), &cache))
            .await
            .unwrap();
        assert!(text.contains("index: schema"), "{text}");

        // …and it reports what the *catalog* says rather than a constant:
        // replace it with the one an older build would have left behind.
        let workspace = WorkspaceId::new("acme").unwrap();
        let project = ProjectId::new("ai-memory").unwrap();
        let layout = qm_core::KeyLayout::new("v1");
        let legacy = qm_core::IndexCatalog {
            schema: qm_core::INDEX_SCHEMA - 1,
            generation: 1,
            splits: Vec::new(),
            covered_until_ms: 0,
        };
        let bytes = serde_json::to_vec(&legacy).unwrap();
        let key = layout.catalog_version(&workspace, &project, &qm_core::content_hash(&bytes));
        bucket
            .put(&object_store::path::Path::from(key.clone()), bytes.into())
            .await
            .unwrap();
        let head = qm_core::CatalogHead {
            schema: qm_core::MANIFEST_SCHEMA,
            generation: 1,
            catalog_key: key,
            updated_at_ms: 1,
        };
        bucket
            .put(
                &object_store::path::Path::from(layout.catalog_head(&workspace, &project)),
                serde_json::to_vec(&head).unwrap().into(),
            )
            .await
            .unwrap();

        let json = execute(
            &cli(&["status", "--json"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        let status: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(
            status["index_schema"],
            serde_json::json!(qm_core::INDEX_SCHEMA - 1),
            "status has to report the catalog's own value: {status}"
        );
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
    async fn publish_success_does_not_read_back_after_committing_the_catalog_head() {
        let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let fault = Arc::new(PublishFaultStore::new(Arc::clone(&raw)));
        let bucket: Arc<dyn ObjectStore> = fault.clone();
        let cache = TempDir::new().unwrap();

        execute(
            &cli(&[
                "write-page",
                "--path",
                "notes/one.md",
                "--body",
                "first page",
            ]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        let seq_before = context(Arc::clone(&bucket), &cache)
            .project
            .load(
                &WorkspaceId::new("acme").unwrap(),
                &ProjectId::new("ai-memory").unwrap(),
            )
            .await
            .unwrap()
            .manifest
            .seq;

        let mut ctx = context(Arc::clone(&bucket), &cache);
        ctx.now_ms = 2_000;
        let report = publish_project(&ctx).await.unwrap();
        assert_eq!(report.manifest_seq, seq_before);
        assert_eq!(report.splits, 1);
        assert!(
            fault.saw_catalog_commit(),
            "the premise: the publish must have committed the catalog head"
        );
        assert_eq!(
            fault.post_publish_reads(),
            0,
            "a successful publish must not add a post-commit status read"
        );
    }

    #[tokio::test]
    async fn maintain_never_reports_published_when_the_publish_stage_fails() {
        let raw: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let fault = Arc::new(PublishFaultStore::failing_catalog_head_put(Arc::clone(
            &raw,
        )));
        let bucket: Arc<dyn ObjectStore> = fault.clone();
        let cache = TempDir::new().unwrap();
        execute(
            &cli(&[
                "capture",
                "--session",
                "sess-publish-fail",
                "--text",
                "cannot publish",
            ]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();

        let mut ctx = context(Arc::clone(&bucket), &cache);
        ctx.now_ms = 2_000;
        let report = match run_maintain(ctx).await {
            TestMaintainResult::Success(report) => {
                panic!("a failed publish must make maintain non-zero: {report:?}")
            }
            TestMaintainResult::Failure(report) => report,
        };
        assert!(report.publish_error.is_some(), "{report:?}");
        assert!(
            !report.published,
            "a failed publish cannot be reported as published: {report:?}"
        );
        assert_eq!(report.failed, 0, "no session itself failed: {report:?}");
        assert!(report.needs_nonzero_exit());
    }

    /// The watermark records what *this machine* published into *one bucket*.
    /// Sharing a cache directory must not let one bucket's position suppress a
    /// publish into another: the second bucket's index stays empty and search
    /// then answers from nothing, with no error anywhere.
    #[tokio::test]
    async fn publish_watermarks_are_scoped_to_the_bucket() {
        let bucket_a: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let bucket_b: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = TempDir::new().unwrap();

        // Bucket B is not empty: another machine already published there. That
        // is what makes the empty-catalog fallback insufficient on its own —
        // only keying the watermark by bucket fixes this case.
        execute(
            &cli(&[
                "write-page",
                "--path",
                "notes/from-b.md",
                "--body",
                "written on another machine",
            ]),
            Context {
                now_ms: 1_000,
                ..context_on(Arc::clone(&bucket_b), &cache, "mbp-b", "bucket-b")
            },
        )
        .await
        .unwrap();
        let on_b = execute(
            &cli(&["publish"]),
            Context {
                now_ms: 1_000,
                ..context_on(Arc::clone(&bucket_b), &cache, "mbp-b", "bucket-b")
            },
        )
        .await
        .unwrap();
        assert!(on_b.contains("published 1 page(s)"), "{on_b}");

        // The same machine, same scope, same cache directory, bucket A.
        execute(
            &cli(&[
                "write-page",
                "--path",
                "notes/only-a.md",
                "--body",
                "only in bucket a",
            ]),
            Context {
                now_ms: 2_000,
                ..context_on(Arc::clone(&bucket_a), &cache, "mbp-a", "bucket-a")
            },
        )
        .await
        .unwrap();
        let a_first = execute(
            &cli(&["publish"]),
            Context {
                now_ms: 2_000,
                ..context_on(Arc::clone(&bucket_a), &cache, "mbp-a", "bucket-a")
            },
        )
        .await
        .unwrap();
        assert!(a_first.contains("published 1 page(s)"), "{a_first}");

        // ... and now the same machine and cache directory against bucket B.
        let b_from_a = execute(
            &cli(&["publish"]),
            Context {
                now_ms: 3_000,
                ..context_on(Arc::clone(&bucket_b), &cache, "mbp-a", "bucket-b")
            },
        )
        .await
        .unwrap();
        assert!(
            b_from_a.contains("published 1 page(s)"),
            "switching buckets must publish, not report up to date: {b_from_a}"
        );
        // Bucket B *had* splits, so this is the bucket-scoped watermark doing
        // the work rather than the empty-catalog fallback.
        assert!(!b_from_a.contains("held no splits"), "{b_from_a}");

        // What was published is really searchable in bucket B.
        let found = execute(
            &cli(&["search", "written on another machine", "--json"]),
            Context {
                now_ms: 4_000,
                ..context_on(Arc::clone(&bucket_b), &cache, "mbp-a", "bucket-b")
            },
        )
        .await
        .unwrap();
        let json: serde_json::Value = serde_json::from_str(&found).unwrap();
        assert_eq!(json["splits_searched"], 2, "{found}");
        assert_eq!(json["hits"].as_array().unwrap().len(), 1, "{found}");

        // Back on bucket A with nothing new: the incremental behaviour is
        // intact, per bucket.
        let a_second = execute(
            &cli(&["publish"]),
            Context {
                now_ms: 5_000,
                ..context_on(Arc::clone(&bucket_a), &cache, "mbp-a", "bucket-a")
            },
        )
        .await
        .unwrap();
        assert!(a_second.contains("nothing to publish"), "{a_second}");
        assert!(a_second.contains("seq 1"), "{a_second}");
    }

    /// A watermark is a claim about a bucket, and the claim can be wrong: a
    /// bucket whose index was never built here holds no splits at all, so
    /// "up to date" would leave search answering from an empty index while the
    /// manifest holds pages.
    #[tokio::test]
    async fn a_watermark_without_any_splits_in_the_bucket_publishes_anyway() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = TempDir::new().unwrap();

        execute(
            &cli(&[
                "write-page",
                "--path",
                "notes/one.md",
                "--body",
                "the only page",
            ]),
            Context {
                now_ms: 1_000,
                ..context_on(Arc::clone(&bucket), &cache, "mbp-a", "bucket-a")
            },
        )
        .await
        .unwrap();

        // This machine believes it published through seq 1 while the bucket
        // holds no splits: the state a wiped index prefix, a fresh bucket or a
        // cache copied between machines leaves behind.
        let ctx = context_on(Arc::clone(&bucket), &cache, "mbp-a", "bucket-a");
        write_watermark(&publish_watermark_path(&ctx), 1, false).unwrap();
        let catalog = ctx
            .project
            .load_catalog(&ctx.workspace, &ctx.project_id)
            .await
            .unwrap();
        assert!(
            catalog.catalog.splits.is_empty(),
            "the premise: the bucket holds no splits"
        );

        let out = execute(
            &cli(&["publish"]),
            Context {
                now_ms: 2_000,
                ..context_on(Arc::clone(&bucket), &cache, "mbp-a", "bucket-a")
            },
        )
        .await
        .unwrap();
        assert!(out.contains("published 1 page(s)"), "{out}");
        assert!(
            out.contains("held no splits"),
            "a full publish has to say why: {out}"
        );

        // Once the bucket has an index, the fallback stops firing: this is
        // still an incremental publish, not a rebuild on every call.
        let again = execute(
            &cli(&["publish"]),
            Context {
                now_ms: 3_000,
                ..context_on(Arc::clone(&bucket), &cache, "mbp-a", "bucket-a")
            },
        )
        .await
        .unwrap();
        assert!(again.contains("nothing to publish"), "{again}");
    }

    /// Two buckets must not share a watermark key, and an endpoint must not
    /// reach the filesystem as a path separator or a Windows-hostile colon.
    #[test]
    fn the_watermark_key_names_the_bucket_and_stays_filename_safe() {
        let cache = TempDir::new().unwrap();
        let ctx = |identity: &str| {
            Context::new(
                Arc::new(InMemory::new()),
                "acme",
                "ai-memory",
                "mbp-a",
                cache.path().to_path_buf(),
                1_000,
                false,
            )
            .unwrap()
            .with_bucket_identity(identity)
        };

        let endpoint = "https://acct.r2.cloudflarestorage.com";
        let a = publish_watermark_path(&ctx(&format!("{endpoint}/bucket-a")));
        let b = publish_watermark_path(&ctx(&format!("{endpoint}/bucket-b")));
        assert_ne!(a, b, "two buckets must not share a watermark");
        for path in [&a, &b] {
            assert_eq!(
                path.parent().unwrap(),
                cache.path(),
                "the key stays one path segment: {}",
                path.display()
            );
            let name = path.file_name().unwrap().to_str().unwrap();
            assert!(!name.contains(':'), "{name}");
            assert!(name.starts_with("publish-acme-ai-memory-mbp-a-"), "{name}");
        }
        // An unknown identity keeps the pre-existing scope-only name, so a
        // cache written before buckets were named is still found.
        assert_eq!(
            publish_watermark_path(&ctx("")),
            cache.path().join("publish-acme-ai-memory-mbp-a.json")
        );
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

        // `--limit 0` produces the same empty arrays, but it is not a claim
        // that the window was quiet: the caller deliberately asked for no
        // entries. Keep the two states distinguishable in human output.
        let zero_limit = execute(
            &cli(&["digest", "--limit", "0"]),
            Context {
                now_ms: 3_000,
                ..context(Arc::clone(&bucket), &cache)
            },
        )
        .await
        .unwrap();
        assert_eq!(
            zero_limit,
            "no entries shown: --limit 0 caps every digest section"
        );
        assert_ne!(zero_limit, empty);
    }

    /// The client builder and the watermark identity must read the **same**
    /// endpoint/bucket names, or a watermark can name a bucket the client never
    /// writes to.
    ///
    /// Pinning the four tables alone is not enough, and neither is this test
    /// on its own: it drives the lookup seam, so it stays green if
    /// `build_bucket_from_env` stops delegating and grows its own copy of the
    /// name list. What it covers is that both paths resolve their names through
    /// the same constant; what covers the production entry point is
    /// `tests/cli_entry_conformance.rs`, which runs the real binary, together
    /// with the one-line delegation in `build_bucket_from_env`.
    ///
    /// The blind spot is symmetric: inlining the shared constant into a
    /// separately spelled-out table on each side leaves *both* this test and
    /// `tests/cli_entry_conformance.rs` green, because the two sides still
    /// resolve the same names. What drifts then is only "the two paths share
    /// one constant" — a watermark-adjacent property of the code, not of the
    /// bucket, and one nothing here can see.
    #[test]
    fn bucket_identity_and_client_share_the_same_environment_names() {
        assert_eq!(S3_ENDPOINT_ENV_NAMES, &["QM_S3_ENDPOINT", "R2_ENDPOINT"]);
        assert_eq!(S3_BUCKET_ENV_NAMES, &["QM_S3_BUCKET", "R2_BUCKET"]);
        assert_eq!(
            S3_ACCESS_KEY_ENV_NAMES,
            &["QM_S3_ACCESS_KEY_ID", "R2_ACCESS_KEY_ID"]
        );
        assert_eq!(
            S3_SECRET_KEY_ENV_NAMES,
            &["QM_S3_SECRET_ACCESS_KEY", "R2_SECRET_ACCESS_KEY"]
        );

        // Answer every alias group the client walks, so it gets past all four
        // `require`s and the test sees the whole table it consults. The builder
        // does not dial the endpoint, so whether it returns `Ok` or `Err` here
        // is irrelevant: only the questions it asked are load-bearing.
        let mut client_asked: Vec<Vec<String>> = Vec::new();
        let _ = build_bucket_from(|names| {
            client_asked.push(names.iter().map(|name| name.to_string()).collect());
            Some("https://dummy.invalid".to_string())
        });
        assert!(
            client_asked.len() > 2,
            "the client must walk past its endpoint and bucket lookups: {client_asked:?}"
        );

        // The identity reads exactly two groups: endpoint, then bucket.
        let mut identity_asked: Vec<Vec<String>> = Vec::new();
        let _ = bucket_identity_from(|names| {
            identity_asked.push(names.iter().map(|name| name.to_string()).collect());
            Some("https://dummy.invalid".to_string())
        });
        assert_eq!(
            identity_asked,
            vec![
                vec!["QM_S3_ENDPOINT".to_string(), "R2_ENDPOINT".to_string()],
                vec!["QM_S3_BUCKET".to_string(), "R2_BUCKET".to_string()],
            ],
            "the identity resolves whole alias groups, not one name at a time"
        );

        // And those are the first two groups the client asks for, in that
        // order. Point either side at a different table and the two diverge.
        assert_eq!(
            client_asked.iter().take(2).cloned().collect::<Vec<_>>(),
            identity_asked,
            "the client must be built from the same endpoint/bucket names the watermark identity is derived from"
        );

        // Exercise the aliases through the same resolver identity uses, so a
        // future change that only updates one table cannot silently make the
        // watermark name a different bucket. The fake resolves within a group
        // the way `config::first_of` does: the member that is set wins, and
        // `QM_S3_*` is listed first.
        let resolve = |values: &mut std::collections::HashMap<String, String>, names: &[&str]| {
            names.iter().find_map(|name| values.remove(*name))
        };
        let mut values: std::collections::HashMap<String, String> =
            std::collections::HashMap::from([
                ("R2_ENDPOINT".to_string(), "https://r2.example".to_string()),
                ("R2_BUCKET".to_string(), "bucket-b".to_string()),
            ]);
        assert_eq!(
            bucket_identity_from(|names| resolve(&mut values, names)),
            "https://r2.example\u{0}bucket-b"
        );

        let mut values: std::collections::HashMap<String, String> =
            std::collections::HashMap::from([
                ("R2_ENDPOINT".to_string(), "https://r2.example".to_string()),
                ("R2_BUCKET".to_string(), "bucket-b".to_string()),
                (
                    "QM_S3_ENDPOINT".to_string(),
                    "https://s3.example".to_string(),
                ),
                ("QM_S3_BUCKET".to_string(), "bucket-a".to_string()),
            ]);
        assert_eq!(
            bucket_identity_from(|names| resolve(&mut values, names)),
            "https://s3.example\u{0}bucket-a",
            "QM_S3_* must take precedence over R2_*"
        );
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

    #[tokio::test]
    async fn maintain_drains_spool_consolidates_publishes_and_is_idempotent() {
        let spool = TempDir::new().unwrap();
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
        let mut producer = context(unreachable, &TempDir::new().unwrap());
        producer.spool_dir = spool.path().to_path_buf();
        let captured = capture_hook_event(
            &producer,
            Some("sess-maintain"),
            None,
            Some("codex"),
            r#"{"session_id":"sess-maintain","hook_event_name":"PostToolUse","tool_response":"cargo t passed"}"#,
            1_000,
        )
        .await
        .unwrap();
        assert!(captured.starts_with("spooled"), "{captured}");
        assert_eq!(std::fs::read_dir(spool.path()).unwrap().count(), 1);

        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = TempDir::new().unwrap();
        let mut first_ctx = context(Arc::clone(&bucket), &cache);
        first_ctx.spool_dir = spool.path().to_path_buf();
        first_ctx.now_ms = 2_000;
        let first = match run_maintain(first_ctx).await {
            TestMaintainResult::Success(report) => report,
            TestMaintainResult::Failure(report) => {
                panic!("maintain must succeed on a healthy bucket: {report:?}")
            }
        };
        assert_eq!(first.drained, 1);
        assert_eq!(first.spool_kept, 0);
        assert_eq!(first.sessions, 1);
        assert_eq!(first.consolidated, 1);
        assert_eq!(first.already_up_to_date, 0);
        assert_eq!(first.skipped_locked, 0);
        assert_eq!(first.skipped_empty, 0);
        assert_eq!(first.failed, 0, "{first:?}");
        assert!(first.published, "{first:?}");
        assert_eq!(first.published_pages, 1);
        assert_eq!(first.splits, 1);
        assert_eq!(
            std::fs::read_dir(spool.path()).unwrap().count(),
            0,
            "a successful drain must clear the spool"
        );

        let search = execute(
            &cli(&["search", "cargo t passed", "--json"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        let search: serde_json::Value = serde_json::from_str(&search).unwrap();
        assert!(
            search["hits"]
                .as_array()
                .unwrap()
                .iter()
                .any(|hit| hit["path"] == "sessions/sess-maintain.md"),
            "maintain must publish the consolidated session: {search}"
        );

        let state = context(Arc::clone(&bucket), &cache);
        let first_seq = state
            .project
            .load(&state.workspace, &state.project_id)
            .await
            .unwrap()
            .manifest
            .seq;
        let first_catalog = state
            .project
            .load_catalog(&state.workspace, &state.project_id)
            .await
            .unwrap();
        let first_splits = first_catalog.catalog.splits.len();
        let split_prefix = format!(
            "{}/index/splits",
            state
                .project
                .layout()
                .scope_prefix(&state.workspace, &state.project_id)
        );
        let split_objects_before = qm_store::CasStore::new(Arc::clone(&bucket), "")
            .list(&split_prefix)
            .await
            .unwrap()
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>();
        assert!(
            !split_objects_before.is_empty(),
            "the premise: the first pass must have uploaded raw split objects"
        );
        let history = execute(
            &cli(&["history", "--path", "sessions/sess-maintain.md", "--json"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        let history: Vec<qm_core::PageVersion> = serde_json::from_str(&history).unwrap();
        assert_eq!(history.len(), 1);

        let mut second_ctx = context(Arc::clone(&bucket), &cache);
        second_ctx.spool_dir = spool.path().to_path_buf();
        second_ctx.now_ms = 3_000;
        let second = match run_maintain(second_ctx).await {
            TestMaintainResult::Success(report) => report,
            TestMaintainResult::Failure(report) => {
                panic!("an already-maintained scope must be a successful no-op: {report:?}")
            }
        };
        assert_eq!(second.drained, 0);
        assert_eq!(second.sessions, 1);
        assert_eq!(second.consolidated, 0);
        assert_eq!(second.already_up_to_date, 1);
        assert_eq!(second.skipped_locked, 0);
        assert_eq!(second.skipped_empty, 0);
        assert_eq!(second.failed, 0);
        assert!(!second.published, "{second:?}");
        assert_eq!(second.manifest_seq, first_seq);
        assert_eq!(second.splits, first_splits);
        let split_objects_after = qm_store::CasStore::new(Arc::clone(&bucket), "")
            .list(&split_prefix)
            .await
            .unwrap()
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>();
        assert_eq!(
            split_objects_after, split_objects_before,
            "the second maintain pass must not add raw split objects"
        );

        let history = execute(
            &cli(&["history", "--path", "sessions/sess-maintain.md", "--json"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        let history: Vec<qm_core::PageVersion> = serde_json::from_str(&history).unwrap();
        assert_eq!(
            history.len(),
            1,
            "the second pass must not duplicate a version"
        );

        let mut human_ctx = context(Arc::clone(&bucket), &cache);
        human_ctx.spool_dir = spool.path().to_path_buf();
        human_ctx.now_ms = 4_000;
        let human = execute(&cli(&["maintain", "--compiler", "rules"]), human_ctx)
            .await
            .unwrap();
        assert!(
            human.contains("already up to date 1") && human.contains("manifest: seq"),
            "the human summary must expose the same no-op state: {human}"
        );
    }

    #[tokio::test]
    async fn maintain_counts_a_held_consolidation_lease_as_skipped_locked() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = TempDir::new().unwrap();
        execute(
            &cli(&[
                "capture",
                "--session",
                "sess-locked",
                "--text",
                "locked chain",
            ]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();

        let lease_ctx = context(Arc::clone(&bucket), &cache);
        let _lease = lease_ctx
            .project
            .acquire_lease(
                "consolidate/acme/ai-memory/sess-locked",
                &lease_ctx.writer,
                1_000,
                60_000,
            )
            .await
            .unwrap()
            .expect("the test must hold the lease");

        let mut ctx = context(Arc::clone(&bucket), &cache);
        ctx.now_ms = 2_000;
        let report = match run_maintain(ctx).await {
            TestMaintainResult::Success(report) => report,
            TestMaintainResult::Failure(report) => {
                panic!("a lease skip is not a failure: {report:?}")
            }
        };
        assert_eq!(report.sessions, 1);
        assert_eq!(report.consolidated, 0);
        assert_eq!(report.skipped_locked, 1);
        assert_eq!(report.skipped_empty, 0);
        assert_eq!(report.failed, 0);
        assert!(!report.published);
        assert_eq!(
            report.splits, 0,
            "nothing was consolidated, so nothing was published"
        );
    }

    #[tokio::test]
    async fn maintain_counts_an_empty_session_separately_from_a_held_lease() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = TempDir::new().unwrap();
        let ctx = context(Arc::clone(&bucket), &cache);
        let session_id = SessionId::new("sess-empty").unwrap();
        ctx.project
            .ingest_observations(IngestObservationsRequest {
                workspace_id: ctx.workspace.clone(),
                project_id: ctx.project_id.clone(),
                session_id,
                writer_id: ctx.writer.clone(),
                observations: Vec::new(),
                now_ms: 1_000,
            })
            .await
            .unwrap();

        let mut maintenance_ctx = context(Arc::clone(&bucket), &cache);
        maintenance_ctx.now_ms = 2_000;
        let report = match run_maintain(maintenance_ctx).await {
            TestMaintainResult::Success(report) => report,
            TestMaintainResult::Failure(report) => {
                panic!("an empty session is not a failure: {report:?}")
            }
        };
        assert_eq!(report.sessions, 1);
        assert_eq!(report.skipped_locked, 0);
        assert_eq!(report.skipped_empty, 1);
        assert_eq!(report.consolidated, 0);
        assert_eq!(report.failed, 0);
        assert!(!report.published);
    }

    #[tokio::test]
    async fn drain_spool_filters_scope_before_applying_the_limit() {
        let spool = TempDir::new().unwrap();
        let mut ctx = context(Arc::new(InMemory::new()), &TempDir::new().unwrap());
        ctx.spool_dir = spool.path().to_path_buf();
        write_spool_entry(
            &spool.path().join("000-other.json"),
            "other-workspace",
            "other-project",
            "sess-other",
            "not ours",
            1,
        );
        write_spool_entry(
            &spool.path().join("001-current.json"),
            "acme",
            "ai-memory",
            "sess-current",
            "ours",
            2,
        );

        let (drained, kept) = drain_spool(&ctx, 1).await.unwrap();
        assert_eq!((drained, kept), (1, 1));
        assert!(
            !spool.path().join("001-current.json").exists(),
            "the current-scope entry must be attempted even though another scope sorts first"
        );
        assert!(
            spool.path().join("000-other.json").exists(),
            "another scope must remain for its own drain"
        );
    }

    #[tokio::test]
    async fn drain_spool_counts_current_scope_entries_left_above_the_limit() {
        let spool = TempDir::new().unwrap();
        let mut ctx = context(Arc::new(InMemory::new()), &TempDir::new().unwrap());
        ctx.spool_dir = spool.path().to_path_buf();
        for (index, name) in ["000.json", "001.json", "002.json"].into_iter().enumerate() {
            write_spool_entry(
                &spool.path().join(name),
                "acme",
                "ai-memory",
                &format!("sess-{index}"),
                name,
                index as i64,
            );
        }

        let (drained, kept) = drain_spool(&ctx, 1).await.unwrap();
        assert_eq!((drained, kept), (1, 2));
        let remaining = std::fs::read_dir(spool.path()).unwrap().count();
        assert_eq!(
            remaining, 2,
            "the two unprocessed current-scope entries must remain"
        );
    }

    #[tokio::test]
    async fn maintain_continues_after_a_session_failure_and_reports_nonzero() {
        let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cache = TempDir::new().unwrap();
        for (session, text) in [
            ("sess-broken", "broken chain must not stop the pass"),
            ("sess-good", "good chain must still become searchable"),
        ] {
            execute(
                &cli(&["capture", "--session", session, "--text", text]),
                context(Arc::clone(&bucket), &cache),
            )
            .await
            .unwrap();
        }

        let ctx = context(Arc::clone(&bucket), &cache);
        let broken = SessionId::new("sess-broken").unwrap();
        let key = ctx
            .project
            .layout()
            .session_head(&ctx.workspace, &ctx.project_id, &broken);
        ctx.bucket
            .put(
                &object_store::path::Path::from(key),
                b"not-json".to_vec().into(),
            )
            .await
            .unwrap();

        let mut maintenance_ctx = context(Arc::clone(&bucket), &cache);
        maintenance_ctx.now_ms = 2_000;
        let report = match run_maintain(maintenance_ctx).await {
            TestMaintainResult::Success(report) => {
                panic!("a corrupt session must make maintain exit non-zero: {report:?}")
            }
            TestMaintainResult::Failure(report) => report,
        };
        assert_eq!(report.sessions, 2);
        assert_eq!(report.consolidated, 1);
        assert_eq!(report.failed, 1, "{report:?}");
        assert_eq!(report.skipped_locked, 0);
        assert_eq!(report.skipped_empty, 0);
        assert_eq!(report.failures[0].session, "sess-broken");
        assert!(
            report.published,
            "the healthy session must still be published"
        );
        assert!(report.needs_nonzero_exit());

        let search = execute(
            &cli(&["search", "good chain", "--json"]),
            context(Arc::clone(&bucket), &cache),
        )
        .await
        .unwrap();
        let search: serde_json::Value = serde_json::from_str(&search).unwrap();
        assert_eq!(search["hits"].as_array().unwrap().len(), 1, "{search}");
    }
}
