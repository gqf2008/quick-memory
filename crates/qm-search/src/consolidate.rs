//! Compiling a session's observations into a page.
//!
//! This is the "compile, not retrieve" half of the design: capture appends raw
//! events, and consolidation turns the ones that matter into a durable page
//! that later searches can find.
//!
//! Compilation is guarded in three ways. It runs under a lease, so two machines
//! do not compile the same session at once. It is a function of the chain's
//! *fingerprint*, not of the body, so a nondeterministic compiler (an LLM) does
//! not cause a new version every time it runs. And a *configured* compiler's
//! failure falls back to the rule renderer, because losing the page is worse
//! than losing polish.
//!
//! Asking for the LLM compiler with nothing configured is a different case and
//! is **not** a fallback: it is a configuration error. Falling back silently
//! would produce a report that names the rule compiler with
//! `used_fallback: false`, which reads as "rules were chosen" rather than "the
//! LLM you asked for was not there".

use anyhow::{Context, Result};
use qm_core::{Observation, PagePath, ProjectId, SessionId, WorkspaceId, WriterId};
use qm_store::{CommitPageRequest, ProjectStore};

use crate::compile::{
    OpenAiCompatCompiler, RuleCompiler, SessionCompiler, chain_fingerprint, fingerprint_of,
    with_fingerprint,
};

/// What a consolidation did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsolidateOutcome {
    /// True when another machine held the lease or there was nothing to compile.
    pub skipped: bool,
    /// True when the lease was held by another machine.
    pub lease_held: bool,
    /// True when the session had no observations to compile.
    pub nothing_to_compile: bool,
    /// True when the chain fingerprint already matched the committed page.
    pub already_up_to_date: bool,
    /// Committed page version, when this run produced one.
    pub page_id: Option<String>,
    /// Observations compiled from the chain.
    pub observations: usize,
    /// Segments the chain was made of.
    pub segments: usize,
    /// Manifest sequence after the commit (unchanged when nothing was written).
    pub manifest_seq: u64,
    /// Which compiler produced the body.
    pub compiler: &'static str,
    /// True when the configured compiler failed and the rules were used.
    pub used_fallback: bool,
}

/// Which compiler to use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompilerChoice {
    /// The deterministic renderer.
    Rules,
    /// An OpenAI-compatible LLM, falling back to rules on failure.
    Llm,
}

/// One consolidation target.
#[derive(Debug, Clone, Copy)]
pub struct ConsolidationRequest<'a> {
    /// Workspace scope.
    pub workspace_id: &'a WorkspaceId,
    /// Project scope.
    pub project_id: &'a ProjectId,
    /// Session to compile.
    pub session_id: &'a SessionId,
    /// Machine doing the work.
    pub consolidator: &'a WriterId,
    /// Timestamp recorded on the commit.
    pub now_ms: i64,
    /// How long the consolidation lease stays valid.
    pub lease_ttl_ms: i64,
}

/// Compile a session's observations into `sessions/<session_id>.md` with the
/// chosen compiler.
///
/// # Errors
/// Propagates chain reads and the page commit. A lease held elsewhere is not an
/// error; it is reported as `skipped`.
pub async fn consolidate_session_with(
    choice: CompilerChoice,
    project_store: &ProjectStore,
    request: ConsolidationRequest<'_>,
) -> Result<ConsolidateOutcome> {
    let ConsolidationRequest {
        workspace_id,
        project_id,
        session_id,
        consolidator,
        now_ms,
        lease_ttl_ms,
    } = request;
    // Resolve which compiler will actually run *before* touching any state.
    // Two reasons, both learned from review: asking for the LLM with nothing
    // configured is a configuration error rather than a fallback, and an error
    // raised after the lease was taken would leave that lease behind — one bad
    // invocation would then make the session `skipped` until the TTL passes.
    // Doing it here also means the empty-session and already-up-to-date early
    // returns cannot quietly succeed under a configuration that is wrong.
    let llm = match choice {
        CompilerChoice::Rules => None,
        CompilerChoice::Llm => Some(OpenAiCompatCompiler::from_env()?.ok_or_else(|| {
            anyhow::anyhow!(
                "the LLM compiler was requested but no provider is configured: set \
                 QM_LLM_BASE_URL, QM_LLM_API_KEY and QM_LLM_MODEL (or ask for rules/auto, \
                 which uses the rule renderer when no provider is configured)"
            )
        })?),
    };

    let scope = format!("consolidate/{workspace_id}/{project_id}/{session_id}");
    let Some(lease) = project_store
        .acquire_lease(&scope, consolidator, now_ms, lease_ttl_ms)
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?
    else {
        return Ok(skipped_outcome(0, 0, 0, true, false));
    };

    let chain = project_store
        .read_session_chain(workspace_id, project_id, session_id)
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    let observations = project_store
        .read_session_observations(workspace_id, project_id, session_id)
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    let manifest_seq = project_store
        .load(workspace_id, project_id)
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?
        .manifest
        .seq;

    if observations.is_empty() {
        lease
            .release(project_store, now_ms)
            .await
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        return Ok(skipped_outcome(0, chain.len(), manifest_seq, false, true));
    }

    let path = PagePath::new(format!("sessions/{session_id}.md"))?;
    let fingerprint = chain_fingerprint(&observations);

    // Idempotency keyed on the chain, not the prose: an LLM that words the page
    // differently each run must not create a version each run.
    if let Some(current) = project_store
        .read_page(workspace_id, project_id, &path)
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?
        && fingerprint_of(&current.body).as_deref() == Some(fingerprint.as_str())
    {
        lease
            .release(project_store, now_ms)
            .await
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        return Ok(ConsolidateOutcome {
            skipped: false,
            lease_held: false,
            nothing_to_compile: false,
            already_up_to_date: true,
            page_id: Some(current.page_id.as_str().to_string()),
            observations: observations.len(),
            segments: chain.len(),
            manifest_seq,
            compiler: "unchanged",
            used_fallback: false,
        });
    }

    let rules = RuleCompiler;
    let (body, compiler, used_fallback) = match llm {
        None => (
            rules
                .compile(session_id, &observations)
                .await
                .context("rule compiler failed")?,
            rules.name(),
            false,
        ),
        // A *configured* provider that fails still falls back: losing the page
        // is worse than losing polish.
        Some(llm) => match llm.compile(session_id, &observations).await {
            Ok(body) if !body.trim().is_empty() => (body, llm.name(), false),
            Ok(_) | Err(_) => (
                rules
                    .compile(session_id, &observations)
                    .await
                    .context("rule compiler failed")?,
                rules.name(),
                true,
            ),
        },
    };

    let outcome = project_store
        .commit_page(CommitPageRequest {
            workspace_id: workspace_id.clone(),
            project_id: project_id.clone(),
            path,
            title: format!("Session {session_id}"),
            body: with_fingerprint(session_id, &fingerprint, &body),
            writer_id: consolidator.clone(),
            now_ms,
        })
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    lease
        .release(project_store, now_ms)
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?;

    Ok(ConsolidateOutcome {
        skipped: false,
        lease_held: false,
        nothing_to_compile: false,
        already_up_to_date: false,
        page_id: Some(outcome.page_id.as_str().to_string()),
        observations: observations.len(),
        segments: chain.len(),
        manifest_seq: outcome.manifest_seq,
        compiler,
        used_fallback,
    })
}

/// Compile with the deterministic renderer.
///
/// # Errors
/// Propagates consolidation failures.
pub async fn consolidate_session(
    project_store: &ProjectStore,
    workspace_id: &WorkspaceId,
    project_id: &ProjectId,
    session_id: &SessionId,
    consolidator: &WriterId,
    now_ms: i64,
    lease_ttl_ms: i64,
) -> Result<ConsolidateOutcome> {
    consolidate_session_with(
        CompilerChoice::Rules,
        project_store,
        ConsolidationRequest {
            workspace_id,
            project_id,
            session_id,
            consolidator,
            now_ms,
            lease_ttl_ms,
        },
    )
    .await
}

fn skipped_outcome(
    observations: usize,
    segments: usize,
    manifest_seq: u64,
    lease_held: bool,
    nothing_to_compile: bool,
) -> ConsolidateOutcome {
    ConsolidateOutcome {
        skipped: true,
        lease_held,
        nothing_to_compile,
        already_up_to_date: false,
        page_id: None,
        observations,
        segments,
        manifest_seq,
        compiler: "none",
        used_fallback: false,
    }
}

/// Render a session's observations as markdown with the rule compiler.
///
/// Kept as a free function because it is the documented floor: the shape of a
/// compiled page must be reproducible without a model.
#[must_use]
pub fn render_session_page(session_id: &SessionId, observations: &[Observation]) -> String {
    let mut body = format!(
        "# Session {session_id}\n\nCompiled from {} observation{}.\n",
        observations.len(),
        if observations.len() == 1 { "" } else { "s" }
    );
    for observation in observations {
        body.push_str(&format!(
            "\n- [{}] {} ({}) {}",
            observation.created_at_ms, observation.kind, observation.actor, observation.text
        ));
    }
    body.push('\n');
    body
}
