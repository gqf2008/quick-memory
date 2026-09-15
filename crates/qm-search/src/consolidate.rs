//! Compiling a session's observations into a page.
//!
//! This is the "compile, not retrieve" half of the design: capture appends raw
//! events, and consolidation turns the ones that matter into a durable page
//! that later searches can find. Compilation is a plain function of the
//! observation chain, so the same chain always renders the same page — which is
//! what lets it be re-run by any machine.
//!
//! Consolidation is optional work and is guarded by a lease, for the same
//! reason compaction is: two machines may try it, and one of them should simply
//! walk away.

use anyhow::{Context, Result};
use qm_core::{Observation, PagePath, ProjectId, SessionId, WorkspaceId, WriterId};
use qm_store::{CommitPageRequest, ProjectStore};

/// What a consolidation did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsolidateOutcome {
    /// True when another machine held the lease or there was nothing to compile.
    pub skipped: bool,
    /// True when the rendered page already matched the committed one.
    pub already_up_to_date: bool,
    /// Committed page version, when this run produced one.
    pub page_id: Option<String>,
    /// Observations compiled from the chain.
    pub observations: usize,
    /// Segments the chain was made of.
    pub segments: usize,
    /// Manifest sequence after the commit (unchanged when nothing was written).
    pub manifest_seq: u64,
}

/// Render a session's observations as markdown.
///
/// Deliberately deterministic and boring: no clock, no model, no ordering
/// freedom. An LLM pass can rewrite the body later through the normal commit
/// path; this renderer is the floor the system always has.
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

/// Compile a session's observations into `sessions/<session_id>.md`.
///
/// # Errors
/// Propagates chain reads and the page commit. A lease held elsewhere is not an
/// error; it is reported as `skipped`.
pub async fn consolidate_session(
    project_store: &ProjectStore,
    workspace_id: &WorkspaceId,
    project_id: &ProjectId,
    session_id: &SessionId,
    consolidator: &WriterId,
    now_ms: i64,
    lease_ttl_ms: i64,
) -> Result<ConsolidateOutcome> {
    let scope = format!("consolidate/{workspace_id}/{project_id}/{session_id}");
    let Some(lease) = project_store
        .acquire_lease(&scope, consolidator, now_ms, lease_ttl_ms)
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?
    else {
        return Ok(ConsolidateOutcome {
            skipped: true,
            already_up_to_date: false,
            page_id: None,
            observations: 0,
            segments: 0,
            manifest_seq: 0,
        });
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
        return Ok(ConsolidateOutcome {
            skipped: true,
            already_up_to_date: false,
            page_id: None,
            observations: 0,
            segments: chain.len(),
            manifest_seq,
        });
    }

    let path = PagePath::new(format!("sessions/{session_id}.md"))?;
    let body = render_session_page(session_id, &observations);

    // Re-running over an unchanged chain must not append a version.
    if let Some(current) = project_store
        .read_page(workspace_id, project_id, &path)
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?
        && current.body == body
    {
        lease
            .release(project_store, now_ms)
            .await
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        return Ok(ConsolidateOutcome {
            skipped: false,
            already_up_to_date: true,
            page_id: Some(current.page_id.as_str().to_string()),
            observations: observations.len(),
            segments: chain.len(),
            manifest_seq,
        });
    }

    let outcome = project_store
        .commit_page(CommitPageRequest {
            workspace_id: workspace_id.clone(),
            project_id: project_id.clone(),
            path,
            title: format!("Session {session_id}"),
            body,
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
        already_up_to_date: false,
        page_id: Some(outcome.page_id.as_str().to_string()),
        observations: observations.len(),
        segments: chain.len(),
        manifest_seq: outcome.manifest_seq,
    })
}

/// Convenience wrapper used by tests and probes.
///
/// # Errors
/// Propagates consolidation failures.
pub async fn consolidate_and_context(
    project_store: &ProjectStore,
    workspace_id: &WorkspaceId,
    project_id: &ProjectId,
    session_id: &SessionId,
    consolidator: &WriterId,
) -> Result<ConsolidateOutcome> {
    consolidate_session(
        project_store,
        workspace_id,
        project_id,
        session_id,
        consolidator,
        0,
        60_000,
    )
    .await
    .context("consolidating session")
}
