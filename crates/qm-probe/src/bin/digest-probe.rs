//! Cross-process evidence for the "what changed recently" digest.
//!
//! `seed` writes one scope's whole history **through the real S3 client**:
//! two page commits, the deletion of one of them, two session heads, and three
//! batons whose stages land at different points around the window — one opened,
//! claimed and finished, one opened and left open, one whose every stage
//! predates the window. `read` is a *second process*: it
//! is handed nothing but the bucket's coordinates and has to rebuild the same
//! picture from the objects alone: no state is carried from one to the other,
//! only the objects the first one committed.
//!
//! `seed` prints the ground truth it created, so a caller can check the read
//! against it. `read` prints the digest **and** the live paths the manifest
//! still answers for — which is what turns "the deletion is gone from the
//! manifest but still in the digest" into a checkable claim instead of a
//! slogan. Neither process reaches an index, a cache, or the other's memory.

use anyhow::{Result, bail};
use clap::{Parser, Subcommand};
use qm_core::{
    MANIFEST_SCHEMA, Observation, PagePath, ProjectId, SessionId, WorkspaceId, WriterId,
    derive_observation_id,
};
use qm_probe::{S3Config, init_tracing};
use qm_store::{CommitPageRequest, IngestObservationsRequest, ProjectStore};

/// The fixed scope both processes agree on.
///
/// Every stub carries its own fresh bucket, so a constant scope stays
/// hermetic while still letting process B start from nothing but the bucket.
const WORKSPACE: &str = "probe-digest-ws";
const PROJECT: &str = "probe-digest-proj";

/// The scenario timeline.
///
/// The commits are deliberately written in the *opposite* order to their
/// clocks: the newest-clock page is committed first, so a reader that returns
/// log order instead of time order is caught rather than flattered.
const ALPHA_AT_MS: i64 = 3_000;
const BETA_AT_MS: i64 = 1_000;
const BETA_DELETED_AT_MS: i64 = 2_000;
/// One session head inside the window, one the window has to drop.
const SESSION_HEAD_AT_MS: i64 = 2_600;
const SESSION_OLD_AT_MS: i64 = 800;
/// A baton whose *finish* is the stage that matters: it is opened and claimed
/// before the window the reader asserts, and finished inside it. A digest that
/// looked only at the open or the claim would drop it — and one that sorted by
/// either would put it behind [`HANDOFF_LATER_AT_MS`] instead of ahead.
const HANDOFF_CREATED_AT_MS: i64 = 400;
const HANDOFF_CLAIMED_AT_MS: i64 = 900;
const HANDOFF_FINISHED_AT_MS: i64 = 3_200;
/// A baton opened inside the window and left open: still only as recent as its
/// creation.
const HANDOFF_LATER_AT_MS: i64 = 2_800;
/// A baton whose every stage predates the window: it must not appear in it,
/// which is the other half of "the window follows the latest stage".
const HANDOFF_EARLY_AT_MS: i64 = 300;

const ALPHA: &str = "notes/alpha.md";
const BETA: &str = "notes/beta.md";

#[derive(Debug, Parser)]
#[command(about = "Write, then read back, a digest across two processes")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Process A: commit the scope's whole history and print the ground truth.
    Seed,
    /// Process B: read the digest from the bucket and print it.
    Read {
        /// Only report activity at or after this caller clock (ms).
        #[arg(long, default_value_t = 0)]
        since_ms: i64,
        /// Cap **each** of the three sections independently.
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let args = Args::parse();
    // The client is built from the same S3 environment the shipped probes use,
    // so this exercises the real HTTP path rather than an in-memory backend.
    let config = S3Config::from_env()?;
    let bucket = config.build_store()?;
    let store = ProjectStore::new(bucket, "v1");

    match args.command {
        Command::Seed => {
            let report = seed(&store).await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::Read { since_ms, limit } => {
            let report = read(&store, since_ms, limit).await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
    }
    Ok(())
}

/// Write the whole scenario and report exactly what landed.
///
/// Every step is asserted here rather than assumed: a deletion that removed
/// nothing must not be reported as a deletion, and a session head that did not
/// move must not be reported as activity.
async fn seed(store: &ProjectStore) -> Result<serde_json::Value> {
    let workspace = WorkspaceId::new(WORKSPACE)?;
    let project = ProjectId::new(PROJECT)?;
    let writer = WriterId::new("probe-digest")?;

    let mut commits = Vec::new();
    for (path, at_ms) in [(ALPHA, ALPHA_AT_MS), (BETA, BETA_AT_MS)] {
        let outcome = store
            .commit_page(CommitPageRequest {
                workspace_id: workspace.clone(),
                project_id: project.clone(),
                path: PagePath::new(path)?,
                title: path.to_string(),
                body: format!("body of {path}"),
                writer_id: writer.clone(),
                now_ms: at_ms,
            })
            .await?;
        commits.push(serde_json::json!({
            "seq": outcome.manifest_seq,
            "at_ms": at_ms,
            "kind": "page_written",
            "path": path,
        }));
    }

    let deleted = store
        .delete_page(
            &workspace,
            &project,
            &PagePath::new(BETA)?,
            &writer,
            BETA_DELETED_AT_MS,
        )
        .await?;
    // A no-op delete would make the centrepiece of this scenario vacuous, so
    // refuse to carry on unless a live page was really removed.
    if deleted.already_deleted || deleted.removed.is_none() {
        bail!("the delete was a no-op; there was no live page at {BETA} to remove");
    }
    commits.push(serde_json::json!({
        "seq": deleted.manifest_seq,
        "at_ms": BETA_DELETED_AT_MS,
        "kind": "page_deleted",
        "path": BETA,
    }));

    // Two session heads, committed through the ordinary ingest path. The
    // newer one gets two batches, so `observations` is a chain total rather
    // than a single batch size.
    let mut sessions = Vec::new();
    for (id, batches) in [
        (
            "probe-sess-new",
            vec![
                (2_100_i64, vec!["wrote the digest probe"]),
                (
                    SESSION_HEAD_AT_MS,
                    vec!["checked ordering", "kept the deletion"],
                ),
            ],
        ),
        (
            "probe-sess-old",
            vec![(SESSION_OLD_AT_MS, vec!["an older session"])],
        ),
    ] {
        let session = SessionId::new(id)?;
        let mut count = 0u64;
        let mut last_seen_ms = i64::MIN;
        for (at_ms, texts) in batches {
            let batch = observations(&session, &texts, at_ms);
            let expected = batch.len();
            let outcome = store
                .ingest_observations(IngestObservationsRequest {
                    workspace_id: workspace.clone(),
                    project_id: project.clone(),
                    session_id: session.clone(),
                    writer_id: writer.clone(),
                    observations: batch,
                    now_ms: at_ms,
                })
                .await?;
            if outcome.accepted != expected {
                bail!(
                    "session {id} committed {} of {expected} observations",
                    outcome.accepted
                );
            }
            count = outcome.count;
            last_seen_ms = at_ms;
        }
        sessions.push(serde_json::json!({
            "session_id": id,
            "last_seen_ms": last_seen_ms,
            "observations": count,
        }));
    }

    // Three batons, so the window has something to include on the finish
    // stage, something to include on the creation stage, and something to
    // exclude entirely.
    let claimer = WriterId::new("probe-digest-claimer")?;
    let (started, created) = store
        .open_handoff(
            &workspace,
            &project,
            "carry the digest work",
            "the S3 stub leg still needs a reader",
            &writer,
            HANDOFF_CREATED_AT_MS,
        )
        .await?;
    if !created {
        bail!("the handoff was a replay; the scenario needs a fresh baton");
    }
    let claimed = store
        .claim_handoff(
            &workspace,
            &project,
            &started.id,
            &claimer,
            HANDOFF_CLAIMED_AT_MS,
        )
        .await?;
    // The claim is not the last stage. Finishing it is what carries the baton
    // into the reader's window and makes it the newest one.
    let finished = store
        .finish_handoff(
            &workspace,
            &project,
            &claimed.id,
            &claimer,
            HANDOFF_FINISHED_AT_MS,
        )
        .await?;
    if finished.finished_at_ms != Some(HANDOFF_FINISHED_AT_MS) {
        bail!(
            "finish_handoff recorded {:?}, not {HANDOFF_FINISHED_AT_MS}",
            finished.finished_at_ms
        );
    }
    // The finish has to outrank the claim and the open on its own, or the
    // window assertion downstream would pass for the wrong reason.
    if qm_store::handoff_activity_ms(&finished) != HANDOFF_FINISHED_AT_MS {
        bail!("the finished baton's activity is not its finish time");
    }
    let (later, _) = store
        .open_handoff(
            &workspace,
            &project,
            "a later baton",
            "opened inside the window",
            &writer,
            HANDOFF_LATER_AT_MS,
        )
        .await?;
    let (early, _) = store
        .open_handoff(
            &workspace,
            &project,
            "an early baton",
            "every stage is before the window",
            &writer,
            HANDOFF_EARLY_AT_MS,
        )
        .await?;

    Ok(serde_json::json!({
        "workspace": WORKSPACE,
        "project": PROJECT,
        "commits": commits,
        "sessions": sessions,
        "handoffs": [
            {
                "id": finished.id,
                "created_at_ms": finished.created_at_ms,
                "claimed_at_ms": finished.claimed_at_ms,
                "finished_at_ms": finished.finished_at_ms,
            },
            {
                "id": later.id,
                "created_at_ms": later.created_at_ms,
                "claimed_at_ms": later.claimed_at_ms,
                "finished_at_ms": later.finished_at_ms,
            },
            {
                "id": early.id,
                "created_at_ms": early.created_at_ms,
                "claimed_at_ms": early.claimed_at_ms,
                "finished_at_ms": early.finished_at_ms,
            },
        ],
    }))
}

/// Read the digest as a machine that has never seen the writer's memory.
async fn read(store: &ProjectStore, since_ms: i64, limit: usize) -> Result<serde_json::Value> {
    let workspace = WorkspaceId::new(WORKSPACE)?;
    let project = ProjectId::new(PROJECT)?;
    let digest = store.digest(&workspace, &project, since_ms, limit).await?;
    // The manifest's own answer, read through the ordinary recent-pages path:
    // the deleted path must not be among the live ones, so the digest keeping
    // it is a claim about history rather than a stale manifest entry.
    let live: Vec<String> = store
        .recent_pages(&workspace, &project, usize::MAX)
        .await?
        .into_iter()
        .map(|(path, _)| path.as_str().to_string())
        .collect();

    Ok(serde_json::json!({
        "since_ms": since_ms,
        "limit": limit,
        "live_pages": live,
        "digest": digest,
    }))
}

/// Observations for one ingest batch, each re-id'd from its own content.
fn observations(session: &SessionId, texts: &[&str], at_ms: i64) -> Vec<Observation> {
    texts
        .iter()
        .enumerate()
        .map(|(index, text)| Observation {
            schema: MANIFEST_SCHEMA,
            observation_id: derive_observation_id(
                session,
                "probe",
                "tool_use",
                text,
                at_ms + index as i64,
            ),
            session_id: session.clone(),
            actor: "probe".to_string(),
            kind: "tool_use".to_string(),
            text: (*text).to_string(),
            created_at_ms: at_ms + index as i64,
        })
        .collect()
}
