//! The digest, read back by a second process over the real S3 protocol.
//!
//! Every other piece of "what changed recently" evidence in this repository
//! runs against `object_store`'s `InMemory` backend, which has no HTTP layer
//! at all. These tests run the `digest-probe` binary as **separate processes**
//! against [`qm_probe::s3_stub::S3Stub`]: process A builds a whole history —
//! two page commits, a deletion, two session heads, two handoffs — and process
//! B, with its own handle and nothing shared but the bucket, has to rebuild the
//! same three-section digest from the objects alone.
//!
//! Two things are load-bearing beyond "it works":
//!
//! - **The deletion has to survive the manifest.** Process B reports both the
//!   digest and the live paths the manifest still answers for, so "the deleted
//!   path is gone from the manifest yet still in the digest" is an assertion,
//!   not a story about the code.
//! - **The positive predicate has a fault control.** A stub that answers its
//!   first listing page as if it were the whole listing must break the very
//!   assertions the healthy leg pins; otherwise a reader that quietly saw a
//!   partial bucket would look identical to one that saw all of it.
//!
//! What this does not prove is the part that needs a real account: signatures,
//! latency, quotas, region behaviour and R2's own XML variants (see
//! `docs/design.md` §5.1 and §11). It runs no real bucket and no Quickwit.

use std::process::{Command, Output};

use qm_probe::s3_stub::{Fault, RecordedRequest, S3Stub, StubOptions};
use serde_json::Value;

const ACCESS_KEY: &str = "stub-access";
const SECRET_KEY: &str = "stub-secret";

/// Run one `digest-probe` command, hermetically: a fresh OS process with an
/// empty environment, so nothing it knows can come from this one.
fn probe(stub: &S3Stub, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_digest-probe"))
        .args(args)
        .env_clear()
        .env("QM_S3_ENDPOINT", stub.endpoint())
        .env("QM_S3_BUCKET", stub.bucket())
        .env("QM_S3_ACCESS_KEY_ID", ACCESS_KEY)
        .env("QM_S3_SECRET_ACCESS_KEY", SECRET_KEY)
        .env("QM_S3_FORCE_PATH_STYLE", "true")
        // The probe prints its answer on stdout; silence the logs rather than
        // pattern-match around them.
        .env("RUST_LOG", "off")
        .output()
        .expect("running the digest-probe binary")
}

/// Process A: build the scope and hand back the ground truth it created.
fn seed(stub: &S3Stub) -> Value {
    parse(&probe(stub, &["seed"]), "seed")
}

/// Process B: read the digest, sharing nothing with `seed` but the socket.
fn read(stub: &S3Stub, since_ms: i64, limit: usize) -> Value {
    parse(
        &probe(
            stub,
            &[
                "read",
                "--since-ms",
                &since_ms.to_string(),
                "--limit",
                &limit.to_string(),
            ],
        ),
        "read",
    )
}

fn parse(output: &Output, what: &str) -> Value {
    assert!(
        output.status.success(),
        "the {what} process must succeed:\n{}",
        combined(output)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str(&stdout).unwrap_or_else(|error| {
        panic!(
            "the {what} process must print JSON, got {error}:\n{}",
            combined(output)
        )
    })
}

fn combined(output: &Output) -> String {
    format!(
        "exit={:?}\n--- stdout ---\n{}\n--- stderr ---\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    )
}

/// Every way the read can disagree with the ground truth the seed reported.
///
/// This is the positive predicate: the healthy leg requires it to be empty,
/// and the fault control requires it to be non-empty. Running the same function
/// in both places is what makes the control evidence that the injected fault
/// flips these assertions rather than some parallel, weaker ones.
///
/// Each section is filtered by its own clock and capped by `limit`
/// independently, which is the contract the digest advertises.
fn digest_violations(seed: &Value, read: &Value) -> Vec<String> {
    let since = read["since_ms"].as_i64().expect("since_ms");
    let limit = read["limit"].as_u64().expect("limit") as usize;
    let mut violations = Vec::new();

    // pages: commits inside the window, ordered by the caller's clock newest
    // first, with `seq` breaking a tie. Log order is deliberately different.
    let mut expected_pages: Vec<(String, String, i64, u64)> = seed["commits"]
        .as_array()
        .expect("seed commits")
        .iter()
        .map(|commit| {
            (
                commit["kind"].as_str().expect("kind").to_string(),
                commit["path"].as_str().expect("path").to_string(),
                commit["at_ms"].as_i64().expect("at_ms"),
                commit["seq"].as_u64().expect("seq"),
            )
        })
        .filter(|(_, _, at_ms, _)| *at_ms >= since)
        .collect();
    expected_pages.sort_by(|a, b| b.2.cmp(&a.2).then_with(|| b.3.cmp(&a.3)));
    expected_pages.truncate(limit);
    let got_pages: Vec<(String, String, i64, u64)> = read["digest"]["pages"]
        .as_array()
        .expect("digest pages")
        .iter()
        .map(|page| {
            (
                page["kind"].as_str().expect("kind").to_string(),
                page["path"].as_str().expect("path").to_string(),
                page["at_ms"].as_i64().expect("at_ms"),
                page["seq"].as_u64().expect("seq"),
            )
        })
        .collect();
    if got_pages != expected_pages {
        violations.push(format!(
            "pages: got {got_pages:?}, expected {expected_pages:?}"
        ));
    }

    // sessions: heads that moved inside the window, newest first, `session_id`
    // breaking a tie.
    let mut expected_sessions: Vec<(String, u64, i64)> = seed["sessions"]
        .as_array()
        .expect("seed sessions")
        .iter()
        .map(|session| {
            (
                session["session_id"]
                    .as_str()
                    .expect("session_id")
                    .to_string(),
                session["observations"].as_u64().expect("observations"),
                session["last_seen_ms"].as_i64().expect("last_seen_ms"),
            )
        })
        .filter(|(_, _, last_seen_ms)| *last_seen_ms >= since)
        .collect();
    expected_sessions.sort_by(|a, b| b.2.cmp(&a.2).then_with(|| a.0.cmp(&b.0)));
    expected_sessions.truncate(limit);
    let got_sessions: Vec<(String, u64, i64)> = read["digest"]["sessions"]
        .as_array()
        .expect("digest sessions")
        .iter()
        .map(|session| {
            (
                session["session_id"]
                    .as_str()
                    .expect("session_id")
                    .to_string(),
                session["observations"].as_u64().expect("observations"),
                session["last_seen_ms"].as_i64().expect("last_seen_ms"),
            )
        })
        .collect();
    if got_sessions != expected_sessions {
        violations.push(format!(
            "sessions: got {got_sessions:?}, expected {expected_sessions:?}"
        ));
    }

    // handoffs: the window and the order both follow the *latest* stage change
    // of created / claimed / finished. A baton opened and claimed before the
    // window but finished inside it therefore belongs in this window, and the
    // finish is also what puts it first; a baton whose every stage is before
    // the window belongs in neither.
    //
    // The activity clock is recomputed here rather than borrowed from
    // `qm_store::handoff_activity_ms`, so the reader is checked against an
    // independent reading of the contract.
    let activity = |created_at_ms: i64, claimed_at_ms: Option<i64>, finished_at_ms: Option<i64>| {
        [Some(created_at_ms), claimed_at_ms, finished_at_ms]
            .into_iter()
            .flatten()
            .max()
            .expect("a handoff has at least its creation time")
    };
    let mut expected_handoffs: Vec<(String, i64, Option<i64>, Option<i64>)> = seed["handoffs"]
        .as_array()
        .expect("seed handoffs")
        .iter()
        .map(|handoff| {
            (
                handoff["id"].as_str().expect("id").to_string(),
                handoff["created_at_ms"].as_i64().expect("created_at_ms"),
                handoff["claimed_at_ms"].as_i64(),
                handoff["finished_at_ms"].as_i64(),
            )
        })
        .filter(|(_, created_at_ms, claimed_at_ms, finished_at_ms)| {
            activity(*created_at_ms, *claimed_at_ms, *finished_at_ms) >= since
        })
        .collect();
    expected_handoffs.sort_by(|a, b| {
        activity(b.1, b.2, b.3)
            .cmp(&activity(a.1, a.2, a.3))
            .then_with(|| a.0.cmp(&b.0))
    });
    expected_handoffs.truncate(limit);
    let got_handoffs: Vec<(String, i64, Option<i64>, Option<i64>)> = read["digest"]["handoffs"]
        .as_array()
        .expect("digest handoffs")
        .iter()
        .map(|handoff| {
            (
                handoff["id"].as_str().expect("id").to_string(),
                handoff["created_at_ms"].as_i64().expect("created_at_ms"),
                handoff["claimed_at_ms"].as_i64(),
                handoff["finished_at_ms"].as_i64(),
            )
        })
        .collect();
    if got_handoffs != expected_handoffs {
        violations.push(format!(
            "handoffs: got {got_handoffs:?}, expected {expected_handoffs:?}"
        ));
    }

    // live_pages: the manifest's own answer, and the point of the whole
    // scenario. Every written path except a deleted one must still be live;
    // the deleted path must not be.
    let mut expected_live: Vec<String> = seed["commits"]
        .as_array()
        .expect("seed commits")
        .iter()
        .filter(|commit| commit["kind"].as_str() == Some("page_written"))
        .map(|commit| commit["path"].as_str().expect("path").to_string())
        .filter(|path| {
            !seed["commits"].as_array().unwrap().iter().any(|commit| {
                commit["kind"].as_str() == Some("page_deleted")
                    && commit["path"].as_str() == Some(path.as_str())
            })
        })
        .collect();
    expected_live.sort();
    let mut got_live: Vec<String> = read["live_pages"]
        .as_array()
        .expect("live_pages")
        .iter()
        .map(|path| path.as_str().expect("path").to_string())
        .collect();
    got_live.sort();
    if got_live != expected_live {
        violations.push(format!(
            "live_pages: got {got_live:?}, expected {expected_live:?}"
        ));
    }

    violations
}

/// The paths commit records name, with their kinds, in the order they arrived.
fn commit_keys(value: &Value) -> Vec<(String, String)> {
    value["digest"]["pages"]
        .as_array()
        .expect("pages")
        .iter()
        .map(|page| {
            (
                page["kind"].as_str().expect("kind").to_string(),
                page["path"].as_str().expect("path").to_string(),
            )
        })
        .collect()
}

/// The listing requests in `window` — everything the reader asked the bucket to
/// enumerate.
fn listings(window: &[RecordedRequest]) -> Vec<&RecordedRequest> {
    window
        .iter()
        .filter(|request| request.target.contains("list-type=2"))
        .collect()
}

/// One object per listing page and one object per `GET`: the reader has to
/// follow the stub's continuation tokens to see the whole scope.
fn paging_stub() -> S3Stub {
    S3Stub::start_with(StubOptions::default().with_page_size(1)).expect("starting the stub")
}

#[test]
fn a_second_process_reads_the_whole_digest_from_the_bucket() {
    let stub = paging_stub();
    let seed = seed(&stub);
    assert_eq!(seed["commits"].as_array().unwrap().len(), 3);
    assert_eq!(seed["sessions"].as_array().unwrap().len(), 2);
    assert_eq!(seed["handoffs"].as_array().unwrap().len(), 3);

    let after_seed = stub.requests();
    let read = read(&stub, 0, 20);

    let violations = digest_violations(&seed, &read);
    assert!(
        violations.is_empty(),
        "the second process must rebuild the whole digest: {}",
        violations.join("; ")
    );
    assert!(!read["digest"]["pages"].as_array().unwrap().is_empty());
    assert!(!read["digest"]["sessions"].as_array().unwrap().is_empty());
    assert!(!read["digest"]["handoffs"].as_array().unwrap().is_empty());

    // The baton that was opened, claimed *and* finished carries all three
    // stages across the process boundary, and the finish is the value the seed
    // recorded — not a re-derivation the reader could have guessed.
    let seeded_finished = seed["handoffs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|handoff| handoff["finished_at_ms"].is_i64())
        .expect("the scenario finishes one baton")
        .clone();
    let read_finished = read["digest"]["handoffs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|handoff| handoff["id"] == seeded_finished["id"])
        .unwrap_or_else(|| panic!("the finished baton must be in the digest: {read}"));
    assert_eq!(
        read_finished["finished_at_ms"], seeded_finished["finished_at_ms"],
        "the digest's finish time must be the one the seed committed: {read}"
    );
    assert_eq!(
        read_finished["claimed_at_ms"], seeded_finished["claimed_at_ms"],
        "{read}"
    );
    assert_eq!(
        read_finished["created_at_ms"], seeded_finished["created_at_ms"],
        "{read}"
    );

    // ...and the finish is what orders the batons: the finished one has the
    // oldest creation and the oldest claim, yet leads because its latest stage
    // is the newest of all three.
    let baton_order: Vec<&str> = read["digest"]["handoffs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|handoff| handoff["id"].as_str().unwrap())
        .collect();
    assert_eq!(
        baton_order.first().copied(),
        seeded_finished["id"].as_str(),
        "the baton finished last must lead the handoff section: {read}"
    );
    let seeded_finished_created = seeded_finished["created_at_ms"].as_i64().unwrap();
    assert!(
        read["digest"]["handoffs"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|handoff| handoff["created_at_ms"].as_i64().unwrap() > seeded_finished_created)
            .count()
            > 0,
        "the baton that leads must not also be the one that was created last, \
         or creation order would explain the result: {read}"
    );

    // The centrepiece: the deletion is reported *and* the manifest has already
    // forgotten the path. Without both halves this would only prove that some
    // history survived somewhere.
    let deleted_path = seed["commits"]
        .as_array()
        .unwrap()
        .iter()
        .find(|commit| commit["kind"].as_str() == Some("page_deleted"))
        .and_then(|commit| commit["path"].as_str())
        .expect("the scenario commits a deletion")
        .to_string();
    assert!(
        commit_keys(&read).contains(&("page_deleted".to_string(), deleted_path.clone())),
        "the digest must keep the deletion: {read}"
    );
    let live = read["live_pages"].as_array().unwrap();
    assert!(
        !live
            .iter()
            .any(|path| path.as_str() == Some(deleted_path.as_str())),
        "the manifest must no longer answer for {deleted_path}: {read}"
    );
    assert!(
        live.iter()
            .any(|path| path.as_str() == Some("notes/alpha.md")),
        "the page that was not deleted must still be live: {read}"
    );

    // Ordering is the caller's clock, not the log: the first commit is the
    // newest page, and the deletion follows it.
    let order = commit_keys(&read);
    assert_eq!(order[0].0, "page_written", "{read}");
    assert_eq!(order[0].1, "notes/alpha.md", "{read}");
    assert_eq!(
        order[1],
        ("page_deleted".to_string(), deleted_path),
        "{read}"
    );

    // ...and that only proves something because the two orders really differ
    // here: the scenario commits against the clock on purpose. A later edit
    // that flattened the timeline would leave the assertions above passing
    // while silently no longer testing ordering at all, so pin the gap.
    let mut by_clock: Vec<u64> = read["digest"]["pages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|page| page["seq"].as_u64().unwrap())
        .collect();
    let mut by_log: Vec<u64> = seed["commits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|commit| commit["seq"].as_u64().unwrap())
        .collect();
    by_log.sort_unstable_by(|a, b| b.cmp(a));
    by_clock.truncate(by_log.len());
    assert_ne!(
        by_clock, by_log,
        "the digest's clock order must differ from the commit log's sequence order: {read}"
    );

    // Tying the read to the wire: the bucket answered one object per page, so
    // the second process can only have seen the whole scope by following the
    // continuation tokens.
    let reader_phase = &stub.requests()[after_seed.len()..];
    let lists = listings(reader_phase);
    assert!(
        lists.len() > 3,
        "each of the three prefixes holds more than one object, so one object per page \
         cannot be enumerated in three requests: {reader_phase:#?}"
    );
    assert!(
        lists
            .iter()
            .any(|request| request.target.contains("continuation-token=")),
        "the reader must follow the stub's continuation tokens: {reader_phase:#?}"
    );
    assert!(
        lists.iter().all(|request| request.status == 200),
        "every listing page must have been served: {reader_phase:#?}"
    );

    stub.shutdown();
}

#[test]
fn the_window_filters_each_section_by_its_own_clock() {
    let stub = paging_stub();
    let seed = seed(&stub);

    // 2_000ms. The older session head and the older page write are out. The
    // finished baton is *in* even though both its open (400ms) and its claim
    // (900ms) are outside, because its finish is at 3_200ms — that is the whole
    // point of following the latest stage. The baton whose every stage is at
    // 300ms is out.
    let windowed = read(&stub, 2_000, 20);
    let windowed_violations = digest_violations(&seed, &windowed);
    assert!(
        windowed_violations.is_empty(),
        "{}",
        windowed_violations.join("; ")
    );

    let sessions = windowed["digest"]["sessions"].as_array().unwrap();
    assert_eq!(sessions.len(), 1, "{windowed}");
    assert_eq!(
        sessions[0]["session_id"], "probe-sess-new",
        "only the session whose head moved inside the window belongs: {windowed}"
    );
    let pages = windowed["digest"]["pages"].as_array().unwrap();
    assert_eq!(pages.len(), 2, "{windowed}");
    assert!(
        pages
            .iter()
            .all(|page| page["at_ms"].as_i64().unwrap() >= 2_000),
        "the page written at 1_000ms must be outside the window: {windowed}"
    );
    let handoffs = windowed["digest"]["handoffs"].as_array().unwrap();
    assert!(
        handoffs.iter().any(|handoff| {
            handoff["created_at_ms"].as_i64() == Some(400)
                && handoff["claimed_at_ms"].as_i64() == Some(900)
                && handoff["finished_at_ms"].as_i64() == Some(3_200)
        }),
        "a baton opened and claimed before the window but finished inside it must appear: {windowed}"
    );
    assert!(
        !handoffs
            .iter()
            .any(|handoff| handoff["created_at_ms"].as_i64() == Some(300)),
        "a baton whose every stage is before the window must not appear: {windowed}"
    );

    // The same scope with a window wide enough for everything, so the window
    // above is shown narrowing an answer rather than describing an empty one.
    let full = read(&stub, 0, 20);
    assert!(
        full["digest"]["sessions"].as_array().unwrap().len() > sessions.len(),
        "{full}"
    );

    stub.shutdown();
}

#[test]
fn the_limit_caps_each_section_independently() {
    let stub = paging_stub();
    let seed = seed(&stub);

    let full = read(&stub, 0, 20);
    let capped = read(&stub, 0, 1);
    let capped_violations = digest_violations(&seed, &capped);
    assert!(
        capped_violations.is_empty(),
        "{}",
        capped_violations.join("; ")
    );

    for section in ["pages", "sessions", "handoffs"] {
        assert_eq!(
            capped["digest"][section].as_array().unwrap().len(),
            1,
            "{section} must be capped on its own: {capped}"
        );
        assert_eq!(
            capped["digest"][section][0], full["digest"][section][0],
            "{section} must keep its newest end when capped: {capped}"
        );
    }

    stub.shutdown();
}

/// Control: a bucket that answers the first listing page as if it were the
/// whole listing. The same predicate the healthy leg requires to be empty must
/// now report the parts of the digest that went missing.
#[test]
fn a_listing_that_stops_at_its_first_page_makes_the_digest_short() {
    let broken = S3Stub::start_with(
        StubOptions::default()
            .with_page_size(1)
            .with_fault(Fault::TruncateListing),
    )
    .expect("starting the stub");

    // The writer does not enumerate anything, so the fault is a reader-side
    // one: process A still commits the whole history.
    let seed = seed(&broken);
    assert_eq!(seed["commits"].as_array().unwrap().len(), 3);
    let after_seed = broken.requests().len();

    let read = read(&broken, 0, 20);
    let violations = digest_violations(&seed, &read);
    assert!(
        !violations.is_empty(),
        "a listing that hides the rest of the bucket must break the digest contract: {read}"
    );
    assert!(
        read["digest"]["pages"]
            .as_array()
            .unwrap()
            .iter()
            .all(|page| page["kind"].as_str() == Some("page_written")),
        "the deletion lives in a commit the truncated listing never showed: {read}"
    );

    // Tie the short answer to the fault: the reader asked each prefix once,
    // got no continuation token, and had no way to learn there was more.
    let reader_phase = &broken.requests()[after_seed..];
    let lists = listings(reader_phase);
    assert_eq!(
        lists.len(),
        3,
        "one page per prefix is the whole answer the fault offers: {reader_phase:#?}"
    );
    assert!(
        lists
            .iter()
            .all(|request| !request.target.contains("continuation-token=")),
        "the faulted stub never offers a second page: {reader_phase:#?}"
    );

    broken.shutdown();
}
