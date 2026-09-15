//! The S3 protocol layer, exercised over a real socket.
//!
//! Everything else in this repository proves the CAS contract against
//! `object_store`'s in-memory backend, which has no HTTP layer: no ETag
//! headers, no conditional request headers, no 404/412 error documents, no
//! `ListObjectsV2` XML. These tests drive the **real S3 client** at
//! [`qm_probe::s3_stub::S3Stub`], so that path is covered too — still without
//! credentials and without reaching any real bucket.
//!
//! What this cannot prove is the part that needs a real account: signatures,
//! latency, quotas, region behaviour and R2's own XML variants (see
//! `docs/design.md` §5.1).

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use object_store::aws::AmazonS3Builder;
use object_store::{BackoffConfig, ObjectStore, RetryConfig};
use qm_probe::s3_stub::{Fault, S3Stub, StubOptions};
use qm_store::{CasStore, ObjectVersion, StoreError, verify_conditional_writes};

const ACCESS_KEY: &str = "stub-access";
const SECRET_KEY: &str = "stub-secret";

/// The same client shape `S3Config::build_store` builds: path style, signed,
/// HTTP allowed because the endpoint is loopback.
fn s3_builder(endpoint: &str, bucket: &str) -> AmazonS3Builder {
    AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region("auto")
        .with_virtual_hosted_style_request(false)
        .with_access_key_id(ACCESS_KEY)
        .with_secret_access_key(SECRET_KEY)
        .with_endpoint(endpoint)
        .with_allow_http(true)
}

fn s3_store(endpoint: &str, bucket: &str) -> Arc<dyn ObjectStore> {
    Arc::new(
        s3_builder(endpoint, bucket)
            .build()
            .expect("building the S3 client against the stub"),
    )
}

/// A client whose retries are cheap, so a test can afford to exhaust them.
///
/// The budget is the point: with the default configuration the client makes
/// eleven attempts with a decorrelated 100 ms+ backoff, which is the right
/// production posture and the wrong shape for a control that has to fail.
fn s3_store_with_retry_budget(
    endpoint: &str,
    bucket: &str,
    attempts: usize,
) -> Arc<dyn ObjectStore> {
    Arc::new(
        s3_builder(endpoint, bucket)
            .with_retry(RetryConfig {
                backoff: BackoffConfig {
                    init_backoff: Duration::from_millis(1),
                    max_backoff: Duration::from_millis(5),
                    base: 2.0,
                },
                max_retries: attempts - 1,
                retry_timeout: Duration::from_secs(30),
            })
            .build()
            .expect("building an S3 client with a small retry budget"),
    )
}

fn cas(stub: &S3Stub) -> CasStore {
    CasStore::new(s3_store(&stub.endpoint(), stub.bucket()), "")
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[tokio::test]
async fn the_conformance_probe_passes_over_real_http() {
    let stub = S3Stub::start().expect("starting the stub");
    let store = cas(&stub);

    let report = verify_conditional_writes(&store, "qm-probe/cas-1")
        .await
        .expect("the probe must pass against a well-behaved stub");
    assert!(report.all_passed(), "{}", report.render());
    assert_eq!(report.steps.len(), 7, "{}", report.render());

    // The probe's own steps are only meaningful if the conditional headers
    // really crossed the wire, so assert on what the server received.
    let requests = stub.requests();
    let creates: Vec<_> = requests
        .iter()
        .filter(|request| request.method == "PUT" && request.if_none_match.as_deref() == Some("*"))
        .collect();
    // Step 1 creates and step 2 repeats the create to prove the bucket, not
    // the client, is the one refusing the duplicate.
    assert_eq!(creates.len(), 2, "create-if-absent, twice: {requests:#?}");
    assert_eq!(creates[0].status, 200);
    assert_eq!(
        creates[1].status, 412,
        "the duplicate must be refused by the bucket"
    );
    assert_eq!(creates[0].key, "qm-probe/cas-1");

    // Step 4 of the probe updates with a deliberately stale ETag. A backend
    // that answers 412 to *that* write is the thing being verified, and it
    // only proves anything if that stale value is what crossed the wire.
    let stale: Vec<_> = requests
        .iter()
        .filter(|request| request.if_match.as_deref() == Some("deadbeef-not-a-real-etag"))
        .collect();
    assert_eq!(stale.len(), 1, "{requests:#?}");
    assert_eq!(stale[0].status, 412);

    let rejected_updates = requests
        .iter()
        .filter(|request| {
            request.method == "PUT" && request.if_match.is_some() && request.status == 412
        })
        .count();
    assert_eq!(
        rejected_updates, 2,
        "the stale and the consumed ETag must each be refused: {requests:#?}"
    );

    // Signed, not signature-skipped: the fake credentials still travel through
    // the SigV4 code path.
    let authorized = requests
        .iter()
        .filter_map(|request| request.authorization.as_deref())
        .filter(|value| value.starts_with("AWS4-HMAC-SHA256"))
        .count();
    assert_eq!(authorized, requests.len(), "every request must be signed");

    // Signed, not merely sent: a conditional header outside `SignedHeaders`
    // could be rewritten in flight without invalidating the signature, and
    // only the header list the client signed can rule that out.
    let mut signed_conditional = 0;
    for request in &requests {
        for name in [
            request.if_match.as_ref().map(|_| "if-match"),
            request.if_none_match.as_ref().map(|_| "if-none-match"),
        ]
        .into_iter()
        .flatten()
        {
            let headers = request
                .authorization
                .as_deref()
                .and_then(|value| value.split_once("SignedHeaders="))
                .and_then(|(_, rest)| rest.split(',').next())
                .unwrap_or_default();
            assert!(
                headers.split(';').any(|entry| entry == name),
                "{name} must be inside SignedHeaders, got {headers:?}: {request:#?}"
            );
            signed_conditional += 1;
        }
    }
    // Pinned so the loop above cannot pass by iterating over nothing: the two
    // creates plus the stale, matching and consumed updates.
    assert_eq!(
        signed_conditional, 5,
        "expected five signed conditional writes: {requests:#?}"
    );

    // HEAD and DELETE are part of the S3 surface the store uses; the probe
    // itself reaches neither.
    let key = "qm-probe/head-and-delete";
    let created = store.create(key, Bytes::from_static(b"v1")).await.unwrap();
    let head = store.head(key).await.expect("HEAD must return metadata");
    assert_eq!(head.etag, created.etag);
    let modified = store
        .last_modified_ms(key)
        .await
        .expect("HEAD must parse")
        .expect("the object exists");
    assert!(
        (now_ms() - modified).abs() < 300_000,
        "Last-Modified must be the time of the write, got {modified}"
    );

    store.delete(key).await.expect("DELETE must succeed");
    assert!(matches!(
        store.read(key).await.unwrap_err(),
        StoreError::NotFound
    ));
    // S3 deletes are idempotent; the store treats a missing key as success.
    store
        .delete(key)
        .await
        .expect("deleting a missing key is not an error");

    stub.shutdown();
}

#[tokio::test]
async fn the_r2_put_only_version_shape_really_happens_and_still_passes() {
    let stub = S3Stub::start_with(StubOptions::default().with_put_version_id(true))
        .expect("starting the stub");
    let store = cas(&stub);
    let key = "qm-probe/r2-shape";

    let created = store.create(key, Bytes::from_static(b"v1")).await.unwrap();
    assert!(
        created.version.is_some(),
        "the stub must model the R2 PUT-only version id, got {created:?}"
    );

    let (_, read) = store.read(key).await.unwrap();
    assert_eq!(
        read.version, None,
        "R2 does not repeat x-amz-version-id on GET; if it did, this test would not exercise the shape"
    );
    assert_eq!(read.etag, created.etag);

    let head = store.head(key).await.expect("HEAD must return metadata");
    assert_eq!(
        head.version, None,
        "and HEAD must not repeat it either, or the shape would not be R2's"
    );

    // The shape only matters if the probe still passes while it holds.
    let report = verify_conditional_writes(&store, "qm-probe/r2-probe")
        .await
        .expect("the probe must pass with a PUT-only version id");
    assert!(report.all_passed(), "{}", report.render());

    stub.shutdown();
}

#[tokio::test]
async fn a_stub_that_ignores_if_match_makes_the_probe_fail() {
    // Positive control in one test: the same probe passes against the
    // well-behaved stub and fails against the one that accepts stale writes.
    let good = S3Stub::start().expect("starting the stub");
    verify_conditional_writes(&cas(&good), "qm-probe/control-ok")
        .await
        .expect("the well-behaved stub must pass");
    good.shutdown();

    let broken = S3Stub::start_with(StubOptions::default().with_fault(Fault::IgnoreIfMatch))
        .expect("starting the stub");
    let error = verify_conditional_writes(&cas(&broken), "qm-probe/control-bad")
        .await
        .expect_err("a backend that ignores If-Match must not pass the probe");
    let rendered = format!("{error}");
    assert!(
        rendered.contains("FAIL stale-etag-rejected"),
        "the stale ETag step must be the one that fails:\n{rendered}"
    );
    assert!(
        rendered.contains("FAIL consumed-etag-rejected"),
        "a replayed ETag must not be accepted either:\n{rendered}"
    );

    let accepted = broken
        .requests()
        .iter()
        .filter(|request| request.method == "PUT" && request.status == 200)
        .count();
    assert!(
        accepted >= 4,
        "the broken backend accepted writes it should have refused: {:#?}",
        broken.requests()
    );

    broken.shutdown();
}

#[tokio::test]
async fn a_stub_that_serves_a_stale_etag_makes_the_probe_fail() {
    let stub = S3Stub::start_with(StubOptions::default().with_fault(Fault::StaleReadEtag))
        .expect("starting the stub");
    let error = verify_conditional_writes(&cas(&stub), "qm-probe/stale-read")
        .await
        .expect_err("an ETag that changes between PUT and GET must not pass");
    let rendered = format!("{error}");
    // The probe stops here rather than rendering a step list: it read an
    // identity the bucket does not honour, so the conditional write that
    // follows is refused. Either way the probe fails loudly, which is the
    // point — a backend whose read identity moves silently would otherwise
    // look healthy.
    assert!(
        rendered.contains("matching update failed"),
        "the read-back identity must be the one that breaks the probe:\n{rendered}"
    );

    // The rejected update really did arrive with the stale ETag the probe read.
    let mismatched = stub
        .requests()
        .into_iter()
        .filter(|request| {
            request.method == "PUT" && request.if_match.is_some() && request.status == 412
        })
        .count();
    assert_eq!(
        mismatched, 2,
        "the stale ETag must be refused against the read-back one"
    );
    stub.shutdown();
}

#[tokio::test]
async fn two_concurrent_conditional_writes_leave_exactly_one_winner() {
    let stub = S3Stub::start().expect("starting the stub");
    let store = cas(&stub);
    let key = "qm-probe/race";

    let version = store.create(key, Bytes::from_static(b"v1")).await.unwrap();

    // Both writers observed the same version and race to replace it: this is
    // the shape the multi-machine design depends on, at the HTTP level.
    let left = store.update(key, Bytes::from_static(b"left"), &version);
    let right = store.update(key, Bytes::from_static(b"right"), &version);
    let (left, right) = tokio::join!(left, right);

    let winners = [&left, &right]
        .iter()
        .filter(|result| result.is_ok())
        .count();
    assert_eq!(
        winners, 1,
        "exactly one conditional write may win: left={left:?} right={right:?}"
    );
    let loser = match (&left, &right) {
        (Ok(_), Err(error)) | (Err(error), Ok(_)) => error,
        other => panic!("expected one winner and one precondition failure: {other:?}"),
    };
    assert!(
        matches!(loser, StoreError::Precondition),
        "the loser must be told the object moved, not something generic, got {loser:?}"
    );

    let expected: &[u8] = if left.is_ok() { b"left" } else { b"right" };
    let (bytes, _) = store.read(key).await.unwrap();
    assert_eq!(
        bytes.as_ref(),
        expected,
        "the surviving content must be the winner's, never a mix"
    );

    let racing: Vec<_> = stub
        .requests()
        .into_iter()
        .filter(|request| {
            request.method == "PUT" && request.if_match.is_some() && request.key == key
        })
        .collect();
    assert_eq!(racing.len(), 2, "{racing:#?}");
    assert_eq!(
        racing[0].if_match, racing[1].if_match,
        "both writers must have raced on the same observed ETag"
    );
    assert_eq!(
        racing
            .iter()
            .filter(|request| request.status == 412)
            .count(),
        1,
        "exactly one of the racing writes must be refused with 412: {racing:#?}"
    );

    stub.shutdown();
}

#[tokio::test]
async fn the_stub_lists_pages_the_s3_backend_follows() {
    let stub =
        S3Stub::start_with(StubOptions::default().with_page_size(2)).expect("starting the stub");
    let store = cas(&stub);

    let mut written = BTreeMap::new();
    for index in 1..=5u8 {
        let key = format!("pages/notes/{index}.md");
        store
            .create(&key, Bytes::from(vec![b'x'; index as usize]))
            .await
            .unwrap();
        written.insert(key, index);
    }
    store
        .create("other/outside.md", Bytes::from_static(b"y"))
        .await
        .unwrap();

    let listed = store.list("pages").await.expect("listing must parse");
    let keys: Vec<&str> = listed.iter().map(|(key, _)| key.as_str()).collect();
    assert_eq!(
        keys,
        vec![
            "pages/notes/1.md",
            "pages/notes/2.md",
            "pages/notes/3.md",
            "pages/notes/4.md",
            "pages/notes/5.md"
        ],
        "the listing must be complete, sorted and scoped to the prefix"
    );
    for (key, version) in &listed {
        let index = *written.get(key).unwrap();
        let etag = version
            .etag
            .as_deref()
            .unwrap_or_else(|| panic!("{key} must carry the ETag from the list XML"));
        assert!(
            etag.ends_with(&format!("-{index}\"")),
            "{etag} must be the ETag of {key}, not a constant"
        );
    }

    // Pagination is what proves the XML, the token and the client's loop all
    // work together: five objects at two per page is three requests.
    let lists: Vec<_> = stub
        .requests()
        .into_iter()
        .filter(|request| request.method == "GET" && request.target.contains("list-type=2"))
        .collect();
    assert_eq!(lists.len(), 3, "expected three pages, got {lists:#?}");
    assert!(
        lists[1].target.contains("continuation-token=")
            && lists[2].target.contains("continuation-token="),
        "the client must follow the stub's continuation tokens: {lists:#?}"
    );
    assert!(
        lists
            .iter()
            .all(|request| request.target.contains("prefix=pages")),
        "every page must be asked for the same prefix: {lists:#?}"
    );

    // 404 handling in both directions: a missing key reads as NotFound, and a
    // conditional update against it is a precondition failure (S3 answers 404
    // to a conditional PUT on a missing object; the client translates it).
    assert!(matches!(
        store.read("pages/notes/missing.md").await.unwrap_err(),
        StoreError::NotFound
    ));
    let stale = ObjectVersion::new(Some("\"stub-1-1\"".to_string()), None);
    let rejected = store
        .update("pages/notes/missing.md", Bytes::from_static(b"z"), &stale)
        .await
        .unwrap_err();
    assert!(
        matches!(rejected, StoreError::Precondition),
        "a conditional PUT on a missing object must read as a precondition failure, got {rejected:?}"
    );

    stub.shutdown();
}

#[tokio::test]
async fn the_probe_binary_passes_and_fails_against_the_stub() {
    let good = S3Stub::start().expect("starting the stub");
    let passed = run_conformance(&good);
    assert!(
        passed.status.success(),
        "the probe binary must pass against the stub:\n{}",
        combined(&passed)
    );
    assert!(
        combined(&passed).contains("all steps passed"),
        "{}",
        combined(&passed)
    );
    good.shutdown();

    let broken = S3Stub::start_with(StubOptions::default().with_fault(Fault::IgnoreIfMatch))
        .expect("starting the stub");
    let failed = run_conformance(&broken);
    assert!(
        !failed.status.success(),
        "the probe binary must fail against a backend that ignores If-Match:\n{}",
        combined(&failed)
    );
    assert!(
        combined(&failed).contains("conditional-write conformance failed")
            && combined(&failed).contains("FAIL stale-etag-rejected"),
        "{}",
        combined(&failed)
    );
    broken.shutdown();
}

/// A failure must arrive as an S3 error document, not an empty body: a real
/// bucket always explains itself, and a client that parses the code should
/// never be the only thing standing between a 404 and a mystery.
#[test]
fn the_stub_answers_failures_with_s3_error_documents() {
    let stub = S3Stub::start().expect("starting the stub");

    let created = raw_request(
        &stub,
        "PUT /stub-bucket/probe/key HTTP/1.1\r\nif-none-match: *\r\ncontent-length: 2\r\n\r\nv1",
    );
    assert!(created.starts_with("HTTP/1.1 200 OK"), "{created}");
    assert!(created.contains("etag: \"stub-"), "{created}");

    let duplicate = raw_request(
        &stub,
        "PUT /stub-bucket/probe/key HTTP/1.1\r\nif-none-match: *\r\ncontent-length: 2\r\n\r\nv2",
    );
    assert!(
        duplicate.starts_with("HTTP/1.1 412 Precondition Failed"),
        "{duplicate}"
    );
    assert!(
        duplicate.contains("content-type: application/xml"),
        "{duplicate}"
    );
    assert!(
        duplicate.contains("<Code>PreconditionFailed</Code>"),
        "a refused conditional write must carry S3 error XML: {duplicate}"
    );

    let missing = raw_request(&stub, "GET /stub-bucket/probe/absent HTTP/1.1\r\n\r\n");
    assert!(missing.starts_with("HTTP/1.1 404 Not Found"), "{missing}");
    assert!(missing.contains("<Code>NoSuchKey</Code>"), "{missing}");

    let wrong_bucket = raw_request(&stub, "GET /other-bucket/probe/key HTTP/1.1\r\n\r\n");
    assert!(
        wrong_bucket.starts_with("HTTP/1.1 404 Not Found"),
        "{wrong_bucket}"
    );
    assert!(
        wrong_bucket.contains("<Code>NoSuchBucket</Code>"),
        "{wrong_bucket}"
    );

    stub.shutdown();
}

/// A conflict on a conditional write is transient in S3 — the client retries
/// exactly that write mode — so one must not reach the probe.
#[tokio::test]
async fn a_transient_conflict_is_retried_and_the_probe_still_passes() {
    let stub = S3Stub::start_with(StubOptions::default().with_conditional_put_conflicts(1))
        .expect("starting the stub");

    let report = verify_conditional_writes(&cas(&stub), "qm-probe/conflict-once")
        .await
        .expect("a conditional write the client retried must not fail the probe");
    assert!(report.all_passed(), "{}", report.render());

    // The probe only passes for the right reason if the injected 409 really
    // crossed the wire and the client really repeated the same write.
    let requests = stub.requests();
    let conflicts: Vec<_> = requests
        .iter()
        .filter(|request| request.status == 409)
        .collect();
    assert_eq!(
        conflicts.len(),
        1,
        "exactly one conflict may be injected: {requests:#?}"
    );
    assert_eq!(conflicts[0].method, "PUT", "{conflicts:#?}");
    let etag = conflicts[0].if_match.clone();
    assert!(
        etag.is_some(),
        "the conflict must land on a conditional write: {conflicts:#?}"
    );

    // The very next request must be the same conditional write repeated: same
    // key, same If-Match, and the one that lands. (Asserting a count of two
    // would also be satisfied by the later replay of that ETag, which is a
    // different request answering 412.)
    let conflict_at = requests
        .iter()
        .position(|request| request.status == 409)
        .expect("the injected conflict must be in the log");
    let retry = &requests[conflict_at + 1];
    assert_eq!(retry.method, "PUT", "{retry:#?}");
    assert_eq!(retry.key, conflicts[0].key, "{retry:#?}");
    assert_eq!(retry.if_match, etag, "{retry:#?}");
    assert_eq!(retry.status, 200, "{retry:#?}");

    stub.shutdown();
}

/// The other half of the injection: a bucket that never stops conflicting has
/// to fail the probe. Without this, "the probe passes with a conflict" would
/// also be satisfied by a stub that swallowed the 409 instead of sending one.
#[tokio::test]
async fn a_bucket_that_never_stops_conflicting_fails_the_probe() {
    // One attempt plus two retries, instead of the client's default eleven.
    const ATTEMPTS: usize = 3;
    let stub =
        S3Stub::start_with(StubOptions::default().with_conditional_put_conflicts(usize::MAX))
            .expect("starting the stub");
    let store = s3_store_with_retry_budget(&stub.endpoint(), stub.bucket(), ATTEMPTS);

    let error = verify_conditional_writes(&CasStore::new(store, ""), "qm-probe/conflict-always")
        .await
        .expect_err("a bucket that never stops conflicting must not pass the probe");
    let rendered = format!("{error}");
    // The probe stops at the first conditional write whose preconditions held:
    // the stale-ETag step is refused (412) before the injection, so the
    // matching update is the one that runs out of attempts.
    assert!(
        rendered.contains("matching update failed"),
        "the write that ran out of retries must be the one that fails:\n{rendered}"
    );

    let conditional: Vec<_> = stub
        .requests()
        .into_iter()
        .filter(|request| request.method == "PUT" && request.if_match.is_some())
        .collect();
    assert_eq!(
        conditional
            .iter()
            .filter(|request| request.status == 409)
            .count(),
        ATTEMPTS,
        "every attempt of the conflicted write must be refused, and the client \
         must use its whole budget before surfacing the failure: {conditional:#?}"
    );

    stub.shutdown();
}

/// A conditional read the stub does not model is refused, not answered with an
/// unconditional `200`: the wrong answer is worse than a missing one, because
/// a probe built on it would look green for the wrong reason.
#[test]
fn conditional_reads_are_refused_rather_than_answered() {
    let stub = S3Stub::start().expect("starting the stub");
    let created = raw_request(
        &stub,
        "PUT /stub-bucket/probe/conditional HTTP/1.1\r\nif-none-match: *\r\n\
         content-length: 2\r\n\r\nv1",
    );
    assert!(created.starts_with("HTTP/1.1 200 OK"), "{created}");

    for request in [
        "GET /stub-bucket/probe/conditional HTTP/1.1\r\nif-none-match: *\r\n\r\n",
        "GET /stub-bucket/probe/conditional HTTP/1.1\r\nif-match: \"whatever\"\r\n\r\n",
    ] {
        let response = raw_request(&stub, request);
        assert!(
            response.starts_with("HTTP/1.1 501 Not Implemented"),
            "{response}"
        );
        assert!(
            response.contains("<Code>NotImplemented</Code>"),
            "a refusal must still explain itself: {response}"
        );
        assert!(response.contains("conditional reads"), "{response}");
    }

    // `HEAD` is refused the same way, minus the body it declines to send.
    let get = raw_request(
        &stub,
        "GET /stub-bucket/probe/conditional HTTP/1.1\r\nif-none-match: *\r\n\r\n",
    );
    let head = raw_request(
        &stub,
        "HEAD /stub-bucket/probe/conditional HTTP/1.1\r\nif-none-match: *\r\n\r\n",
    );
    assert!(head.starts_with("HTTP/1.1 501 Not Implemented"), "{head}");
    assert_eq!(
        header_value(&head, "content-length"),
        header_value(&get, "content-length"),
        "a HEAD refusal must advertise the body it declines to send"
    );
    assert!(
        head.ends_with("\r\n\r\n"),
        "a HEAD response carries no body: {head}"
    );

    // The unconditional read still works, so the refusal is about the
    // condition and not about the key or the method.
    let plain = raw_request(&stub, "GET /stub-bucket/probe/conditional HTTP/1.1\r\n\r\n");
    assert!(plain.starts_with("HTTP/1.1 200 OK"), "{plain}");
    assert!(plain.ends_with("v1"), "{plain}");

    stub.shutdown();
}

/// `MaxKeys` is the size the client *asked* for, not the size the stub chose to
/// serve; a client that trusts the echo must not be told 2 when it asked 1000.
#[test]
fn the_list_response_echoes_the_requested_page_size() {
    let stub =
        S3Stub::start_with(StubOptions::default().with_page_size(2)).expect("starting the stub");
    for index in 1..=3 {
        let body = format!("v{index}");
        let created = raw_request(
            &stub,
            &format!(
                "PUT /stub-bucket/pages/{index}.md HTTP/1.1\r\nif-none-match: *\r\n\
                 content-length: {}\r\n\r\n{body}",
                body.len()
            ),
        );
        assert!(created.starts_with("HTTP/1.1 200 OK"), "{created}");
    }

    let listed = raw_request(
        &stub,
        "GET /stub-bucket?list-type=2&prefix=pages&max-keys=1000 HTTP/1.1\r\n\r\n",
    );
    assert!(listed.contains("<MaxKeys>1000</MaxKeys>"), "{listed}");
    assert!(listed.contains("<KeyCount>2</KeyCount>"), "{listed}");
    assert!(
        listed.contains("<IsTruncated>true</IsTruncated>"),
        "{listed}"
    );

    // And a size the stub has to honour literally, so the echo is not the
    // only thing the number is wired to.
    let one = raw_request(
        &stub,
        "GET /stub-bucket?list-type=2&prefix=pages&max-keys=1 HTTP/1.1\r\n\r\n",
    );
    assert!(one.contains("<MaxKeys>1</MaxKeys>"), "{one}");
    assert!(one.contains("<KeyCount>1</KeyCount>"), "{one}");

    stub.shutdown();
}

/// `If-Match: *` asserts existence, not identity. Comparing it as an ETag
/// would refuse every object that does exist, which is the opposite of what a
/// client asking "is it still there" is told.
#[test]
fn if_match_asterisk_asserts_existence() {
    let stub = S3Stub::start().expect("starting the stub");
    let created = raw_request(
        &stub,
        "PUT /stub-bucket/probe/star HTTP/1.1\r\nif-none-match: *\r\ncontent-length: 2\r\n\r\nv1",
    );
    assert!(created.starts_with("HTTP/1.1 200 OK"), "{created}");

    let replaced = raw_request(
        &stub,
        "PUT /stub-bucket/probe/star HTTP/1.1\r\nif-match: *\r\ncontent-length: 2\r\n\r\nv2",
    );
    assert!(
        replaced.starts_with("HTTP/1.1 200 OK"),
        "an existing object satisfies If-Match: *: {replaced}"
    );
    let read = raw_request(&stub, "GET /stub-bucket/probe/star HTTP/1.1\r\n\r\n");
    assert!(
        read.ends_with("v2"),
        "the wildcard write must have landed: {read}"
    );

    let missing = raw_request(
        &stub,
        "PUT /stub-bucket/probe/absent HTTP/1.1\r\nif-match: *\r\ncontent-length: 2\r\n\r\nv2",
    );
    assert!(
        missing.starts_with("HTTP/1.1 404 Not Found"),
        "a missing object is the answer S3 gives any If-Match on it: {missing}"
    );
    assert!(missing.contains("<Code>NoSuchKey</Code>"), "{missing}");

    stub.shutdown();
}

/// One response header, for assertions that have to compare two responses.
fn header_value(response: &str, name: &str) -> String {
    response
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{name}: ")))
        .unwrap_or_else(|| panic!("{name} must be in the response:\n{response}"))
        .trim()
        .to_string()
}

fn raw_request(stub: &S3Stub, request: &str) -> String {
    let mut stream = TcpStream::connect(stub.addr()).expect("connecting to the stub");
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .expect("a read deadline on the test client");
    stream
        .write_all(request.as_bytes())
        .expect("writing the request");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("reading the response to EOF");
    response
}

fn run_conformance(stub: &S3Stub) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_cas-conformance"))
        // A hermetic environment: the probe must take every setting from the
        // stub, never from whatever the developer's shell exports.
        .env_clear()
        .env("QM_S3_ENDPOINT", stub.endpoint())
        .env("QM_S3_BUCKET", stub.bucket())
        .env("QM_S3_ACCESS_KEY_ID", ACCESS_KEY)
        .env("QM_S3_SECRET_ACCESS_KEY", SECRET_KEY)
        .env("QM_S3_FORCE_PATH_STYLE", "true")
        .output()
        .expect("running the cas-conformance probe binary")
}

fn combined(output: &std::process::Output) -> String {
    format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}
