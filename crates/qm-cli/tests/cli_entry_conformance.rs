//! Conformance: the **real** `qm` binary reads the bucket environment the
//! operators are told to set.
//!
//! The library guard test drives the lookup seam in `build_bucket_from`, so on
//! its own it stays green if `build_bucket_from_env` stops delegating and grows
//! its own copy of the name table. Only a real process, started with a cleared
//! environment, can say whether an operator's `QM_S3_BUCKET` actually reaches
//! the client — which is what this checks.
//!
//! The CLI has no synthetic bucket and this test has no credentials, so the
//! command is *expected* to fail; what matters is why it fails. A page path the
//! CLI rejects keeps the failure local and instant, while still happening after
//! `main` has built the client from the environment.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use clap::CommandFactory;

use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use qm_core::{KeyLayout, ProjectId, SessionId, WorkspaceId};
use qm_probe::s3_stub::S3Stub;
use tempfile::TempDir;

/// The names `docs/ops.md` tells an operator to set. The endpoint points at a
/// closed port on purpose: should a request ever be attempted, it stays on
/// loopback and fails instead of reaching a real bucket.
const DOCUMENTED_BUCKET_ENV: [(&str, &str); 4] = [
    ("QM_S3_ENDPOINT", "http://127.0.0.1:9"),
    ("QM_S3_BUCKET", "bucket-a"),
    ("QM_S3_ACCESS_KEY_ID", "docs-access-key"),
    ("QM_S3_SECRET_ACCESS_KEY", "docs-secret-key"),
];

/// The `R2_*` aliases the same resolver documents as the fallback.
const DOCUMENTED_R2_ALIASES: [(&str, &str); 4] = [
    ("R2_ENDPOINT", "http://127.0.0.1:9"),
    ("R2_BUCKET", "bucket-a"),
    ("R2_ACCESS_KEY_ID", "docs-access-key"),
    ("R2_SECRET_ACCESS_KEY", "docs-secret-key"),
];

fn run_qm(env: &[(&str, &str)]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_qm"));
    // A cleared environment is the point: the names set below have to be
    // enough on their own, and an inherited bucket variable must not be able to
    // stand in for one that is missing.
    command
        .env_clear()
        .args(["read-page", "--path", "../not-portable"]);
    for (key, value) in env {
        command.env(key, value);
    }
    command.output().expect("running the qm binary")
}

fn run_qm_args(args: &[&str], env: &[(&str, String)]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_qm"));
    command.env_clear().args(args);
    for (key, value) in env {
        command.env(key, value);
    }
    command.output().expect("running the qm binary")
}

fn repo_root() -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    root.canonicalize().unwrap_or_else(|error| {
        panic!("resolving repository root from {}: {error}", root.display())
    })
}

fn documented_number_before(text: &str, marker: &str, context: &str) -> usize {
    let end = text
        .find(marker)
        .unwrap_or_else(|| panic!("{context}: marker not found: {marker:?}"));
    let before = text[..end].trim_end_matches(|ch: char| ch.is_whitespace() || ch == '*');
    let digits: String = before
        .chars()
        .rev()
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>()
        .chars()
        .rev()
        .collect();
    digits
        .parse()
        .unwrap_or_else(|error| panic!("{context}: no numeric count before {marker:?}: {error}"))
}

#[test]
fn documented_cli_and_mcp_counts_match_sources() {
    let root = repo_root();
    let readme = fs::read_to_string(root.join("README.md"))
        .unwrap_or_else(|error| panic!("reading README.md: {error}"));
    let design = fs::read_to_string(root.join("docs/design.md"))
        .unwrap_or_else(|error| panic!("reading docs/design.md: {error}"));
    let source = fs::read_to_string(root.join("crates/qm-mcp/src/lib.rs"))
        .unwrap_or_else(|error| panic!("reading crates/qm-mcp/src/lib.rs: {error}"));

    let cli_count = qm_cli::Cli::command()
        .get_subcommands()
        .filter(|command| command.get_name() != "help")
        .count();
    let mcp_count = source
        .lines()
        .filter(|line| line.trim_start().starts_with("#[tool("))
        .count();

    assert!(
        cli_count > 0,
        "the CLI must expose top-level business commands"
    );
    assert!(mcp_count > 0, "the MCP source must expose tools");

    for (label, text, marker) in [
        ("README CLI", readme.as_str(), "** 个子命令"),
        ("design §11 CLI", design.as_str(), " 个顶层子命令"),
    ] {
        assert_eq!(
            documented_number_before(text, marker, label),
            cli_count,
            "{label} must match the Clap command count"
        );
    }

    for (label, text, marker) in [
        ("README MCP", readme.as_str(), "** 个（"),
        ("design §6.7 MCP", design.as_str(), " 个 `memory_*` 工具"),
        ("design §11 MCP", design.as_str(), " 个工具，与 CLI"),
    ] {
        assert_eq!(
            documented_number_before(text, marker, label),
            mcp_count,
            "{label} must match the source #[tool( count"
        );
    }
}

fn stderr_of(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn the_real_binary_reads_the_documented_bucket_environment() {
    for (label, env) in [
        ("QM_S3_*", DOCUMENTED_BUCKET_ENV),
        ("the R2_* aliases", DOCUMENTED_R2_ALIASES),
    ] {
        let output = run_qm(&env);
        let stderr = stderr_of(&output);

        // Premise: the process got as far as the command's own validation,
        // which is only reachable once `build_bucket_from_env` handed `main` a
        // client built from the environment above.
        assert!(
            stderr.contains("is not portable"),
            "{label} must be enough to build the client and reach the command's \
             own validation; stderr was:\n{stderr}"
        );
        assert!(
            !stderr.contains("missing "),
            "{label} must be the names the client is built from; stderr was:\n{stderr}"
        );
    }

    // Positive control for the assertion above: drop the bucket name and the
    // same command must fail with exactly the string the check looks for. If
    // this ever stops firing, `!stderr.contains("missing ")` is vacuous.
    let without_bucket: Vec<(&str, &str)> = DOCUMENTED_BUCKET_ENV
        .iter()
        .copied()
        .filter(|(key, _)| *key != "QM_S3_BUCKET")
        .collect();
    let stderr = stderr_of(&run_qm(&without_bucket));
    assert!(
        stderr.contains("missing bucket name") && stderr.contains("QM_S3_BUCKET"),
        "the negative assertion above is only meaningful while this string is \
         reachable; stderr was:\n{stderr}"
    );
}

/// The storage-form switch is read by the real binary, and an unrecognised
/// value stops it before it touches a bucket.
///
/// The bucket environment here is complete but points at a closed port, so the
/// only thing that can produce this failure is the switch itself: `Context::new`
/// resolves it before the command runs, and the command is the first thing that
/// would ever send a request. A build that ignored `QM_MANIFEST_FORMAT` would
/// instead reach the command's own path validation and print `is not portable`,
/// which is the assertion that makes this test discriminating.
#[test]
fn the_real_binary_refuses_a_storage_form_it_does_not_know() {
    let with_bad_form: Vec<(&str, &str)> = DOCUMENTED_BUCKET_ENV
        .iter()
        .copied()
        .chain([("QM_MANIFEST_FORMAT", "3")])
        .collect();
    let stderr = stderr_of(&run_qm(&with_bad_form));
    assert!(
        stderr.contains("QM_MANIFEST_FORMAT") && stderr.contains("\"3\""),
        "the binary has to reject a form it does not know, naming the setting and \
         the value; stderr was:\n{stderr}"
    );
    assert!(
        !stderr.contains("is not portable"),
        "the refusal must happen while the context is built, not after the command \
         ran; stderr was:\n{stderr}"
    );

    // Both documented values are accepted, so the rejection above is about the
    // value and not about the variable existing at all.
    for form in ["1", "2"] {
        let env: Vec<(&str, &str)> = DOCUMENTED_BUCKET_ENV
            .iter()
            .copied()
            .chain([("QM_MANIFEST_FORMAT", form)])
            .collect();
        let stderr = stderr_of(&run_qm(&env));
        assert!(
            stderr.contains("is not portable"),
            "QM_MANIFEST_FORMAT={form} must be accepted and reach the command; \
             stderr was:\n{stderr}"
        );
    }
}

/// A maintain pass with one broken session must still print its complete JSON
/// report on stdout and exit non-zero. This runs the real `qm` binary against a
/// local S3 protocol stub, so the process boundary is part of the evidence.
#[tokio::test]
async fn the_real_binary_exits_nonzero_with_a_maintain_json_report() {
    let stub = S3Stub::start().expect("starting the S3 stub");
    let cache = TempDir::new().unwrap();
    let spool = TempDir::new().unwrap();
    let endpoint = stub.endpoint();
    let env = vec![
        ("QM_S3_ENDPOINT", endpoint.clone()),
        ("QM_S3_BUCKET", stub.bucket().to_string()),
        ("QM_S3_ACCESS_KEY_ID", "stub-access".to_string()),
        ("QM_S3_SECRET_ACCESS_KEY", "stub-secret".to_string()),
        ("QM_S3_FORCE_PATH_STYLE", "true".to_string()),
        ("QM_CACHE_DIR", cache.path().to_string_lossy().to_string()),
        ("QM_SPOOL_DIR", spool.path().to_string_lossy().to_string()),
    ];

    for (session, text) in [
        ("sess-broken", "broken chain must not stop the pass"),
        ("sess-good", "good chain must still become searchable"),
    ] {
        let output = run_qm_args(&["capture", "--session", session, "--text", text], &env);
        assert!(
            output.status.success(),
            "capture for {session} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let bucket = AmazonS3Builder::new()
        .with_bucket_name(stub.bucket())
        .with_region("auto")
        .with_endpoint(stub.endpoint())
        .with_allow_http(true)
        .with_access_key_id("stub-access")
        .with_secret_access_key("stub-secret")
        .with_virtual_hosted_style_request(false)
        .build()
        .unwrap();
    let workspace = WorkspaceId::new("default").unwrap();
    let project = ProjectId::new("default").unwrap();
    let session = SessionId::new("sess-broken").unwrap();
    let key = KeyLayout::new("v1").session_head(&workspace, &project, &session);
    bucket
        .put(
            &object_store::path::Path::from(key),
            b"not-json".to_vec().into(),
        )
        .await
        .unwrap();

    let output = run_qm_args(&["maintain", "--compiler", "rules", "--json"], &env);
    assert!(
        !output.status.success(),
        "a broken session must make the real binary exit non-zero"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let report: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap_or_else(|error| {
        panic!("maintain stdout must stay valid JSON ({error}):\n{stdout}")
    });
    assert_eq!(report["sessions"], 2);
    assert_eq!(report["consolidated"], 1);
    assert_eq!(report["failed"], 1);
    assert_eq!(report["published"], true);
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("maintain completed with failures"),
        "the non-zero path must identify the failed maintain pass: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    stub.shutdown();
}
