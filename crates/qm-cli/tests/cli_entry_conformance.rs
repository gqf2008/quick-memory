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

use std::process::{Command, Output};

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
