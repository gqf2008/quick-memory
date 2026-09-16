//! Conformance: the **real** `qm` binary reads the machine-local config file.
//!
//! `crates/qm-cli/src/config.rs` has unit tests for the parser, the path rules
//! and the precedence decision. On their own those stay green if `main` never
//! calls the loader — so every test here starts the actual binary with a
//! cleared environment and looks at what it does, which is the only thing that
//! can say whether an operator's `~/.quick-memory/env` reaches the bucket
//! client.
//!
//! `HOME` is always a temporary directory, and `QM_CONFIG_FILE` always points
//! inside one: the developer's own config file must not be able to make these
//! tests pass or fail.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use qm_probe::s3_stub::S3Stub;
use tempfile::TempDir;

/// Run the real binary with a cleared environment.
fn run_qm(home: &Path, args: &[&str], env: &[(&str, &str)]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_qm"));
    command.env_clear().env("HOME", home).args(args);
    for (key, value) in env {
        command.env(key, value);
    }
    command.output().expect("running the qm binary")
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// Write `~/.quick-memory/env` for a temporary home and return its path.
fn write_default_config(home: &Path, contents: &str) -> PathBuf {
    let dir = home.join(".quick-memory");
    fs::create_dir_all(&dir).expect("creating the config directory");
    let path = dir.join("env");
    write_file(&path, contents);
    path
}

/// Write a config file with the mode the tests want to exercise.
fn write_file(path: &Path, contents: &str) {
    fs::write(path, contents).expect("writing the config file");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).expect("tightening the mode");
    }
}

/// A named file: what `QM_CONFIG_FILE` points at in most of these tests.
fn write_named_config(home: &Path, contents: &str) -> PathBuf {
    let path = home.join("named.env");
    write_file(&path, contents);
    path
}

fn stub_settings(stub: &S3Stub) -> String {
    format!(
        "QM_S3_ENDPOINT=\"{}\"\n\
         QM_S3_BUCKET=\"{}\"\n\
         QM_S3_ACCESS_KEY_ID=\"stub-access\"\n\
         QM_S3_SECRET_ACCESS_KEY=\"stub-secret\"\n\
         QM_S3_FORCE_PATH_STYLE=\"true\"\n",
        stub.endpoint(),
        stub.bucket()
    )
}

#[test]
fn the_named_file_alone_supplies_the_endpoint() {
    // The endpoint is the whole setting: if the file did not supply it, the run
    // would stop at "missing S3 endpoint" and never look for a bucket.
    let home = TempDir::new().unwrap();
    let path = write_named_config(home.path(), "QM_S3_ENDPOINT=\"http://127.0.0.1:1\"\n");

    let output = run_qm(
        home.path(),
        &["status", "--json"],
        &[("QM_CONFIG_FILE", path.to_str().unwrap())],
    );
    let stderr = stderr(&output);

    assert!(
        stderr.contains("missing bucket name"),
        "the endpoint came from the file, so the next missing setting is the bucket: {stderr}"
    );
    assert!(
        !stderr.contains("missing S3 endpoint"),
        "the file supplied the endpoint: {stderr}"
    );
}

#[test]
fn the_default_path_is_loaded_without_naming_it() {
    let home = TempDir::new().unwrap();
    write_default_config(home.path(), "QM_S3_ENDPOINT=\"http://127.0.0.1:1\"\n");

    let output = run_qm(home.path(), &["status", "--json"], &[]);
    let stderr = stderr(&output);

    assert!(
        stderr.contains("missing bucket name") && !stderr.contains("missing S3 endpoint"),
        "~/.quick-memory/env is the documented default: {stderr}"
    );
}

#[test]
fn without_any_file_the_endpoint_is_still_required() {
    // The negative control for the two tests above: an empty home must not
    // quietly produce a working configuration out of nothing.
    let home = TempDir::new().unwrap();
    let output = run_qm(home.path(), &["status", "--json"], &[]);

    assert!(
        stderr(&output).contains("missing S3 endpoint"),
        "no file and no environment has to fail the way it always did: {}",
        stderr(&output)
    );
    assert!(!output.status.success());
}

#[test]
fn a_named_file_that_is_not_there_is_an_error_that_names_it() {
    // Pointing at a specific file is a decision; silently falling back to the
    // environment would hide the typo in exactly the case the operator was
    // being explicit about.
    let home = TempDir::new().unwrap();
    let missing = home.path().join("not-here.env");
    let output = run_qm(
        home.path(),
        &["status", "--json"],
        &[("QM_CONFIG_FILE", missing.to_str().unwrap())],
    );

    assert!(!output.status.success());
    assert!(
        stderr(&output).contains("not-here.env"),
        "the error has to name the path: {}",
        stderr(&output)
    );
}

#[test]
fn the_file_reaches_the_bucket_client_not_just_the_scope() {
    // Endpoint *and* bucket both come from the file: the failure is a
    // connection to the endpoint the file named, inside the bucket it named.
    let home = TempDir::new().unwrap();
    let path = write_named_config(
        home.path(),
        "QM_S3_ENDPOINT=\"http://127.0.0.1:1\"\n\
         QM_S3_BUCKET=\"bucket-from-file\"\n\
         QM_S3_ACCESS_KEY_ID=\"key-from-file\"\n\
         QM_S3_SECRET_ACCESS_KEY=\"secret-from-file\"\n",
    );

    let output = run_qm(
        home.path(),
        &["status", "--json"],
        &[("QM_CONFIG_FILE", path.to_str().unwrap())],
    );
    let stderr = stderr(&output);

    assert!(!output.status.success(), "a closed port cannot answer");
    assert!(
        stderr.contains("127.0.0.1:1"),
        "the request went to the endpoint the file named: {stderr}"
    );
    assert!(
        !stderr.contains("missing S3 endpoint") && !stderr.contains("missing bucket name"),
        "the file supplied both: {stderr}"
    );
}

#[test]
fn an_environment_variable_beats_the_same_key_in_the_file() {
    // Precedence, observed the way an operator would notice it going wrong: a
    // one-off `QM_S3_ENDPOINT=... qm status` has to reach *that* endpoint.
    let home = TempDir::new().unwrap();
    let path = write_named_config(
        home.path(),
        "QM_S3_ENDPOINT=\"http://127.0.0.1:1\"\n\
         QM_S3_BUCKET=\"bucket-from-file\"\n\
         QM_S3_ACCESS_KEY_ID=\"key-from-file\"\n\
         QM_S3_SECRET_ACCESS_KEY=\"secret-from-file\"\n",
    );

    let output = run_qm(
        home.path(),
        &["status", "--json"],
        &[
            ("QM_CONFIG_FILE", path.to_str().unwrap()),
            ("QM_S3_ENDPOINT", "http://127.0.0.1:2"),
        ],
    );
    let stderr = stderr(&output);

    assert!(
        stderr.contains("127.0.0.1:2") && !stderr.contains("127.0.0.1:1"),
        "the environment wins, so the request went to :2: {stderr}"
    );
}

#[tokio::test]
async fn the_flag_beats_the_file_which_beats_the_default() {
    // The scope settings carry `env = ...` in clap, so clap has already filled
    // them before the file is merged: this is the test that says the merge
    // still lands, and lands in the documented order.
    let stub = S3Stub::start().expect("starting the S3 stub");
    let home = TempDir::new().unwrap();
    let path = write_named_config(
        home.path(),
        &format!(
            "{}QM_WORKSPACE=\"workspace-from-file\"\nQM_PROJECT=\"project-from-file\"\n",
            stub_settings(&stub)
        ),
    );
    let config = ("QM_CONFIG_FILE", path.to_str().unwrap());

    let from_file = run_qm(home.path(), &["status", "--json"], &[config]);
    assert!(
        from_file.status.success(),
        "status against the stub: {}",
        stderr(&from_file)
    );
    assert!(
        stdout(&from_file).contains("\"workspace\":\"workspace-from-file\"")
            && stdout(&from_file).contains("\"project\":\"project-from-file\""),
        "the file supplies the scope: {}",
        stdout(&from_file)
    );

    let from_flag = run_qm(
        home.path(),
        &["status", "--json", "--workspace", "workspace-from-flag"],
        &[config],
    );
    assert!(
        stdout(&from_flag).contains("\"workspace\":\"workspace-from-flag\""),
        "a flag the operator typed wins: {}",
        stdout(&from_flag)
    );

    let from_env = run_qm(
        home.path(),
        &["status", "--json"],
        &[config, ("QM_WORKSPACE", "workspace-from-env")],
    );
    assert!(
        stdout(&from_env).contains("\"workspace\":\"workspace-from-env\""),
        "the environment beats the file here too: {}",
        stdout(&from_env)
    );
}

#[test]
fn a_config_file_other_accounts_can_read_is_reported() {
    let home = TempDir::new().unwrap();
    let path = write_named_config(home.path(), "QM_S3_ENDPOINT=\"http://127.0.0.1:1\"\n");

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        let wide = run_qm(
            home.path(),
            &["status", "--json"],
            &[("QM_CONFIG_FILE", path.to_str().unwrap())],
        );
        assert!(
            stderr(&wide).contains("chmod 600"),
            "a world-readable credentials file has to be called out: {}",
            stderr(&wide)
        );

        // The other direction: the normal mode must stay quiet, or the warning
        // is noise an operator learns to ignore.
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let tight = run_qm(
            home.path(),
            &["status", "--json"],
            &[("QM_CONFIG_FILE", path.to_str().unwrap())],
        );
        assert!(
            !stderr(&tight).contains("chmod 600"),
            "0600 is the documented mode and must not warn: {}",
            stderr(&tight)
        );
    }
}

#[test]
fn a_setting_the_file_cannot_serve_is_called_out() {
    // `QM_EMBEDDING_*` is read by the search crate from the process
    // environment. Accepting it here without a word would be a setting that
    // looks like it took effect and did not.
    let home = TempDir::new().unwrap();
    let path = write_named_config(
        home.path(),
        "QM_S3_ENDPOINT=\"http://127.0.0.1:1\"\n\
         QM_EMBEDDING_BASE_URL=\"http://127.0.0.1:9\"\n",
    );

    let output = run_qm(
        home.path(),
        &["status", "--json"],
        &[("QM_CONFIG_FILE", path.to_str().unwrap())],
    );

    assert!(
        stderr(&output).contains("does not serve QM_EMBEDDING_BASE_URL"),
        "the file has to say out loud what it is not honoring: {}",
        stderr(&output)
    );
}
