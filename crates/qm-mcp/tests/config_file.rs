//! Conformance: the **real** `qm-mcp` binary reads the machine-local config
//! file.
//!
//! An MCP client that passes no environment at all is a normal way to start
//! this server, so the file has to be enough on its own. These tests start the
//! actual binary with a cleared environment; `HOME` is a temporary directory so
//! a developer's own `~/.quick-memory/env` cannot influence the result.

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use tempfile::TempDir;

/// A server started with a cleared environment, plus its stdin/stdout.
struct Server {
    child: Child,
    stdin: std::process::ChildStdin,
    stdout: BufReader<std::process::ChildStdout>,
}

impl Server {
    fn start(home: &Path, env: &[(&str, &str)]) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_qm-mcp"));
        command
            .env_clear()
            .env("HOME", home)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        for (key, value) in env {
            command.env(key, value);
        }
        let mut child = command.spawn().expect("spawning qm-mcp");
        let stdin = child.stdin.take().expect("stdin");
        let stdout = BufReader::new(child.stdout.take().expect("stdout"));
        Self {
            child,
            stdin,
            stdout,
        }
    }

    /// Send `initialize` and return the server's answer.
    fn handshake(&mut self) -> serde_json::Value {
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "qm-config-test", "version": "0.0.1"}
            }
        });
        writeln!(self.stdin, "{request}").expect("writing initialize");
        self.stdin.flush().expect("flushing initialize");

        let mut line = String::new();
        self.stdout
            .read_line(&mut line)
            .expect("reading the answer");
        assert!(!line.trim().is_empty(), "the server closed the stream");
        serde_json::from_str(&line).unwrap_or_else(|error| panic!("bad JSON {error}: {line}"))
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn write_config(home: &Path, contents: &str) -> PathBuf {
    let path = home.join("env");
    fs::write(&path, contents).expect("writing the config file");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).expect("tightening the mode");
    }
    path
}

#[test]
fn the_server_starts_from_the_file_alone() {
    // The bucket settings exist only in the file. If the file were ignored, the
    // server would exit at startup with "missing S3 endpoint" and never answer
    // the handshake — so answering it is the evidence.
    let home = TempDir::new().unwrap();
    let path = write_config(
        home.path(),
        "QM_S3_ENDPOINT=\"http://127.0.0.1:1\"\n\
         QM_S3_BUCKET=\"bucket-from-file\"\n\
         QM_S3_ACCESS_KEY_ID=\"key-from-file\"\n\
         QM_S3_SECRET_ACCESS_KEY=\"secret-from-file\"\n\
         QM_WORKSPACE=\"workspace-from-file\"\n",
    );

    let mut server = Server::start(home.path(), &[("QM_CONFIG_FILE", path.to_str().unwrap())]);
    let response = server.handshake();

    assert_eq!(response["id"], 1, "{response}");
    assert_eq!(
        response["result"]["serverInfo"]["name"], "quick-memory",
        "the server came up, so the file supplied the bucket: {response}"
    );
}

#[test]
fn without_the_file_the_missing_endpoint_is_loud() {
    // The negative control: no file, no environment, no scope default can
    // substitute for a bucket that was never named.
    let home = TempDir::new().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_qm-mcp"))
        .env_clear()
        .env("HOME", home.path())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("running qm-mcp");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "the server cannot serve a bucket it was not told about"
    );
    assert!(
        stderr.contains("missing S3 endpoint"),
        "and it has to say so rather than start anyway: {stderr}"
    );
}
