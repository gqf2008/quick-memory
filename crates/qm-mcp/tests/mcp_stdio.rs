//! Protocol-level check: speak MCP to the real binary over stdio.
//!
//! This is the only test that exercises the wire format rather than a Rust
//! call, so it catches transport and handshake mistakes that unit tests cannot.
//! It runs against `--synthetic-bucket`, which is explicitly not a backend:
//! the real one is verified by the probes, with credentials.

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

struct Client {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl Client {
    fn start() -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_qm-mcp"))
            .arg("--synthetic-bucket")
            .env("QM_WORKSPACE", "acme")
            .env("QM_PROJECT", "ai-memory")
            .env("QM_WRITER", "test-machine")
            .env(
                "QM_CACHE_DIR",
                std::env::temp_dir().join(format!("qm-mcp-test-{}", std::process::id())),
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawning qm-mcp");
        let stdin = child.stdin.take().expect("stdin");
        let stdout = BufReader::new(child.stdout.take().expect("stdout"));
        Self {
            child,
            stdin,
            stdout,
        }
    }

    fn send(&mut self, message: serde_json::Value) {
        let line = serde_json::to_string(&message).expect("serialize");
        writeln!(self.stdin, "{line}").expect("write");
        self.stdin.flush().expect("flush");
    }

    fn recv(&mut self) -> serde_json::Value {
        let mut line = String::new();
        self.stdout.read_line(&mut line).expect("read");
        assert!(!line.trim().is_empty(), "server closed the stream");
        serde_json::from_str(&line).unwrap_or_else(|error| panic!("bad JSON {error}: {line}"))
    }

    /// `initialize` + `initialized`, as a client must do before any call.
    fn handshake(&mut self) {
        self.send(serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "qm-test", "version": "0.0.1"}
            }
        }));
        let response = self.recv();
        assert_eq!(response["id"], 1, "{response}");
        assert_eq!(
            response["result"]["serverInfo"]["name"], "quick-memory",
            "{response}"
        );
        self.send(serde_json::json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized"
        }));
    }

    fn call_tool(
        &mut self,
        id: i64,
        name: &str,
        arguments: serde_json::Value,
    ) -> serde_json::Value {
        let response = self.call_tool_raw(id, name, arguments);
        assert_eq!(response["id"], id, "{response}");
        assert!(response["error"].is_null(), "{response}");
        response
    }

    /// Same call, but tolerating a JSON-RPC error so failure paths are testable.
    fn call_tool_raw(
        &mut self,
        id: i64,
        name: &str,
        arguments: serde_json::Value,
    ) -> serde_json::Value {
        self.send(serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments}
        }));
        self.recv()
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn tool_text(response: &serde_json::Value) -> String {
    response["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("no text content: {response}"))
        .to_string()
}

#[test]
fn mcp_handshake_lists_tools_and_runs_a_capture_search_round_trip() {
    let mut client = Client::start();
    client.handshake();

    client.send(serde_json::json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/list",
        "params": {}
    }));
    let listed = client.recv();
    let names: Vec<String> = listed["result"]["tools"]
        .as_array()
        .expect("tools array")
        .iter()
        .filter_map(|tool| tool["name"].as_str().map(ToString::to_string))
        .collect();
    for expected in [
        "memory_capture",
        "memory_consolidate",
        "memory_search",
        "memory_write_page",
        "memory_read_page",
        "memory_delete_page",
        "memory_publish",
        "memory_compact",
        "memory_sessions",
        "memory_status",
        "memory_history",
        "memory_restore",
        "memory_log",
        "memory_compact_session",
        "memory_handoff_open",
        "memory_handoff_list",
        "memory_handoff_claim",
        "memory_handoff_done",
    ] {
        assert!(
            names.contains(&expected.to_string()),
            "missing {expected}: {names:?}"
        );
    }
    // Every tool must be described well enough for an agent to choose it.
    for tool in listed["result"]["tools"].as_array().unwrap() {
        assert!(
            tool["description"].as_str().is_some_and(|d| !d.is_empty()),
            "tool without description: {tool}"
        );
        assert!(
            tool["inputSchema"].is_object(),
            "tool without schema: {tool}"
        );
    }

    // Capture → consolidate → publish → search, over the wire.
    let captured = client.call_tool(
        3,
        "memory_capture",
        serde_json::json!({
            "session": "sess-mcp",
            "text": "switched the index to tantivy splits"
        }),
    );
    let text = tool_text(&captured);
    assert!(
        text.contains("captured") || text.contains("already"),
        "{text}"
    );

    let _ = client.call_tool(
        4,
        "memory_consolidate",
        serde_json::json!({"session": "sess-mcp"}),
    );
    let _ = client.call_tool(5, "memory_publish", serde_json::json!({}));

    let searched = client.call_tool(
        6,
        "memory_search",
        serde_json::json!({"query": "tantivy", "limit": 5}),
    );
    let text = tool_text(&searched);
    assert!(text.contains("sessions/sess-mcp.md"), "{text}");

    // Handoffs: open, list, claim, and refuse a second claim — over the wire.
    let opened = client.call_tool(
        8,
        "memory_handoff_open",
        serde_json::json!({
            "title": "finish the rebuild",
            "body": "compaction is pending"
        }),
    );
    // MCP tools answer in JSON, so an agent can act on the result directly.
    let opened_text = tool_text(&opened);
    let opened_json: serde_json::Value = serde_json::from_str(&opened_text)
        .unwrap_or_else(|error| panic!("open must be JSON: {error}: {opened_text}"));
    assert_eq!(opened_json["title"], "finish the rebuild", "{opened_json}");
    assert_eq!(opened_json["claimed_by"], serde_json::Value::Null);
    let opened_id = opened_json["id"].as_str().expect("handoff id").to_string();

    let listed = client.call_tool(
        9,
        "memory_handoff_list",
        serde_json::json!({"state": "all"}),
    );
    let listed_text = tool_text(&listed);
    let handoffs: serde_json::Value = serde_json::from_str(&listed_text)
        .unwrap_or_else(|error| panic!("list must be JSON: {error}: {listed_text}"));
    let id = handoffs[0]["id"].as_str().expect("handoff id").to_string();
    assert_eq!(
        id, opened_id,
        "the listed handoff must be the one just opened"
    );

    client.call_tool(10, "memory_handoff_claim", serde_json::json!({"id": id}));
    let second_claim =
        client.call_tool_raw(11, "memory_handoff_claim", serde_json::json!({"id": id}));
    assert!(
        !second_claim["error"].is_null(),
        "a second claim must fail: {second_claim}"
    );
    client.call_tool(12, "memory_handoff_done", serde_json::json!({"id": id}));

    // Global search reaches every project in the workspace.
    let global = client.call_tool(
        20,
        "memory_search",
        serde_json::json!({"query": "tantivy", "global": true}),
    );
    let global_text = tool_text(&global);
    assert!(
        global_text.contains("\"project_id\""),
        "global hits must carry provenance: {global_text}"
    );

    // Session retention: a dry run first, then applied.
    let plan = client.call_tool(
        18,
        "memory_compact_session",
        serde_json::json!({"session": "sess-mcp", "keep_last": 1, "keep_ms": 0}),
    );
    let plan_text = tool_text(&plan);
    let plan_json: serde_json::Value = serde_json::from_str(&plan_text)
        .unwrap_or_else(|error| panic!("dry run must be JSON: {error}: {plan_text}"));
    assert_eq!(plan_json["applied"], false, "{plan_json}");
    let applied = client.call_tool(
        19,
        "memory_compact_session",
        serde_json::json!({
            "session": "sess-mcp",
            "keep_last": 1,
            "keep_ms": 0,
            "apply": true
        }),
    );
    let applied_text = tool_text(&applied);
    assert!(
        applied_text.contains("segments") || applied_text.contains("applied"),
        "{applied_text}"
    );

    // History and restore over the wire: the older body comes back as a new
    // version rather than overwriting anything.
    let written = client.call_tool(
        13,
        "memory_write_page",
        serde_json::json!({"path": "notes/history.md", "body": "first body"}),
    );
    let _ = written;
    let _ = client.call_tool(
        14,
        "memory_write_page",
        serde_json::json!({"path": "notes/history.md", "body": "second body"}),
    );
    let history = client.call_tool(
        15,
        "memory_history",
        serde_json::json!({"path": "notes/history.md"}),
    );
    let history_text = tool_text(&history);
    let versions: serde_json::Value = serde_json::from_str(&history_text)
        .unwrap_or_else(|error| panic!("history must be JSON: {error}: {history_text}"));
    assert_eq!(versions.as_array().map(Vec::len), Some(2), "{versions}");
    let oldest = versions[0]["page_id"]
        .as_str()
        .expect("version id")
        .to_string();

    let restored = client.call_tool(
        16,
        "memory_restore",
        serde_json::json!({"path": "notes/history.md", "version": oldest}),
    );
    assert!(
        tool_text(&restored).contains("restored_from"),
        "{}",
        tool_text(&restored)
    );
    let current = client.call_tool(
        17,
        "memory_read_page",
        serde_json::json!({"path": "notes/history.md"}),
    );
    assert!(
        tool_text(&current).contains("first body"),
        "{}",
        tool_text(&current)
    );

    // A tool that fails must report the failure rather than invent success.
    let bad = client.call_tool_raw(
        7,
        "memory_read_page",
        serde_json::json!({"path": "../escape"}),
    );
    assert!(
        !bad["error"].is_null() || bad["result"]["isError"] == true,
        "an invalid path must not look like a success: {bad}"
    );
    assert!(
        bad["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("portable")),
        "the error must explain the rejection: {bad}"
    );
}
