//! Turning a session's observations into page text.
//!
//! Two things matter here. First, the compiler is pluggable: the rule-based
//! renderer is the floor that always exists, and an LLM can rewrite the body
//! without changing any of the surrounding protocol. Second, compilation must
//! be idempotent even when the compiler is not: an LLM returns different prose
//! every run, so "has this chain already been compiled?" is answered by a
//! fingerprint of the *chain*, embedded in the page, never by comparing bodies.

use std::future::Future;
use std::pin::Pin;

use anyhow::{Context, Result, bail};
use qm_core::{Observation, SessionId, content_hash};
use serde_json::Value;

/// Marker that carries the compiled-chain fingerprint inside the page.
const FINGERPRINT_PREFIX: &str = "<!-- qm:compiled ";

/// Fingerprint of the observation chain a page was compiled from.
///
/// Covers the ordered observation ids, so appending, reordering, or replacing
/// an observation invalidates it while a replay does not.
#[must_use]
pub fn chain_fingerprint(observations: &[Observation]) -> String {
    let mut material = String::new();
    for observation in observations {
        material.push_str(&observation.observation_id);
        material.push('\n');
    }
    content_hash(material.as_bytes())
}

/// Read the fingerprint a page was compiled from.
#[must_use]
pub fn fingerprint_of(body: &str) -> Option<String> {
    let start = body.find(FINGERPRINT_PREFIX)? + FINGERPRINT_PREFIX.len();
    let rest = &body[start..];
    let end = rest.find(" -->")?;
    Some(rest[..end].trim().to_string())
}

/// Wrap compiled text with the fingerprint that produced it.
#[must_use]
pub fn with_fingerprint(session: &SessionId, fingerprint: &str, body: &str) -> String {
    format!(
        "# Session {session}\n\n{FINGERPRINT_PREFIX}{fingerprint} -->\n\n{}\n",
        body.trim()
    )
}

/// A pluggable session compiler.
pub trait SessionCompiler: Send + Sync {
    /// Short name recorded in the consolidation outcome.
    fn name(&self) -> &'static str {
        "custom"
    }

    /// Produce the page body for a chain. Implementations should be tolerant:
    /// the caller falls back to the rule renderer when this fails.
    fn compile<'a>(
        &'a self,
        session: &'a SessionId,
        observations: &'a [Observation],
    ) -> Pin<Box<dyn Future<Output = Result<String>> + Send + 'a>>;
}

/// The deterministic renderer: always available, never fails.
#[derive(Debug, Default, Clone, Copy)]
pub struct RuleCompiler;

impl SessionCompiler for RuleCompiler {
    fn name(&self) -> &'static str {
        "rules"
    }

    fn compile<'a>(
        &'a self,
        _session: &'a SessionId,
        observations: &'a [Observation],
    ) -> Pin<Box<dyn Future<Output = Result<String>> + Send + 'a>> {
        Box::pin(async move {
            let mut body = String::new();
            for observation in observations {
                body.push_str(&format!(
                    "- [{}] {} ({}) {}\n",
                    observation.created_at_ms,
                    observation.kind,
                    observation.actor,
                    observation.text
                ));
            }
            Ok(body)
        })
    }
}

/// An OpenAI-compatible chat completion compiler.
#[derive(Debug, Clone)]
pub struct OpenAiCompatCompiler {
    base_url: String,
    api_key: String,
    model: String,
    client: reqwest::Client,
}

/// Instruction given to the model. Kept tight on purpose: the renderer, not the
/// model, decides the surrounding page shape.
const COMPILER_SYSTEM_PROMPT: &str = "You compile an agent session into one durable memory page. \
Return markdown only, no preamble. Keep decisions, gotchas, file paths, commands and \
open questions; drop small talk and repeated noise. Never invent facts that are not in \
the observations. Be concise.";

impl OpenAiCompatCompiler {
    /// Build a compiler for an OpenAI-compatible endpoint.
    ///
    /// # Errors
    /// Fails when any of the three settings is blank.
    pub fn new(base_url: &str, api_key: &str, model: &str) -> Result<Self> {
        if base_url.trim().is_empty() || api_key.trim().is_empty() || model.trim().is_empty() {
            bail!("llm compiler needs a base url, an api key and a model");
        }
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key: api_key.to_string(),
            model: model.to_string(),
            client: reqwest::Client::new(),
        })
    }

    /// Build from `QM_LLM_*`, or `None` when the endpoint is not configured.
    ///
    /// # Errors
    /// Fails when a partial configuration is present.
    pub fn from_env() -> Result<Option<Self>> {
        let base_url = std::env::var("QM_LLM_BASE_URL").unwrap_or_default();
        if base_url.trim().is_empty() {
            return Ok(None);
        }
        let api_key = std::env::var("QM_LLM_API_KEY")
            .context("QM_LLM_BASE_URL is set but QM_LLM_API_KEY is not")?;
        let model = std::env::var("QM_LLM_MODEL")
            .context("QM_LLM_BASE_URL is set but QM_LLM_MODEL is not")?;
        Self::new(&base_url, &api_key, &model).map(Some)
    }

    /// Render the observation chain into the user message.
    fn prompt(observations: &[Observation]) -> String {
        let mut prompt = String::from("Compile these session observations into one page:\n\n");
        for observation in observations {
            prompt.push_str(&format!(
                "[{}] {} ({}): {}\n",
                observation.created_at_ms, observation.kind, observation.actor, observation.text
            ));
        }
        prompt
    }
}

/// Extract the assistant text from an OpenAI-compatible response.
///
/// # Errors
/// Fails when the response has no usable content, which the caller treats as a
/// compile failure and falls back from.
pub fn parse_chat_completion(response: &Value) -> Result<String> {
    let content = response
        .get("choices")
        .and_then(|choices| choices.get(0))
        .and_then(|choice| choice.get("message"))
        .and_then(|message| message.get("content"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|content| !content.is_empty())
        .context("chat completion had no content")?;
    Ok(content.to_string())
}

impl SessionCompiler for OpenAiCompatCompiler {
    fn name(&self) -> &'static str {
        "llm"
    }

    fn compile<'a>(
        &'a self,
        _session: &'a SessionId,
        observations: &'a [Observation],
    ) -> Pin<Box<dyn Future<Output = Result<String>> + Send + 'a>> {
        Box::pin(async move {
            let body = serde_json::json!({
                "model": self.model,
                "temperature": 0,
                "messages": [
                    {"role": "system", "content": COMPILER_SYSTEM_PROMPT},
                    {"role": "user", "content": Self::prompt(observations)},
                ],
            });
            let response = self
                .client
                .post(format!("{}/chat/completions", self.base_url))
                .bearer_auth(&self.api_key)
                .json(&body)
                .send()
                .await
                .context("calling the llm endpoint")?;
            let status = response.status();
            let payload: Value = response
                .json()
                .await
                .context("the llm endpoint did not return JSON")?;
            if !status.is_success() {
                bail!("llm endpoint returned {status}: {payload}");
            }
            parse_chat_completion(&payload)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use qm_core::{MANIFEST_SCHEMA, derive_observation_id};

    fn observation(session: &SessionId, text: &str, at: i64) -> Observation {
        Observation {
            schema: MANIFEST_SCHEMA,
            observation_id: derive_observation_id(session, "codex", "tool_use", text, at),
            session_id: session.clone(),
            actor: "codex".into(),
            kind: "tool_use".into(),
            text: text.into(),
            created_at_ms: at,
        }
    }

    #[test]
    fn fingerprints_track_the_chain_and_round_trip_through_the_page() {
        let session = SessionId::new("sess-1").unwrap();
        let first = vec![observation(&session, "one", 1)];
        let second = vec![
            observation(&session, "one", 1),
            observation(&session, "two", 2),
        ];

        let fingerprint = chain_fingerprint(&first);
        assert_eq!(fingerprint, chain_fingerprint(&first), "replays must match");
        assert_ne!(fingerprint, chain_fingerprint(&second));

        let page = with_fingerprint(&session, &fingerprint, "- did the thing");
        assert_eq!(fingerprint_of(&page).as_deref(), Some(fingerprint.as_str()));
        assert!(fingerprint_of("# Session x\n\nno marker here").is_none());
    }

    #[test]
    fn the_rule_compiler_renders_every_observation() {
        let session = SessionId::new("sess-1").unwrap();
        let observations = vec![
            observation(&session, "switched to tantivy splits", 1),
            observation(&session, "ran the suite", 2),
        ];
        let body =
            futures::executor::block_on(RuleCompiler.compile(&session, &observations)).unwrap();
        assert!(body.contains("switched to tantivy splits"));
        assert!(body.contains("ran the suite"));
        assert!(body.contains("tool_use"));
    }

    #[test]
    fn chat_completions_are_parsed_and_bad_shapes_rejected() {
        let good = serde_json::json!({
            "choices": [{"message": {"content": "  # Memory  "}}]
        });
        assert_eq!(parse_chat_completion(&good).unwrap(), "# Memory");
        assert!(parse_chat_completion(&serde_json::json!({"choices": []})).is_err());
        assert!(
            parse_chat_completion(&serde_json::json!({
                "choices": [{"message": {"content": "   "}}]
            }))
            .is_err()
        );
    }

    /// A real HTTP round trip against a stub endpoint: proves the request shape
    /// and the response parsing without any external service.
    #[tokio::test]
    async fn the_llm_compiler_speaks_to_an_openai_compatible_endpoint() {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = vec![0u8; 8192];
            let read = stream.read(&mut request).unwrap();
            let request = String::from_utf8_lossy(&request[..read]).to_string();
            assert!(request.starts_with("POST /chat/completions"), "{request}");
            assert!(
                request.contains("authorization: Bearer test-key"),
                "{request}"
            );
            assert!(request.contains("switched to tantivy splits"), "{request}");
            let body = r##"{"choices":[{"message":{"content":"# Session\n\n- used tantivy"}}]}"##;
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).unwrap();
        });

        let compiler =
            OpenAiCompatCompiler::new(&format!("http://{address}"), "test-key", "test-model")
                .unwrap();
        let session = SessionId::new("sess-1").unwrap();
        let observations = vec![observation(&session, "switched to tantivy splits", 1)];
        let body = compiler.compile(&session, &observations).await.unwrap();
        assert!(body.contains("used tantivy"), "{body}");
        server.join().unwrap();
    }
}
