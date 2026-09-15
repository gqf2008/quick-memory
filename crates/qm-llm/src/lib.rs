//! Pluggable embedding providers: the boundary a vector search stream will
//! later build on, and nothing more. This crate deliberately does **not** touch
//! the index or the read path yet — it fixes the shape of an embedder and, more
//! importantly, the **failure modes** a provider must never soften.
//!
//! Two providers ship here:
//!
//! - [`DeterministicEmbedder`]: a hash-derived vector. Same text in, same
//!   numbers out, on any machine and any run — which is what makes it usable as
//!   a test double for a *derived* index that must be reproducible.
//! - [`OpenAiCompatEmbedder`]: an OpenAI-compatible `POST {base}/embeddings`
//!   endpoint, configured from `QM_EMBEDDING_*`.
//!
//! Both are **fail closed**. A response whose vector count or width disagrees
//! with the request is an error, never a truncated or zero-padded result: a
//! vector whose dimensions do not line up would corrupt every later distance
//! comparison, and a derived index that silently repairs its input is worse
//! than one that fails loudly. See [`parse_embeddings`] for the checks.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Upper bound on one OpenAI-compatible embedding request.
///
/// This is a liveness boundary, not a latency target: without it a provider
/// that accepts a connection and never answers would park the read path (and
/// any publish sharing it) indefinitely. The value is deliberately generous so
/// a healthy slow request is not mistaken for a dead one, while still bounding
/// the wait when the provider is actually stuck.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

// Environment names for the OpenAI-compatible provider. Kept as named
// constants so the missing/partial-configuration tests refer to them, not to
// string literals that could drift from the implementation.
const ENV_BASE_URL: &str = "QM_EMBEDDING_BASE_URL";
const ENV_API_KEY: &str = "QM_EMBEDDING_API_KEY";
const ENV_MODEL: &str = "QM_EMBEDDING_MODEL";

/// The boxed, `Send` future an [`Embedder`] returns. Named so the trait method
/// stays readable; it is exactly
/// `Pin<Box<dyn Future<Output = Result<Vec<Vec<f32>>>> + Send + 'a>>`.
pub type EmbedFuture<'a> = Pin<Box<dyn Future<Output = Result<Vec<Vec<f32>>>> + Send + 'a>>;

/// Which embedder produced a vector: provider family, model and width.
///
/// A split's vectors are only comparable to a query vector produced by the
/// *same* embedder. Width is the coarse half of that check (a mismatch is
/// caught when two vectors are compared), but two models of equal width
/// produce unrelated coordinates, so the identity travels with the vectors
/// and is compared before they are used. Deliberately serde-free: the crate is
/// a provider boundary, and how a split *records* this belongs to the search
/// crate that owns the on-disk split format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbedderIdentity {
    /// Provider family, e.g. `openai-compatible` or `deterministic`.
    pub provider: String,
    /// Model name as the provider knows it.
    pub model: String,
    /// Width of every vector the embedder produces.
    pub dim: usize,
}

/// A provider that turns text into fixed-width vectors.
///
/// `dyn`-safe on purpose: the read path will hold a `Box<dyn Embedder>` chosen
/// at startup, so the trait must not carry generic methods or `Self` returns.
pub trait Embedder: Send + Sync {
    /// Width of every vector this embedder returns.
    fn dim(&self) -> usize;

    /// Who this embedder is: provider family, model and width.
    ///
    /// Required rather than defaulted on purpose. A default would let a new
    /// implementation claim an identity it did not earn, and the whole point
    /// of recording provenance is that a mismatch is visible instead of
    /// silently comparing vectors from two different models.
    fn identity(&self) -> EmbedderIdentity;

    /// Embed a batch. The returned vector must have exactly one row per input,
    /// each row exactly [`Embedder::dim`] wide.
    fn embed<'a>(&'a self, texts: &'a [String]) -> EmbedFuture<'a>;
}

/// A reproducible, dependency-free embedder for tests and offline fallback.
///
/// Vectors are derived from the text with SHA-256 in counter mode, so the same
/// text maps to the same numbers on every machine and every run. This is a
/// *stable* embedding, not a *semantic* one: it proves the plumbing without
/// pretending to model meaning.
#[derive(Debug, Clone, Copy)]
pub struct DeterministicEmbedder {
    dim: usize,
}

impl DeterministicEmbedder {
    /// Build an embedder that produces `dim`-wide vectors.
    #[must_use]
    pub fn new(dim: usize) -> Self {
        Self { dim }
    }
}

impl Embedder for DeterministicEmbedder {
    fn dim(&self) -> usize {
        self.dim
    }

    fn identity(&self) -> EmbedderIdentity {
        EmbedderIdentity {
            provider: "deterministic".to_string(),
            // The model is the expansion scheme, not a service model name: any
            // change to `hash_vector` changes every stored vector and must be
            // recorded as a different model.
            model: "sha256-v1".to_string(),
            dim: self.dim,
        }
    }

    fn embed<'a>(&'a self, texts: &'a [String]) -> EmbedFuture<'a> {
        Box::pin(async move {
            Ok(texts
                .iter()
                .map(|text| hash_vector(text, self.dim))
                .collect())
        })
    }
}

/// Counter-mode hash expansion: one SHA-256 block per dimension, mapped onto
/// `[-1, 1]`. Seeding each block with the text keeps the whole vector a
/// function of the input, and the explicit index keeps dimensions independent.
fn hash_vector(text: &str, dim: usize) -> Vec<f32> {
    (0..dim)
        .map(|index| {
            let mut hasher = Sha256::new();
            hasher.update(text.as_bytes());
            hasher.update((index as u64).to_le_bytes());
            let digest = hasher.finalize();
            let raw = u32::from_le_bytes([digest[0], digest[1], digest[2], digest[3]]);
            // Map the full u32 range onto [-1, 1] without narrowing bias.
            let unit = f64::from(raw) / f64::from(u32::MAX) * 2.0 - 1.0;
            unit as f32
        })
        .collect()
}

/// An embedder backed by an OpenAI-compatible `POST {base}/embeddings`.
pub struct OpenAiCompatEmbedder {
    base_url: String,
    api_key: String,
    model: String,
    dim: usize,
    client: reqwest::Client,
}

impl OpenAiCompatEmbedder {
    /// Build an embedder for an OpenAI-compatible endpoint.
    ///
    /// # Errors
    /// Fails when any setting is blank, or when `dim` is zero — a zero-width
    /// vector cannot be compared, so it is a configuration error rather than a
    /// degenerate-but-valid embedder.
    pub fn new(base_url: &str, api_key: &str, model: &str, dim: usize) -> Result<Self> {
        Self::with_timeout(base_url, api_key, model, dim, REQUEST_TIMEOUT)
    }

    /// Build an embedder with an explicit request timeout.
    ///
    /// Private so the production timeout stays one policy rather than a knob
    /// every caller has to remember; the timeout test uses it to keep the
    /// stalled-provider control fast and deterministic.
    fn with_timeout(
        base_url: &str,
        api_key: &str,
        model: &str,
        dim: usize,
        request_timeout: Duration,
    ) -> Result<Self> {
        if base_url.trim().is_empty() || api_key.trim().is_empty() || model.trim().is_empty() {
            bail!("embedding provider needs a base url, an api key and a model");
        }
        if dim == 0 {
            bail!("embedding provider needs a non-zero dimension");
        }
        if request_timeout.is_zero() {
            bail!("embedding provider needs a non-zero request timeout");
        }
        let client = reqwest::Client::builder()
            .timeout(request_timeout)
            .build()
            .context("building the embedding HTTP client")?;
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key: api_key.to_string(),
            model: model.to_string(),
            dim,
            client,
        })
    }

    /// Build from `QM_EMBEDDING_*`, or `None` when the endpoint is unset.
    ///
    /// `dim` is a required part of the configuration but has no environment
    /// variable of its own: the caller knows the width its store expects, and
    /// guessing it from a response would defeat the width check below.
    ///
    /// # Errors
    /// Fails when the endpoint is set but the key or model is missing.
    pub fn from_env(dim: usize) -> Result<Option<Self>> {
        Self::from_lookup(dim, |name| std::env::var(name).ok())
    }

    /// Environment-free core of [`OpenAiCompatEmbedder::from_env`], so the
    /// missing/partial configuration cases are testable without mutating the
    /// process environment (which edition 2024 makes `unsafe`).
    fn from_lookup(dim: usize, lookup: impl Fn(&str) -> Option<String>) -> Result<Option<Self>> {
        let present = |value: Option<String>| value.filter(|value| !value.trim().is_empty());
        let Some(base_url) = present(lookup(ENV_BASE_URL)) else {
            return Ok(None);
        };
        let api_key = present(lookup(ENV_API_KEY))
            .with_context(|| format!("{ENV_BASE_URL} is set but {ENV_API_KEY} is not"))?;
        let model = present(lookup(ENV_MODEL))
            .with_context(|| format!("{ENV_BASE_URL} is set but {ENV_MODEL} is not"))?;
        Self::new(&base_url, &api_key, &model, dim).map(Some)
    }
}

impl std::fmt::Debug for OpenAiCompatEmbedder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print the key: this value lives for the life of the read path,
        // so a stray `{:?}` in a log line would otherwise leak the credential.
        f.debug_struct("OpenAiCompatEmbedder")
            .field("base_url", &self.base_url)
            .field("model", &self.model)
            .field("dim", &self.dim)
            .field("api_key", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl Embedder for OpenAiCompatEmbedder {
    fn dim(&self) -> usize {
        self.dim
    }

    fn identity(&self) -> EmbedderIdentity {
        EmbedderIdentity {
            provider: "openai-compatible".to_string(),
            model: self.model.clone(),
            dim: self.dim,
        }
    }

    fn embed<'a>(&'a self, texts: &'a [String]) -> EmbedFuture<'a> {
        Box::pin(async move {
            // An empty batch is zero vectors, not a request that must answer
            // with zero vectors.
            if texts.is_empty() {
                return Ok(Vec::new());
            }
            let body = serde_json::json!({ "model": self.model, "input": texts });
            let response = self
                .client
                .post(format!("{}/embeddings", self.base_url))
                .bearer_auth(&self.api_key)
                .json(&body)
                .send()
                .await
                .context("calling the embedding endpoint")?;
            let status = response.status();
            let payload: Value = response
                .json()
                .await
                .context("the embedding endpoint did not return JSON")?;
            if !status.is_success() {
                bail!("embedding endpoint returned {status}: {payload}");
            }
            parse_embeddings(&payload, texts.len(), self.dim)
        })
    }
}

/// Extract embeddings from an OpenAI-compatible response.
///
/// Enforces the two contracts callers rely on and refuses to repair a breach:
/// exactly one vector per input, and every vector exactly `expected_dim` wide.
/// A short, long, or wrong-width response is an error — never truncated or
/// zero-padded.
///
/// # Errors
/// Fails when `data` is missing, the vector count differs, a row has no
/// `embedding`, a width differs, or a component is not a number.
pub fn parse_embeddings(
    response: &Value,
    expected_count: usize,
    expected_dim: usize,
) -> Result<Vec<Vec<f32>>> {
    let data = response
        .get("data")
        .and_then(Value::as_array)
        .context("embedding response had no `data` array")?;
    if data.len() != expected_count {
        bail!(
            "embedding response had {} vectors for {expected_count} inputs",
            data.len()
        );
    }
    let mut out = Vec::with_capacity(expected_count);
    for (index, item) in data.iter().enumerate() {
        let vector = item
            .get("embedding")
            .and_then(Value::as_array)
            .with_context(|| format!("embedding {index} had no `embedding` array"))?;
        if vector.len() != expected_dim {
            bail!(
                "embedding {index} had {} dimensions, expected {expected_dim}",
                vector.len()
            );
        }
        let mut row = Vec::with_capacity(expected_dim);
        for (position, value) in vector.iter().enumerate() {
            let number = value.as_f64().with_context(|| {
                format!("embedding {index} dimension {position} was not a number")
            })?;
            row.push(number as f32);
        }
        out.push(row);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_deterministic_embedder_is_reproducible_and_width_correct() {
        let embedder = DeterministicEmbedder::new(8);
        assert_eq!(embedder.dim(), 8);

        let texts = vec!["alpha".to_string(), "beta".to_string()];
        let first = embedder.embed(&texts).await.unwrap();
        let second = embedder.embed(&texts).await.unwrap();

        assert_eq!(first, second, "the same input must embed identically");
        assert_eq!(first.len(), 2);
        for row in &first {
            assert_eq!(row.len(), 8);
            for value in row {
                assert!((-1.0..=1.0).contains(value), "{value} out of range");
            }
        }
        assert_ne!(first[0], first[1], "distinct texts must differ");
    }

    /// The identity is what makes two equal-width models distinguishable, so
    /// each provider must name itself and the model it actually calls.
    #[test]
    fn each_provider_reports_its_own_identity() {
        let deterministic = DeterministicEmbedder::new(8).identity();
        assert_eq!(deterministic.provider, "deterministic");
        assert_eq!(deterministic.model, "sha256-v1");
        assert_eq!(deterministic.dim, 8);

        // The model name is the configured one, not a placeholder: swapping
        // models is exactly the case the identity exists to catch.
        let openai = OpenAiCompatEmbedder::new("http://x", "k", "text-embedding-3-small", 1536)
            .unwrap()
            .identity();
        assert_eq!(openai.provider, "openai-compatible");
        assert_eq!(openai.model, "text-embedding-3-small");
        assert_eq!(openai.dim, 1536);

        // Same width, different model: equal dims must not imply equality.
        let other = OpenAiCompatEmbedder::new("http://x", "k", "text-embedding-3-large", 1536)
            .unwrap()
            .identity();
        assert_eq!(openai.dim, other.dim);
        assert_ne!(openai, other);
    }

    #[tokio::test]
    async fn a_stub_embedding_endpoint_is_called_and_parsed() {
        let body = server_once(
            r#"{"data":[{"embedding":[0.1,0.2,0.3]},{"embedding":[0.4,0.5,0.6]}]}"#,
            |request| {
                assert!(request.starts_with("POST /embeddings"), "{request}");
                assert!(
                    request.contains("authorization: Bearer test-key"),
                    "{request}"
                );
                assert!(request.contains("alpha"), "{request}");
                assert!(request.contains("beta"), "{request}");
                assert!(request.contains("\"model\":\"test-model\""), "{request}");
            },
        );

        let embedder = OpenAiCompatEmbedder::new(
            &format!("http://{}", body.address),
            "test-key",
            "test-model",
            3,
        )
        .unwrap();
        let texts = vec!["alpha".to_string(), "beta".to_string()];
        let vectors = embedder.embed(&texts).await.unwrap();
        assert_eq!(vectors.len(), 2);
        assert_eq!(vectors[0], vec![0.1, 0.2, 0.3]);
        assert_eq!(vectors[1], vec![0.4, 0.5, 0.6]);
        body.join();
    }

    #[tokio::test]
    async fn a_stalled_embedding_endpoint_times_out_instead_of_waiting_forever() {
        let server = stall_server();
        let embedder = OpenAiCompatEmbedder::with_timeout(
            &format!("http://{}", server.address),
            "k",
            "m",
            3,
            Duration::from_millis(100),
        )
        .unwrap();
        let texts = vec!["alpha".to_string()];

        let result = tokio::time::timeout(Duration::from_secs(2), embedder.embed(&texts)).await;
        match result {
            Ok(Err(error)) => {
                let message = format!("{error:#}");
                assert!(
                    message.contains("timed out") || message.contains("timeout"),
                    "the request must fail with a timeout, got: {message}"
                );
            }
            Ok(Ok(_)) => panic!("a stalled provider must not answer successfully"),
            Err(_) => panic!(
                "the embedder must enforce its own request timeout; the outer test deadline fired first"
            ),
        }
        server.join();
    }

    #[tokio::test]
    async fn a_wrong_count_response_surfaces_as_an_error_through_embed() {
        let body = server_once(r#"{"data":[{"embedding":[0.1,0.2,0.3]}]}"#, |_| {});
        let embedder =
            OpenAiCompatEmbedder::new(&format!("http://{}", body.address), "k", "m", 3).unwrap();
        let texts = vec!["alpha".to_string(), "beta".to_string()];
        let error = embedder.embed(&texts).await.unwrap_err();
        assert!(
            error.to_string().contains("1 vectors for 2 inputs"),
            "{error}"
        );
        body.join();
    }

    /// The three fail-closed cases at the parsing boundary. Each is a breach of
    /// a stated contract, so each must be an error rather than a repaired value.
    #[test]
    fn parse_embeddings_fails_closed_on_every_contract_breach() {
        // Wrong count: one vector for two inputs.
        let short = serde_json::json!({"data": [{"embedding": [0.1, 0.2]}]});
        assert!(parse_embeddings(&short, 2, 2).is_err());

        // Wrong width: three components for a width-two embedder.
        let wide = serde_json::json!({"data": [{"embedding": [0.1, 0.2, 0.3]}, {"embedding": [0.4, 0.5, 0.6]}]});
        let error = parse_embeddings(&wide, 2, 2).unwrap_err();
        assert!(error.to_string().contains("3 dimensions"), "{error}");

        // Missing field entirely.
        let empty = serde_json::json!({"model": "x"});
        let error = parse_embeddings(&empty, 1, 2).unwrap_err();
        assert!(error.to_string().contains("`data`"), "{error}");

        // A row without its `embedding` is the same class of breach.
        let rowless = serde_json::json!({"data": [{"index": 0}]});
        assert!(parse_embeddings(&rowless, 1, 2).is_err());

        // …and the positive control: this exact shape passes.
        let good = serde_json::json!({"data": [{"embedding": [0.1, 0.2]}]});
        assert_eq!(
            parse_embeddings(&good, 1, 2).unwrap(),
            vec![vec![0.1f32, 0.2f32]]
        );
    }

    #[test]
    fn from_env_returns_none_when_unset_and_errors_when_half_configured() {
        // Nothing configured: absent, not an error.
        assert!(
            OpenAiCompatEmbedder::from_lookup(4, |_| None)
                .unwrap()
                .is_none()
        );

        // Base URL without a key or model: partial configuration is an error.
        let error = OpenAiCompatEmbedder::from_lookup(4, |name| match name {
            ENV_BASE_URL => Some("http://localhost:1234/v1".to_string()),
            _ => None,
        })
        .unwrap_err();
        assert!(error.to_string().contains(ENV_API_KEY), "{error}");

        let error = OpenAiCompatEmbedder::from_lookup(4, |name| match name {
            ENV_BASE_URL => Some("http://localhost:1234/v1".to_string()),
            ENV_API_KEY => Some("k".to_string()),
            _ => None,
        })
        .unwrap_err();
        assert!(error.to_string().contains(ENV_MODEL), "{error}");

        // Fully configured: built, with the caller's width.
        let embedder = OpenAiCompatEmbedder::from_lookup(4, |name| match name {
            ENV_BASE_URL => Some("http://localhost:1234/v1".to_string()),
            ENV_API_KEY => Some("k".to_string()),
            ENV_MODEL => Some("m".to_string()),
            _ => None,
        })
        .unwrap()
        .unwrap();
        assert_eq!(embedder.dim(), 4);
    }

    #[test]
    fn constructors_reject_blank_settings_and_zero_width() {
        assert!(OpenAiCompatEmbedder::new("", "k", "m", 3).is_err());
        assert!(OpenAiCompatEmbedder::new("http://x", "  ", "m", 3).is_err());
        assert!(OpenAiCompatEmbedder::new("http://x", "k", "", 3).is_err());
        assert!(OpenAiCompatEmbedder::new("http://x", "k", "m", 0).is_err());
    }

    /// A server that accepts one request and then never answers it.
    ///
    /// It stays open until the owning test stops it, so the endpoint really is
    /// a stall rather than a slow response. The stop flag exists so a failed
    /// assertion can join the helper instead of leaking its thread.
    struct StallServer {
        address: std::net::SocketAddr,
        stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
        handle: Option<std::thread::JoinHandle<()>>,
    }

    impl StallServer {
        fn join(mut self) {
            self.stop();
        }

        fn stop(&mut self) {
            self.stop.store(true, std::sync::atomic::Ordering::SeqCst);
            if let Some(handle) = self.handle.take() {
                let _ = handle.join();
            }
        }
    }

    impl Drop for StallServer {
        fn drop(&mut self) {
            self.stop();
        }
    }

    fn stall_server() -> StallServer {
        use std::io::Read;
        use std::net::TcpListener;
        use std::sync::atomic::{AtomicBool, Ordering};

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let stop = std::sync::Arc::new(AtomicBool::new(false));
        let serve_stop = std::sync::Arc::clone(&stop);
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0u8; 8192];
            let _ = stream.read(&mut request).unwrap();
            while !serve_stop.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(10));
            }
        });
        StallServer {
            address,
            stop,
            handle: Some(handle),
        }
    }

    /// A one-shot HTTP server: accepts a single request, asserts its shape with
    /// `check`, and answers with `body`. The request-shape assertions run on the
    /// server thread so a mismatch fails the test instead of hanging the client.
    struct StubServer {
        address: std::net::SocketAddr,
        handle: std::thread::JoinHandle<()>,
    }

    impl StubServer {
        fn join(self) {
            self.handle.join().unwrap();
        }
    }

    fn server_once(body: &'static str, check: fn(&str)) -> StubServer {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = vec![0u8; 8192];
            let read = stream.read(&mut request).unwrap();
            let request = String::from_utf8_lossy(&request[..read]).to_string();
            check(&request);
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).unwrap();
        });
        StubServer { address, handle }
    }
}
