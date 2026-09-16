//! The vector retrieval stream: dense embeddings as a fourth RRF list.
//!
//! A vector stream answers a question keyword matching cannot: "which pages
//! mean something close to this, even though they share no words with it?" It
//! is deliberately the *weakest* signal in the fusion — it only ever adds
//! candidates behind the direct streams — because a cosine score is not
//! comparable to a BM25 score and a near-orthogonal neighbour is not evidence
//! of relevance. Candidates whose similarity is not positive are dropped for
//! exactly that reason; see [`search_vector`].
//!
//! Embeddings live in the split as one bytes fast field per document (raw
//! little-endian `f32`s), so scoring a split is a column scan rather than a
//! document scan. That is brute force by design for v1: an approximate
//! neighbour structure is an optimisation, and an optimisation that changes
//! recall would have to be measured before it could be trusted.

use std::path::Path;

use anyhow::{Context, Result, bail};
use qm_llm::{Embedder, OpenAiCompatEmbedder};
use tantivy::schema::Value;
use tantivy::{DocAddress, Index};

use crate::Hit;

/// Names the embedding width. The width is configuration, never a guess: a
/// provider that answers with a different width is a contract breach, and
/// reading a stored width that disagrees with the query would compare
/// unrelated coordinates.
const ENV_DIM: &str = "QM_EMBEDDING_DIM";

/// Width assumed when [`ENV_DIM`] is unset: the OpenAI `text-embedding-3-small`
/// default, so the common configuration needs no extra variable.
const DEFAULT_DIM: usize = 1536;

/// Build the embedding provider from `QM_EMBEDDING_*`, or `None` when the
/// endpoint is unset.
///
/// Unset is not an error: a bucket with no provider still publishes and
/// searches, just without the vector stream. A *partially* configured provider
/// (url but no key or model) is an error, which `qm_llm::OpenAiCompatEmbedder`
/// enforces.
///
/// # Errors
/// Fails when the configuration is partial, the width is zero or unparseable,
/// or the provider rejects the settings.
pub fn embedder_from_env() -> Result<Option<Box<dyn Embedder>>> {
    let dim = match std::env::var(ENV_DIM) {
        Ok(raw) if !raw.trim().is_empty() => raw
            .trim()
            .parse::<usize>()
            .with_context(|| format!("{ENV_DIM} must be a positive integer, got {raw:?}"))?,
        _ => DEFAULT_DIM,
    };
    if dim == 0 {
        bail!("{ENV_DIM} must not be zero: a zero-width embedding cannot be compared");
    }
    Ok(
        OpenAiCompatEmbedder::from_env(dim)?
            .map(|embedder| Box::new(embedder) as Box<dyn Embedder>),
    )
}

/// Embed a batch of texts, enforcing the provider's contract.
///
/// The provider's own implementation checks this too, but the read path cannot
/// assume it: an [`Embedder`] is a trait any caller can implement, and a
/// response with the wrong number of rows or the wrong width would silently
/// corrupt every later comparison. So the check is stated here, once, at the
/// boundary that consumes the vectors — a breach is an error, never a
/// truncated or zero-padded result.
///
/// # Errors
/// Fails when the call fails, when the row count differs from `texts`, or when
/// any row's width differs from [`Embedder::dim`].
pub async fn embed_texts(embedder: &dyn Embedder, texts: &[String]) -> Result<Vec<Vec<f32>>> {
    let vectors = embedder.embed(texts).await.context("embedding text")?;
    if vectors.len() != texts.len() {
        bail!(
            "the embedding provider returned {} vector(s) for {} input(s)",
            vectors.len(),
            texts.len()
        );
    }
    for (index, vector) in vectors.iter().enumerate() {
        if vector.len() != embedder.dim() {
            bail!(
                "the embedding provider returned a {}-wide vector at position {index}, but its \
                 configured width is {}",
                vector.len(),
                embedder.dim()
            );
        }
    }
    Ok(vectors)
}

/// Embed a single string and return its vector.
///
/// # Errors
/// Propagates [`embed_texts`] failures, including a response that does not
/// contain exactly one vector.
pub async fn embed_one(embedder: &dyn Embedder, text: &str) -> Result<Vec<f32>> {
    let mut vectors = embed_texts(embedder, std::slice::from_ref(&text.to_string())).await?;
    vectors
        .pop()
        .ok_or_else(|| anyhow::anyhow!("the embedding provider returned no vector for one input"))
}

/// Encode an embedding as raw little-endian `f32` bytes.
///
/// Little-endian because the split is a shared, portable artefact: a reader on
/// any machine must decode the same numbers.
#[must_use]
pub fn encode(embedding: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(embedding.len() * 4);
    for value in embedding {
        out.extend_from_slice(&value.to_le_bytes());
    }
    out
}

/// Decode the bytes written by [`encode`].
///
/// # Errors
/// Fails when the byte length is not a multiple of four, rather than dropping
/// or padding the trailing bytes into a shorter vector.
pub fn decode(bytes: &[u8]) -> Result<Vec<f32>> {
    if !bytes.len().is_multiple_of(4) {
        bail!(
            "embedded vector has {} byte(s), which is not a whole number of f32 values",
            bytes.len()
        );
    }
    Ok(bytes
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect())
}

/// Cosine similarity between two embeddings.
///
/// # Errors
/// Fails on a width mismatch and on a zero-norm vector. Both are contract
/// breaches rather than degenerate scores: silently returning `0.0` for a
/// vector of the wrong width would turn a broken index into a plausible-looking
/// ranking, which is the failure mode this crate refuses everywhere else.
pub fn cosine(a: &[f32], b: &[f32]) -> Result<f32> {
    if a.len() != b.len() {
        bail!(
            "cannot compare embeddings of different widths: query has {}, stored value has {}",
            a.len(),
            b.len()
        );
    }
    if a.is_empty() {
        bail!("cannot compare empty embeddings: the query embedding has zero dimensions");
    }
    let mut dot = 0.0f64;
    let mut norm_a = 0.0f64;
    let mut norm_b = 0.0f64;
    for (x, y) in a.iter().zip(b) {
        if !x.is_finite() || !y.is_finite() {
            bail!("cannot compare embeddings that contain a non-finite value");
        }
        dot += f64::from(*x) * f64::from(*y);
        norm_a += f64::from(*x) * f64::from(*x);
        norm_b += f64::from(*y) * f64::from(*y);
    }
    if norm_a == 0.0 {
        bail!("cannot compare embeddings: the query embedding has zero magnitude");
    }
    if norm_b == 0.0 {
        bail!("cannot compare embeddings: a stored embedding has zero magnitude");
    }
    Ok((dot / (norm_a.sqrt() * norm_b.sqrt())) as f32)
}

/// Score one split by cosine similarity and return the best `limit` documents.
///
/// Only documents whose similarity is **strictly positive** become candidates.
/// Cosine is not a confidence and not comparable across queries: an orthogonal
/// or anti-correlated page carries no evidence that it is about the query, so
/// letting it into the fusion would inject noise into every search that
/// configured a provider. This is a floor, not a tuned threshold — relevance
/// ordering is still the fusion's job.
///
/// # Errors
/// Fails when the split cannot be opened or a stored embedding is malformed or
/// disagrees in width with `query`. A split that was built without embeddings
/// contributes nothing and is not an error: the corpus may legitimately mix
/// split generations.
pub fn search_vector(dir: &Path, query: &[f32], stream: &str, limit: usize) -> Result<Vec<Hit>> {
    if query.is_empty() {
        bail!("cannot run the vector stream with an empty query embedding");
    }
    let index = Index::open_in_dir(dir).context("opening index")?;
    // The tokenizer registry lives on the `Index`, not in the directory: an
    // index opened here needs it as much as one opened for a keyword search.
    index.tokenizers().register(
        crate::cjk::TOKENIZER_NAME,
        tantivy::tokenizer::TextAnalyzer::builder(crate::cjk::CjkTokenizer).build(),
    );
    let reader = index.reader().context("opening reader")?;
    let searcher = reader.searcher();

    let mut scored: Vec<(f32, DocAddress)> = Vec::new();
    let mut buffer = Vec::new();
    for (segment_ord, segment) in searcher.segment_readers().iter().enumerate() {
        let Some(column) = segment.fast_fields().bytes("embedding")? else {
            continue;
        };
        let alive = segment.alive_bitset();
        for doc_id in 0..segment.max_doc() {
            if alive.is_some_and(|bitset| !bitset.is_alive(doc_id)) {
                continue;
            }
            let Some(ord) = column.term_ords(doc_id).next() else {
                continue;
            };
            buffer.clear();
            if !column
                .ord_to_bytes(ord, &mut buffer)
                .context("reading an embedded vector")?
            {
                bail!("embedding column is missing the term for ordinal {ord}");
            }
            let stored = decode(&buffer)?;
            let similarity = cosine(query, &stored)?;
            if similarity > 0.0 {
                scored.push((similarity, DocAddress::new(segment_ord as u32, doc_id)));
            }
        }
    }

    // The page id is the stable tie-break, so two machines that score the same
    // split produce the same list.
    scored.sort_by(|a, b| {
        b.0.partial_cmp(&a.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.1.cmp(&b.1))
    });
    scored.truncate(limit);

    let s = index.schema();
    let optional = |name: &str| s.get_field(name).ok();
    let path_field = optional("path");
    let page_field = optional("page_id");
    let workspace_field = optional("workspace_id");
    let project_field = optional("project_id");
    let title_field = optional("title");
    let updated_field = optional("updated_at_ms");

    let mut hits = Vec::with_capacity(scored.len());
    for (score, address) in scored {
        let doc: tantivy::TantivyDocument = searcher.doc(address)?;
        let get = |field: Option<tantivy::schema::Field>| {
            field
                .and_then(|field| doc.get_first(field))
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .to_string()
        };
        hits.push(Hit {
            workspace_id: get(workspace_field),
            project_id: get(project_field),
            path: get(path_field),
            page_id: get(page_field),
            title: get(title_field),
            score,
            fused_score: score,
            recency_multiplier: 1.0,
            updated_at_ms: updated_field
                .and_then(|field| doc.get_first(field))
                .and_then(|value| value.as_i64())
                .unwrap_or_default(),
            streams: vec![stream.to_string()],
        });
    }
    hits.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.page_id.cmp(&b.page_id))
    });
    Ok(hits)
}
