//! Intake scrubbing: the boundary every untrusted observation crosses.
//!
//! Capture is automatic, so the text arriving here is whatever an agent wrote
//! into a prompt or a tool call — including secrets it pasted. Nothing reaches
//! the bucket or the index before passing through [`scrub`].
//!
//! This is a bounded first cut: it removes obvious credential shapes and
//! enforces the size backstop. It is not a general-purpose DLP classifier, and
//! it does not try to be — a false negative here must not be mistaken for a
//! guarantee, which is why the rule lives at the typed boundary rather than in
//! each caller.

use std::sync::OnceLock;

use regex::Regex;

/// Hard cap on one observation's text, after scrubbing.
pub const MAX_OBSERVATION_BYTES: usize = 16 * 1024;

/// Replacement written in place of a redacted value.
pub const REDACTED: &str = "[REDACTED]";

fn patterns() -> &'static [Regex] {
    static PATTERNS: OnceLock<Vec<Regex>> = OnceLock::new();
    PATTERNS.get_or_init(|| {
        [
            // Bearer tokens in headers or prose.
            r"(?i)\b(bearer)\s+[A-Za-z0-9._~+/=-]{8,}",
            // key=value / key: value credential assignments.
            r"(?i)\b(password|passwd|secret|token|api[_-]?key|access[_-]?key|private[_-]?key)\b\s*[:=]\s*[^\s,;'\x22]{4,}",
            // Well-known credential shapes.
            r"\bsk-[A-Za-z0-9_-]{16,}",
            r"\bAKIA[0-9A-Z]{16}\b",
            r"\bghp_[A-Za-z0-9]{20,}\b",
            r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b",
            // Credentials embedded in URLs.
            r"://[^/\s:@]{1,64}:[^/\s@]{4,}@",
        ]
        .iter()
        .filter_map(|pattern| Regex::new(pattern).ok())
        .collect()
    })
}

/// Redact obvious credentials and bound the result.
#[must_use]
pub fn scrub(text: &str) -> String {
    let mut out = text.to_string();
    for pattern in patterns() {
        out = pattern.replace_all(&out, REDACTED).to_string();
    }
    if out.len() > MAX_OBSERVATION_BYTES {
        // Truncate on a character boundary so the result stays valid UTF-8.
        let mut cut = MAX_OBSERVATION_BYTES;
        while cut > 0 && !out.is_char_boundary(cut) {
            cut -= 1;
        }
        out.truncate(cut);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_common_credential_shapes() {
        let scrubbed = scrub(
            "run with Authorization: Bearer abcdefghijklmnop and api_key=abcd1234 \
             plus sk-abcdefghijklmnopqrstuvwx and AWS AKIAABCDEFGHIJKLMNOP \
             and postgres://admin:hunter2@db.internal/prod",
        );
        assert!(!scrubbed.contains("abcdefghijklmnop"), "{scrubbed}");
        assert!(!scrubbed.contains("abcd1234"), "{scrubbed}");
        assert!(
            !scrubbed.contains("sk-abcdefghijklmnopqrstuvwx"),
            "{scrubbed}"
        );
        assert!(!scrubbed.contains("AKIAABCDEFGHIJKLMNOP"), "{scrubbed}");
        assert!(!scrubbed.contains("hunter2"), "{scrubbed}");
        assert!(scrubbed.contains("Authorization"), "{scrubbed}");
        assert!(scrubbed.contains(REDACTED), "{scrubbed}");
    }

    #[test]
    fn keeps_ordinary_text_and_bounds_size() {
        let plain = "the build failed because the cache was cold";
        assert_eq!(scrub(plain), plain);

        let huge = "x".repeat(MAX_OBSERVATION_BYTES * 2);
        let bounded = scrub(&huge);
        assert_eq!(bounded.len(), MAX_OBSERVATION_BYTES);
        assert!(bounded.is_char_boundary(bounded.len()));
    }

    #[test]
    fn multibyte_text_is_truncated_on_a_character_boundary() {
        let huge = "记".repeat(MAX_OBSERVATION_BYTES);
        let bounded = scrub(&huge);
        assert!(bounded.len() <= MAX_OBSERVATION_BYTES);
        assert!(std::str::from_utf8(bounded.as_bytes()).is_ok());
        assert!(bounded.is_char_boundary(bounded.len()));
    }
}
