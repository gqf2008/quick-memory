//! Entity and link extraction from page bodies.
//!
//! Retrieval quality comes from matching on more than prose. A page that
//! *declares* an identifier — a wiki link, a backticked path, a file path, a
//! tag — is a stronger answer for that identifier than a page that merely
//! mentions it in a sentence, and links give a second, cheaper signal.
//!
//! Extraction is deliberately lexical: no model, no vocabulary, so the same
//! body always yields the same fields and a rebuild is reproducible.

/// Maximum number of entities kept per page.
///
/// Bounded so an adversarial body cannot inflate an index document without
/// limit; the first occurrences win because they appear earliest in the text.
pub const MAX_ENTITIES: usize = 64;

/// What a page body declares.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Extraction {
    /// Identifiers the page declares, in first-seen order, deduplicated.
    pub entities: Vec<String>,
    /// Link targets the page points at.
    pub links: Vec<String>,
}

/// Extract entities and links from a markdown body.
#[must_use]
pub fn extract(body: &str) -> Extraction {
    let mut extraction = Extraction::default();
    // Entities and links are separate namespaces: the same word may legitimately
    // be both, and deduplicating across them would drop one of the two.
    let mut seen_entities: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut seen_links: std::collections::HashSet<String> = std::collections::HashSet::new();

    let push = |namespace: &mut std::collections::HashSet<String>,
                target: &mut Vec<String>,
                value: &str| {
        let trimmed = value
            .trim()
            .trim_matches(|c: char| c == '`' || c == '"' || c == '\'');
        if trimmed.is_empty() || trimmed.len() > 128 {
            return;
        }
        if namespace.insert(trimmed.to_lowercase()) {
            target.push(trimmed.to_string());
        }
    };

    // Wiki links: `[[target]]` or `[[target|alias]]`.
    let mut rest = body;
    while let Some(start) = rest.find("[[") {
        let after = &rest[start + 2..];
        let Some(end) = after.find("]]") else { break };
        let inner = &after[..end];
        let target = inner.split('|').next().unwrap_or(inner).trim();
        if !target.is_empty() {
            push(&mut seen_links, &mut extraction.links, target);
            // The last path segment is the entity the link is *about*.
            let leaf = target.rsplit('/').next().unwrap_or(target);
            push(
                &mut seen_entities,
                &mut extraction.entities,
                leaf.trim_end_matches(".md"),
            );
        }
        rest = &after[end + 2..];
    }

    // Backticked spans, split on whitespace and punctuation that never appears
    // inside an identifier.
    let mut rest = body;
    while let Some(start) = rest.find('`') {
        let after = &rest[start + 1..];
        let Some(end) = after.find('`') else { break };
        for token in after[..end].split(|c: char| c.is_whitespace() || c == ',' || c == ';') {
            if looks_like_identifier(token) {
                push(&mut seen_entities, &mut extraction.entities, token);
            }
        }
        rest = &after[end + 1..];
    }

    // Bare path-like tokens: `crates/qm-search/src/lib.rs`, `docs/design.md`.
    for token in body.split_whitespace() {
        let cleaned = token.trim_matches(|c: char| {
            matches!(
                c,
                '(' | ')' | '[' | ']' | '{' | '}' | ',' | '.' | ':' | ';' | '"' | '\''
            )
        });
        if looks_like_path(cleaned) {
            push(&mut seen_entities, &mut extraction.entities, cleaned);
        }
    }

    // Tags: `#release-notes`.
    let mut rest = body;
    while let Some(start) = rest.find('#') {
        let after = &rest[start + 1..];
        let token: String = after
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '/'))
            .collect();
        if token.len() >= 2 {
            push(&mut seen_entities, &mut extraction.entities, &token);
        }
        if after.is_empty() {
            break;
        }
        rest = &after[token.len().max(1)..];
    }

    extraction.entities.truncate(MAX_ENTITIES);
    extraction.links.truncate(MAX_ENTITIES);
    extraction
}

/// Whether a token looks like an identifier worth matching on.
fn looks_like_identifier(token: &str) -> bool {
    let token = token.trim();
    token.len() >= 2
        && token.len() <= 128
        && token
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | ':' | '/' | '#'))
        && token.chars().any(|c| c.is_ascii_alphabetic())
}

/// Whether a token looks like a path or file reference.
fn looks_like_path(token: &str) -> bool {
    if !token.contains('/') && !token.contains('.') {
        return false;
    }
    let has_slash = token.contains('/');
    let known_suffix = [
        ".rs", ".md", ".toml", ".json", ".yaml", ".yml", ".py", ".ts", ".tsx", ".go", ".sql", ".sh",
    ]
    .iter()
    .any(|suffix| token.ends_with(suffix));
    (has_slash && token.len() >= 4) || known_suffix
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_wiki_links_and_the_entity_they_point_at() {
        let extraction = extract("See [[notes/raft.md]] and [[quickwit|the engine]] for detail.");
        assert!(extraction.links.contains(&"notes/raft.md".to_string()));
        assert!(extraction.links.contains(&"quickwit".to_string()));
        assert!(extraction.entities.contains(&"raft".to_string()));
        assert!(extraction.entities.contains(&"quickwit".to_string()));
    }

    #[test]
    fn extracts_paths_backticks_and_tags() {
        let extraction = extract(
            "Ran `cargo test -p qm-search` after editing crates/qm-search/src/lib.rs #release-notes",
        );
        assert!(extraction.entities.contains(&"cargo".to_string()));
        assert!(extraction.entities.contains(&"qm-search".to_string()));
        assert!(
            extraction
                .entities
                .contains(&"crates/qm-search/src/lib.rs".to_string())
        );
        assert!(extraction.entities.contains(&"release-notes".to_string()));
    }

    #[test]
    fn deduplicates_case_insensitively_and_bounds_the_count() {
        let body = "`tantivy` `Tantivy` `TANTIVY`";
        let extraction = extract(body);
        assert_eq!(
            extraction
                .entities
                .iter()
                .filter(|e| e.eq_ignore_ascii_case("tantivy"))
                .count(),
            1,
            "{:?}",
            extraction.entities
        );

        let mut huge = String::new();
        for index in 0..(MAX_ENTITIES * 2) {
            huge.push_str(&format!("`entity-{index:04}` "));
        }
        assert_eq!(extract(&huge).entities.len(), MAX_ENTITIES);
    }

    #[test]
    fn ordinary_prose_yields_no_entities() {
        let extraction = extract("the build failed because the cache was cold");
        assert!(extraction.entities.is_empty(), "{:?}", extraction.entities);
        assert!(extraction.links.is_empty());
    }
}
