//! The analyzer this crate indexes with — and therefore queries with.
//!
//! tantivy's `default` tokenizer splits on anything that is not alphanumeric,
//! and CJK characters *are* alphanumeric to Unicode. A Chinese sentence without
//! punctuation therefore arrives as **one** token: a page whose body says
//! `潜在租约泄漏` is indexed under exactly that term, and searching `租约` — the
//! word a person would actually type — finds nothing at all. That is not a
//! theoretical gap: on 2026-09-16 a real bucket answered `hits=0` for `租约`
//! while the page body contained the word twice, and answered `hits=1` for the
//! whole run.
//!
//! [`CjkTokenizer`] keeps what `default` does for Latin text and adds what CJK
//! needs:
//!
//! - a run of Latin letters and digits stays one lower-cased token, and is
//!   dropped when it is longer than 40 bytes — the same bound `default` applies,
//!   so a base64 blob still does not become a term;
//! - every CJK character is emitted, and so is every adjacent pair of them,
//!   both at the position of their first character.
//!
//! Unigrams are what make a one-character query answer; bigrams are what make a
//! two- or three-character word precise. Indexing and query parsing run this
//! same analyzer, which is the whole point: a query is split exactly the way the
//! document it is looking for was split.

use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

/// Name this analyzer is registered under, and the name the schema asks for.
pub const TOKENIZER_NAME: &str = "cjk";

/// Longest Latin/digit run that becomes a term, in bytes.
///
/// tantivy's `default` analyzer is `SimpleTokenizer + RemoveLongFilter(40) +
/// LowerCaser`, and the filter chain is evaluated inside-out: the length
/// predicate runs on the **raw** token and the lower-casing is applied to what
/// survives. The bound is therefore exclusive *and* measured before
/// lower-casing — the reverse of what this module did once, which real
/// characters show: `İ` is 2 bytes and lower-cases to 3, so nineteen of them are
/// inside the bound going in (`default` keeps them) and 57 bytes coming out,
/// while nineteen `ẞ` are 57 bytes going in (dropped) and 38 coming out. The
/// differential test against tantivy's own analyzer is what pins the order.
const MAX_LATIN_TOKEN_BYTES: usize = 40;

/// Lower-case a token the way tantivy's `LowerCaser` does: character by
/// character, with no word-final special cases.
///
/// `str::to_lowercase` implements the Unicode *default* mappings, which turn
/// `ΣΣ` into `σς`; tantivy folds each character on its own and gets `σσ`. A
/// token that differs from `default`'s is a token that searches differently.
fn lowercase_like_tantivy(text: &str) -> String {
    text.chars().flat_map(char::to_lowercase).collect()
}

/// Splits text into CJK unigrams and bigrams, and Latin/digit runs.
#[derive(Clone, Default)]
pub struct CjkTokenizer;

/// The token stream [`CjkTokenizer`] produces.
pub struct CjkTokenStream {
    tokens: Vec<Token>,
    /// Index of the current token; 0 means "nothing yielded yet".
    cursor: usize,
}

impl Tokenizer for CjkTokenizer {
    type TokenStream<'a> = CjkTokenStream;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        CjkTokenStream {
            tokens: tokenize(text),
            cursor: 0,
        }
    }
}

impl TokenStream for CjkTokenStream {
    fn advance(&mut self) -> bool {
        if self.cursor >= self.tokens.len() {
            return false;
        }
        self.cursor += 1;
        true
    }

    fn token(&self) -> &Token {
        &self.tokens[self.cursor - 1]
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.tokens[self.cursor - 1]
    }
}

/// Whether a character is one this tokenizer expands into n-grams.
fn is_cjk(ch: char) -> bool {
    matches!(ch,
        // Hiragana and katakana.
        '\u{3040}'..='\u{30ff}'
        // CJK Unified Ideographs: extension A, the main block, and the
        // compatibility forms...
        | '\u{3400}'..='\u{4dbf}'
        | '\u{4e00}'..='\u{9fff}'
        | '\u{f900}'..='\u{faff}'
        // ...plus extensions B and beyond, which are single characters outside
        // the BMP (rare in prose, but they are still Chinese text). Extensions
        // G and H live even further out, in plane 3.
        | '\u{20000}'..='\u{2fa1f}'
        | '\u{30000}'..='\u{323af}'
        // Hangul syllables and the compatibility jamo, and halfwidth katakana.
        | '\u{3130}'..='\u{318f}'
        | '\u{ac00}'..='\u{d7af}'
        | '\u{ff66}'..='\u{ff9f}')
}

/// Split `text` into the tokens the index stores.
fn tokenize(text: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    // Position in the token stream: CJK runs advance it by their length, so a
    // run's unigrams and bigrams overlap the way a phrase query expects.
    let mut position = 0usize;

    let mut chars = text.char_indices().peekable();
    while let Some(&(start, ch)) = chars.peek() {
        if !ch.is_alphanumeric() {
            chars.next();
            continue;
        }
        // A word: the maximal run of alphanumerics, exactly what `default`
        // would hand to its filters.
        let mut end = start;
        let mut word_chars: Vec<(usize, char)> = Vec::new();
        while let Some(&(offset, ch)) = chars.peek() {
            if !ch.is_alphanumeric() {
                break;
            }
            chars.next();
            word_chars.push((offset, ch));
            end = offset + ch.len_utf8();
        }
        let word = &text[start..end];

        if !word.chars().any(is_cjk) {
            // Strictly below the bound, measured on the raw word: that is the
            // order `default` uses (`RemoveLongFilter` sees the token before
            // `LowerCaser` does).
            if word.len() < MAX_LATIN_TOKEN_BYTES {
                tokens.push(Token {
                    offset_from: start,
                    offset_to: end,
                    position,
                    text: lowercase_like_tantivy(word),
                    position_length: 1,
                });
            }
            position += 1;
            continue;
        }

        // Mixed words are split again by script, so a Latin identifier that
        // happens to sit against a Chinese word stays one searchable token.
        let mut index = 0usize;
        while index < word_chars.len() {
            let cjk = is_cjk(word_chars[index].1);
            let run_start = index;
            while index < word_chars.len() && is_cjk(word_chars[index].1) == cjk {
                index += 1;
            }
            let run = &word_chars[run_start..index];
            if !cjk {
                let from = run[0].0;
                let last = run[run.len() - 1];
                let to = last.0 + last.1.len_utf8();
                if to - from < MAX_LATIN_TOKEN_BYTES {
                    tokens.push(Token {
                        offset_from: from,
                        offset_to: to,
                        position,
                        text: lowercase_like_tantivy(&text[from..to]),
                        position_length: 1,
                    });
                }
                position += 1;
                continue;
            }
            for (offset, (from, _)) in run.iter().enumerate() {
                let to = from + word_chars[run_start + offset].1.len_utf8();
                tokens.push(Token {
                    offset_from: *from,
                    offset_to: to,
                    position: position + offset,
                    text: word_chars[run_start + offset].1.to_lowercase().collect(),
                    position_length: 1,
                });
            }
            for offset in 0..run.len().saturating_sub(1) {
                let from = run[offset].0;
                let last = run[offset + 1];
                tokens.push(Token {
                    offset_from: from,
                    offset_to: last.0 + last.1.len_utf8(),
                    position: position + offset,
                    text: run[offset]
                        .1
                        .to_lowercase()
                        .chain(run[offset + 1].1.to_lowercase())
                        .collect(),
                    position_length: 1,
                });
            }
            position += run.len();
        }
    }
    tokens
}

#[cfg(test)]
mod tests {
    use super::*;

    fn texts(text: &str) -> Vec<String> {
        let mut tokenizer = CjkTokenizer;
        let mut stream = tokenizer.token_stream(text);
        let mut out = Vec::new();
        while stream.advance() {
            out.push(stream.token().text.clone());
        }
        out
    }

    #[test]
    fn a_chinese_run_becomes_unigrams_and_bigrams() {
        let tokens = texts("潜在租约泄漏");
        for expected in ["租", "约", "租约", "约泄", "泄漏", "潜在"] {
            assert!(
                tokens.iter().any(|token| token == expected),
                "{expected:?} has to be a term: {tokens:?}"
            );
        }
        // The whole run must not survive as one term: that is the bug.
        assert!(
            !tokens.iter().any(|token| token == "潜在租约泄漏"),
            "a whole run is exactly what made the word unfindable: {tokens:?}"
        );
    }

    #[test]
    fn latin_words_and_digits_stay_whole_and_lower_cased() {
        assert_eq!(texts("Hello World 2026"), ["hello", "world", "2026"]);
    }

    #[test]
    fn the_long_latin_bound_matches_tantivys_default() {
        // The four byte lengths around the edge, so an off-by-one cannot return.
        for length in [38, 39] {
            assert_eq!(
                texts(&"x".repeat(length)).len(),
                1,
                "{length} bytes is inside the bound"
            );
        }
        for length in [40, 41] {
            assert!(
                texts(&"x".repeat(length)).is_empty(),
                "{length} bytes is past the bound"
            );
        }
        // The bound is on the *raw* token, and these two characters show why
        // the order matters: nineteen `İ` are 38 bytes raw (kept) and 57 bytes
        // lower-cased; nineteen `ẞ` are 57 raw (dropped) and 38 lower-cased.
        let dotted = "İ".repeat(19);
        assert_eq!(dotted.len(), 38);
        assert_eq!(dotted.to_lowercase().len(), 57);
        assert_eq!(texts(&dotted).len(), 1, "default keeps what is raw-short");

        let sharp = "ẞ".repeat(19);
        assert_eq!(sharp.len(), 57);
        assert_eq!(sharp.to_lowercase().len(), 38);
        assert!(
            texts(&sharp).is_empty(),
            "default drops what is raw-long, however short it becomes"
        );
    }

    /// The test that would have caught the reversed filter order: run tantivy's
    /// own `default` analyzer and this tokenizer over the same pure-Latin
    /// inputs and require identical token text, in order.
    ///
    /// Comparing against the real analyzer is the point — a hand-written
    /// expectation only restates whatever the implementation believes.
    #[test]
    fn pure_latin_input_matches_tantivys_default_analyzer() {
        use tantivy::tokenizer::{
            LowerCaser, RemoveLongFilter, SimpleTokenizer, TextAnalyzer, Tokenizer,
        };

        let mut default = TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(RemoveLongFilter::limit(MAX_LATIN_TOKEN_BYTES))
            .filter(LowerCaser)
            .build();
        let mut ours = CjkTokenizer;

        let inputs = [
            "Hello World 2026",
            "a-b_c",
            &"x".repeat(39),
            &"x".repeat(40),
            &"İ".repeat(19),
            &"ẞ".repeat(19),
            &"İ".repeat(20),
            "ΣΣ sigma",
            &"abcİdef".repeat(5),
        ];
        for input in inputs {
            let expected: Vec<String> = {
                let mut stream = default.token_stream(input);
                let mut out = Vec::new();
                while stream.advance() {
                    out.push(stream.token().text.clone());
                }
                out
            };
            let actual: Vec<String> = {
                let mut stream = ours.token_stream(input);
                let mut out = Vec::new();
                while stream.advance() {
                    out.push(stream.token().text.clone());
                }
                out
            };
            assert_eq!(actual, expected, "differed on {input:?}");
        }
    }

    #[test]
    fn cjk_outside_the_basic_plane_is_expanded_too() {
        // U+20000 is CJK extension B: one character, still Chinese text.
        let tokens = texts("𠀀𠀁租");
        assert!(
            tokens.iter().any(|token| token == "𠀀𠀁"),
            "{tokens:?} should carry the surrogate-pair bigram"
        );
        assert!(tokens.iter().any(|token| token == "𠀀"));

        // Extension G starts at U+30000 and extension H ends at U+323AF. Each
        // end is asserted as a *precise* unigram and as a bigram crossing into a
        // known-CJK neighbour: a looser "some one-character token exists" is
        // satisfied by the trailing 租 even when the whole range is missing —
        // and an off-by-one at either end would slip through just as easily.
        for (name, ch) in [
            // The block spans U+30000–U+3134F but the *assigned* characters
            // stop at U+3134A; a block end that is unassigned is not
            // alphanumeric and is dropped as a boundary, which is a different
            // behaviour entirely.
            ("extension G start", '\u{30000}'),
            ("extension G last assigned", '\u{3134a}'),
            ("extension H start", '\u{31350}'),
            ("extension H end", '\u{323af}'),
        ] {
            let text = ch.to_string();
            let tokens = texts(&format!("{text}租"));
            assert!(
                tokens.iter().any(|token| token == &text),
                "{name} (U+{:05X}) has no unigram: {tokens:?}",
                ch as u32
            );
            assert!(
                tokens.iter().any(|token| token == &format!("{text}租")),
                "{name} (U+{:05X}) does not pair with its CJK neighbour: {tokens:?}",
                ch as u32
            );
        }
    }

    #[test]
    fn a_latin_word_touching_chinese_stays_searchable() {
        let tokens = texts("作用域listbuckets无法枚举");
        assert!(
            tokens.iter().any(|token| token == "listbuckets"),
            "the identifier has to survive next to CJK text: {tokens:?}"
        );
        assert!(tokens.iter().any(|token| token == "作用"));
    }

    #[test]
    fn punctuation_and_whitespace_are_boundaries() {
        assert_eq!(texts("租约, 泄漏"), texts("租约 泄漏"));
        assert!(texts("！！！").is_empty());
    }

    #[test]
    fn positions_of_a_run_are_consecutive() {
        // A phrase query over adjacent bigrams depends on this.
        let mut tokenizer = CjkTokenizer;
        let mut stream = tokenizer.token_stream("租约泄漏");
        let mut positions = Vec::new();
        while stream.advance() {
            if stream.token().text.chars().count() == 2 {
                positions.push((stream.token().text.clone(), stream.token().position));
            }
        }
        positions.sort_by_key(|(_, position)| *position);
        assert_eq!(
            positions,
            [
                ("租约".to_string(), 0),
                ("约泄".to_string(), 1),
                ("泄漏".to_string(), 2)
            ]
        );
    }
}
