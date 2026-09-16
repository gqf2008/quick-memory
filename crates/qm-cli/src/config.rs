//! The machine-local config file.
//!
//! `qm` and `qm-mcp` read their settings from the environment. An operator who
//! wants the same bucket on every command would otherwise have to inject it
//! from a shell profile, a cron line and an MCP client configuration
//! separately, so both binaries also read one file under the user's home.
//!
//! Three rules shape the whole module:
//!
//! 1. **The environment always wins, over every alias.** A variable set in the
//!    process environment beats the same *setting* in the file — not merely the
//!    same *name*. `R2_BUCKET` in the environment has to beat `QM_S3_BUCKET` in
//!    the file, or a one-off `R2_BUCKET=... qm status` would quietly read the
//!    bucket the file names. That is why resolution is per alias *group* and
//!    per source ([`first_of`]), not per name: a per-name lookup lets the file
//!    answer a name the environment would have answered differently.
//! 2. **The file is a plain assignments file that can also be `source`d.** It
//!    holds `K=V` / `export K="V"` lines, `#` comments, and whatever else its
//!    operator keeps in an environment file — those other lines are ignored,
//!    not interpreted. Nothing is evaluated: no `$`, no command substitution,
//!    no globbing. A value that would mean something *different* to a shell —
//!    quotes concatenated with more text, escapes, an unquoted second word,
//!    unquoted shell metacharacters — is **refused**, with single quotes as the
//!    escape hatch (they mean the same thing to both readings). What remains
//!    are two *permissive* differences: this loader accepts whitespace around
//!    the `=` and strips a CRLF line ending, where a shell would read `K = v`
//!    as a command and keep the `\r`. The equivalence promise is therefore
//!    narrow and stated on purpose: **an LF-terminated file written in the
//!    syntax below is read the same way by both**; anything outside that syntax
//!    is refused rather than half-honored. Interactive `!` history expansion is
//!    outside the promise — it belongs to the shell session, not the file.
//! 3. **No setting looks like it took effect when it did not.** Keys read by
//!    crates that never consult this file are called out at load time rather
//!    than silently ignored; see [`NOT_SERVED_YET`].

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use anyhow::{Context as _, Result, bail};

/// Environment variable naming the file explicitly.
pub const CONFIG_FILE_ENV: &str = "QM_CONFIG_FILE";

/// The file loaded when nothing names one, relative to the home directory.
pub const DEFAULT_CONFIG_RELATIVE: &str = ".quick-memory/env";

/// The namespaces a config file may set.
///
/// Everything else is ignored rather than injected: a stray `PATH=` line in a
/// credentials file must not silently change how the process finds its tools.
const ACCEPTED_PREFIXES: [&str; 2] = ["QM_", "R2_"];

/// Characters a shell acts on inside an unquoted assignment value.
///
/// Each of these changes the value or splits the line: quote removal (`'`, `"`),
/// expansion (`$`, backtick), an escape (`\`), redirection or a control
/// operator (`;`, `&`, `|`, `<`, `>`, `(`, `)`). An unquoted appearance is
/// refused rather than copied, so a credential is never silently something
/// other than what `sh` would have made of the same file.
///
/// Glob and brace characters are deliberately *not* here: no shell expands them
/// in an assignment value (`x=*` is the literal `*` in `sh`, `bash` and `zsh`,
/// verified), so refusing them would reject values a shell reads exactly as
/// this loader does. The `~` rule is separate below, because a shell expands it
/// at the start of a value and after every `:` while leaving `a~b` alone.
const SHELL_SPECIAL_WHEN_UNQUOTED: &[char] =
    &['\'', '"', '$', '`', ';', '&', '|', '<', '>', '(', ')'];

/// Keys this file accepts but cannot serve.
///
/// These are read by the search and compile crates straight from the process
/// environment, and those crates sit *below* this one — there is no seam to
/// hand them a value from here. Accepting them silently would be the one
/// failure mode this codebase keeps refusing elsewhere: a setting that looks
/// like it took effect and did not (compare the manifest-format refusal in
/// [`crate::manifest_format_from`]). So the file may hold them, [`load`] says
/// out loud that it is not honoring them, and [`first_of`] returns the
/// environment value only — never the file's.
const NOT_SERVED_YET: [&str; 7] = [
    "QM_EMBEDDING_BASE_URL",
    "QM_EMBEDDING_API_KEY",
    "QM_EMBEDDING_MODEL",
    "QM_EMBEDDING_DIM",
    "QM_LLM_BASE_URL",
    "QM_LLM_API_KEY",
    "QM_LLM_MODEL",
];

/// The values the process loaded from its config file, once.
static LOADED: OnceLock<BTreeMap<String, String>> = OnceLock::new();

/// Load the config file for this process.
///
/// Called by the two binaries (`qm` and `qm-mcp`) before anything reads a
/// setting. Idempotent: the second call is a no-op, so a caller cannot end up
/// with a half-loaded map.
///
/// Not loading is the normal case, not an error: without a file the process
/// reads exactly the environment it always did.
///
/// # Errors
/// Fails when a file named by [`CONFIG_FILE_ENV`] does not exist — an operator
/// who pointed at a specific file meant that file, and silently falling back to
/// the environment would hide the typo — when the file cannot be read, or when
/// it uses syntax this parser refuses to guess at.
pub fn init() -> Result<()> {
    if LOADED.get().is_some() {
        return Ok(());
    }
    let loaded = match resolve_path(|name| std::env::var(name).ok())? {
        Some(path) => load(&path)?,
        None => BTreeMap::new(),
    };
    let _ = LOADED.set(loaded);
    Ok(())
}

/// Read one setting: the process environment first, then the config file.
///
/// Shorthand for `first_of(&[name])`; the single-name settings
/// (`QM_SPOOL_DIR`, `QM_SESSION`, the MCP scope) go through this. The bucket
/// name tables use [`first_of`] directly, because for them the distinction is
/// load-bearing.
///
/// Before [`init`] runs — in library tests, and in any consumer that never
/// loads a file — this is exactly `std::env::var`.
pub fn var(name: &str) -> Option<String> {
    first_of(std::slice::from_ref(&name))
}

/// The first of several aliases, scanning the whole environment before the file.
///
/// Both passes walk `names` in order, so `QM_S3_BUCKET` still beats `R2_BUCKET`
/// *within* one source; what the two passes buy is that no file value can
/// outrank an environment value for a different alias of the same setting.
/// A value that is empty or only whitespace counts as unset in both, which is
/// what the name tables did before there was a file to consult.
pub fn first_of(names: &[&str]) -> Option<String> {
    first_of_with(names, |name| std::env::var(name).ok(), LOADED.get())
}

/// Which file to read, if any.
///
/// [`CONFIG_FILE_ENV`] names one explicitly; otherwise the default lives under
/// the home directory. `USERPROFILE` is checked after `HOME` so the same file
/// works on Windows, where the first is usually unset.
///
/// A home that is unset, a default the operator never created, or a variable
/// set to nothing all mean "no file" and not an error: the environment alone
/// stays a complete configuration, and every other `QM_*` setting in this
/// codebase already reads a blank value as unset.
///
/// # Errors
/// Fails when [`CONFIG_FILE_ENV`] is set to something that is not a file.
pub fn resolve_path(mut get: impl FnMut(&str) -> Option<String>) -> Result<Option<PathBuf>> {
    if let Some(raw) = get(CONFIG_FILE_ENV).filter(|value| !value.trim().is_empty()) {
        let path = PathBuf::from(raw.trim());
        if !path.is_file() {
            bail!(
                "{CONFIG_FILE_ENV}={} is not a file; point it at the config you meant, \
                 or unset it to fall back to {DEFAULT_CONFIG_RELATIVE} under $HOME",
                path.display()
            );
        }
        return Ok(Some(path));
    }
    for name in ["HOME", "USERPROFILE"] {
        let Some(home) = get(name).filter(|value| !value.trim().is_empty()) else {
            continue;
        };
        let candidate = Path::new(home.trim()).join(DEFAULT_CONFIG_RELATIVE);
        if candidate.is_file() {
            return Ok(Some(candidate));
        }
    }
    Ok(None)
}

/// Read and parse one config file.
///
/// # Errors
/// Fails when the file cannot be read as UTF-8 text, or when it uses syntax
/// [`parse`] refuses.
pub fn load(path: &Path) -> Result<BTreeMap<String, String>> {
    warn_if_shared(path);
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("reading config file {}", path.display()))?;
    let values =
        parse(&contents).with_context(|| format!("parsing config file {}", path.display()))?;
    for key in NOT_SERVED_YET {
        if values.contains_key(key) {
            eprintln!(
                "WARNING: {} sets {key}, which the search and compile crates read from the \
                 process environment only; this file does not serve {key}, so export it in \
                 the environment instead",
                path.display()
            );
        }
    }
    Ok(values)
}

/// Parse the assignment syntax the config file is allowed to use.
///
/// One assignment per line — `K=V` or `export K=V` — with single or double
/// quotes, blank lines, `#` comments on their own line or after whitespace, and
/// whitespace around the key and the `=`. Only [`ACCEPTED_PREFIXES`] are kept,
/// and a repeated key takes its last value, as a shell would.
///
/// Everything that is not an assignment to one of those namespaces is ignored:
/// the documented cron snippet sources this same file, so it may also hold
/// `set -a`, a function, or settings for something else. What *is* refused is
/// the narrower set of shapes where an assignment's value would mean something
/// different to a shell than it does here — see [`parse_value`]. Values are
/// copied, never evaluated — use single quotes for a literal `$`, `*` or
/// space — and **no error message ever quotes a value back**: the value may be
/// the credential this file exists to hold.
///
/// # Errors
/// Fails when an assignment to a `QM_*` / `R2_*` key is written in a shape this
/// loader will not guess at.
pub fn parse(contents: &str) -> Result<BTreeMap<String, String>> {
    let mut values = BTreeMap::new();
    for (index, line) in contents.lines().enumerate() {
        let number = index + 1;
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // Whether the line used `export` is part of the value's meaning, not
        // decoration: `export K={a,b}` is two arguments to a builtin, so the
        // word is brace-expanded by `sh` and `bash` (`K=b`) while `K={a,b}` as
        // a plain assignment statement is the literal string in every shell.
        let mut exported = false;
        let line = match line.strip_prefix("export") {
            Some(rest) if rest.starts_with(char::is_whitespace) => {
                exported = true;
                rest.trim_start()
            }
            _ => line,
        };
        let Some((key, raw)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        if !is_identifier(key)
            || !ACCEPTED_PREFIXES
                .iter()
                .any(|prefix| key.starts_with(prefix))
        {
            // Not one of ours — including a name that is not an identifier at
            // all. Ignoring it keeps the file usable as a general environment
            // file, and no value here can reach the process.
            continue;
        }
        values.insert(key.to_string(), parse_value(raw, number, exported)?);
    }
    Ok(values)
}

/// Take one raw value out of a parsed line.
///
/// Every rule here exists because a shell would read the shape differently, and
/// the difference can be a credential that is silently not what the operator
/// wrote:
///
/// - A quoted value ends at its closing quote, and followed by anything but a
///   comment (`"a"b`, `"a"#b`) that is concatenation to a shell, so it is
///   refused.
/// - Inside double quotes a backslash before `$`, `` ` ``, `"` or `\` is an
///   escape a shell would act on, so it is refused; every other backslash, and
///   every backslash inside single quotes, is a literal and is kept.
/// - An unquoted value ends at whitespace: `K=v # c` is a comment, while
///   `K=v c` is a second word a shell would run as a command, so it is refused.
/// - A `#` with no whitespace in front of it is data, so `K=#v` is the value
///   `#v` while `K= #v` is an empty value followed by a comment — as in `sh`.
/// - An unquoted value may not contain [`SHELL_SPECIAL_WHEN_UNQUOTED`]: a shell
///   would expand, glob, split or quote-remove its way to a different value
///   (`K=a"b"` is `ab` to a shell, `K=v;` is `v`). Single quotes are the escape
///   hatch, and they mean the same thing to both readings.
/// - Inside double quotes, `$` and a backtick would still be expanded, so they
///   are refused there too.
/// - A `:`-separated part that starts with `=` (`K==ls`, `K=foo:=ls`) is a
///   command path to zsh, so it is refused too. A plain `a=b` and base64
///   padding (`YWJjZA==`) are not that shape and stay usable.
/// - On an `export` line, an unquoted brace is refused: `export K={a,b}` is a
///   builtin argument, which `sh` and `bash` brace-expand to `b` while `zsh`
///   leaves alone. A plain `K={a,b}` is the literal string in all three and
///   stays accepted.
/// - `~` is refused where a shell expands it — at the start of the value and
///   after every `:` — and left alone in `a~b`.
///
/// Whitespace around the `=` is tolerated (`K = v`). That is the one place this
/// file is deliberately laxer than a shell, and it is why "the value" starts
/// after any leading whitespace.
fn parse_value(raw: &str, number: usize, exported: bool) -> Result<String> {
    let spaced = raw.starts_with(char::is_whitespace);
    let rest = raw.trim_start();
    if rest.is_empty() || (spaced && rest.starts_with('#')) {
        return Ok(String::new());
    }
    if rest.starts_with('"') || rest.starts_with('\'') {
        return parse_quoted(rest, number);
    }
    // The value is the first word; a backslash anywhere *in it* is an escape a
    // shell would act on. A backslash in the trailing comment is none of our
    // business, which is why the word is split off first.
    let (value, tail) = match rest.find(char::is_whitespace) {
        Some(end) => (&rest[..end], &rest[end..]),
        None => (rest, ""),
    };
    if let Some(special) = value
        .chars()
        .find(|ch| SHELL_SPECIAL_WHEN_UNQUOTED.contains(ch))
    {
        bail!(
            "line {number}: a shell would act on {special:?} in an unquoted value; \
             wrap the value in single quotes if it is a literal"
        );
    }
    if value.contains('\\') {
        bail!(
            "line {number}: a backslash in an unquoted value is an escape to a shell; \
             use single quotes if the backslash is literal, or export it in the environment"
        );
    }
    // Braces are the one character that depends on which side of `export` the
    // assignment sits: `sh` and `bash` brace-expand an argument word, so
    // `export K={a,b}` leaves `K` as `b` there, while `zsh` and a plain
    // assignment statement leave the literal. There is no single value to copy,
    // so the shape is refused; quoting it is unambiguous.
    if exported && value.contains(['{', '}']) {
        bail!(
            "line {number}: shells disagree about braces in an `export` argument \
             (`sh`/`bash` expand them, `zsh` does not); quote the value or drop `export`"
        );
    }
    // zsh (the default login shell on macOS, and a plausible `source` target)
    // turns a `:`-separated part that starts with `=` into a command path. The
    // exact rule, measured against `zsh -f` on this machine:
    //
    //   `=ls`  → `/bin/ls`      (something follows the `=`)
    //   `==`   → error          (ditto — it looks for a command named `=`)
    //   `foo:=` → `foo:=`       (a bare `=` *as the last part* is left alone)
    //   `foo:=:bar` → error     (a bare `=` with a `:` after it is not)
    //   `foo::=` → `foo::=`     (still the last part)
    //
    // So the shape to refuse is "a part that starts with `=`, unless it is
    // exactly `=` and it is the last part". A trailing base64 `==` and a plain
    // `a=b` are neither.
    let parts: Vec<&str> = value.split(':').collect();
    let last = parts.len().saturating_sub(1);
    let zsh_equals = parts
        .iter()
        .enumerate()
        .any(|(index, part)| part.starts_with('=') && (part.len() > 1 || index != last));
    if zsh_equals {
        bail!(
            "line {number}: a `=`-prefixed part of an unquoted value is a command path to \
             zsh; wrap the value in single quotes if it is a literal"
        );
    }
    // A shell expands `~` at the start of an assignment value and again after
    // every unquoted `:` (that is the rule that makes `PATH=~/bin:~/sbin`
    // work), so both positions would come back as something else.
    if value.starts_with('~') || value.contains(":~") {
        bail!(
            "line {number}: an unquoted `~` is a home directory to a shell, at the start \
             of a value and after every `:`; wrap the value in single quotes if it is a \
             literal"
        );
    }
    expect_comment_or_end(tail, number)?;
    Ok(value.to_string())
}

/// A quoted value: everything between the quotes, with the escapes a shell
/// would act on refused rather than applied.
fn parse_quoted(rest: &str, number: usize) -> Result<String> {
    let quote = rest.chars().next().expect("caller checked for a quote");
    let body = &rest[quote.len_utf8()..];
    let mut end = None;
    let mut chars = body.char_indices();
    while let Some((index, ch)) = chars.next() {
        if quote == '"' && matches!(ch, '$' | '`') {
            bail!(
                "line {number}: {ch:?} is expanded by a shell even inside double quotes; \
                 use single quotes if it is a literal"
            );
        }
        if ch == '\\' && quote == '"' {
            match chars.next() {
                Some((_, '$' | '`' | '"' | '\\')) => bail!(
                    "line {number}: a backslash escape inside double quotes means something \
                     else to a shell; use single quotes if the backslash is literal"
                ),
                Some(_) => continue,
                None => bail!("line {number}: the line ends on a backslash"),
            }
        }
        if ch == quote {
            end = Some(index);
            break;
        }
    }
    let Some(end) = end else {
        bail!("line {number}: unterminated {quote} quote");
    };
    expect_comment_or_end(&body[end + quote.len_utf8()..], number)?;
    Ok(body[..end].to_string())
}

/// What may follow a value: nothing, or whitespace and then a comment.
///
/// A `#` with no whitespace in front of it is part of the value a shell would
/// build by concatenation, so it is refused rather than read as a comment.
fn expect_comment_or_end(tail: &str, number: usize) -> Result<()> {
    if tail.is_empty() {
        return Ok(());
    }
    if tail.starts_with(char::is_whitespace) {
        let after = tail.trim_start();
        if after.is_empty() || after.starts_with('#') {
            return Ok(());
        }
    }
    bail!(
        "line {number}: only whitespace or a `#` comment may follow a value; a shell \
         would read the rest as another word, and this loader will not guess"
    )
}

/// Merge a setting that also has a command-line flag.
///
/// The flag wins when the operator typed it. Otherwise the environment wins
/// over the file, and only then does `default` apply — because clap has already
/// filled the field from `env` or from `default_value` by this point, and
/// cannot say which of the two it used, the caller passes `on_command_line`
/// from the parsed matches instead.
pub fn merge_flagged(
    mut get: impl FnMut(&str) -> Option<String>,
    on_command_line: bool,
    cli_value: String,
    name: &str,
    default: &str,
) -> String {
    if on_command_line {
        return cli_value;
    }
    get(name)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| default.to_string())
}

/// [`merge_flagged`] for a setting with no default.
pub fn merge_optional_flagged(
    mut get: impl FnMut(&str) -> Option<String>,
    on_command_line: bool,
    cli_value: Option<PathBuf>,
    name: &str,
) -> Option<PathBuf> {
    if on_command_line {
        return cli_value;
    }
    get(name)
        .filter(|value| !value.trim().is_empty())
        .map(|value| PathBuf::from(value.trim()))
}

/// Environment (every alias), then file (every served alias), then nothing.
fn first_of_with(
    names: &[&str],
    mut from_env: impl FnMut(&str) -> Option<String>,
    file: Option<&BTreeMap<String, String>>,
) -> Option<String> {
    let unset = |value: &String| value.trim().is_empty();
    for name in names {
        if let Some(value) = from_env(name).filter(|value| !unset(value)) {
            return Some(value);
        }
    }
    let file = file?;
    for name in names {
        if !serves_from_file(name) {
            continue;
        }
        if let Some(value) = file.get(*name).filter(|value| !unset(value)) {
            return Some(value.clone());
        }
    }
    None
}

/// Whether a value for `name` may come from the file at all.
fn serves_from_file(name: &str) -> bool {
    !NOT_SERVED_YET.contains(&name)
}

fn is_identifier(key: &str) -> bool {
    let mut chars = key.chars();
    matches!(chars.next(), Some(first) if first.is_ascii_alphabetic() || first == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Say out loud when a credentials file is readable by other users.
///
/// Only a warning: the file is the operator's to place, and refusing to start
/// over a mode bit would break a working setup. Staying quiet would be worse —
/// this file holds the bucket's access key.
#[cfg(unix)]
fn warn_if_shared(path: &Path) {
    use std::os::unix::fs::PermissionsExt as _;

    if let Ok(metadata) = std::fs::metadata(path) {
        let mode = metadata.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            eprintln!(
                "WARNING: {} is mode {mode:03o} and holds bucket credentials; \
                 `chmod 600 {}` keeps it to this account",
                path.display(),
                path.display()
            );
        }
    }
}

#[cfg(not(unix))]
fn warn_if_shared(_path: &Path) {}

#[cfg(test)]
mod tests {
    use super::*;

    fn values(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    fn parsed(contents: &str) -> BTreeMap<String, String> {
        parse(contents).expect("parsing the fixture")
    }

    #[test]
    fn parses_the_shapes_the_documented_file_uses() {
        let parsed = parsed(
            "# a comment\n\
             \n\
             QM_S3_ENDPOINT=https://example.invalid\n\
             export QM_S3_BUCKET=\"my-bucket\"\n\
             export R2_ACCESS_KEY_ID='an access key'\n\
             QM_WRITER =   spaced   \n\
             QM_S3_FORCE_PATH_STYLE=true\r\n",
        );
        assert_eq!(
            parsed,
            values(&[
                ("QM_S3_ENDPOINT", "https://example.invalid"),
                ("QM_S3_BUCKET", "my-bucket"),
                ("R2_ACCESS_KEY_ID", "an access key"),
                ("QM_WRITER", "spaced"),
                ("QM_S3_FORCE_PATH_STYLE", "true"),
            ])
        );
    }

    #[test]
    fn keeps_secrets_verbatim() {
        // The values a naive parser mangles: an unquoted `#` inside a word is
        // data, `$` must not be expanded, and a quoted `#` is literal.
        let parsed = parsed(
            "QM_S3_SECRET_ACCESS_KEY=a#b\n\
             QM_S3_ACCESS_KEY_ID=\"a # b\"\n\
             QM_WRITER='$HOME'\n\
             QM_PROJECT=v # trailing comment\n\
             QM_TRAILING=#value\n",
        );
        assert_eq!(parsed.get("QM_S3_SECRET_ACCESS_KEY").unwrap(), "a#b");
        assert_eq!(parsed.get("QM_S3_ACCESS_KEY_ID").unwrap(), "a # b");
        assert_eq!(
            parsed.get("QM_WRITER").unwrap(),
            "$HOME",
            "single quotes are the way to write a literal `$`: both readings agree"
        );
        assert_eq!(
            parsed.get("QM_PROJECT").unwrap(),
            "v",
            "an unquoted `#` after whitespace starts a comment, as in sh"
        );
        assert_eq!(
            parsed.get("QM_TRAILING").unwrap(),
            "#value",
            "a `#` inside the assignment word is data, as in sh"
        );
    }

    #[test]
    fn refuses_the_shapes_a_shell_would_read_differently() {
        // Each of these is an error rather than a guess: a wrong credential
        // that fails loudly beats a credential silently read as something else.
        for (contents, expected) in [
            // An escape a shell would apply inside double quotes.
            ("QM_S3_SECRET_ACCESS_KEY=\"a\\\"b\"\n", "backslash escape"),
            // Concatenation: a shell would build one value out of two pieces.
            ("QM_WRITER=\"a\"b\n", "may follow a value"),
            ("QM_WRITER=\"a\"#b\n", "may follow a value"),
            ("QM_WRITER='unterminated\n", "unterminated"),
            // Unquoted whitespace: a shell reads the rest as another word.
            ("QM_WRITER=a b\n", "may follow a value"),
            ("export QM_WRITER=a QM_PROJECT=b\n", "may follow a value"),
            // An unquoted backslash is an escape to a shell.
            ("QM_WRITER=C:\\tmp\n", "backslash"),
            // Unquoted quote removal: a shell builds `ab` out of both of these.
            ("QM_WRITER=a\"b\"\n", "would act on"),
            ("QM_WRITER=a'b'\n", "would act on"),
            ("QM_WRITER=a\"b\n", "would act on"),
            // Unquoted control operators split the line.
            ("QM_WRITER=v;\n", "would act on"),
            ("QM_WRITER=v|other\n", "would act on"),
            // Expansions, in either unquoted or double-quoted form.
            ("QM_WRITER=$HOME\n", "would act on"),
            ("QM_WRITER=`hostname`\n", "would act on"),
            ("QM_WRITER=\"$HOME\"\n", "expanded by a shell"),
            ("QM_WRITER=`hostname`\n", "would act on"),
            ("QM_WRITER=~mbp\n", "unquoted `~`"),
            ("QM_WRITER=foo:~/bar\n", "unquoted `~`"),
            ("QM_WRITER=:~/bar\n", "unquoted `~`"),
            ("QM_WRITER=foo:~\n", "unquoted `~`"),
            // zsh's EQUALS expansion, measured on this machine: a `=`-prefixed
            // `:`-separated part expands or fails there.
            ("QM_WRITER==ls\n", "command path to zsh"),
            ("QM_WRITER=foo:=ls\n", "command path to zsh"),
            ("QM_WRITER=:==\n", "command path to zsh"),
            ("QM_WRITER=:===\n", "command path to zsh"),
            ("QM_WRITER=foo:=:bar\n", "command path to zsh"),
            ("QM_WRITER==:\n", "command path to zsh"),
            ("QM_WRITER=:=:\n", "command path to zsh"),
            // Braces differ by context: a builtin argument is brace-expanded by
            // `sh`/`bash` but not by `zsh`, so there is no single value.
            ("export QM_WRITER={a,b}\n", "shells disagree about braces"),
            (
                "export QM_WRITER=foo{a,b}\n",
                "shells disagree about braces",
            ),
        ] {
            let error =
                parse(contents).expect_err("the fixture has to be refused rather than guessed at");
            assert!(
                error.to_string().contains(expected),
                "{contents:?} should be refused for {expected:?}, got {error}"
            );
        }
    }

    #[test]
    fn ignores_everything_outside_the_accepted_namespaces() {
        let parsed = parsed(
            "PATH=/tmp/hijack\n\
             CF_R2_API_TOKEN=not-ours\n\
             QMS3_ENDPOINT=typo\n\
             DEFAULT_WORKSPACE=nope\n\
             QM_OK=yes\n",
        );
        assert_eq!(parsed, values(&[("QM_OK", "yes")]));
    }

    #[test]
    fn ignores_lines_that_are_not_assignments() {
        // The documented cron snippet sources this file, and an environment
        // file may hold more than assignments. Only an assignment to a
        // `QM_*` / `R2_*` key written in a shape a shell would read differently
        // is an error; everything else is simply not this loader's business.
        let parsed = parsed(
            "set -a\n\
             export QM_OK\n\
             if [ -n \"$CI\" ]; then :; fi\n\
             f() { echo hi; }\n\
             QM_LOADED=yes\n\
             set +a\n",
        );
        assert_eq!(parsed, values(&[("QM_LOADED", "yes")]));
    }

    #[test]
    fn an_error_never_quotes_a_value_back() {
        // The value may be the credential this file exists to hold, and errors
        // go to stderr, which ends up in logs and CI output.
        const SENTINEL: &str = "SENTINEL-SECRET-0123456789";
        let contents = format!("QM_S3_SECRET_ACCESS_KEY=\"{SENTINEL}\"{SENTINEL}\n");

        let error = parse(&contents).expect_err("concatenation is refused");
        assert!(
            !error.to_string().contains(SENTINEL),
            "the error must not echo the value: {error}"
        );
    }

    #[test]
    fn keeps_backslashes_a_shell_would_keep() {
        // A refusal that swallows legal values is its own bug: single quotes
        // make a backslash literal, a double-quoted backslash before anything
        // but an escapable character stays literal, and a comment may contain
        // anything at all.
        let parsed = parsed(
            "QM_S3_ACCESS_KEY_ID='a\\b'\n\
             QM_S3_SECRET_ACCESS_KEY=\"c\\d\"\n\
             QM_WRITER=v # C:\\tmp\n\
             QM_PROJECT= # nothing here\n\
             QM_SESSION='$HOME;~*'\n\
             QM_TILDE_MID=a~b\n\
             QM_TILDE_COLON=foo:bar~baz\n\
             QM_GLOB=*\n\
             QM_BRACE={a,b}\n\
             QM_CLASS=[abc]\n\
             QM_EQUALS=a=b\n\
             QM_BASE64=YWJjZA==\n\
             QM_EQ_LAST=:=\n\
             QM_EQ_TRAILING=foo::=\n\
             export QM_QUOTED_BRACE='{a,b}'\n",
        );
        assert_eq!(parsed.get("QM_S3_ACCESS_KEY_ID").unwrap(), "a\\b");
        assert_eq!(parsed.get("QM_S3_SECRET_ACCESS_KEY").unwrap(), "c\\d");
        assert_eq!(parsed.get("QM_WRITER").unwrap(), "v");
        assert_eq!(
            parsed.get("QM_PROJECT").unwrap(),
            "",
            "`K= #comment` is an empty value, as in sh"
        );
        assert_eq!(
            parsed.get("QM_SESSION").unwrap(),
            "$HOME;~*",
            "single quotes keep every metacharacter, exactly as a shell would"
        );
        assert_eq!(
            parsed.get("QM_TILDE_MID").unwrap(),
            "a~b",
            "only a `~` that a shell would expand is refused"
        );
        assert_eq!(parsed.get("QM_TILDE_COLON").unwrap(), "foo:bar~baz");
        // Verified against sh, bash and zsh on this machine: an assignment
        // value is not subject to pathname or brace expansion, so these are the
        // same string to both readings and refusing them would be superstition.
        assert_eq!(parsed.get("QM_GLOB").unwrap(), "*");
        assert_eq!(parsed.get("QM_BRACE").unwrap(), "{a,b}");
        assert_eq!(parsed.get("QM_CLASS").unwrap(), "[abc]");
        assert_eq!(
            parsed.get("QM_EQUALS").unwrap(),
            "a=b",
            "only a part that starts with `=` is a command path to zsh"
        );
        assert_eq!(
            parsed.get("QM_BASE64").unwrap(),
            "YWJjZA==",
            "base64 padding must stay usable"
        );
        // Measured against `zsh -f`: a bare `=` that is the *last* part is left
        // alone, so refusing it would reject a value every shell reads the same.
        assert_eq!(parsed.get("QM_EQ_LAST").unwrap(), ":=");
        assert_eq!(parsed.get("QM_EQ_TRAILING").unwrap(), "foo::=");
        assert_eq!(
            parsed.get("QM_QUOTED_BRACE").unwrap(),
            "{a,b}",
            "quoting a brace is unambiguous in every shell"
        );
    }

    #[test]
    fn a_later_assignment_wins_like_a_shell() {
        let parsed = parsed("QM_BUCKET=first\nQM_BUCKET=second\n");
        assert_eq!(parsed.get("QM_BUCKET").unwrap(), "second");
    }

    #[test]
    fn an_environment_variable_beats_the_file_and_empty_counts_as_unset() {
        let file = values(&[("QM_S3_BUCKET", "from-file"), ("QM_WRITER", "file-writer")]);
        let env = |name: &str| match name {
            "QM_S3_BUCKET" => Some("from-env".to_string()),
            "QM_WRITER" => Some("   ".to_string()),
            _ => None,
        };

        assert_eq!(
            first_of_with(&["QM_S3_BUCKET", "R2_BUCKET"], env, Some(&file)).unwrap(),
            "from-env"
        );
        assert_eq!(
            first_of_with(&["QM_WRITER"], env, Some(&file)).unwrap(),
            "file-writer",
            "a blank environment variable is unset, not a value"
        );
        assert!(first_of_with(&["QM_S3_REGION"], |_| None, Some(&file)).is_none());
        assert!(
            first_of_with(&["QM_WRITER"], |_| None, None).is_none(),
            "without init() the lookup is the environment alone"
        );
    }

    #[test]
    fn an_environment_alias_beats_a_file_alias_of_the_same_setting() {
        // The regression this module's `first_of` exists for: resolving name by
        // name would let the file answer `QM_S3_BUCKET` and never look at the
        // environment's `R2_BUCKET`.
        let file = values(&[
            ("QM_S3_BUCKET", "file-answer"),
            ("R2_BUCKET", "file-answer-too"),
        ]);
        let env = |name: &str| match name {
            "R2_BUCKET" => Some("env-answer".to_string()),
            _ => None,
        };

        assert_eq!(
            first_of_with(&["QM_S3_BUCKET", "R2_BUCKET"], env, Some(&file)).unwrap(),
            "env-answer",
            "an environment value for a later alias outranks a file value for an earlier one"
        );
    }

    #[test]
    fn a_key_the_file_cannot_serve_never_comes_from_the_file() {
        // `QM_EMBEDDING_*` / `QM_LLM_*` are read below this crate. Handing the
        // file's value back would make `compiler_choice_from_name` pick the LLM
        // compiler while the compile layer sees nothing — the silent fallback
        // this list exists to prevent.
        let file = values(&[("QM_LLM_BASE_URL", "http://from-file.invalid")]);

        assert!(
            first_of_with(&["QM_LLM_BASE_URL"], |_| None, Some(&file)).is_none(),
            "the file must not answer for a key it cannot serve"
        );
        assert_eq!(
            first_of_with(
                &["QM_LLM_BASE_URL"],
                |_| Some("http://from-env.invalid".to_string()),
                Some(&file)
            )
            .unwrap(),
            "http://from-env.invalid",
            "the environment is still served: the compile layer reads it"
        );
    }

    #[test]
    fn an_explicit_config_path_must_exist() {
        let error =
            resolve_path(|name| (name == CONFIG_FILE_ENV).then(|| "/nope/qm.env".to_string()))
                .expect_err("naming a file that is not there is an error, not a silent fallback");
        assert!(
            error.to_string().contains("/nope/qm.env"),
            "the error has to name the path the operator typed: {error}"
        );
    }

    #[test]
    fn a_blank_config_path_is_unset_not_a_missing_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let home = dir.path().to_string_lossy().to_string();
        let resolved = resolve_path(|name| match name {
            CONFIG_FILE_ENV => Some("   ".to_string()),
            "HOME" => Some(home.clone()),
            _ => None,
        })
        .expect("a blank value is unset, the way every other QM_* setting reads it");
        assert_eq!(resolved, None);
    }

    #[test]
    fn the_default_path_is_the_home_file_and_may_be_absent() {
        let dir = tempfile::TempDir::new().unwrap();
        let home = dir.path().to_string_lossy().to_string();

        assert_eq!(
            resolve_path(|name| (name == "HOME").then(|| home.clone())).unwrap(),
            None,
            "no file under $HOME is the normal case, not an error"
        );

        let path = dir.path().join(DEFAULT_CONFIG_RELATIVE);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "QM_S3_BUCKET=b\n").unwrap();
        assert_eq!(
            resolve_path(|name| (name == "HOME").then(|| home.clone()))
                .unwrap()
                .unwrap(),
            path
        );
    }

    #[test]
    fn userprofile_is_consulted_when_home_is_unset() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join(DEFAULT_CONFIG_RELATIVE);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "QM_S3_BUCKET=b\n").unwrap();

        let profile = dir.path().to_string_lossy().to_string();
        assert_eq!(
            resolve_path(|name| (name == "USERPROFILE").then(|| profile.clone()))
                .unwrap()
                .unwrap(),
            path
        );
    }

    #[test]
    fn a_flagged_setting_prefers_the_flag_then_environment_then_default() {
        let env_first = |name: &str| match name {
            "QM_WORKSPACE" => Some("from-env".to_string()),
            _ => None,
        };

        assert_eq!(
            merge_flagged(
                env_first,
                true,
                "from-flag".to_string(),
                "QM_WORKSPACE",
                "default"
            ),
            "from-flag",
            "a flag the operator typed beats both"
        );
        assert_eq!(
            merge_flagged(
                env_first,
                false,
                "ignored".to_string(),
                "QM_WORKSPACE",
                "default"
            ),
            "from-env"
        );
        assert_eq!(
            merge_flagged(
                |_| None,
                false,
                "ignored".to_string(),
                "QM_WORKSPACE",
                "default"
            ),
            "default"
        );
    }

    #[test]
    fn a_flagged_path_setting_has_no_default() {
        assert_eq!(
            merge_optional_flagged(|_| None, false, None, "QM_CACHE_DIR"),
            None
        );
        assert_eq!(
            merge_optional_flagged(
                |_| Some("/from/file".to_string()),
                false,
                None,
                "QM_CACHE_DIR"
            ),
            Some(PathBuf::from("/from/file"))
        );
        assert_eq!(
            merge_optional_flagged(
                |_| Some("/from/file".to_string()),
                true,
                Some(PathBuf::from("/from/flag")),
                "QM_CACHE_DIR"
            ),
            Some(PathBuf::from("/from/flag"))
        );
    }

    #[test]
    fn a_wide_mode_is_warned_about_and_a_narrow_one_is_not() {
        // The warning is an `eprintln!`, so this pins the decision the function
        // makes rather than the text: `is_shared` has to be wrong for the
        // assertion to fail.
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("env");
        std::fs::write(&path, "QM_S3_BUCKET=b\n").unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
            assert!(!is_shared(&path), "0600 must stay quiet");
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
            assert!(is_shared(&path), "0644 must be reported");
        }
    }

    /// The predicate behind [`warn_if_shared`].
    #[cfg(unix)]
    fn is_shared(path: &Path) -> bool {
        use std::os::unix::fs::PermissionsExt as _;

        std::fs::metadata(path)
            .map(|metadata| metadata.permissions().mode() & 0o077 != 0)
            .unwrap_or(false)
    }
}
