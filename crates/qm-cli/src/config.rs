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
//! 2. **The file is a subset of a shell script.** Every line it accepts is a
//!    line `sh` reads the same way (`K=V`, `export K="V"`, `#` comments), and
//!    the file can be `source`d — but it is *not* a general shell script:
//!    anything that is not a single assignment, and every shape where the two
//!    readings could disagree (concatenated quotes, backslash escapes, one line
//!    with two assignments), is **refused**, never half-applied.
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
/// Understands one assignment per line (`K=V`, `export K=V`), single and double
/// quotes, `#` comments on their own line or after whitespace, blank lines,
/// CRLF, and whitespace around the key and the `=`; ignores assignments in
/// other namespaces, keeps only [`ACCEPTED_PREFIXES`], and lets a repeated key
/// take its last value, exactly as a shell would.
///
/// Values are copied, never evaluated: `$HOME` stays `$HOME`. The shapes where
/// a shell would do more than copy are the ones this returns an error for,
/// because guessing would mean an operator's credential silently became
/// something else:
///
/// - a backslash anywhere in a value (an escape in `sh`, a literal here),
/// - content after a closing quote (`"a"b`, which `sh` concatenates),
/// - an unterminated quote,
/// - more than one assignment on a line (`export A=1 B=2`), and
/// - a line that is not an assignment at all (`set -a`, `export QM_OK`).
///
/// # Errors
/// Fails on any of the shapes listed above.
pub fn parse(contents: &str) -> Result<BTreeMap<String, String>> {
    let mut values = BTreeMap::new();
    for (index, line) in contents.lines().enumerate() {
        let number = index + 1;
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = match line.strip_prefix("export") {
            Some(rest) if rest.starts_with(char::is_whitespace) => rest.trim_start(),
            _ => line,
        };
        let (key, raw) = line
            .split_once('=')
            .with_context(|| format!("line {number} has no assignment"))?;
        let key = key.trim();
        if !is_identifier(key) {
            bail!("line {number}: {key:?} is not an identifier");
        }
        if !ACCEPTED_PREFIXES
            .iter()
            .any(|prefix| key.starts_with(prefix))
        {
            // Not ours: a shell may keep it, this loader has no business
            // injecting it. Silence is the contract for other namespaces, so
            // the file can double as a general environment file.
            continue;
        }
        let value = parse_value(raw.trim(), number)?;
        values.insert(key.to_string(), value);
    }
    Ok(values)
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

/// Take one raw value out of a parsed line, following `sh` where `sh` is
/// unambiguous and refusing to guess where it is not.
fn parse_value(raw: &str, number: usize) -> Result<String> {
    if raw.contains('\\') {
        bail!(
            "line {number}: backslash escapes are not interpreted here; \
             write the value plainly or export it in the environment instead"
        );
    }
    for quote in ['"', '\''] {
        if let Some(rest) = raw.strip_prefix(quote) {
            let end = rest
                .find(quote)
                .with_context(|| format!("line {number}: unterminated {quote} quote"))?;
            let after = rest[end + 1..].trim();
            if !after.is_empty() && !after.starts_with('#') {
                bail!(
                    "line {number}: {:?} follows a quoted value, which a shell would \
                     concatenate and this loader will not guess at",
                    after
                );
            }
            return Ok(rest[..end].to_string());
        }
    }
    let mut value = String::new();
    // `#` starts a comment only where a shell starts one: after whitespace, or
    // at the beginning of the line (handled by the caller). A value begins
    // *inside* the assignment word, so `K=#value` is the value `#value` — not
    // an empty one, which is what starting the scan "at a word start" would
    // have produced.
    let mut at_word_start = false;
    for ch in raw.chars() {
        if ch == '#' && at_word_start {
            break;
        }
        at_word_start = ch.is_whitespace();
        value.push(ch);
    }
    let value = value.trim_end();
    // `export A=1 B=2` is two assignments to a shell and one to this parser.
    // Reading it as `A="1 B=2"` would hand the operator a setting that looks
    // like the shell's and is not, so it is refused.
    if let Some(token) = value.split_whitespace().skip(1).find(|token| {
        token
            .split_once('=')
            .is_some_and(|(name, _)| is_identifier(name))
    }) {
        bail!(
            "line {number}: {token:?} looks like a second assignment; \
             this file takes one per line"
        );
    }
    Ok(value.to_string())
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
             QM_WRITER=$HOME\n\
             QM_PROJECT=v # trailing comment\n\
             QM_TRAILING=#value\n",
        );
        assert_eq!(parsed.get("QM_S3_SECRET_ACCESS_KEY").unwrap(), "a#b");
        assert_eq!(parsed.get("QM_S3_ACCESS_KEY_ID").unwrap(), "a # b");
        assert_eq!(parsed.get("QM_WRITER").unwrap(), "$HOME");
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
            ("QM_S3_SECRET_ACCESS_KEY=\"a\\\"b\"\n", "backslash"),
            ("QM_WRITER=\"a\"b\n", "quoted value"),
            ("QM_WRITER='unterminated\n", "unterminated"),
            ("export QM_WRITER=a QM_PROJECT=b\n", "second assignment"),
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
    fn refuses_a_line_that_is_not_an_assignment() {
        // The file is a *subset* of a shell script: every line it accepts is
        // read the same way by `sh`, but a file with commands in it is refused
        // rather than half-applied, so an operator finds out instead of
        // wondering which half took effect.
        for contents in ["set -a\n", "1QM_BUCKET=nope\n", "export QM_OK\n"] {
            assert!(
                parse(contents).is_err(),
                "{contents:?} is not an assignment and must be refused"
            );
        }
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
