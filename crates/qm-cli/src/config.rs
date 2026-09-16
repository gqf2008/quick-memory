//! The machine-local config file.
//!
//! `qm` and `qm-mcp` read their settings from the environment. An operator who
//! wants the same bucket on every command would otherwise have to inject it
//! from a shell profile, a cron line and an MCP client configuration
//! separately, so both binaries also read one file under the user's home.
//!
//! Two rules shape the whole module:
//!
//! 1. **The environment always wins.** A variable that is set in the process
//!    environment beats the same key in the file; the file only fills what the
//!    environment left empty. Anything else would make `QM_S3_BUCKET=... qm
//!    status` silently read a different bucket than the operator just named.
//! 2. **The file is also a shell script.** Every line this parser understands
//!    is a line `sh` would read the same way (`K=V`, `export K="V"`, `#`
//!    comments), because the same file is what the documented cron snippet
//!    sources. Nothing here evaluates a shell; the two readings have to agree
//!    by construction, and the cases where they could drift are pinned by
//!    tests.

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

/// Keys this file accepts but cannot serve yet.
///
/// `QM_EMBEDDING_*` and `QM_LLM_*` are read by the search and compile crates
/// straight from the process environment, and those crates sit *below* this
/// one. Accepting them silently would be the one failure mode this codebase
/// keeps refusing elsewhere: a setting that looks like it took effect and did
/// not (compare the manifest-format refusal in [`crate::manifest_format_from`]).
/// So they load, and the load says out loud that it is not honoring them.
const NOT_SERVED_YET: [&str; 6] = [
    "QM_EMBEDDING_BASE_URL",
    "QM_EMBEDDING_API_KEY",
    "QM_EMBEDDING_MODEL",
    "QM_EMBEDDING_DIM",
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
/// the environment would hide the typo — or when the file cannot be read.
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
/// This is the single lookup the bucket client, the bucket identity and the
/// scope all share, so they cannot end up consulting different sources. A value
/// that is empty or only whitespace counts as unset on both sides — that is
/// what the name tables did before there was a file to consult.
///
/// Before [`init`] runs — in library tests, and in any consumer that never
/// loads a file — this is exactly `std::env::var`.
pub fn var(name: &str) -> Option<String> {
    resolve(name, std::env::var(name).ok(), LOADED.get())
}

/// Which file to read, if any.
///
/// [`CONFIG_FILE_ENV`] names one explicitly; otherwise the default lives under
/// the home directory. `USERPROFILE` is checked after `HOME` so the same file
/// works on Windows, where the first is usually unset.
///
/// A home that is unset, or a default the operator never created, means "no
/// file" and not an error: the environment alone stays a complete
/// configuration.
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
/// Fails when the file cannot be read as UTF-8 text.
pub fn load(path: &Path) -> Result<BTreeMap<String, String>> {
    warn_if_shared(path);
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("reading config file {}", path.display()))?;
    let values = parse(&contents);
    for key in NOT_SERVED_YET {
        if values.contains_key(key) {
            eprintln!(
                "WARNING: {} sets {key}, which the search and compile crates read from the \
                 process environment only; this file does not serve {key} yet, so export it \
                 in the environment instead",
                path.display()
            );
        }
    }
    Ok(values)
}

/// Parse the assignment syntax the config file is allowed to use.
///
/// Understands `K=V`, `export K=V`, single and double quotes, `#` comments on
/// their own line, blank lines, CRLF, and whitespace around the key and the
/// `=`; ignores everything else. Only [`ACCEPTED_PREFIXES`] are kept, and a
/// repeated key takes its last value, exactly as a shell would.
///
/// Quoting follows `sh` for the cases that matter to a credentials file: a
/// quoted value ends at its closing quote, an unquoted `#` starts a comment
/// only at the start of a word, and an unterminated quote is taken to the end
/// of the line rather than dropped. Backslash escapes and command substitution
/// are *not* interpreted — a value is copied, never evaluated — so a secret
/// containing `$` or a backslash survives verbatim.
pub fn parse(contents: &str) -> BTreeMap<String, String> {
    let mut values = BTreeMap::new();
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = match line.strip_prefix("export") {
            Some(rest) if rest.starts_with(char::is_whitespace) => rest.trim_start(),
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
            continue;
        }
        values.insert(key.to_string(), parse_value(raw.trim()));
    }
    values
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

/// Environment, then file, then nothing.
fn resolve(
    name: &str,
    from_env: Option<String>,
    file: Option<&BTreeMap<String, String>>,
) -> Option<String> {
    let unset = |value: &String| value.trim().is_empty();
    if let Some(value) = from_env.filter(|value| !unset(value)) {
        return Some(value);
    }
    file?.get(name).filter(|value| !unset(value)).cloned()
}

fn is_identifier(key: &str) -> bool {
    let mut chars = key.chars();
    matches!(chars.next(), Some(first) if first.is_ascii_alphabetic() || first == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Take one raw value out of a parsed line, following `sh` quoting.
fn parse_value(raw: &str) -> String {
    for quote in ['"', '\''] {
        if let Some(rest) = raw.strip_prefix(quote) {
            return match rest.find(quote) {
                Some(end) => rest[..end].to_string(),
                // An unterminated quote swallows the rest of the line, which is
                // what `sh` does too. Refusing the whole file would be worse:
                // one missing quote would take every other setting with it.
                None => rest.to_string(),
            };
        }
    }
    let mut value = String::new();
    let mut at_word_start = true;
    for ch in raw.chars() {
        if ch == '#' && at_word_start {
            break;
        }
        at_word_start = ch.is_whitespace();
        value.push(ch);
    }
    value.trim_end().to_string()
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

    #[test]
    fn parses_the_shapes_the_documented_file_uses() {
        let parsed = parse(
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
        // The values below are the ones a naive shell-subset parser mangles:
        // an unquoted `#` inside a word is data, `$` must not be expanded, and
        // a backslash must survive unmangled.
        let parsed = parse(
            "QM_S3_SECRET_ACCESS_KEY=a#b\n\
             QM_S3_ACCESS_KEY_ID=\"a # b\"\n\
             QM_WRITER=$HOME\n\
             R2_BUCKET=back\\slash\n\
             QM_PROJECT=v # trailing comment\n",
        );
        assert_eq!(parsed.get("QM_S3_SECRET_ACCESS_KEY").unwrap(), "a#b");
        assert_eq!(parsed.get("QM_S3_ACCESS_KEY_ID").unwrap(), "a # b");
        assert_eq!(parsed.get("QM_WRITER").unwrap(), "$HOME");
        assert_eq!(parsed.get("R2_BUCKET").unwrap(), "back\\slash");
        assert_eq!(
            parsed.get("QM_PROJECT").unwrap(),
            "v",
            "an unquoted `#` after whitespace starts a comment, as in sh"
        );
    }

    #[test]
    fn ignores_everything_outside_the_accepted_namespaces() {
        let parsed = parse(
            "PATH=/tmp/hijack\n\
             CF_R2_API_TOKEN=not-ours\n\
             QMS3_ENDPOINT=typo\n\
             1QM_BUCKET=nope\n\
             DEFAULT_WORKSPACE=nope\n\
             QM_OK=yes\n",
        );
        assert_eq!(parsed, values(&[("QM_OK", "yes")]));
    }

    #[test]
    fn a_later_assignment_wins_like_a_shell() {
        let parsed = parse("QM_BUCKET=first\nQM_BUCKET=second\n");
        assert_eq!(parsed.get("QM_BUCKET").unwrap(), "second");
    }

    #[test]
    fn an_environment_variable_beats_the_file_and_empty_counts_as_unset() {
        let file = values(&[("QM_S3_BUCKET", "from-file"), ("QM_WRITER", "file-writer")]);

        assert_eq!(
            resolve("QM_S3_BUCKET", Some("from-env".to_string()), Some(&file)).unwrap(),
            "from-env"
        );
        assert_eq!(
            resolve("QM_WRITER", Some("   ".to_string()), Some(&file)).unwrap(),
            "file-writer",
            "a blank environment variable is unset, not a value"
        );
        assert_eq!(
            resolve("QM_WRITER", None, Some(&file)).unwrap(),
            "file-writer"
        );
        assert!(resolve("QM_S3_REGION", None, Some(&file)).is_none());
        assert!(
            resolve("QM_WRITER", None, None).is_none(),
            "without init() the lookup is the environment alone"
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
        let file = values(&[("QM_WORKSPACE", "from-file")]);
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
                |name: &str| resolve(name, None, Some(&file)),
                false,
                "ignored".to_string(),
                "QM_WORKSPACE",
                "default"
            ),
            "from-file"
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
        // The warning is a `eprintln!`, so this pins the decision the function
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
