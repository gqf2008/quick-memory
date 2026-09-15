//! Reader for Quickwit's `.split` container.
//!
//! Quickwit writes each split as one file that hides a small static filesystem:
//!
//! ```text
//! [ file bytes concatenated ]
//! [ FileMetadata: 8-byte header + JSON {files: {name: {start, end}}} ]
//! [ FileMetadata length: u64 LE ]
//! [ hotcache ]
//! [ hotcache length: u64 LE ]
//! ```
//!
//! The layout is public (`docs/internals/split-format.md` upstream), so a
//! machine that has no Quickwit cluster can still read a split: parse the
//! footer, slice the files out, and open the result with tantivy. That is what
//! keeps "any machine can search the full corpus" true even when the index was
//! produced by a Quickwit indexer elsewhere.
//!
//! Two things this module deliberately does not do: it does not use the
//! hotcache (an optimisation, not part of the tantivy index), and it does not
//! assume the offsets are valid — every range is bounds-checked before use.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

/// Magic number of the versioned component header wrapping the file metadata.
pub const FILE_METADATA_MAGIC: u32 = 403_881_646;
/// Supported version of that header.
pub const FILE_METADATA_VERSION: u32 = 1;
/// Bytes of each footer length field.
const FOOTER_LENGTH_BYTES: usize = 8;
/// Bytes of the versioned component header.
const COMPONENT_HEADER_BYTES: usize = 8;

#[derive(Debug, Deserialize)]
struct Offsets {
    start: u64,
    end: u64,
}

#[derive(Debug, Deserialize)]
struct FileMetadata {
    files: HashMap<String, Offsets>,
}

/// What one extraction produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractReport {
    /// Files written, sorted.
    pub files: Vec<String>,
    /// Size of the hotcache that was skipped, in bytes.
    pub hotcache_bytes: usize,
}

/// Whether `bytes` looks like a Quickwit split container.
#[must_use]
pub fn is_split(bytes: &[u8]) -> bool {
    parse_footer(bytes).is_ok()
}

/// Extract the files of a split into `dest`.
///
/// # Errors
/// Fails when the footer is truncated, the metadata header is wrong, a JSON
/// offset map cannot be parsed, or a range points outside the file.
pub fn extract_split(bytes: &[u8], dest: &Path) -> Result<ExtractReport> {
    let (metadata, hotcache_bytes) = parse_footer(bytes)?;
    std::fs::create_dir_all(dest).with_context(|| format!("creating {}", dest.display()))?;

    let mut files = Vec::with_capacity(metadata.files.len());
    for (name, offsets) in &metadata.files {
        // The name comes from the file we are reading, so treat it as hostile:
        // a split must never be able to write outside the cache directory.
        if name.is_empty()
            || name.contains('/')
            || name.contains('\\')
            || name == "."
            || name == ".."
        {
            bail!("refusing unsafe file name in split metadata: {name:?}");
        }
        let start = usize::try_from(offsets.start).context("offset too large")?;
        let end = usize::try_from(offsets.end).context("offset too large")?;
        if start > end || end > bytes.len() {
            bail!(
                "split metadata range for {name:?} is {start}..{end}, file is {} bytes",
                bytes.len()
            );
        }
        std::fs::write(dest.join(name), &bytes[start..end])
            .with_context(|| format!("writing {name}"))?;
        files.push(name.clone());
    }
    files.sort();
    Ok(ExtractReport {
        files,
        hotcache_bytes,
    })
}

/// Parse the footer, returning the metadata and the hotcache length.
fn parse_footer(bytes: &[u8]) -> Result<(FileMetadata, usize)> {
    let len = bytes.len();
    if len < 2 * FOOTER_LENGTH_BYTES + COMPONENT_HEADER_BYTES {
        bail!("file is too small to be a split ({len} bytes)");
    }

    // Every length here comes from an untrusted file, so all arithmetic is
    // checked: a corrupt footer must produce an error, never an overflow.
    let hotcache_len = read_len(bytes, len - FOOTER_LENGTH_BYTES)?;
    let metadata_len_end = len
        .checked_sub(FOOTER_LENGTH_BYTES)
        .and_then(|end| end.checked_sub(hotcache_len))
        .with_context(|| format!("hotcache length {hotcache_len} exceeds the file"))?;
    if metadata_len_end < FOOTER_LENGTH_BYTES + COMPONENT_HEADER_BYTES {
        bail!("hotcache length {hotcache_len} leaves no room for file metadata");
    }
    let metadata_len = read_len(bytes, metadata_len_end - FOOTER_LENGTH_BYTES)?;
    let metadata_start = metadata_len_end
        .checked_sub(FOOTER_LENGTH_BYTES)
        .and_then(|end| end.checked_sub(metadata_len))
        .with_context(|| format!("metadata length {metadata_len} exceeds the file"))?;
    let metadata_bytes = &bytes[metadata_start..metadata_len_end - FOOTER_LENGTH_BYTES];

    if metadata_bytes.len() < COMPONENT_HEADER_BYTES {
        bail!("file metadata is truncated");
    }
    let magic = u32::from_le_bytes(metadata_bytes[0..4].try_into().expect("4 bytes"));
    let version = u32::from_le_bytes(metadata_bytes[4..8].try_into().expect("4 bytes"));
    if magic != FILE_METADATA_MAGIC {
        bail!("not a quickwit split: metadata magic {magic} != {FILE_METADATA_MAGIC}");
    }
    if version != FILE_METADATA_VERSION {
        bail!("unsupported split metadata version {version}");
    }

    let metadata: FileMetadata = serde_json::from_slice(&metadata_bytes[COMPONENT_HEADER_BYTES..])
        .context("parsing split file metadata")?;
    Ok((metadata, hotcache_len))
}

fn read_len(bytes: &[u8], at: usize) -> Result<usize> {
    let slice = bytes
        .get(at..at + FOOTER_LENGTH_BYTES)
        .context("footer length field is out of bounds")?;
    let value = u64::from_le_bytes(slice.try_into().expect("8 bytes"));
    usize::try_from(value).context("footer length does not fit in usize")
}

/// Helpers shared with the crate's other tests.
#[cfg(test)]
pub(crate) mod tests_support {
    use super::tests::bundle;

    /// Wrap an index directory's files into a `.split` container.
    pub(crate) fn bundle_index(files: &[(String, Vec<u8>)]) -> Vec<u8> {
        let leaked: Vec<(&str, Vec<u8>)> = files
            .iter()
            .map(|(name, bytes)| (name.as_str(), bytes.clone()))
            .collect();
        bundle(&leaked)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    /// Build a container in the documented format. Mirrors what a Quickwit
    /// indexer writes, so the reader is exercised against the real layout
    /// rather than against its own idea of it.
    pub(crate) fn bundle(files: &[(&str, Vec<u8>)]) -> Vec<u8> {
        let mut body = Vec::new();
        let mut offsets = String::from("{\"files\":{");
        for (index, (name, bytes)) in files.iter().enumerate() {
            let start = body.len();
            body.extend_from_slice(bytes);
            if index > 0 {
                offsets.push(',');
            }
            offsets.push_str(&format!(
                "\"{name}\":{{\"start\":{start},\"end\":{}}}",
                body.len()
            ));
        }
        offsets.push_str("}}");

        let mut metadata = Vec::new();
        metadata.extend_from_slice(&FILE_METADATA_MAGIC.to_le_bytes());
        metadata.extend_from_slice(&FILE_METADATA_VERSION.to_le_bytes());
        metadata.extend_from_slice(offsets.as_bytes());

        let mut out = body;
        out.extend_from_slice(&metadata);
        out.extend_from_slice(&(metadata.len() as u64).to_le_bytes());
        let hotcache = b"hotcache-bytes".to_vec();
        out.extend_from_slice(&hotcache);
        out.extend_from_slice(&(hotcache.len() as u64).to_le_bytes());
        out
    }

    #[test]
    fn extracts_files_from_a_split_container() {
        let split = bundle(&[
            ("meta.json", b"{\"segments\":[]}".to_vec()),
            ("abc.idx", vec![1, 2, 3, 4]),
        ]);
        assert!(is_split(&split));
        let dest = tempfile::TempDir::new().unwrap();
        let report = extract_split(&split, dest.path()).unwrap();
        assert_eq!(report.files, vec!["abc.idx", "meta.json"]);
        assert_eq!(report.hotcache_bytes, "hotcache-bytes".len());
        assert_eq!(
            std::fs::read(dest.path().join("abc.idx")).unwrap(),
            vec![1, 2, 3, 4]
        );
        assert_eq!(
            std::fs::read(dest.path().join("meta.json")).unwrap(),
            b"{\"segments\":[]}"
        );
    }

    #[test]
    fn rejects_truncated_and_hostile_splits() {
        assert!(!is_split(b"tiny"));
        let mut split = bundle(&[("meta.json", b"{}".to_vec())]);
        let len = split.len();
        split.truncate(len / 2);
        assert!(!is_split(&split));

        // A name that tries to escape the cache directory must be refused.
        let hostile = bundle(&[("../escape", b"boom".to_vec())]);
        let dest = tempfile::TempDir::new().unwrap();
        assert!(extract_split(&hostile, dest.path()).is_err());
    }

    /// The S0/S2 question in its sharpest form: a split produced by another
    /// process is extracted and searched with nothing but tantivy.
    #[test]
    fn a_bundled_index_is_searchable_after_extraction() {
        let build = tempfile::TempDir::new().unwrap();
        let docs = vec![super::super::PageDoc {
            workspace_id: "acme".into(),
            project_id: "ai-memory".into(),
            path: "notes/quickwit.md".into(),
            page_id: "p1".into(),
            title: "Quickwit splits".into(),
            body: "tantivy splits stored in object storage".into(),
            updated_at_ms: 1,
        }];
        super::super::build_index(build.path(), &docs).unwrap();

        // Bundle every file of the built index, the way an indexer would.
        let mut files: Vec<(String, Vec<u8>)> = std::fs::read_dir(build.path())
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                let name = entry.file_name().to_string_lossy().to_string();
                (name, std::fs::read(entry.path()).unwrap())
            })
            .filter(|(name, _)| !name.ends_with(".lock"))
            .collect();
        files.sort();
        let refs: Vec<(&str, Vec<u8>)> = files
            .into_iter()
            .map(|(name, bytes)| (Box::leak(name.into_boxed_str()) as &str, bytes))
            .collect();
        let split = bundle(&refs);

        let cache = tempfile::TempDir::new().unwrap();
        extract_split(&split, cache.path()).unwrap();
        let hits = super::super::search(cache.path(), "tantivy", 5).unwrap();
        assert_eq!(hits.len(), 1, "{hits:?}");
        assert_eq!(hits[0].path, "notes/quickwit.md");
    }

    #[test]
    fn offset_map_shape_matches_quickwit() {
        // Guard against a serde representation change: the offsets are read as
        // {"start": .., "end": ..} in the JSON produced by Quickwit.
        let json = r#"{"files":{"a.idx":{"start":0,"end":4}}}"#;
        let metadata: FileMetadata = serde_json::from_str(json).unwrap();
        assert_eq!(metadata.files["a.idx"].start, 0);
        assert_eq!(metadata.files["a.idx"].end, 4);
        let _: PathBuf = PathBuf::from("a.idx");
    }
}
