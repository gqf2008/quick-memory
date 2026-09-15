//! Pure domain types and the object-key layout. No IO, no async.
//!
//! Everything here is a boundary type: identifiers and paths are validated
//! once, at construction, and then reused. Object keys are derived from them
//! so that two machines can never disagree about where a byte lives.

use std::fmt;

use serde::{Deserialize, Serialize};

mod model;

pub use model::{
    CatalogHead, IndexCatalog, Lease, MANIFEST_SCHEMA, Manifest, PageEntry, PageVersion,
    SplitEntry, Tombstone, WalEntry, content_hash, derive_page_id,
};

/// Version prefix of the object layout. Bumping it is a breaking change.
pub const KEY_ROOT: &str = "v1";

/// Maximum length of a single identifier segment in an object key.
const MAX_ID_LEN: usize = 128;

/// Errors raised while constructing boundary types.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CoreError {
    /// An identifier was empty or whitespace-only.
    #[error("identifier must not be empty")]
    EmptyId,
    /// An identifier contained a character that is not key-safe.
    #[error("identifier {0:?} contains a forbidden character")]
    InvalidId(String),
    /// A page path could not be used as a portable object key.
    #[error("page path {0:?} is not portable")]
    InvalidPagePath(String),
}

fn validate_id(raw: &str, kind: &str) -> Result<String, CoreError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(CoreError::EmptyId);
    }
    if trimmed.len() > MAX_ID_LEN
        || !trimmed
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        || trimmed == "."
        || trimmed == ".."
    {
        return Err(CoreError::InvalidId(format!("{kind}: {raw:?}")));
    }
    Ok(trimmed.to_string())
}

macro_rules! id_type {
    ($name:ident, $kind:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            /// Validate and construct the identifier.
            pub fn new(raw: impl AsRef<str>) -> Result<Self, CoreError> {
                validate_id(raw.as_ref(), $kind).map(Self)
            }

            /// Borrow the validated value.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl TryFrom<String> for $name {
            type Error = CoreError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}

id_type!(WorkspaceId, "workspace");
id_type!(ProjectId, "project");
id_type!(WriterId, "writer");
id_type!(PageId, "page");
id_type!(ObservationId, "observation");

impl PageId {
    /// Construct from a value this crate derived, bypassing validation.
    ///
    /// Only for values whose charset is known by construction (a hex digest);
    /// everything user-supplied goes through `PageId::new`.
    pub(crate) fn trusted(raw: String) -> Self {
        Self(raw)
    }
}

/// A page path relative to its project, e.g. `notes/raft.md`.
///
/// Rejects traversal, absolute paths, backslashes and control characters: the
/// value goes straight into an object key and a local cache path.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct PagePath(String);

impl PagePath {
    /// Validate and normalise a page path.
    pub fn new(raw: impl AsRef<str>) -> Result<Self, CoreError> {
        let raw = raw.as_ref().trim();
        let raw = raw.strip_prefix('/').unwrap_or(raw);
        let invalid = raw.is_empty()
            || raw.ends_with('/')
            || raw.contains('\\')
            || raw.chars().any(|c| c.is_control())
            || raw
                .split('/')
                .any(|seg| seg.is_empty() || seg == "." || seg == "..");
        if invalid {
            return Err(CoreError::InvalidPagePath(raw.to_string()));
        }
        Ok(Self(raw.to_string()))
    }

    /// Borrow the validated path.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PagePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for PagePath {
    type Error = CoreError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<PagePath> for String {
    fn from(value: PagePath) -> Self {
        value.0
    }
}

/// Derives every object key the system uses, under a fixed root prefix.
#[derive(Debug, Clone)]
pub struct KeyLayout {
    root: String,
}

impl Default for KeyLayout {
    fn default() -> Self {
        Self::new(KEY_ROOT)
    }
}

impl KeyLayout {
    /// Build a layout rooted at `root` (default [`KEY_ROOT`]).
    #[must_use]
    pub fn new(root: impl Into<String>) -> Self {
        Self {
            root: root.into().trim_matches('/').to_string(),
        }
    }

    fn scope_prefix(&self, ws: &WorkspaceId, proj: &ProjectId) -> String {
        format!("{}/ws/{}/proj/{}", self.root, ws, proj)
    }

    /// Per-project commit point: the only object whose CAS makes writes visible.
    #[must_use]
    pub fn manifest(&self, ws: &WorkspaceId, proj: &ProjectId) -> String {
        format!("{}/manifest.json", self.scope_prefix(ws, proj))
    }

    /// Committed write-ahead record. The key is the derived event id, so a
    /// replayed commit writes the same object instead of a duplicate.
    #[must_use]
    pub fn wal_entry(&self, ws: &WorkspaceId, proj: &ProjectId, event_id: &str) -> String {
        format!("{}/wal/{event_id}.json", self.scope_prefix(ws, proj))
    }

    /// Prefix holding every WAL record of a project.
    #[must_use]
    pub fn wal_prefix(&self, ws: &WorkspaceId, proj: &ProjectId) -> String {
        format!("{}/wal", self.scope_prefix(ws, proj))
    }

    /// Immutable page body version.
    #[must_use]
    pub fn page_version(
        &self,
        ws: &WorkspaceId,
        proj: &ProjectId,
        path: &PagePath,
        page: &PageId,
    ) -> String {
        format!(
            "{}/pages/{}/versions/{}.md",
            self.scope_prefix(ws, proj),
            path.as_str(),
            page
        )
    }

    /// Immutable raw observation.
    #[must_use]
    pub fn observation(&self, ws: &WorkspaceId, proj: &ProjectId, obs: &ObservationId) -> String {
        format!("{}/observations/{}.json", self.scope_prefix(ws, proj), obs)
    }

    /// CAS head pointing at the current index catalog version.
    #[must_use]
    pub fn catalog_head(&self, ws: &WorkspaceId, proj: &ProjectId) -> String {
        format!("{}/index/head.json", self.scope_prefix(ws, proj))
    }

    /// Immutable index catalog version, addressed by its content hash.
    ///
    /// Content-addressed for the same reason page versions are: a CAS retry
    /// must be able to rewrite the same key with identical bytes, and a reader
    /// that pinned an older generation must still find it.
    #[must_use]
    pub fn catalog_version(
        &self,
        ws: &WorkspaceId,
        proj: &ProjectId,
        content_hash: &str,
    ) -> String {
        format!(
            "{}/index/catalog/{content_hash}.json",
            self.scope_prefix(ws, proj)
        )
    }

    /// Directory prefix of one writer's split. Every file of the split lives
    /// under it, so a reader can materialise the whole index from a `list`.
    #[must_use]
    pub fn split_prefix(
        &self,
        ws: &WorkspaceId,
        proj: &ProjectId,
        writer: &WriterId,
        seq: u64,
    ) -> String {
        format!(
            "{}/index/splits/{}/{seq:010}",
            self.scope_prefix(ws, proj),
            writer
        )
    }

    /// Lease object guarding an optional, abandonable job (compaction, GC).
    #[must_use]
    pub fn lease(&self, scope: &str) -> String {
        format!("{}/leases/{scope}.pb", self.root)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ws() -> WorkspaceId {
        WorkspaceId::new("acme").unwrap()
    }

    fn proj() -> ProjectId {
        ProjectId::new("ai-memory").unwrap()
    }

    #[test]
    fn ids_reject_traversal_and_separators() {
        assert!(WorkspaceId::new("").is_err());
        assert!(WorkspaceId::new("..").is_err());
        assert!(WorkspaceId::new("a/b").is_err());
        assert!(WorkspaceId::new("a b").is_err());
        assert!(WorkspaceId::new("a-b_c.1").is_ok());
    }

    #[test]
    fn page_paths_reject_traversal() {
        assert!(PagePath::new("").is_err());
        assert!(PagePath::new("../etc/passwd").is_err());
        assert!(PagePath::new("notes/../../x").is_err());
        assert!(PagePath::new("notes/").is_err());
        assert!(PagePath::new("c:\\x").is_err());
        assert_eq!(PagePath::new("/notes/a.md").unwrap().as_str(), "notes/a.md");
    }

    #[test]
    fn keys_are_stable_and_padded() {
        let layout = KeyLayout::default();
        let path = PagePath::new("notes/a.md").unwrap();
        let page = PageId::new("abc").unwrap();
        assert_eq!(
            layout.manifest(&ws(), &proj()),
            "v1/ws/acme/proj/ai-memory/manifest.json"
        );
        assert_eq!(
            layout.page_version(&ws(), &proj(), &path, &page),
            "v1/ws/acme/proj/ai-memory/pages/notes/a.md/versions/abc.md"
        );
        assert_eq!(
            layout.catalog_version(&ws(), &proj(), "abc123"),
            "v1/ws/acme/proj/ai-memory/index/catalog/abc123.json"
        );
        let writer = WriterId::new("mbp-1").unwrap();
        assert_eq!(
            layout.split_prefix(&ws(), &proj(), &writer, 3),
            "v1/ws/acme/proj/ai-memory/index/splits/mbp-1/0000000003"
        );
    }
}
