//! Object-store CAS primitives.
//!
//! The whole multi-machine design rests on one primitive: an object whose
//! replacement is conditional on the version we last read. S3/R2 call that
//! ETag CAS. Two rules are load-bearing here:
//!
//! 1. **ETag is the identity.** R2 may return a write-only version id from
//!    `PUT` that later `GET`/`HEAD` calls do not repeat, so a version token is
//!    only a fallback for backends that have no ETag at all. Comparing the two
//!    fields structurally (as `object_store` does) turns the same object into a
//!    false mismatch.
//! 2. **A missing ETag fails closed.** A conditional write we cannot bind to an
//!    identity is refused rather than downgraded to an unconditional one.
//!
//! The probe in [`verify_conditional_writes`] exists because a backend that
//! silently ignores preconditions would otherwise look healthy until two
//! machines overwrote each other.

use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{Error as OsError, ObjectStore, PutMode, PutPayload, UpdateVersion};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Identity of an object version, ETag first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectVersion {
    /// Content ETag. Authoritative whenever present.
    pub etag: Option<String>,
    /// Backend version id. Used only when no ETag exists.
    pub version: Option<String>,
}

impl ObjectVersion {
    /// Construct from the two fields a backend may return.
    #[must_use]
    pub fn new(etag: Option<String>, version: Option<String>) -> Self {
        Self { etag, version }
    }

    /// The ETag, if the backend supplied one.
    #[must_use]
    pub fn etag(&self) -> Option<&str> {
        self.etag.as_deref()
    }

    /// Require an ETag: conditional operations without one fail closed.
    ///
    /// # Errors
    /// Returns [`StoreError::MissingEtag`] when no ETag is available.
    pub fn require_etag(&self) -> Result<&str, StoreError> {
        self.etag.as_deref().ok_or(StoreError::MissingEtag)
    }

    fn as_update_version(&self) -> Result<UpdateVersion, StoreError> {
        Ok(UpdateVersion {
            e_tag: Some(self.require_etag()?.to_string()),
            // Never a PUT-response version id: R2 does not return it on read.
            version: None,
        })
    }

    fn from_put(result: object_store::PutResult) -> Result<Self, StoreError> {
        let v = Self::new(result.e_tag, result.version);
        match (&v.etag, &v.version) {
            (None, None) => Err(StoreError::MissingEtag),
            _ => Ok(v),
        }
    }

    fn from_meta(meta: &object_store::ObjectMeta) -> Result<Self, StoreError> {
        let v = Self::new(meta.e_tag.clone(), meta.version.clone());
        match (&v.etag, &v.version) {
            (None, None) => Err(StoreError::MissingEtag),
            _ => Ok(v),
        }
    }
}

/// Failure modes of the CAS layer.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The object does not exist.
    #[error("object not found")]
    NotFound,
    /// A precondition failed: the object exists (create) or moved (update).
    #[error("precondition failed")]
    Precondition,
    /// The object already exists. Some backends report a rejected
    /// create-if-absent this way instead of as a precondition failure.
    #[error("object already exists")]
    AlreadyExists,
    /// The backend returned no version identity for a conditional operation.
    #[error("backend returned no ETag for a conditional operation")]
    MissingEtag,
    /// Another machine kept winning the commit race for this scope.
    #[error("commit conflict after {attempts} attempts")]
    Conflict {
        /// Attempts made before giving up.
        attempts: u32,
    },
    /// A stored object was unreadable, mismatched, or otherwise inconsistent.
    #[error("corrupt object: {0}")]
    Corrupt(String),
    /// The handoff is no longer open (someone else took it, or it is done).
    #[error("handoff {id} is not open (state: {state:?})")]
    HandoffNotOpen {
        /// Handoff id.
        id: String,
        /// State it is in now.
        state: qm_core::HandoffState,
    },
    /// The handoff belongs to another machine's claim.
    #[error("handoff {id} is claimed by {owner}")]
    HandoffOwnedByAnother {
        /// Handoff id.
        id: String,
        /// Current claimer.
        owner: String,
    },
    /// Any other backend failure.
    #[error("object store error: {0}")]
    Backend(String),
}

impl From<qm_core::CoreError> for StoreError {
    fn from(error: qm_core::CoreError) -> Self {
        // A key in the bucket that does not yield a valid identifier means the
        // stored data is not what this code believes it to be.
        Self::Corrupt(error.to_string())
    }
}

impl From<OsError> for StoreError {
    fn from(error: OsError) -> Self {
        match error {
            OsError::NotFound { .. } => Self::NotFound,
            OsError::Precondition { .. } => Self::Precondition,
            OsError::AlreadyExists { .. } => Self::AlreadyExists,
            other => Self::Backend(other.to_string()),
        }
    }
}

mod gc;
mod project;

pub use gc::GcOutcome;
pub use project::{
    CommitOutcome, CommitPageRequest, DeleteOutcome, IngestObservationsRequest, IngestOutcome,
    LeaseGuard, LoadedCatalog, LoadedManifest, ProjectStore, PublishOutcome, ReplaceCatalogOutcome,
    RetryPolicy,
};

/// Serialize a value for an object body.
pub(crate) fn encode<T: Serialize>(value: &T) -> Result<Bytes, StoreError> {
    serde_json::to_vec(value)
        .map(Bytes::from)
        .map_err(|error| StoreError::Corrupt(error.to_string()))
}

/// Parse an object body.
pub(crate) fn decode<T: DeserializeOwned>(bytes: &Bytes, key: &str) -> Result<T, StoreError> {
    serde_json::from_slice(bytes).map_err(|error| StoreError::Corrupt(format!("{key}: {error}")))
}

/// CAS operations over one object store and one key prefix.
pub struct CasStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl CasStore {
    /// Wrap a store; `prefix` scopes every key (empty means the bucket root).
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        Self {
            store,
            prefix: prefix.into().trim_matches('/').to_string(),
        }
    }

    /// The configured key prefix.
    #[must_use]
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Borrow the underlying store (used by the index layer).
    #[must_use]
    pub fn store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.store)
    }

    fn path(&self, key: &str) -> Result<ObjectPath, StoreError> {
        let key = key.trim_start_matches('/');
        let full = if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}/{key}", self.prefix)
        };
        ObjectPath::parse(full).map_err(|e| StoreError::Backend(e.to_string()))
    }

    fn prefix_path(&self, prefix: &str) -> Result<ObjectPath, StoreError> {
        let trimmed = prefix.trim_matches('/');
        if trimmed.is_empty() {
            return if self.prefix.is_empty() {
                Ok(ObjectPath::default())
            } else {
                ObjectPath::parse(&self.prefix).map_err(|e| StoreError::Backend(e.to_string()))
            };
        }
        self.path(trimmed)
    }

    /// Read an object and the version it was read at.
    ///
    /// # Errors
    /// [`StoreError::NotFound`] when absent, [`StoreError::Backend`] otherwise.
    pub async fn read(&self, key: &str) -> Result<(Bytes, ObjectVersion), StoreError> {
        let path = self.path(key)?;
        let result = self.store.get(&path).await?;
        let version = ObjectVersion::from_meta(&result.meta)?;
        let bytes = result.bytes().await?;
        Ok((bytes, version))
    }

    /// Read only the version metadata.
    ///
    /// # Errors
    /// [`StoreError::NotFound`] when absent, [`StoreError::Backend`] otherwise.
    pub async fn head(&self, key: &str) -> Result<ObjectVersion, StoreError> {
        let path = self.path(key)?;
        let meta = self.store.head(&path).await?;
        ObjectVersion::from_meta(&meta)
    }

    /// Last-modified time of an object in milliseconds since the epoch.
    ///
    /// `None` means the object is gone, which every caller here treats as
    /// "nothing to do" rather than an error.
    ///
    /// # Errors
    /// Propagates backend failures other than a missing object.
    pub async fn last_modified_ms(&self, key: &str) -> Result<Option<i64>, StoreError> {
        let path = self.path(key)?;
        match self.store.head(&path).await {
            Ok(meta) => Ok(Some(meta.last_modified.timestamp_millis())),
            Err(OsError::NotFound { .. }) => Ok(None),
            Err(other) => Err(other.into()),
        }
    }

    /// Create an object that must not already exist (`If-None-Match: *`).
    ///
    /// # Errors
    /// [`StoreError::Precondition`] when the key exists.
    pub async fn create(&self, key: &str, bytes: Bytes) -> Result<ObjectVersion, StoreError> {
        let path = self.path(key)?;
        let result = self
            .store
            .put_opts(&path, PutPayload::from(bytes), PutMode::Create.into())
            .await?;
        ObjectVersion::from_put(result)
    }

    /// Replace an object only if it still matches `expected`.
    ///
    /// # Errors
    /// [`StoreError::Precondition`] when the object moved or vanished.
    pub async fn update(
        &self,
        key: &str,
        bytes: Bytes,
        expected: &ObjectVersion,
    ) -> Result<ObjectVersion, StoreError> {
        let path = self.path(key)?;
        let mode = PutMode::Update(expected.as_update_version()?);
        let result = self
            .store
            .put_opts(&path, PutPayload::from(bytes), mode.into())
            .await?;
        ObjectVersion::from_put(result)
    }

    /// Best-effort delete. Callers treat "already gone" as success.
    ///
    /// # Errors
    /// [`StoreError::Backend`] on any transport/permission failure.
    pub async fn delete(&self, key: &str) -> Result<(), StoreError> {
        let path = self.path(key)?;
        match self.store.delete(&path).await {
            Ok(()) | Err(OsError::NotFound { .. }) => Ok(()),
            Err(other) => Err(other.into()),
        }
    }

    /// List `(relative key, version)` pairs under a prefix, sorted by key.
    ///
    /// # Errors
    /// [`StoreError::Backend`] on listing failures.
    pub async fn list(&self, prefix: &str) -> Result<Vec<(String, ObjectVersion)>, StoreError> {
        let base = self.prefix_path(prefix)?;
        let prefix_len = if self.prefix.is_empty() {
            0
        } else {
            self.prefix.len() + 1
        };
        let mut stream = self.store.list(Some(&base));
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            let meta = item?;
            let full = meta.location.as_ref().to_string();
            let relative = full.get(prefix_len..).unwrap_or(&full).to_string();
            out.push((relative, ObjectVersion::from_meta(&meta)?));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }
}

/// The minimal surface the conformance probe exercises.
///
/// Kept separate from [`CasStore`] so tests can inject a backend that ignores
/// preconditions and prove the probe actually reports it.
pub trait CasBackend {
    /// Create-if-absent.
    fn create(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> impl std::future::Future<Output = Result<ObjectVersion, StoreError>>;
    /// Update-if-match.
    fn update(
        &self,
        key: &str,
        bytes: Bytes,
        expected: &ObjectVersion,
    ) -> impl std::future::Future<Output = Result<ObjectVersion, StoreError>>;
    /// Read with version.
    fn read(
        &self,
        key: &str,
    ) -> impl std::future::Future<Output = Result<(Bytes, ObjectVersion), StoreError>>;
}

impl CasBackend for CasStore {
    async fn create(&self, key: &str, bytes: Bytes) -> Result<ObjectVersion, StoreError> {
        CasStore::create(self, key, bytes).await
    }

    async fn update(
        &self,
        key: &str,
        bytes: Bytes,
        expected: &ObjectVersion,
    ) -> Result<ObjectVersion, StoreError> {
        CasStore::update(self, key, bytes, expected).await
    }

    async fn read(&self, key: &str) -> Result<(Bytes, ObjectVersion), StoreError> {
        CasStore::read(self, key).await
    }
}

/// Outcome of one probe step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeStep {
    /// What was checked.
    pub name: &'static str,
    /// Whether the backend behaved as required.
    pub ok: bool,
    /// Human-readable detail (observed ETags etc.).
    pub detail: String,
}

/// Full report from [`verify_conditional_writes`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeReport {
    /// Every step, in order.
    pub steps: Vec<ProbeStep>,
}

impl ProbeReport {
    /// True when every step passed.
    #[must_use]
    pub fn all_passed(&self) -> bool {
        self.steps.iter().all(|s| s.ok)
    }

    /// Render a one-line-per-step summary.
    #[must_use]
    pub fn render(&self) -> String {
        self.steps
            .iter()
            .map(|s| {
                format!(
                    "{} {} — {}",
                    if s.ok { "PASS" } else { "FAIL" },
                    s.name,
                    s.detail
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

/// Verify that a backend really implements create-if-absent and update-if-match.
///
/// The negative steps are the point: a backend that accepts a stale ETag looks
/// fine in every happy-path test and silently loses writes under concurrency.
///
/// # Errors
/// Returns `Err` when any step violates the contract, with the full report in
/// the message.
pub async fn verify_conditional_writes<B: CasBackend>(
    backend: &B,
    key: &str,
) -> anyhow::Result<ProbeReport> {
    let mut steps = Vec::new();
    let mut push = |name: &'static str, ok: bool, detail: String| {
        steps.push(ProbeStep { name, ok, detail });
    };

    // 1. create-if-absent succeeds and yields an identity.
    let created = backend
        .create(key, Bytes::from_static(b"v1"))
        .await
        .map_err(|e| anyhow::anyhow!("create failed: {e}"))?;
    let etag = created.etag().map(ToString::to_string);
    push(
        "create-returns-etag",
        etag.is_some(),
        format!("etag={etag:?} version={:?}", created.version),
    );

    // 2. creating the same key again must fail.
    let dup = backend.create(key, Bytes::from_static(b"v2")).await;
    // Backends disagree on the error shape for a rejected create: 412 maps to
    // Precondition, while some transports report AlreadyExists. Both mean the
    // precondition held; a generic backend error does not.
    push(
        "duplicate-create-rejected",
        matches!(
            dup,
            Err(StoreError::Precondition | StoreError::AlreadyExists)
        ),
        format!("{dup:?}"),
    );

    // 3. the read-back ETag must equal the create ETag (identity is stable
    //    even when the version id disappears between PUT and GET).
    let (_, read_version) = backend
        .read(key)
        .await
        .map_err(|e| anyhow::anyhow!("read-back failed: {e}"))?;
    push(
        "etag-stable-across-read",
        read_version.etag == created.etag && created.etag.is_some(),
        format!("put={:?} read={:?}", created.etag, read_version.etag),
    );

    // 4. a stale ETag must be rejected.
    let stale = ObjectVersion::new(Some("deadbeef-not-a-real-etag".to_string()), None);
    let stale_result = backend.update(key, Bytes::from_static(b"v3"), &stale).await;
    push(
        "stale-etag-rejected",
        matches!(stale_result, Err(StoreError::Precondition)),
        format!("{stale_result:?}"),
    );

    // 5. the matching ETag must be accepted and move the identity.
    let updated = backend
        .update(key, Bytes::from_static(b"v3"), &read_version)
        .await
        .map_err(|e| anyhow::anyhow!("matching update failed: {e}"))?;
    push(
        "matching-etag-accepted",
        updated.etag.is_some() && updated.etag != read_version.etag,
        format!("before={:?} after={:?}", read_version.etag, updated.etag),
    );

    // 6. the previous ETag is now stale and must be rejected.
    let replayed = backend
        .update(key, Bytes::from_static(b"v4"), &read_version)
        .await;
    push(
        "consumed-etag-rejected",
        matches!(replayed, Err(StoreError::Precondition)),
        format!("{replayed:?}"),
    );

    // 7. content is what the last accepted write put there.
    let (bytes, _) = backend
        .read(key)
        .await
        .map_err(|e| anyhow::anyhow!("final read failed: {e}"))?;
    push(
        "final-content",
        bytes == Bytes::from_static(b"v3"),
        format!("{bytes:?}"),
    );

    let report = ProbeReport { steps };
    if report.all_passed() {
        Ok(report)
    } else {
        Err(anyhow::anyhow!(
            "conditional-write conformance failed:\n{}",
            report.render()
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use object_store::memory::InMemory;

    use super::*;

    fn mem_store() -> CasStore {
        CasStore::new(Arc::new(InMemory::new()), "probe")
    }

    #[tokio::test]
    async fn in_memory_backend_passes_conformance() {
        let store = mem_store();
        let report = verify_conditional_writes(&store, "cas").await.unwrap();
        assert!(report.all_passed(), "{}", report.render());
        // Every negative step must actually have run.
        let names: Vec<&str> = report.steps.iter().map(|s| s.name).collect();
        assert!(names.contains(&"duplicate-create-rejected"));
        assert!(names.contains(&"stale-etag-rejected"));
        assert!(names.contains(&"consumed-etag-rejected"));
    }

    /// A backend that accepts every write. The probe must reject it — this is
    /// the positive control for the checker itself.
    struct PermissiveBackend {
        objects: Mutex<HashMap<String, (Bytes, u64)>>,
    }

    impl PermissiveBackend {
        fn new() -> Self {
            Self {
                objects: Mutex::new(HashMap::new()),
            }
        }
    }

    impl CasBackend for PermissiveBackend {
        async fn create(&self, key: &str, bytes: Bytes) -> Result<ObjectVersion, StoreError> {
            let mut objects = self.objects.lock().unwrap();
            let seq = objects.len() as u64 + 1;
            objects.insert(key.to_string(), (bytes, seq));
            Ok(ObjectVersion::new(Some(format!("etag-{seq}")), None))
        }

        async fn update(
            &self,
            key: &str,
            bytes: Bytes,
            _expected: &ObjectVersion,
        ) -> Result<ObjectVersion, StoreError> {
            let mut objects = self.objects.lock().unwrap();
            let seq = objects.len() as u64 + 1;
            objects.insert(key.to_string(), (bytes, seq));
            Ok(ObjectVersion::new(Some(format!("etag-{seq}")), None))
        }

        async fn read(&self, key: &str) -> Result<(Bytes, ObjectVersion), StoreError> {
            let objects = self.objects.lock().unwrap();
            match objects.get(key) {
                Some((bytes, seq)) => Ok((
                    bytes.clone(),
                    ObjectVersion::new(Some(format!("etag-{seq}")), None),
                )),
                None => Err(StoreError::NotFound),
            }
        }
    }

    #[tokio::test]
    async fn probe_detects_a_backend_that_ignores_preconditions() {
        let backend = PermissiveBackend::new();
        let error = verify_conditional_writes(&backend, "cas")
            .await
            .expect_err("a permissive backend must be reported, not accepted");
        let message = error.to_string();
        assert!(message.contains("duplicate-create-rejected"), "{message}");
        assert!(message.contains("stale-etag-rejected"), "{message}");
    }
}
