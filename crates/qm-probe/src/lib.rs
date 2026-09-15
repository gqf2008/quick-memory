//! Shared plumbing for the S0 probes.
//!
//! The probes refuse to report success without a real backend: a missing
//! credential is an error, not a skip. A conformance check that silently
//! passes when it never ran is worse than no check at all.

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;

/// S3-compatible connection settings, resolved from the environment.
#[derive(Debug, Clone)]
pub struct S3Config {
    /// Endpoint URL, e.g. `https://<account>.r2.cloudflarestorage.com`.
    pub endpoint: String,
    /// Bucket name.
    pub bucket: String,
    /// Region; R2 wants `auto`.
    pub region: String,
    /// Access key id.
    pub access_key_id: String,
    /// Secret access key.
    pub secret_access_key: String,
    /// Path-style requests. R2 needs `true`.
    pub force_path_style: bool,
    /// Key prefix every probe object lives under.
    pub prefix: String,
}

fn env_first(names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| {
        std::env::var(name)
            .ok()
            .filter(|value| !value.trim().is_empty())
    })
}

fn require(names: &[&str], what: &str) -> Result<String> {
    env_first(names).ok_or_else(|| {
        anyhow::anyhow!(
            "missing {what}; set one of {names:?} (a probe that cannot reach the \
             backend must fail, not skip)"
        )
    })
}

impl S3Config {
    /// Resolve configuration from `QM_S3_*` (falling back to `R2_*`).
    ///
    /// # Errors
    /// Fails when endpoint, bucket, or credentials are absent.
    pub fn from_env() -> Result<Self> {
        let endpoint = require(&["QM_S3_ENDPOINT", "R2_ENDPOINT"], "S3 endpoint")?;
        let bucket = require(&["QM_S3_BUCKET", "R2_BUCKET"], "bucket name")?;
        let access_key_id = require(
            &["QM_S3_ACCESS_KEY_ID", "R2_ACCESS_KEY_ID"],
            "access key id",
        )?;
        let secret_access_key = require(
            &["QM_S3_SECRET_ACCESS_KEY", "R2_SECRET_ACCESS_KEY"],
            "secret access key",
        )?;
        let region = env_first(&["QM_S3_REGION"]).unwrap_or_else(|| "auto".to_string());
        let force_path_style = env_first(&["QM_S3_FORCE_PATH_STYLE"])
            .map(|value| !matches!(value.as_str(), "0" | "false" | "no"))
            .unwrap_or(true);
        let prefix = env_first(&["QM_S3_PREFIX"]).unwrap_or_else(|| "qm-probe".to_string());
        Ok(Self {
            endpoint,
            bucket,
            region,
            access_key_id,
            secret_access_key,
            force_path_style,
            prefix,
        })
    }

    /// Build an object store client for this configuration.
    ///
    /// # Errors
    /// Fails when the client cannot be constructed.
    pub fn build_store(&self) -> Result<Arc<dyn ObjectStore>> {
        if !self.endpoint.starts_with("http") {
            bail!("endpoint must be an http(s) URL, got {:?}", self.endpoint);
        }
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&self.bucket)
            .with_region(&self.region)
            .with_virtual_hosted_style_request(!self.force_path_style)
            .with_access_key_id(&self.access_key_id)
            .with_secret_access_key(&self.secret_access_key)
            .with_endpoint(&self.endpoint);
        if self.endpoint.starts_with("http://") {
            builder = builder.with_allow_http(true);
        }
        let store = builder.build().context("building S3 client")?;
        Ok(Arc::new(store))
    }
}

/// Initialize stdout tracing for the probe binaries.
pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();
}
