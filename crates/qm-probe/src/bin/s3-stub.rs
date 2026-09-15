//! Run the in-process S3 stub as a standalone server.
//!
//! The probe binaries (`cas-conformance`, `manifest-probe`, …) take their
//! endpoint from `QM_S3_*`, so proving that *those* processes walk the real S3
//! HTTP path needs a stub the probe can reach over a socket. This binary
//! publishes the port it bound (on stdout and, when asked, in a file) and then
//! parks until the caller stops it.
//!
//! Test and CI auxiliary only: the fake bucket has no authentication, no
//! multipart upload and no durability, and no product code path reaches it.
//! `qm_probe::s3_stub` documents what it models; `docs/design.md` §5.1
//! documents what that does and does not prove.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use qm_probe::s3_stub::{Fault, S3Stub, StubOptions};

/// Faults the standalone stub can inject, to show a probe failing on purpose.
#[derive(Copy, Clone, Debug, ValueEnum)]
enum FaultArg {
    /// Accept stale `If-Match` ETags (a backend that ignores preconditions).
    IgnoreIfMatch,
    /// Answer GET/HEAD with an ETag no write ever returned.
    StaleReadEtag,
    /// Answer the first page of a listing as if it were the whole listing.
    TruncateListing,
    /// Answer GET with a truncated body and an honest content-length.
    TruncateReadBody,
}

impl From<FaultArg> for Fault {
    fn from(value: FaultArg) -> Self {
        match value {
            FaultArg::IgnoreIfMatch => Self::IgnoreIfMatch,
            FaultArg::StaleReadEtag => Self::StaleReadEtag,
            FaultArg::TruncateListing => Self::TruncateListing,
            FaultArg::TruncateReadBody => Self::TruncateReadBody,
        }
    }
}

#[derive(Debug, Parser)]
#[command(about = "Serve a minimal in-memory S3 bucket for protocol probes")]
struct Args {
    /// Bucket name to serve.
    #[arg(long, default_value = "stub-bucket")]
    bucket: String,
    /// Loopback port to bind; 0 picks an ephemeral one.
    #[arg(long, default_value_t = 0)]
    port: u16,
    /// Write the bound port here, for scripts that must not race stdout.
    #[arg(long)]
    port_file: Option<PathBuf>,
    /// Inject a fault; repeatable.
    #[arg(long, value_enum)]
    fault: Vec<FaultArg>,
    /// Answer PUT with `x-amz-version-id` and never repeat it on GET/HEAD.
    #[arg(long, default_value_t = false)]
    r2_version_id: bool,
    /// Objects per list page, to force the client to paginate.
    #[arg(long, default_value_t = 1000)]
    page_size: usize,
}

fn main() -> ExitCode {
    let args = Args::parse();
    let mut options = StubOptions::default()
        .with_bucket(&args.bucket)
        .with_page_size(args.page_size)
        .with_put_version_id(args.r2_version_id);
    for fault in &args.fault {
        options = options.with_fault((*fault).into());
    }

    let stub = match S3Stub::start_on(args.port, options) {
        Ok(stub) => stub,
        Err(error) => {
            eprintln!("could not bind 127.0.0.1:{}: {error}", args.port);
            return ExitCode::from(2);
        }
    };

    if let Some(path) = &args.port_file
        && let Err(error) = std::fs::write(path, format!("{}\n", stub.port()))
    {
        eprintln!("could not write {}: {error}", path.display());
        return ExitCode::from(2);
    }

    println!(
        "s3 stub listening on {} bucket={} faults={:?}",
        stub.endpoint(),
        stub.bucket(),
        args.fault
    );
    println!(
        "export QM_S3_ENDPOINT={} QM_S3_BUCKET={} \
         QM_S3_ACCESS_KEY_ID=stub-access QM_S3_SECRET_ACCESS_KEY=stub-secret \
         QM_S3_FORCE_PATH_STYLE=true",
        stub.endpoint(),
        stub.bucket()
    );

    // The caller owns the lifetime: nothing a probe can send is a shutdown
    // signal, so `kill` (or the end of the script) is always the exit.
    loop {
        std::thread::park_timeout(Duration::from_secs(3600));
    }
}
