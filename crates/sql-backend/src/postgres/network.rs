//! TLS connector setup for the remote Postgres driver.

use rustls::{ClientConfig, RootCertStore};
use storage_types::{StorageError, StorageResult};
use tokio_postgres_rustls::MakeRustlsConnect;

pub(super) fn build_tls_connector() -> StorageResult<MakeRustlsConnect> {
    let mut roots = RootCertStore::empty();
    let certs = rustls_native_certs::load_native_certs();
    if certs.errors.is_empty() && certs.certs.is_empty() {
        return Err(StorageError::internal(
            "no system root certificates for Postgres TLS",
        ));
    }
    for cert in certs.certs {
        roots
            .add(cert)
            .map_err(|_| StorageError::internal("invalid system root certificate"))?;
    }
    let config =
        ClientConfig::builder_with_provider(rustls::crypto::aws_lc_rs::default_provider().into())
            .with_safe_default_protocol_versions()
            .map_err(|_| StorageError::internal("invalid Postgres TLS protocol versions"))?
            .with_root_certificates(roots)
            .with_no_client_auth();
    Ok(MakeRustlsConnect::new(config))
}
