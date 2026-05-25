// This file is part of Edgehog.
//
// Copyright 2026 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//! TLS configuration for the Edgehog Device Runtime

use std::sync::Arc;

use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, Error};
use tracing::info;
use tracing::instrument;

/// Returns a configured TLS client.
#[instrument]
pub fn config() -> Result<ClientConfig, Error> {
    // This step is to ensure a CryptoProvider is configured also in the tests.
    let provider = CryptoProvider::get_default()
        .map(Arc::clone)
        .unwrap_or_else(|| Arc::new(rustls::crypto::aws_lc_rs::default_provider()));

    cfg_if::cfg_if! {
        if #[cfg(feature = "platform-verifier")] {
            use rustls_platform_verifier::BuilderVerifierExt;

            let config = ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()?
                .with_platform_verifier()?
                .with_no_client_auth();

            info!("tls client configured with platform verifier");

            Ok(config)
        } else if #[cfg(feature = "webpki-roots")]{
            let config = ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()?
                .with_root_certificates(Arc::clone(get_root_certificates_webpki()))
                .with_no_client_auth();

            info!("tls client configured with webpki roots");

            Ok(config)
        } else {
            let config = ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()?
                .with_root_certificates(Arc::clone(get_root_certificates_default()))
                .with_no_client_auth();

            info!("tls client configured with native roots");

            Ok(config)
        }
    }
}

#[cfg(feature = "webpki-roots")]
pub fn get_root_certificates_webpki() -> &Arc<RootCertStore> {
    static ROOTS: std::sync::OnceLock<Arc<RootCertStore>> = std::sync::OnceLock::new();

    ROOTS.get_or_init(|| {
        let roots = rustls::RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        };

        Arc::new(roots)
    })
}

#[cfg(not(any(feature = "webpki-roots", feature = "platform-verifier")))]
pub fn get_root_certificates_default() -> &Arc<RootCertStore> {
    static ROOTS: std::sync::OnceLock<Arc<RootCertStore>> = std::sync::OnceLock::new();

    ROOTS.get_or_init(|| {
        let res = rustls_native_certs::load_native_certs();

        for error in res.errors {
            error!(%error, "couldn't load native certificate");
        }

        let mut roots = rustls::RootCertStore::empty();

        let (added, ignored) = roots.add_parsable_certificates(res.certs);

        info!(added, ignored, "native certificates loaded");

        Arc::new(roots)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configure() {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .unwrap();

        config().unwrap();
    }
}
