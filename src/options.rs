/*
 * This file is part of Astarte.
 *
 * Copyright 2021 SECO Mind Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
//! Provides functionality to configure an instance of the
//! [AstarteDeviceSdk][crate::AstarteDeviceSdk].
use std::ffi::OsStr;
use std::fmt::Debug;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Mutex;

use async_trait::async_trait;
use bson::ser::Error;
use log::debug;
use pairing::PairingError;
use rumqttc::AsyncClient;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::mpsc;

use crate::AstarteDeviceSdk;
use crate::EventReceiver;
use crate::EventSender;
use crate::connection::Connection;
use crate::connection::mqtt::Mqtt;
use crate::crypto::CryptoError;
use crate::interface::{Interface, InterfaceError};
use crate::interfaces::Interfaces;
use crate::pairing;
use crate::store::memory::MemoryStore;
use crate::store::PropertyStore;

/// Astarte options error.
///
/// Possible errors used by the Astarte options module.
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum BuilderError {
    #[error("private key or CSR creation failed")]
    CryptoGeneration(#[from] CryptoError),

    #[error("device must have at least an interface")]
    MissingInterfaces,

    #[error("error creating interface")]
    Interface(#[from] InterfaceError),

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error("configuration error")]
    ConfigError(String),

    #[error(transparent)]
    MqttError(#[from] rumqttc::ClientError),

    #[error("pairing error")]
    PairingError(#[from] PairingError),

    #[error(transparent)]
    DbError(#[from] sqlx::Error),

    #[error(transparent)]
    PkiError(#[from] webpki::Error),
}

/// Structure used to store the configuration options for an instance of
/// [AstarteDeviceSdk][crate::AstarteDeviceSdk].
#[derive(Clone)]
pub struct DeviceBuilder<S> {
    pub(crate) interfaces: Interfaces,
    pub(crate) store: S,
}

impl<S> Debug for DeviceBuilder<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AstarteOptions")
            .field("interfaces", &self.interfaces)
            // We manually implement Debug for the store, so we can avoid have a trait bound on
            // `S` to implement [Display].
            .finish_non_exhaustive()
    }
}

#[derive(Serialize, Deserialize)]
pub struct MqttConfig {
    pub(crate) realm: String,
    pub(crate) device_id: String,
    pub(crate) credentials_secret: String,
    pub(crate) pairing_url: String,
    pub(crate) ignore_ssl_errors: bool,
    pub(crate) keepalive: std::time::Duration,
}

impl Debug for MqttConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttOptions")
            .field("realm", &self.realm)
            .field("device_id", &self.device_id)
            .field("credentials_secret", &"REDACTED")
            .field("pairing_url", &self.pairing_url)
            .field("ignore_ssl_errors", &self.ignore_ssl_errors)
            .field("keepalive", &self.keepalive)
            .finish_non_exhaustive()
    }
}

impl MqttConfig {
    /// Create a new instance of the MqttOptions
    pub fn new(realm: &str,
        device_id: &str,
        credentials_secret: &str,
        pairing_url: &str) -> Self {

        Self {
            realm: realm.to_owned(),
            device_id: device_id.to_owned(),
            credentials_secret: credentials_secret.to_owned(),
            pairing_url: pairing_url.to_owned(),
            ignore_ssl_errors: false,
            keepalive: std::time::Duration::from_secs(30),
        }
    }

    /// Configure the keep alive timeout.
    ///
    /// The MQTT broker will be pinged when no data exchange has appened
    /// for the duration of the keep alive timeout.
    pub fn keepalive(mut self, duration: std::time::Duration) -> Self {
        self.keepalive = duration;

        self
    }

    /// Ignore TLS/SSL certificate errors.
    pub fn ignore_ssl_errors(mut self) -> Self {
        self.ignore_ssl_errors = true;

        self
    }

    async fn connect(self) -> Result<Mqtt, crate::Error> {
        let mqtt_options = pairing::get_transport_config(&self).await?;

        debug!("{:#?}", mqtt_options);

        let (client, eventloop) = AsyncClient::new(mqtt_options, 50);

        Ok(Mqtt::new(self.realm, self.device_id, eventloop, client))
    }
}

#[cfg(feature="message-hub-client")]
#[derive(Serialize, Deserialize)]
pub struct GrpcConfig {
    pub(crate) endpoint: String,
}

#[cfg(feature="message-hub-client")]
impl GrpcOptions {
    pub fn new(endpoint: &str) -> Self {
        Self {
            endpoint: endpoint.to_owned(),
        }
    }
}

impl DeviceBuilder<MemoryStore> {
    // TODO rewrite example and comment
    /// Create a new instance of the astarte options.
    ///
    /// ```no_run
    /// use astarte_device_sdk::options::AstarteOptions;
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let realm = "realm_name";
    ///     let device_id = "device_id";
    ///     let credentials_secret = "device_credentials_secret";
    ///     let pairing_url = "astarte_cluster_pairing_url";
    ///
    ///     let mut sdk_options =
    ///         AstarteOptions::new(&realm, &device_id, &credentials_secret, &pairing_url)
    ///             .interface_directory("path/to/interfaces")
    ///             .unwrap()
    ///             .keepalive(std::time::Duration::from_secs(90));
    /// }
    /// ```
    pub fn new() -> DeviceBuilder<MemoryStore> {
        DeviceBuilder {
            interfaces: Interfaces::new(),
            store: MemoryStore::new(),
        }
    }
}

impl<S> DeviceBuilder<S>
where
    S: PropertyStore,
{
    /// Set the backing storage for the device.
    ///
    /// This will store and retrieve the device's properties.
    pub fn store<T>(self, store: T) -> DeviceBuilder<T> {
        DeviceBuilder {
            interfaces: self.interfaces,
            store,
        }
    }

    /// Create a new instance of the astarte builder with the specified backing store.
    pub fn with_store(store: S) -> Self {
        Self {
            interfaces: Interfaces::new(),
            store,
        }
    }

    pub async fn connect_mqtt(self, mqtt_options: MqttConfig) -> Result<(AstarteDeviceSdk<S, Mqtt>, EventReceiver), crate::Error> {
        let connection = mqtt_options.connect().await?;

        Ok(self.build(connection))
    }

    pub(crate) fn build<C>(self, connection: C) -> (AstarteDeviceSdk<S, C>, EventReceiver)
    where
        C: Connection<S> + 'static,
    {
        const MQTT_CHANNEL_SIZE: usize = 50;

        let (tx, rx) = mpsc::channel(MQTT_CHANNEL_SIZE);

        (AstarteDeviceSdk::new(self.interfaces, self.store, connection, tx), rx)
    }
}

impl<S> DeviceBuilder<S> {
    /// Add a single interface from the provided `.json` file.
    ///
    /// It will validate that the interfaces are the same, or a newer version of the interfaces
    /// with the same name that are already present.
    pub fn interface_file(mut self, file_path: &Path) -> Result<Self, BuilderError> {
        let interface = Interface::from_file(file_path)?;
        let name = interface.interface_name();

        debug!("Added interface {}", name);

        self.interfaces.add(interface)?;

        Ok(self)
    }

    /// Add all the interfaces from the `.json` files contained in the specified folder.
    pub fn interface_directory(self, interfaces_directory: &str) -> Result<Self, BuilderError> {
        walk_dir_json(interfaces_directory)?
            .iter()
            .try_fold(self, |acc, path| acc.interface_file(path))
    }
}

/// Walks a directory returning an array of json files
fn walk_dir_json<P: AsRef<Path>>(path: P) -> Result<Vec<PathBuf>, io::Error> {
    std::fs::read_dir(path)?
        .map(|res| {
            res.and_then(|entry| {
                let path = entry.path();
                let metadata = entry.metadata()?;

                Ok((path, metadata))
            })
        })
        .filter_map(|res| match res {
            Ok((path, metadata)) => {
                if metadata.is_file() && path.extension() == Some(OsStr::new("json")) {
                    Some(Ok(path))
                } else {
                    None
                }
            }
            Err(e) => Some(Err(e)),
        })
        .collect()
}

#[cfg(test)]
mod test {
    use super::{DeviceBuilder, MqttConfig};

    #[test]
    fn interface_directory() {
        let res = DeviceBuilder::new()
            .interface_directory("examples/individual_datastream/interfaces");

        assert!(
            res.is_ok(),
            "Failed to load interfaces from directory: {:?}",
            res
        );
    }

    #[test]
    fn interface_existing_directory() {
        let res = DeviceBuilder::new()
            .interface_directory("examples/individual_datastream/interfaces")
            .unwrap()
            .interface_directory("examples/individual_datastream/interfaces");

        assert!(
            res.is_ok(),
            "Failed to load interfaces from directory: {:?}",
            res
        );
    }

    #[test]
    fn connect_mqtt() {
        let builder = DeviceBuilder::new()
            .interface_directory("examples/individual_datastream/interfaces");

        let device = builder.unwrap()
            .connect(MqttConfig::new("realm", "device_id", "sec", "pairing_url")).await;
    }
}
