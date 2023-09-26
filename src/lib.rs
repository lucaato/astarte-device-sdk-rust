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
#![doc = include_str!("../README.md")]

pub mod builder;
pub mod connection;
pub mod crypto;
pub mod error;
pub mod interface;
mod interfaces;
#[cfg(test)]
mod mock;
pub mod pairing;
pub mod payload;
pub mod properties;
pub mod registration;
mod retry;
mod shared;
pub mod store;
mod topic;
pub mod types;
mod validate;

use async_trait::async_trait;
use connection::{Connection, ReceivedEvent, Registry};
use interfaces::Interfaces;
use tokio::sync::{mpsc, RwLock};

use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt::{self, Debug};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

// Re-export rumqttc since we return its types in some methods
pub use chrono;
pub use rumqttc;

use log::{debug, error, info, trace, warn};

/// Re-exported internal structs
pub use crate::interface::Interface;

use crate::error::Error;
use crate::interface::mapping::path::MappingPath;
use crate::interface::{Aggregation as InterfaceAggregation, InterfaceError};
use crate::interfaces::MappingRef;
use crate::interfaces::PropertyRef;
use crate::shared::SharedDevice;
use crate::store::wrapper::StoreWrapper;
use crate::store::PropertyStore;
use crate::types::{AstarteType, TypeError};
use crate::validate::{validate_send_individual, validate_send_object};

/// A **trait** required by all data to be sent using
/// [send_object()][crate::AstarteDeviceSdk::send_object] and
/// [send_object_with_timestamp()][crate::AstarteDeviceSdk::send_object_with_timestamp].
/// It ensures correct parsing of the data.
///
/// The returned hash map should have as values the data to transmit for each
/// object endpoint and as keys the endpoints themselves.
///
/// The Astarte Device SDK provides a procedural macro that can be used to automatically
/// generate `AstarteAggregate` implementations for Structs.
/// To use the procedural macro enable the feature `derive`.
pub trait AstarteAggregate {
    /// Parse this data structure into a `HashMap` compatible with transmission of Astarte objects.
    /// ```
    /// use std::collections::HashMap;
    /// use std::convert::TryInto;
    ///
    /// use astarte_device_sdk::{types::AstarteType, error::Error, AstarteAggregate};
    ///
    /// struct Person {
    ///     name: String,
    ///     age: i32,
    ///     phones: Vec<String>,
    /// }
    ///
    /// // This is what #[derive(AstarteAggregate)] would generate.
    /// impl AstarteAggregate for Person {
    ///     fn astarte_aggregate(self) -> Result<HashMap<String, AstarteType>, Error>
    ///     {
    ///         let mut r = HashMap::new();
    ///         r.insert("name".to_string(), self.name.try_into()?);
    ///         r.insert("age".to_string(), self.age.try_into()?);
    ///         r.insert("phones".to_string(), self.phones.try_into()?);
    ///         Ok(r)
    ///     }
    /// }
    /// ```
    fn astarte_aggregate(self) -> Result<HashMap<String, AstarteType>, Error>;
}

impl AstarteAggregate for HashMap<String, AstarteType> {
    fn astarte_aggregate(self) -> Result<HashMap<String, AstarteType>, Error> {
        Ok(self)
    }
}

/// Sender end of the channel for the [`AstarteDeviceDataEvent`].
pub type EventSender = mpsc::Sender<Result<AstarteDeviceDataEvent, Error>>;
/// Receiver end of the channel for the [`AstarteDeviceDataEvent`].
pub type EventReceiver = mpsc::Receiver<Result<AstarteDeviceDataEvent, Error>>;

// Re-export #[derive(AstarteAggregate)].
//
// The reason re-exporting is not enabled by default is that disabling it would
// be annoying for crates that provide handwritten impls or data formats. They
// would need to disable default features and then explicitly re-enable std.
#[cfg(feature = "derive")]
#[allow(unused_imports)]
#[macro_use]
extern crate astarte_device_sdk_derive;

/// Derive macro to implement `AstarteAggregate` trait with `feature = ["derive"]`.
#[cfg(feature = "derive")]
pub use astarte_device_sdk_derive::AstarteAggregate;

/// Astarte device implementation.
///
/// Provides functionality to transmit and receive individual and object datastreams as well
/// as properties.
pub struct AstarteDeviceSdk<S, C> {
    connection: C,
    shared: Arc<SharedDevice<S>>,
}

/// Manual implementation of [`Clone`] since the inner shared device doesn't requires the [`Clone`]
/// trait since it's inside an [`Arc`].
impl<S, C> Clone for AstarteDeviceSdk<S, C>
where
    C: Clone,
{
    fn clone(&self) -> Self {
        Self {
            connection: self.connection.clone(),
            shared: Arc::clone(&self.shared),
        }
    }
}

/// Payload format for an Astarte device event data.
#[derive(Debug, Clone, PartialEq)]
pub enum Aggregation {
    /// Individual data, can be both from a datastream or property.
    Individual(AstarteType),
    /// Object data, also called aggregate. Can only be from a datastream.
    Object(HashMap<String, AstarteType>),
}

/// Astarte device event data structure.
///
/// Data structure returned when an instance of [`AstarteDeviceSdk`] polls a valid event.
#[derive(Debug, Clone)]
pub struct AstarteDeviceDataEvent {
    /// Interface on which the event has been triggered
    pub interface: String,
    /// Path to the endpoint for which the event has been triggered
    pub path: String,
    /// Payload of the event
    pub data: Aggregation,
}

#[async_trait]
pub trait Device {
    // ------------------------------------------------------------------------
    // object types
    // ------------------------------------------------------------------------

    /// Send an object datastreamy on an interface, with an explicit timestamp.
    ///
    /// ```no_run
    /// # use astarte_device_sdk::{AstarteDeviceSdk, Device, builder::{DeviceBuilder, MqttConfig}, AstarteAggregate};
    /// #[cfg(not(feature = "derive"))]
    /// use astarte_device_sdk_derive::AstarteAggregate;
    /// use chrono::{TimeZone, Utc};
    ///
    /// #[derive(AstarteAggregate)]
    /// struct TestObject {
    ///     endpoint1: f64,
    ///     endpoint2: bool,
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mqtt_config = MqttConfig::new("_", "_", "_", "_");
    ///
    ///     let (mut device, _rx_events) = DeviceBuilder::new()
    ///         .connect_mqtt(mqtt_config).await.unwrap();
    ///
    ///     let data = TestObject {
    ///         endpoint1: 1.34,
    ///         endpoint2: false
    ///     };
    ///     let timestamp = Utc.timestamp_opt(1537449422, 0).unwrap();
    ///     device.send_object_with_timestamp("my.interface.name", "/endpoint/path", data, timestamp)
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
    async fn send_object_with_timestamp<D>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), Error>
    where
        D: AstarteAggregate + Send;

    /// Send an object datastream on an interface.
    ///
    /// The usage is the same of
    /// [send_object_with_timestamp()][crate::AstarteDeviceSdk::send_object_with_timestamp],
    /// without the timestamp.
    async fn send_object<D>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
    ) -> Result<(), Error>
    where
        D: AstarteAggregate + Send;

    // ------------------------------------------------------------------------
    // individual types
    // ------------------------------------------------------------------------

    /// Send an individual datastream/property on an interface, with an explicit timestamp.
    ///
    /// ```no_run
    /// use astarte_device_sdk::{AstarteDeviceSdk, Device, builder::{DeviceBuilder, MqttConfig}};
    /// use chrono::{TimeZone, Utc};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mqtt_config = MqttConfig::new("_", "_", "_", "_");
    ///
    ///     let (mut device, _rx_events) = DeviceBuilder::new()
    ///         .connect_mqtt(mqtt_config).await.unwrap();
    ///
    ///     let value: i32 = 42;
    ///     let timestamp = Utc.timestamp_opt(1537449422, 0).unwrap();
    ///     device.send_with_timestamp("my.interface.name", "/endpoint/path", value, timestamp)
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
    async fn send_with_timestamp<D>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), Error>
    where
        D: TryInto<AstarteType> + Send;

    /// Send an individual datastream/property on an interface.
    ///
    /// The usage is the same of
    /// [send_with_timestamp()][crate::AstarteDeviceSdk::send_with_timestamp],
    /// without the timestamp.
    async fn send<D>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
    ) -> Result<(), Error>
    where
        D: TryInto<AstarteType> + Send;

    /// Poll updates from the connection implementation, can be placed in a loop to receive data.
    ///
    /// This is a blocking function. It should be placed on a dedicated thread/task or as the main
    /// thread.
    ///
    /// ```no_run
    /// use astarte_device_sdk::{AstarteDeviceSdk, Device, builder::{DeviceBuilder, MqttConfig}};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mqtt_config = MqttConfig::new("_", "_", "_", "_");
    ///
    ///     let (mut device, mut rx_events) = DeviceBuilder::new()
    ///         .connect_mqtt(mqtt_config).await.unwrap();
    ///
    ///     tokio::spawn(async move {
    ///         while let Some(event) = rx_events.recv().await {
    ///             assert!(event.is_ok());
    ///         };
    ///     });
    ///
    ///     device.handle_events().await;
    /// }
    /// ```
    async fn handle_events(&mut self) -> Result<(), crate::Error>;

    /// Unset a device property.
    ///
    /// ```no_run
    /// use astarte_device_sdk::{AstarteDeviceSdk, Device, builder::{DeviceBuilder, MqttConfig}};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mqtt_config = MqttConfig::new("_", "_", "_", "_");
    ///
    ///     let (mut device, _rx_events) = DeviceBuilder::new()
    ///         .connect_mqtt(mqtt_config).await.unwrap();
    ///
    ///     device
    ///         .unset("my.interface.name", "/endpoint/path",)
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
    async fn unset(&self, interface_name: &str, interface_path: &str) -> Result<(), Error>;
}

#[async_trait]
pub trait InterfaceRegistry {
    /// Add a new [`Interface`] to the device interfaces.
    async fn add_interface(&self, interface: Interface) -> Result<(), Error>;

    /// Add a new interface from the provided file.
    async fn add_interface_from_file(&self, file_path: &str) -> Result<(), Error>;

    /// Add a new interface from a string. The string should contain a valid json formatted
    /// interface.
    async fn add_interface_from_str(&self, json_str: &str) -> Result<(), Error>;

    /// Remove the interface with the name specified as argument.
    async fn remove_interface(&self, interface_name: &str) -> Result<(), Error>;
}

#[async_trait]
pub trait PropertyRegistry {
    /// Get property, when present, from the allocated storage.
    ///
    /// ```no_run
    /// use astarte_device_sdk::{
    ///     AstarteDeviceSdk, PropertyRegistry, store::sqlite::SqliteStore, builder::DeviceBuilder,
    ///     builder::MqttConfig, types::AstarteType
    /// };
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let database = SqliteStore::new("path/to/database/file.sqlite")
    ///         .await
    ///         .unwrap();
    ///     let mqtt_config = MqttConfig::new("_", "_", "_", "_");
    ///
    ///     let (mut device, _rx_events) = DeviceBuilder::new().store(database)
    ///         .connect_mqtt(mqtt_config).await.unwrap();
    ///
    ///     let property_value: Option<AstarteType> = device
    ///         .get_property("my.interface.name", "/endpoint/path",)
    ///         .await
    ///         .unwrap();
    /// }
    /// ```
    async fn get_property(&self, interface: &str, path: &str)
        -> Result<Option<AstarteType>, Error>;
}

#[async_trait]
impl<S, C> Device for AstarteDeviceSdk<S, C>
where
    S: PropertyStore,
    C: Connection<S>,
{
    async fn send_object_with_timestamp<D>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), Error>
    where
        D: AstarteAggregate + Send,
    {
        let path = MappingPath::try_from(interface_path)?;

        self.send_object_impl(interface_name, &path, data, Some(timestamp))
            .await
    }

    async fn send_object<D>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
    ) -> Result<(), Error>
    where
        D: AstarteAggregate + Send,
    {
        let path = MappingPath::try_from(interface_path)?;

        self.send_object_impl(interface_name, &path, data, None)
            .await
    }

    async fn send<D>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
    ) -> Result<(), Error>
    where
        D: TryInto<AstarteType> + Send,
    {
        let path = MappingPath::try_from(interface_path)?;

        self.send_store_impl(interface_name, &path, data, None)
            .await
    }

    async fn send_with_timestamp<D>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), Error>
    where
        D: TryInto<AstarteType> + Send,
    {
        let mapping = MappingPath::try_from(interface_path)?;

        self.send_store_impl(interface_name, &mapping, data, Some(timestamp))
            .await
    }

    async fn unset(&self, interface_name: &str, interface_path: &str) -> Result<(), Error> {
        trace!("unsetting {} {}", interface_name, interface_path);

        let path = MappingPath::try_from(interface_path)?;

        self.send_store_impl(interface_name, &path, AstarteType::Unset, None)
            .await
    }

    async fn handle_events(&mut self) -> Result<(), crate::Error> {
        loop {
            let event_payload = self.connection.next_event(&self.shared).await?;
            let device = self.clone();

            tokio::spawn(async move {
                let data = device
                    .handle_event(&event_payload)
                    .await
                    .map(|aggregation| AstarteDeviceDataEvent {
                        interface: event_payload.interface,
                        path: event_payload.path,
                        data: aggregation,
                    });

                device.tx.send(data).await.expect("Channel dropped")
            });
        }
    }
}

#[async_trait]
impl<S, C> InterfaceRegistry for AstarteDeviceSdk<S, C>
where
    S: PropertyStore,
    C: Connection<S> + Registry,
{
    async fn add_interface(&self, interface: Interface) -> Result<(), Error> {
        if interface.ownership() == interface::Ownership::Server {
            self.connection
                .subscribe(interface.interface_name())
                .await?;
        }

        self.interfaces.write().await.add(interface)?;
        self.connection
            .send_introspection(self.interfaces.read().await.get_introspection_string())
            .await?;

        Ok(())
    }

    async fn add_interface_from_file(&self, file_path: &str) -> Result<(), Error> {
        let path = Path::new(file_path);
        let interface = Interface::from_file(path)?;

        self.add_interface(interface).await
    }

    async fn add_interface_from_str(&self, json_str: &str) -> Result<(), Error> {
        let interface: Interface = Interface::from_str(json_str)?;

        self.add_interface(interface).await
    }

    async fn remove_interface(&self, interface_name: &str) -> Result<(), Error> {
        let interface = self.remove_interface_from_map(interface_name).await?;
        self.remove_properties_from_store(interface_name).await?;
        self.connection
            .send_introspection(self.interfaces.read().await.get_introspection_string())
            .await?;

        if interface.ownership() == interface::Ownership::Server {
            self.connection.unsubscribe(interface_name).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl<S, C> PropertyRegistry for AstarteDeviceSdk<S, C>
where
    S: PropertyStore,
    C: Connection<S>,
{
    async fn get_property(
        &self,
        interface: &str,
        path: &str,
    ) -> Result<Option<AstarteType>, Error> {
        let path_mapping = MappingPath::try_from(path)?;

        let interfaces = &self.interfaces.read().await;
        let mapping = interfaces.property_mapping(interface, &path_mapping)?;

        self.property(&mapping, &path_mapping).await
    }
}

impl<S, C> AstarteDeviceSdk<S, C> {
    pub(crate) fn new(interfaces: Interfaces, store: S, connection: C, tx: EventSender) -> Self
    where
        S: PropertyStore,
        C: Connection<S>,
    {
        Self {
            shared: Arc::new(SharedDevice {
                interfaces: RwLock::new(interfaces),
                store: StoreWrapper::new(store),
                tx,
            }),
            connection,
        }
    }

    async fn handle_event(
        &self,
        connection_event: &ReceivedEvent<C::Payload>,
    ) -> Result<Aggregation, crate::Error>
    where
        S: PropertyStore,
        C: Connection<S>,
    {
        let ReceivedEvent {
            interface,
            path,
            payload,
        } = connection_event;

        let path = MappingPath::try_from(path.as_str())?;

        let interfaces = self.interfaces.read().await;
        let interface = interfaces.get(interface).ok_or_else(|| {
            warn!("publish on missing interface {interface} ({path})");
            Error::MissingInterface(interface.to_string())
        })?;

        let (data, timestamp) = match interface.aggregation() {
            InterfaceAggregation::Individual => {
                self.handle_payload_individual(interface, &path, payload)
                    .await?
            }
            InterfaceAggregation::Object => {
                self.handle_payload_object(interface, &path, payload)
                    .await?
            }
        };

        debug!("received {{v: {data:?}, t: {timestamp:?}}}");

        Ok(data)
    }

    /// Handles the payload of an interface with [`InterfaceAggregation::Individual`]
    async fn handle_payload_individual<'a>(
        &self,
        interface: &Interface,
        path: &MappingPath<'a>,
        payload: &C::Payload,
    ) -> Result<(Aggregation, Option<chrono::DateTime<chrono::Utc>>), Error>
    where
        S: PropertyStore,
        C: Connection<S>,
    {
        let mapping = interface
            .as_mapping_ref(path)
            .ok_or_else(|| Error::MissingMapping {
                interface: interface.interface_name().to_string(),
                mapping: path.to_string(),
            })?;

        let (data, timestamp) = self.connection.deserialize_individual(mapping, payload)?;

        if interface.is_property() {
            let interface_name = interface.interface_name();
            let version_major = interface.version_major();

            self.store
                .store_prop(interface_name, path.as_str(), &data, version_major)
                .await?;

            info!("property stored {interface}:{version_major} {path} ");
        }

        Ok((Aggregation::Individual(data), timestamp))
    }

    /// Handles the payload of an interface with [`InterfaceAggregation::Object`]
    async fn handle_payload_object<'a>(
        &self,
        interface: &Interface,
        path: &MappingPath<'a>,
        payload: &C::Payload,
    ) -> Result<(Aggregation, Option<chrono::DateTime<chrono::Utc>>), Error>
    where
        S: PropertyStore,
        C: Connection<S>,
    {
        let object = interface.as_object_ref().ok_or(Error::Aggregation {
            exp: InterfaceAggregation::Object,
            got: InterfaceAggregation::Individual,
        })?;

        let (data, timestamp) = self.connection.deserialize_object(object, path, payload)?;

        Ok((Aggregation::Object(data), timestamp))
    }

    /// Get property, when present, from the allocated storage.
    ///
    /// This will use a [`MappingPath`] to get the property, which is an parsed endpoint.
    pub(crate) async fn property(
        &self,
        mapping: &MappingRef<'_, PropertyRef<'_>>,
        path: &MappingPath<'_>,
    ) -> Result<Option<AstarteType>, Error>
    where
        S: PropertyStore,
    {
        let interface = mapping.interface().interface_name();
        let path = path.as_str();

        let value = self
            .store
            .load_prop(interface, path, mapping.interface().version_major())
            .await?;

        let value = match value {
            Some(AstarteType::Unset) if !mapping.allow_unset() => {
                error!("stored property is unset, but interface doesn't allow it");
                self.store.delete_prop(interface, path).await?;

                None
            }
            Some(AstarteType::Unset) => Some(AstarteType::Unset),
            Some(value) if value != mapping.mapping_type() => {
                error!(
                    "stored property type mismatch, expected {} got {:?}",
                    mapping.mapping_type(),
                    value
                );
                self.store.delete_prop(interface, path).await?;

                None
            }

            Some(value) => Some(value),
            None => None,
        };

        Ok(value)
    }

    /// Checks wether a passed interface is a property and if it is already stored with the same value.
    /// Useful to prevent sending a property twice with the same value.
    /// The returned value is [`Option::None`] if the interface is not of property type.
    /// When the returned value is [`Option::Some`] the [`Result`] represents:
    /// - The property is already stored [`Result::Ok`].
    /// - The property is not stored or has a new value [`Result::Err`].
    async fn is_property_stored<'a>(
        &self,
        mapping: &MappingRef<'a, &'a Interface>,
        path: &MappingPath<'_>,
        new: &AstarteType,
    ) -> Result<Option<Result<PropertyRef<'a>, PropertyRef<'a>>>, Error>
    where
        S: PropertyStore,
    {
        let opt_property = mapping.as_prop();

        if let Some(prop_mapping) = opt_property {
            // Check if this property is already in db
            let stored = self.property(&prop_mapping, &path).await?;

            match stored {
                Some(value) if value.eq(new) => Ok(Some(Ok(*prop_mapping.interface()))),
                Some(_) | None => Ok(Some(Err(*prop_mapping.interface()))),
            }
        } else {
            Ok(None)
        }
    }

    // Dead code allowed, this function will be used when the GRPC connection implementation will be added to the sdk
    #[allow(dead_code)]
    async fn send_impl<D>(
        &self,
        interface_name: &str,
        path: &MappingPath<'_>,
        data: D,
        timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), Error>
    where
        C: Connection<S>,
        D: TryInto<AstarteType> + Send,
    {
        debug!("sending {} {}", interface_name, path);

        let interfaces = self.interfaces.read().await;
        let mapping = interfaces.interface_mapping(interface_name, path)?;

        // TODO like it has to be done for objects i need to move the validation in an independant function
        let data = data.try_into().map_err(|_| TypeError::Conversion)?;

        self.connection
            .send_individual(mapping, path, &data, timestamp)
            .await?;

        Ok(())
    }

    async fn send_store_impl<D>(
        &self,
        interface_name: &str,
        path: &MappingPath<'_>,
        data: D,
        timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), Error>
    where
        S: PropertyStore,
        C: Connection<S>,
        D: TryInto<AstarteType> + Send,
    {
        let interfaces = self.interfaces.read().await;
        let mapping = interfaces.interface_mapping(interface_name, path)?;

        let data = data.try_into().map_err(|_| TypeError::Conversion)?;

        if let Err(err) = validate_send_individual(mapping, &timestamp) {
            error!("send validation failed: {err}");

            #[cfg(debug_assertions)]
            return Err(Error::Validation(err));
        }

        trace!("sending individual type {}", data.display_type());

        let prop_stored = self.is_property_stored(&mapping, path, &data).await?;

        if prop_stored.is_some_and(|res| res.is_ok()) {
            debug!("property was already sent, no need to send it again");
            return Ok(());
        }

        // TODO like it has to be done for objects i need to move the validation in an independant function
        self.connection
            .send_individual(mapping, path, &data, timestamp)
            .await?;

        // Store the property in the database after it has been successfully sent
        // We need to manage only the Err case since the Ok was already checked before
        if let Some(Err(property)) = prop_stored {
            self.store
                .store_prop(
                    property.interface_name(),
                    path.as_str(),
                    &data,
                    property.version_major(),
                )
                .await?;

            info!("Stored new property in database");
        }

        Ok(())
    }

    async fn send_object_impl<'a, D>(
        &self,
        interface_name: &str,
        path: &MappingPath<'a>,
        data: D,
        timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), Error>
    where
        D: AstarteAggregate + Send,
        S: PropertyStore,
        C: Connection<S>,
    {
        let interfaces = self.interfaces.read().await;
        let interface = interfaces
            .get(interface_name)
            .ok_or_else(|| Error::MissingInterface(interface_name.to_string()))?;

        let object = interface
            .as_object_ref()
            .ok_or_else(|| Error::Aggregation {
                exp: InterfaceAggregation::Object,
                got: interface.aggregation(),
            })?;

        debug!("sending {} {}", interface_name, path);

        let aggregate = data.astarte_aggregate()?;

        if let Err(err) = validate_send_object(object, &timestamp) {
            error!("Send validation failed: {err}");

            #[cfg(debug_assertions)]
            return Err(Error::Validation(err));
        }

        // TODO move part of the logic of the serialize_object (The validation part to it's own function under validation that returns a wrapper object, connection should only accept that wrapper object)
        self.connection
            .send_object(object, path, &aggregate, timestamp)
            .await
    }

    async fn remove_interface_from_map(&self, interface_name: &str) -> Result<Interface, Error>
    where
        S: PropertyStore,
        C: Connection<S> + Registry,
    {
        self.interfaces
            .write()
            .await
            .remove(interface_name)
            .ok_or_else(|| {
                Error::Interface(InterfaceError::InterfaceNotFound {
                    name: interface_name.to_string(),
                })
            })
    }

    async fn remove_properties_from_store(&self, interface_name: &str) -> Result<(), Error>
    where
        S: PropertyStore,
        C: Connection<S> + Registry,
    {
        let interfaces = self.interfaces.read().await;
        let mappings = interfaces.get_property(interface_name);

        let property = match mappings {
            Some(property) => property,
            None => return Ok(()),
        };

        for mapping in property.iter_mappings() {
            let path = mapping.endpoint();
            self.store.delete_prop(interface_name, path).await?;
            debug!("Stored property {}{} deleted", interface_name, path);
        }

        Ok(())
    }
}

impl<S, C> fmt::Debug for AstarteDeviceSdk<S, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AstarteDeviceSdk")
            .field("interfaces", &self.interfaces)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod test {
    use base64::Engine;
    use mockall::predicate;
    use rumqttc::Event;
    use std::collections::HashMap;
    use std::str::FromStr;
    use tokio::sync::mpsc;

    use crate::connection::mqtt::Mqtt;
    use crate::interfaces::Interfaces;
    use crate::payload::Payload;
    use crate::properties::tests::PROPERTIES_PAYLOAD;
    use crate::store::memory::MemoryStore;
    use crate::{
        self as astarte_device_sdk, Device, EventReceiver, Interface, InterfaceRegistry,
        PropertyRegistry,
    };
    use astarte_device_sdk::AstarteAggregate;
    use astarte_device_sdk::{types::AstarteType, Aggregation, AstarteDeviceSdk};
    use astarte_device_sdk_derive::astarte_aggregate;
    #[cfg(not(feature = "derive"))]
    use astarte_device_sdk_derive::AstarteAggregate;

    use crate::connection::mqtt::{AsyncClient, EventLoop};

    // Interfaces
    pub(crate) const OBJECT_DEVICE_DATASTREAM: &str = include_str!("../examples/object_datastream/interfaces/org.astarte-platform.rust.examples.object-datastream.DeviceDatastream.json");
    pub(crate) const INDIVIDUAL_SERVER_DATASTREAM: &str = include_str!("../examples/individual_datastream/interfaces/org.astarte-platform.rust.examples.individual-datastream.ServerDatastream.json");
    pub(crate) const DEVICE_PROPERTIES: &str = include_str!("../examples/individual_properties/interfaces/org.astarte-platform.rust.examples.individual-properties.DeviceProperties.json");
    pub(crate) const SERVER_PROPERTIES: &str = include_str!("../examples/individual_properties/interfaces/org.astarte-platform.rust.examples.individual-properties.ServerProperties.json");

    pub(crate) fn mock_astarte_device<I>(
        client: AsyncClient,
        eventloop: EventLoop,
        interfaces: I,
    ) -> (AstarteDeviceSdk<MemoryStore, Mqtt>, EventReceiver)
    where
        I: IntoIterator<Item = Interface>,
    {
        let (tx, rx) = mpsc::channel(50);

        let sdk = AstarteDeviceSdk::new(
            Interfaces::from_iter(
                interfaces
                    .into_iter()
                    .map(|i| (i.interface_name().to_string(), i)),
            ),
            MemoryStore::new(),
            Mqtt::new(
                "realm".to_string(),
                "device_id".to_string(),
                eventloop,
                client,
            ),
            tx,
        );

        (sdk, rx)
    }

    #[derive(AstarteAggregate)]
    #[astarte_aggregate(rename_all = "lowercase")]
    struct MyLowerCasedAggregate {
        endpoint01: f64,
        endpoint02: i32,
        endpoint03: bool,
        endpoint04: i64,
        endpoint05: String,
        endpoint06: Vec<u8>,
        endpoint07: chrono::DateTime<chrono::Utc>,
        endpoint08: Vec<f64>,
        endpoint09: Vec<i32>,
        endpoint10: Vec<bool>,
        endpoint11: Vec<i64>,
        endpoint12: Vec<String>,
        endpoint13: Vec<Vec<u8>>,
        endpoint14: Vec<chrono::DateTime<chrono::Utc>>,
    }

    #[test]
    fn test_astarte_aggregate_trait_lower_case_attribute() {
        let my_aggregate = MyLowerCasedAggregate {
            endpoint01: 4.34,
            endpoint02: 1,
            endpoint03: true,
            endpoint04: 45543543534,
            endpoint05: "Hello".to_string(),
            endpoint06: base64::engine::general_purpose::STANDARD
                .decode("aGVsbG8=")
                .unwrap(),
            endpoint07: chrono::offset::Utc::now(),
            endpoint08: Vec::from([43.5, 10.5, 11.9]),
            endpoint09: Vec::from([-4, 123, -2222, 30]),
            endpoint10: Vec::from([true, false]),
            endpoint11: Vec::from([53267895478, 53267895428, 53267895118]),
            endpoint12: Vec::from(["Test ".to_string(), "String".to_string()]),
            endpoint13: Vec::from([
                base64::engine::general_purpose::STANDARD
                    .decode("aGVsbG8=")
                    .unwrap(),
                base64::engine::general_purpose::STANDARD
                    .decode("aGVsbG8=")
                    .unwrap(),
            ]),
            endpoint14: Vec::from([chrono::offset::Utc::now(), chrono::offset::Utc::now()]),
        };
        let expected_res = HashMap::from([
            (
                "endpoint01".to_string(),
                AstarteType::Double(my_aggregate.endpoint01),
            ),
            (
                "endpoint02".to_string(),
                AstarteType::Integer(my_aggregate.endpoint02),
            ),
            (
                "endpoint03".to_string(),
                AstarteType::Boolean(my_aggregate.endpoint03),
            ),
            (
                "endpoint04".to_string(),
                AstarteType::LongInteger(my_aggregate.endpoint04),
            ),
            (
                "endpoint05".to_string(),
                AstarteType::String(my_aggregate.endpoint05.clone()),
            ),
            (
                "endpoint06".to_string(),
                AstarteType::BinaryBlob(my_aggregate.endpoint06.clone()),
            ),
            (
                "endpoint07".to_string(),
                AstarteType::DateTime(my_aggregate.endpoint07),
            ),
            (
                "endpoint08".to_string(),
                AstarteType::DoubleArray(my_aggregate.endpoint08.clone()),
            ),
            (
                "endpoint09".to_string(),
                AstarteType::IntegerArray(my_aggregate.endpoint09.clone()),
            ),
            (
                "endpoint10".to_string(),
                AstarteType::BooleanArray(my_aggregate.endpoint10.clone()),
            ),
            (
                "endpoint11".to_string(),
                AstarteType::LongIntegerArray(my_aggregate.endpoint11.clone()),
            ),
            (
                "endpoint12".to_string(),
                AstarteType::StringArray(my_aggregate.endpoint12.clone()),
            ),
            (
                "endpoint13".to_string(),
                AstarteType::BinaryBlobArray(my_aggregate.endpoint13.clone()),
            ),
            (
                "endpoint14".to_string(),
                AstarteType::DateTimeArray(my_aggregate.endpoint14.clone()),
            ),
        ]);
        assert_eq!(expected_res, my_aggregate.astarte_aggregate().unwrap());
        println!("{expected_res:?}");
    }

    #[derive(AstarteAggregate)]
    #[astarte_aggregate(rename_all = "UPPERCASE")]
    struct MyUpperCasedAggregate {
        first_endpoint: f64,
        second_endpoint: f64,
    }

    #[test]
    fn test_astarte_aggregate_trait_upper_case_attribute() {
        let my_aggregate = MyUpperCasedAggregate {
            first_endpoint: 4.34,
            second_endpoint: 23.0,
        };
        let expected_res = HashMap::from([
            (
                "FIRST_ENDPOINT".to_string(),
                AstarteType::Double(my_aggregate.first_endpoint),
            ),
            (
                "SECOND_ENDPOINT".to_string(),
                AstarteType::Double(my_aggregate.second_endpoint),
            ),
        ]);
        assert_eq!(expected_res, my_aggregate.astarte_aggregate().unwrap());
    }

    #[derive(AstarteAggregate)]
    #[astarte_aggregate(rename_all = "PascalCase")]
    struct MyPascalCasedAggregate {
        first_endpoint: f64,
        second_endpoint: f64,
    }

    #[test]
    fn test_astarte_aggregate_trait_pascal_case_attribute() {
        let my_aggregate = MyPascalCasedAggregate {
            first_endpoint: 4.34,
            second_endpoint: 23.0,
        };
        let expected_res = HashMap::from([
            (
                "FirstEndpoint".to_string(),
                AstarteType::Double(my_aggregate.first_endpoint),
            ),
            (
                "SecondEndpoint".to_string(),
                AstarteType::Double(my_aggregate.second_endpoint),
            ),
        ]);
        assert_eq!(expected_res, my_aggregate.astarte_aggregate().unwrap());
    }

    #[tokio::test]
    async fn test_property_set_unset() {
        let eventloop = EventLoop::default();

        let mut client = AsyncClient::default();

        client
            .expect_publish::<String, Vec<u8>>()
            .returning(|_, _, _, _| Ok(()));

        let (device, _rx) = mock_astarte_device(
            client,
            eventloop,
            [Interface::from_str(DEVICE_PROPERTIES).unwrap()],
        );

        let expected = AstarteType::String("value".to_string());
        device
            .send(
                "org.astarte-platform.rust.examples.individual-properties.DeviceProperties",
                "/1/name",
                expected.clone(),
            )
            .await
            .expect("Failed to send property");

        let val = device
            .get_property(
                "org.astarte-platform.rust.examples.individual-properties.DeviceProperties",
                "/1/name",
            )
            .await
            .expect("Failed to get property")
            .expect("Property not found");
        assert_eq!(expected, val);

        device
            .unset(
                "org.astarte-platform.rust.examples.individual-properties.DeviceProperties",
                "/1/name",
            )
            .await
            .expect("Failed to unset property");

        let val = device
            .get_property(
                "org.astarte-platform.rust.examples.individual-properties.DeviceProperties",
                "/1/name",
            )
            .await
            .expect("Failed to get property")
            .expect("Property not found");

        assert_eq!(AstarteType::Unset, val);
    }

    #[tokio::test]
    async fn test_add_remove_interface() {
        let eventloope = EventLoop::default();

        let mut client = AsyncClient::default();

        client
            .expect_subscribe::<String>()
            .once()
            .with(
                predicate::eq("realm/device_id/org.astarte-platform.rust.examples.individual-datastream.ServerDatastream/#".to_string()),
                predicate::always()
            )
            .returning(|_, _| { Ok(()) });

        client
            .expect_publish::<String, String>()
            .with(
                predicate::eq("realm/device_id".to_string()),
                predicate::always(),
                predicate::eq(false),
                predicate::eq(
                    "org.astarte-platform.rust.examples.individual-datastream.ServerDatastream:0:1"
                        .to_string(),
                ),
            )
            .returning(|_, _, _, _| Ok(()));

        client
            .expect_publish::<String, String>()
            .with(
                predicate::eq("realm/device_id".to_string()),
                predicate::always(),
                predicate::eq(false),
                predicate::eq(String::new()),
            )
            .returning(|_, _, _, _| Ok(()));

        client
            .expect_unsubscribe::<String>()
            .with(predicate::eq("realm/device_id/org.astarte-platform.rust.examples.individual-datastream.ServerDatastream/#".to_string()))
            .returning(|_| Ok(()));

        let (astarte, _rx) = mock_astarte_device(client, eventloope, []);

        astarte
            .add_interface_from_str(INDIVIDUAL_SERVER_DATASTREAM)
            .await
            .unwrap();

        astarte
            .remove_interface(
                "org.astarte-platform.rust.examples.individual-datastream.ServerDatastream",
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_handle_event() {
        let mut client = AsyncClient::default();

        client
            .expect_clone()
            // number of calls not limited since the clone it's inside a loop
            .returning(AsyncClient::default);

        client
            .expect_publish::<String, Vec<u8>>()
            .once()
            .with(
                predicate::eq("realm/device_id/org.astarte-platform.rust.examples.individual-properties.DeviceProperties/1/name".to_string()),
                predicate::always(),
                predicate::always(),
                predicate::function(|buf: &Vec<u8>| {
                    let doc= bson::Document::from_reader(buf.as_slice()).unwrap();

                    let value = doc.get("v").unwrap().as_str().unwrap();

                    value == "name number 1"
                }),
            )
            .returning(|_, _, _, _| Ok(()));

        let mut eventloope = EventLoop::default();

        let data = bson::doc! {
            "v": true
        };

        // Purge properties
        eventloope.expect_poll().once().returning(|| {
            Ok(Event::Incoming(rumqttc::Packet::Publish(
                rumqttc::Publish::new(
                    "realm/device_id/control/consumer/properties",
                    rumqttc::QoS::AtLeastOnce,
                    PROPERTIES_PAYLOAD,
                ),
            )))
        });

        // Send properties
        eventloope.expect_poll().once().returning(move || {
            Ok(Event::Incoming(rumqttc::Packet::Publish(
                rumqttc::Publish::new(
                    "realm/device_id/org.astarte-platform.rust.examples.individual-properties.ServerProperties/1/enable",
                    rumqttc::QoS::AtLeastOnce,
                    bson::to_vec(&data).unwrap()
                ),
            )))
        });

        let (mut astarte, mut rx) = mock_astarte_device(
            client,
            eventloope,
            [
                Interface::from_str(DEVICE_PROPERTIES).unwrap(),
                Interface::from_str(SERVER_PROPERTIES).unwrap(),
            ],
        );

        astarte
            .send(
                "org.astarte-platform.rust.examples.individual-properties.DeviceProperties",
                "/1/name",
                "name number 1".to_string(),
            )
            .await
            .unwrap();

        let handle_events = tokio::spawn(async move {
            astarte
                .handle_events()
                .await
                .expect("failed to poll events");
        });

        let event = rx.recv().await.expect("no event received");

        assert!(
            event.is_ok(),
            "Error handling event {:?}",
            event.unwrap_err()
        );

        let event = event.unwrap();

        assert_eq!("/1/enable", event.path);

        match event.data {
            Aggregation::Individual(AstarteType::Boolean(val)) => {
                assert!(val);
            }
            _ => panic!("Wrong data type {:?}", event.data),
        }

        handle_events.abort();
        let _ = handle_events.await;
    }

    #[tokio::test]
    async fn test_unset_property() {
        let mut client = AsyncClient::default();

        let value = AstarteType::String(String::from("name number 1"));
        let buf = Payload::new(&value).to_vec().unwrap();

        let unset = Vec::new();

        client
            .expect_publish::<String, Vec<u8>>()
            .once()
            .with(
                predicate::eq("realm/device_id/org.astarte-platform.rust.examples.individual-properties.DeviceProperties/1/name".to_string()),
                predicate::always(),
                predicate::always(),
                predicate::eq(buf),
            )
            .returning(|_, _, _, _| Ok(()));

        client
            .expect_publish::<String, Vec<u8>>()
            .once()
            .with(
                predicate::eq("realm/device_id/org.astarte-platform.rust.examples.individual-properties.DeviceProperties/1/name".to_string()),
                predicate::always(),
                predicate::always(),
                predicate::eq(unset)
            )
            .returning(|_, _, _, _| Ok(()));

        let eventloope = EventLoop::default();

        let (astarte, _rx) = mock_astarte_device(
            client,
            eventloope,
            [Interface::from_str(DEVICE_PROPERTIES).unwrap()],
        );

        astarte
            .send(
                "org.astarte-platform.rust.examples.individual-properties.DeviceProperties",
                "/1/name",
                "name number 1".to_string(),
            )
            .await
            .unwrap();

        astarte
            .unset(
                "org.astarte-platform.rust.examples.individual-properties.DeviceProperties",
                "/1/name",
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_receive_object() {
        let mut client = AsyncClient::default();

        client
            .expect_clone()
            // number of calls not limited since the clone it's inside a loop
            .returning(AsyncClient::default);

        let mut eventloope = EventLoop::default();

        let data = bson::doc! {
            "v": {
                "endpoint1": 4.2,
                "endpoint2": "obj",
                "endpoint3": [true],
            }
        };

        // Send object
        eventloope.expect_poll().returning(move || {
            Ok(Event::Incoming(rumqttc::Packet::Publish(
                rumqttc::Publish::new(
                    "realm/device_id/org.astarte-platform.rust.examples.object-datastream.DeviceDatastream/1",
                    rumqttc::QoS::AtLeastOnce,
                    bson::to_vec(&data).unwrap()
                ),
            )))
        });

        let (mut astarte, mut rx) = mock_astarte_device(
            client,
            eventloope,
            [Interface::from_str(OBJECT_DEVICE_DATASTREAM).unwrap()],
        );

        let handle_events = tokio::spawn(async move {
            astarte
                .handle_events()
                .await
                .expect("failed to poll events");
        });

        let event = rx.recv().await.expect("no event received");

        assert!(
            event.is_ok(),
            "Error handling event {:?}",
            event.unwrap_err()
        );

        let event = event.unwrap();

        let mut obj = HashMap::new();
        obj.insert("endpoint1".to_string(), AstarteType::Double(4.2));
        obj.insert(
            "endpoint2".to_string(),
            AstarteType::String("obj".to_string()),
        );
        obj.insert(
            "endpoint3".to_string(),
            AstarteType::BooleanArray(vec![true]),
        );
        let expected = Aggregation::Object(obj);

        assert_eq!(
            "org.astarte-platform.rust.examples.object-datastream.DeviceDatastream",
            event.interface
        );
        assert_eq!("/1", event.path);
        assert_eq!(expected, event.data);

        handle_events.abort();
        let _ = handle_events.await;
    }
}
