use std::{path::Path, str::FromStr};

use async_trait::async_trait;
use log::trace;

use crate::{Timestamp, AstarteAggregate, types::AstarteType, Interface, store::PropertyStore, connection::{Connection, Registry}, interface::{mapping::path::MappingPath, Ownership}, AstarteDeviceDataEvent, AstarteDeviceSdk};

#[async_trait]
pub trait Device {
    /// Send an object datastream on an interface, with an explicit timestamp.
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
    ///     let mqtt_config = MqttConfig::new("realm_id", "device_id", "credential_secret", "pairing_url");
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
        timestamp: Timestamp,
    ) -> Result<(), crate::Error>
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
    ) -> Result<(), crate::Error>
    where
        D: AstarteAggregate + Send;

    /// Send an individual datastream/property on an interface, with an explicit timestamp.
    ///
    /// ```no_run
    /// use astarte_device_sdk::{AstarteDeviceSdk, Device, builder::{DeviceBuilder, MqttConfig}};
    /// use chrono::{TimeZone, Utc};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mqtt_config = MqttConfig::new("realm_id", "device_id", "credential_secret", "pairing_url");
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
    ) -> Result<(), crate::Error>
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
    ) -> Result<(), crate::Error>
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
    ///     let mqtt_config = MqttConfig::new("realm_id", "device_id", "credential_secret", "pairing_url");
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
    ///     let mqtt_config = MqttConfig::new("realm_id", "device_id", "credential_secret", "pairing_url");
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
    async fn unset(&self, interface_name: &str, interface_path: &str) -> Result<(), crate::Error>;
}

#[async_trait]
pub trait InterfaceRegistry {
    /// Add a new [`Interface`] to the device interfaces.
    async fn add_interface(&self, interface: Interface) -> Result<(), crate::Error>;

    /// Add a new interface from the provided file.
    async fn add_interface_from_file<P>(&self, file_path: P) -> Result<(), crate::Error>
    where
        P: AsRef<Path> + Send;

    /// Add a new interface from a string. The string should contain a valid json formatted
    /// interface.
    async fn add_interface_from_str(&self, json_str: &str) -> Result<(), crate::Error>;

    /// Remove the interface with the name specified as argument.
    async fn remove_interface(&self, interface_name: &str) -> Result<(), crate::Error>;
}

#[async_trait]
impl<S, C> Device for AstarteDeviceSdk<S, C>
where
    S: PropertyStore,
    C: Connection<S> + Send + Sync + 'static,
{
    async fn send_object_with_timestamp<D>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), crate::Error>
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
    ) -> Result<(), crate::Error>
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
    ) -> Result<(), crate::Error>
    where
        D: TryInto<AstarteType> + Send,
    {
        let path = MappingPath::try_from(interface_path)?;

        self.send_individual_impl(interface_name, &path, data, None)
            .await
    }

    async fn send_with_timestamp<D>(
        &self,
        interface_name: &str,
        interface_path: &str,
        data: D,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), crate::Error>
    where
        D: TryInto<AstarteType> + Send,
    {
        let mapping = MappingPath::try_from(interface_path)?;

        self.send_individual_impl(interface_name, &mapping, data, Some(timestamp))
            .await
    }

    async fn unset(&self, interface_name: &str, interface_path: &str) -> Result<(), crate::Error> {
        trace!("unsetting {} {}", interface_name, interface_path);

        let path = MappingPath::try_from(interface_path)?;

        self.send_individual_impl(interface_name, &path, AstarteType::Unset, None)
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
    C: Connection<S> + Registry + Send + Sync,
{
    async fn add_interface(&self, interface: Interface) -> Result<(), crate::Error> {
        if interface.ownership() == Ownership::Server {
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

    async fn add_interface_from_file<P>(&self, file_path: P) -> Result<(), crate::Error>
    where
        P: AsRef<Path> + Send,
    {
        let interface = Interface::from_file(file_path.as_ref())?;

        self.add_interface(interface).await
    }

    async fn add_interface_from_str(&self, json_str: &str) -> Result<(), crate::Error> {
        let interface: Interface = Interface::from_str(json_str)?;

        self.add_interface(interface).await
    }

    async fn remove_interface(&self, interface_name: &str) -> Result<(), crate::Error> {
        let interface = self.remove_interface_from_map(interface_name).await?;
        self.remove_properties_from_store(interface_name).await?;
        self.connection
            .send_introspection(self.interfaces.read().await.get_introspection_string())
            .await?;

        if interface.ownership() == Ownership::Server {
            self.connection.unsubscribe(interface_name).await?;
        }

        Ok(())
    }
}
