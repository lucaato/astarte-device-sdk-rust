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
use std::collections::HashMap;

use async_trait::async_trait;

use crate::{
    interface::mapping::path::MappingPath,
    interfaces::{MappingRef, ObjectRef},
    shared::SharedDevice,
    types::AstarteType,
    Interface, Timestamp,
};

pub mod mqtt;

pub(crate) struct ReceivedEvent<P: Send> {
    pub(crate) interface: String,
    pub(crate) path: String,
    pub(crate) payload: P,
}

#[async_trait]
pub(crate) trait Connection<S>: Send + Sync + Clone + 'static {
    type Payload: Send + Sync + 'static;

    async fn connect(&self, device: &SharedDevice<S>) -> Result<(), crate::Error>;

    async fn next_event(
        &self,
        device: &SharedDevice<S>,
    ) -> Result<ReceivedEvent<Self::Payload>, crate::Error>;

    fn deserialize_individual(
        &self,
        mappig: MappingRef<'_, &Interface>,
        payload: &Self::Payload,
    ) -> Result<(AstarteType, Option<Timestamp>), crate::Error>;

    fn deserialize_object(
        &self,
        object: ObjectRef,
        path: &MappingPath<'_>,
        payload: &Self::Payload,
    ) -> Result<(HashMap<String, AstarteType>, Option<Timestamp>), crate::Error>;

    async fn send_individual<'a>(
        &self,
        mapping: MappingRef<'a, &'a Interface>,
        path: &MappingPath<'_>,
        data: &AstarteType,
        timestamp: Option<Timestamp>,
    ) -> Result<(), crate::Error>;

    async fn send_object(
        &self,
        object: ObjectRef<'_>,
        path: &MappingPath<'_>,
        data: &HashMap<String, AstarteType>,
        timestamp: Option<Timestamp>,
    ) -> Result<(), crate::Error>;
}

#[async_trait]
pub(crate) trait Registry {
    async fn subscribe(&self, interface: &str) -> Result<(), crate::Error>;

    async fn unsubscribe(&self, interface: &str) -> Result<(), crate::Error>;

    async fn send_introspection(&self, introspection: String) -> Result<(), crate::Error>;
}
