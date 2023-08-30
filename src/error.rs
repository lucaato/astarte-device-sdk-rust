// This file is part of Astarte.
//
// Copyright 2023 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//! Error types for the Astarte SDK.

use std::convert::Infallible;

use crate::interface::mapping::path::MappingError;
use crate::interface::InterfaceError;
use crate::options::BuilderError;
use crate::payload::PayloadError;
use crate::properties::PropertiesError;
use crate::store::error::StoreError;
use crate::topic::TopicError;
use crate::types::TypeError;

/// Astarte error.
///
/// Possible errors returned by functions of the Astarte device SDK.
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("bson client error")]
    BsonClientError(#[from] rumqttc::ClientError),

    #[error("mqtt connection error")]
    ConnectionError(#[from] rumqttc::ConnectionError),

    /// The e connection poll reached the max number of retries.
    #[error("mqtt connection reached max retries")]
    ConnectionTimeout,

    #[error("send error ({0})")]
    SendError(String),

    #[error("receive error ({0})")]
    ReceiveError(String),

    #[error("options error")]
    OptionsError(#[from] BuilderError),

    #[error("invalid interface")]
    Interface(#[from] InterfaceError),

    #[error("generic error ({0})")]
    Reported(String),

    #[error("generic error")]
    Unreported,

    #[error("infallible error")]
    Infallible(#[from] Infallible),

    #[error("invalid topic {}",.0.topic())]
    InvalidTopic(#[from] TopicError),

    #[error("invalid mapping path '{}'", .0.path())]
    InvalidEndpoint(#[from] MappingError),

    /// Errors when converting between Astarte types.
    #[error("couldn't convert to Astarte Type")]
    Types(#[from] TypeError),

    /// Errors that can occur handling the payload.
    #[error("couldn't process payload")]
    Payload(#[from] PayloadError),

    /// Error while parsing the /control/consumer/properties payload.
    #[error("couldn't handle properties")]
    Properties(#[from] PropertiesError),

    /// Error returned by a store operation.
    #[error("could't complete store operation")]
    Database(#[from] StoreError),
}
