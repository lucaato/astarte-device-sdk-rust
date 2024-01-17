// This file is part of Astarte.
// Copyright 2023 SECO Mind Srl
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

//! Errors generated by the interface module.

use std::io;

use super::{
    mapping::endpoint::EndpointError, validation::VersionChangeError, MAX_INTERFACE_MAPPINGS,
};

/// Error for parsing and validating an interface.
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum InterfaceError {
    /// Couldn't parse the interface JSON.
    #[error("cannot parse interface JSON")]
    Parse(#[from] serde_json::Error),
    /// Cannot read the interface file.
    #[error("cannot read interface file")]
    Io(#[from] io::Error),
    /// Both major and minor are 0.
    #[error("wrong major and minor")]
    MajorMinor,
    /// The interface has no mapping with the given path.
    #[error("couldn't find the mapping '{path}' in the interface")]
    MappingNotFound { path: String },
    /// The database retention policy is set to `use_ttl` but the TTL was not specified.
    #[error("Database retention policy set to `use_ttl` but the TTL was not specified")]
    MissingTtl,
    /// Error while parsing the endpoint.
    #[error("invalid endpoint")]
    InvalidEndpoint(#[from] EndpointError),
    /// The interface has no mappings.
    #[error("interface with no mappings")]
    EmptyMappings,
    /// The object must have the mappings with the same ttl and retention policy.
    #[error("object with inconsistent mappings")]
    InconsistentMapping,
    /// The object interface must have the same levels for every mapping, except the last one.
    #[error("object with inconsistent endpoints")]
    InconsistentEndpoints,
    /// The interfaced endpoints must all be unique.
    #[error("duplicate endpoint mapping '{endpoint}' and '{duplicate}'")]
    DuplicateMapping { endpoint: String, duplicate: String },
    /// The object interface should have at least 2 levels.
    #[error("object endpoint should have at least 2 levels: '{0}'")]
    ObjectEndpointTooShort(String),
    /// The name of the interface was changed.
    #[error( r#"this version has a different name than the previous version name: {name} prev_name: {prev_name}"#)]
    NameMismatch { name: String, prev_name: String },
    /// Invalid version change.
    #[error("invalid version: {0}")]
    Version(VersionChangeError),
    /// Interface with too many mappings
    #[error(
        "too many mappings {0}, interfaces can have a max of {} mappings",
        MAX_INTERFACE_MAPPINGS
    )]
    TooManyMappings(usize),
}
