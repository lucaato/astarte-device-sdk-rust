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

//! Parses a topic into the different components.

use tracing::trace;

use super::ClientId;

/// Error returned when parsing a topic.
///
/// We expect the topic to be in the form `<realm>/<device_id>/<interface>/<path>`.
#[non_exhaustive]
#[derive(Debug, Clone, thiserror::Error)]
pub enum TopicError {
    /// Empty topic
    #[error("topic is empty")]
    Empty,
    /// The topic should start with `<realm>/<device_id>`.
    #[error(
        "the topic should start with <realm>/<device_id> equal to {client_id}, received: {topic}"
    )]
    UnknownClientId {
        /// Client id
        client_id: String,
        /// Topic
        topic: String,
    },
    /// The topic should be in the form `<realm>/<device_id>/<interface>/<path>`.
    #[error(
        "the topic should be in the form <realm>/<device_id>/<interface>/<path>, received: {0}"
    )]
    Malformed(String),
}

impl TopicError {
    /// Return the topic str
    pub fn topic(&self) -> &str {
        match self {
            TopicError::Empty => "",
            TopicError::UnknownClientId { topic, .. } => topic,
            TopicError::Malformed(topic) => topic,
        }
    }
}

#[derive(Debug)]
pub(crate) enum ParsedTopic<'a> {
    PurgeProperties,
    InterfacePath { interface: &'a str, path: &'a str },
}

impl<'a> ParsedTopic<'a> {
    const PURGE_PROPERTIES_TOPIC: &'static str = "control/consumer/properties";

    pub(crate) fn try_parse(client_id: ClientId<&str>, topic: &'a str) -> Result<Self, TopicError> {
        if topic.is_empty() {
            return Err(TopicError::Empty);
        }

        let rest = topic
            .strip_prefix(client_id.realm)
            .and_then(|s| s.strip_prefix('/'))
            .and_then(|s| s.strip_prefix(client_id.device_id))
            .ok_or(TopicError::UnknownClientId {
                client_id: client_id.to_string(),
                topic: topic.to_string(),
            })?;

        let rest = rest
            .strip_prefix('/')
            .ok_or_else(|| TopicError::Malformed(topic.to_string()))?;

        trace!("rest: {}", rest);

        if rest == Self::PURGE_PROPERTIES_TOPIC {
            return Ok(Self::PurgeProperties);
        }

        let idx = rest
            .find('/')
            .ok_or_else(|| TopicError::Malformed(topic.to_string()))?;

        trace!("slash idx: {}", idx);

        let (interface, path) = rest.split_at(idx);

        trace!("interface: {}", interface);
        trace!("path: {}", path);

        if interface.is_empty() || path.is_empty() {
            return Err(TopicError::Malformed(topic.to_string()));
        }

        Ok(Self::InterfacePath { interface, path })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CLIENT_ID: ClientId<&'static str> = ClientId {
        realm: "test",
        device_id: "u-WraCwtK_G_fjJf63TiAw",
    };

    #[test]
    fn test_parse_topic() {
        let topic = "test/u-WraCwtK_G_fjJf63TiAw/com.interface.test/led/red".to_owned();
        let ParsedTopic::InterfacePath { interface, path } =
            ParsedTopic::try_parse(CLIENT_ID, &topic).unwrap()
        else {
            panic!("Wrong variant parsed");
        };

        assert_eq!(interface, "com.interface.test");
        assert_eq!(path, "/led/red");
    }

    #[test]
    fn test_parse_purge_properties_topic() {
        let topic = "test/u-WraCwtK_G_fjJf63TiAw/control/consumer/properties".to_owned();
        let parsed_topic = ParsedTopic::try_parse(CLIENT_ID, &topic);

        assert!(matches!(parsed_topic, Ok(ParsedTopic::PurgeProperties)));
    }

    // currently we won't fail if the topic after the client id contains a sting that starts
    // with the purge properties topic
    #[test]
    fn test_parse_almost_purge_properties_topic() {
        let topic = "test/u-WraCwtK_G_fjJf63TiAw/control/consumer/properties/another".to_owned();
        let ParsedTopic::InterfacePath { interface, path } =
            ParsedTopic::try_parse(CLIENT_ID, &topic).unwrap()
        else {
            panic!("Wrong variant parsed");
        };

        assert_eq!(interface, "control");
        assert_eq!(path, "/consumer/properties/another");
    }

    #[test]
    fn test_parse_topic_empty() {
        let topic = "".to_owned();
        let err = ParsedTopic::try_parse(CLIENT_ID, &topic).unwrap_err();

        assert!(matches!(err, TopicError::Empty));
    }

    #[test]
    fn test_parse_topic_client_id() {
        let err = ParsedTopic::try_parse(CLIENT_ID, &CLIENT_ID.to_string()).unwrap_err();

        assert!(matches!(err, TopicError::Malformed(_)));
    }

    #[test]
    fn test_parse_topic_malformed() {
        let topic = "test/u-WraCwtK_G_fjJf63TiAw/com.interface.test".to_owned();
        let err = ParsedTopic::try_parse(CLIENT_ID, &topic).unwrap_err();

        assert!(matches!(err, TopicError::Malformed(_)));
    }

    #[test]
    fn test_parse_unknown_client_id() {
        let topic = "test/u-WraCwtK_G_different/com.interface.test/led/red".to_owned();
        let err = ParsedTopic::try_parse(CLIENT_ID, &topic).unwrap_err();

        assert!(matches!(err, TopicError::UnknownClientId { .. }));
    }
}
