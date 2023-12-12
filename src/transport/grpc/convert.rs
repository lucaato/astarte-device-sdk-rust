/*
 * This file is part of Astarte.
 *
 * Copyright 2023 SECO Mind Srl
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

//! Contains conversion traits to convert the Astarte types in the protobuf format to the
//! Astarte types from the Astarte device SDK.

use std::collections::HashMap;
use std::num::TryFromIntError;
use std::str::FromStr;

use astarte_message_hub_proto;
use chrono::DateTime;

use crate::transport::grpc::GrpcReceivePayload;
use crate::transport::ReceivedEvent;
use crate::types::AstarteType;
use crate::validate::ValidatedIndividual;
use crate::validate::ValidatedObject;
use crate::Aggregation;
use crate::AstarteDeviceDataEvent;
use crate::Timestamp;

#[derive(thiserror::Error, Debug)]
pub enum MessageHubProtoError {
    /// Wrapper for integer conversion errors
    #[error(transparent)]
    TryFromIntError(#[from] TryFromIntError),

    /// Failed to convert between types
    #[error("unable to convert type")]
    ConversionError,

    /// Expected field was not found
    #[error("Missing the expected field '{0}'")]
    ExpectedField(String),

    /// Cannot convert [`AstarteType::Unset`] to a proto [`IndividualData`]
    #[error("Cannot perform conversion of an Unset to a proto IndividualData")]
    UnsetConversion,

    /// Date conversion error
    #[error("Error while converting a proto date: {0}")]
    DateConversion(String),
}

// TODO could be rewrote to accept chaining of accesses
macro_rules! expect_field {
    ($wrapper:ident.$field:ident) => {{
        $wrapper.$field.ok_or(MessageHubProtoError::ExpectedField(
            stringify!($field).to_string(),
        ))
    }};
}

/// This macro can be used to implement the TryFrom trait for the AstarteType from one or more of
/// the protobuf types.
macro_rules! impl_individual_data_to_astarte_type_conversion_traits {
    (scalar $($typ:ident, $astartedatatype:ident),*; vector $($arraytyp:ident, $astartearraydatatype:ident),*) => {
        use astarte_message_hub_proto::astarte_data_type_individual::IndividualData;

        impl TryFrom<IndividualData> for AstarteType {
            type Error = MessageHubProtoError;

            fn try_from(
                d: IndividualData
            ) -> Result<Self, Self::Error> {

                match d {
                    $(
                    IndividualData::$typ(val) => {
                        Ok(AstarteType::$astartedatatype(val.into()))
                    }
                    )?
                    IndividualData::AstarteDateTime(val) => {
                        Ok(AstarteType::DateTime(val.try_into()
                            .map_err(|e: &str| MessageHubProtoError::DateConversion(e.to_owned()))?))
                    }
                    $(
                    IndividualData::$arraytyp(val) => {
                        Ok(AstarteType::$astartearraydatatype(val.values.into()))
                    }
                    )?
                    IndividualData::AstarteDateTimeArray(val) => {
                        let timestamps = val.values.into_iter()
                            .map(|t| t.try_into())
                            .collect::<Result<Vec<DateTime<chrono::Utc>>, &str>>()
                            .map_err(|e| MessageHubProtoError::DateConversion(e.to_owned()))?;

                        Ok(AstarteType::DateTimeArray(timestamps))
                    }
                }
            }
        }
    }
}

impl_individual_data_to_astarte_type_conversion_traits!(
    scalar
    AstarteDouble, Double,
    AstarteInteger,  Integer,
    AstarteBoolean, Boolean,
    AstarteLongInteger,LongInteger,
    AstarteString, String,
    AstarteBinaryBlob, BinaryBlob;
    vector
    AstarteDoubleArray, DoubleArray,
    AstarteIntegerArray, IntegerArray,
    AstarteBooleanArray, BooleanArray,
    AstarteLongIntegerArray, LongIntegerArray,
    AstarteStringArray, StringArray,
    AstarteBinaryBlobArray, BinaryBlobArray
);

impl TryFrom<astarte_message_hub_proto::AstarteDataTypeIndividual> for AstarteType {
    type Error = MessageHubProtoError;

    fn try_from(
        value: astarte_message_hub_proto::AstarteDataTypeIndividual,
    ) -> Result<Self, Self::Error> {
        let individual_data = expect_field!(value.individual_data)?;

        individual_data.try_into()
    }
}

/// Implements the TryFrom trait for the AstarteDataTypeIndividual for any AstarteType.
macro_rules! impl_astarte_type_to_individual_data_conversion_traits {
    ($($typ:ident),*) => {
        impl TryFrom<AstarteType> for astarte_message_hub_proto::AstarteDataTypeIndividual {
            type Error = MessageHubProtoError;

            fn try_from(d: AstarteType) -> Result<Self, Self::Error> {
                match d {
                    $(
                    AstarteType::$typ(val) => Ok(val.into()),
                    )*
                    AstarteType::Unset => Err(MessageHubProtoError::UnsetConversion),
                }
            }
        }
    }
}

impl_astarte_type_to_individual_data_conversion_traits!(
    Double,
    Integer,
    Boolean,
    LongInteger,
    String,
    BinaryBlob,
    DateTime,
    DoubleArray,
    IntegerArray,
    BooleanArray,
    LongIntegerArray,
    StringArray,
    BinaryBlobArray,
    DateTimeArray
);

impl TryFrom<astarte_message_hub_proto::AstarteMessage> for AstarteDeviceDataEvent {
    type Error = MessageHubProtoError;

    fn try_from(
        astarte_message: astarte_message_hub_proto::AstarteMessage,
    ) -> Result<Self, Self::Error> {
        let astarte_sdk_aggregation = match expect_field!(astarte_message.payload)? {
            astarte_message_hub_proto::astarte_message::Payload::AstarteData(astarte_data_type) => {
                match expect_field!(astarte_data_type.data)? {
                    astarte_message_hub_proto::astarte_data_type::Data::AstarteIndividual(
                        astarte_individual,
                    ) => Aggregation::Individual(
                        expect_field!(astarte_individual.individual_data)?.try_into()?,
                    ),
                    astarte_message_hub_proto::astarte_data_type::Data::AstarteObject(
                        astarte_object,
                    ) => {
                        let astarte_sdk_aggregation =
                            map_values_to_astarte_type(astarte_object.object_data)?;
                        Aggregation::Object(astarte_sdk_aggregation)
                    }
                }
            }
            astarte_message_hub_proto::astarte_message::Payload::AstarteUnset(_) => {
                Aggregation::Individual(AstarteType::Unset)
            }
        };

        Ok(AstarteDeviceDataEvent {
            interface: astarte_message.interface_name,
            path: astarte_message.path,
            data: astarte_sdk_aggregation,
        })
    }
}

impl TryFrom<astarte_message_hub_proto::types::InterfaceJson> for crate::Interface {
    type Error = crate::Error;

    fn try_from(
        interface: astarte_message_hub_proto::types::InterfaceJson,
    ) -> Result<Self, Self::Error> {
        let interface_str = String::from_utf8_lossy(&interface.0);

        crate::Interface::from_str(interface_str.as_ref()).map_err(Self::Error::Interface)
    }
}

// FIXME this is probably useless ?
impl TryFrom<AstarteDeviceDataEvent> for astarte_message_hub_proto::AstarteMessage {
    type Error = MessageHubProtoError;

    fn try_from(value: AstarteDeviceDataEvent) -> Result<Self, Self::Error> {
        use astarte_message_hub_proto::astarte_message::Payload;

        let payload: Payload = value.data.try_into()?;

        Ok(astarte_message_hub_proto::AstarteMessage {
            interface_name: value.interface.clone(),
            path: value.path.clone(),
            timestamp: None,
            payload: Some(payload),
        })
    }
}

impl TryFrom<astarte_message_hub_proto::AstarteMessage> for ReceivedEvent<GrpcReceivePayload> {
    type Error = MessageHubProtoError;

    fn try_from(message: astarte_message_hub_proto::AstarteMessage) -> Result<Self, Self::Error> {
        let interface = message.interface_name;
        let path = message.path;
        let data = message
            .payload
            .and_then(|p| p.take_data())
            .and_then(|d| d.data)
            .ok_or(MessageHubProtoError::ExpectedField(
                "Message field payload.data expected but not found".to_owned(),
            ))?;
        let timestamp: Option<Timestamp> =
            message
                .timestamp
                .map(Timestamp::try_from)
                .transpose()
                .map_err(|e| MessageHubProtoError::DateConversion(e.to_owned()))?;

        Ok(ReceivedEvent {
            interface,
            path,
            payload: GrpcReceivePayload::new(data, timestamp),
        })
    }
}

impl<'a> TryFrom<ValidatedIndividual<'a>> for astarte_message_hub_proto::AstarteMessage {
    type Error = MessageHubProtoError;

    fn try_from(value: ValidatedIndividual<'a>) -> Result<Self, Self::Error> {
        use astarte_message_hub_proto::astarte_message::Payload;

        let interface_name = value.mapping().interface().interface_name().to_owned();
        let path = value.path().as_str().to_owned();
        let timestamp: Option<pbjson_types::Timestamp> = value.timestamp().map(|t| t.into());
        let payload: Option<Payload> = Some(value.into_data().try_into()?);

        Ok(astarte_message_hub_proto::AstarteMessage {
            interface_name,
            path,
            timestamp,
            payload,
        })
    }
}

impl<'a> TryFrom<ValidatedObject<'a>> for astarte_message_hub_proto::AstarteMessage {
    type Error = MessageHubProtoError;

    fn try_from(value: ValidatedObject<'a>) -> Result<Self, Self::Error> {
        use astarte_message_hub_proto::astarte_message::Payload;

        let interface_name = value.object().interface.interface_name().to_owned();
        let path = value.path().as_str().to_owned();
        let timestamp: Option<pbjson_types::Timestamp> = value.timestamp().map(|t| t.into());
        let astarte_data: astarte_message_hub_proto::AstarteDataType = value.into_data()
            .into_iter()
            .map(|(k, v)| v.try_into().map(|t| (k, t)))
            .collect::<Result<HashMap<String, astarte_message_hub_proto::AstarteDataTypeIndividual>, _>>()?
            .into();

        let payload = Some(Payload::AstarteData(astarte_data));

        Ok(astarte_message_hub_proto::AstarteMessage {
            interface_name,
            path,
            timestamp,
            payload,
        })
    }
}

impl TryFrom<Aggregation> for astarte_message_hub_proto::astarte_message::Payload {
    type Error = MessageHubProtoError;

    fn try_from(value: Aggregation) -> Result<Self, Self::Error> {
        use astarte_message_hub_proto::astarte_data_type::Data;
        use astarte_message_hub_proto::astarte_message::Payload;

        let payload = match value {
            Aggregation::Individual(astarte_type) => {
                if let AstarteType::Unset = astarte_type {
                    Payload::AstarteUnset(astarte_message_hub_proto::AstarteUnset {})
                } else {
                    let individual_type: astarte_message_hub_proto::AstarteDataTypeIndividual =
                        astarte_type.try_into()?;

                    Payload::AstarteData(astarte_message_hub_proto::AstarteDataType {
                        data: Some(Data::AstarteIndividual(individual_type)),
                    })
                }
            }
            Aggregation::Object(astarte_map) => {
                let astarte_data: astarte_message_hub_proto::AstarteDataType = astarte_map
                    .into_iter()
                    .map(|(k, v)| (k, v.try_into().unwrap()))
                    .collect::<HashMap<String, astarte_message_hub_proto::AstarteDataTypeIndividual>>()
                    .into();

                Payload::AstarteData(astarte_data)
            }
        };

        Ok(payload)
    }
}

impl TryFrom<AstarteType> for astarte_message_hub_proto::astarte_message::Payload {
    type Error = MessageHubProtoError;

    fn try_from(astarte_device_sdk_type: AstarteType) -> Result<Self, Self::Error> {
        use astarte_message_hub_proto::astarte_data_type::Data;
        use astarte_message_hub_proto::astarte_message::Payload;
        use astarte_message_hub_proto::AstarteDataType;

        let payload = if let AstarteType::Unset = astarte_device_sdk_type {
            Payload::AstarteUnset(astarte_message_hub_proto::AstarteUnset {})
        } else {
            let individual_type: astarte_message_hub_proto::AstarteDataTypeIndividual =
                astarte_device_sdk_type.try_into()?;
            Payload::AstarteData(AstarteDataType {
                data: Some(Data::AstarteIndividual(individual_type)),
            })
        };
        Ok(payload)
    }
}

/// This function can be used to convert a map of (String, crate:types::AstarteType) into a
/// map of (String,  AstarteDataTypeIndividual).
pub fn map_values_to_astarte_data_type_individual(
    value: HashMap<String, AstarteType>,
) -> Result<
    HashMap<String, astarte_message_hub_proto::AstarteDataTypeIndividual>,
    MessageHubProtoError,
> {
    let mut map: HashMap<String, astarte_message_hub_proto::AstarteDataTypeIndividual> =
        Default::default();
    for (key, astarte_type) in value.into_iter() {
        map.insert(key, astarte_type.try_into()?);
    }
    Ok(map)
}

/// This function can be used to convert a map of (String, AstarteDataTypeIndividual) into a
/// map of (String, AstarteType).
/// It can be useful when a method accept an astarte_device_sdk::AstarteAggregate.
pub fn map_values_to_astarte_type(
    value: HashMap<String, astarte_message_hub_proto::AstarteDataTypeIndividual>,
) -> Result<HashMap<String, AstarteType>, MessageHubProtoError> {
    let mut map: HashMap<String, AstarteType> = Default::default();

    for (key, astarte_data) in value.into_iter() {
        map.insert(
            key,
            astarte_data
                .individual_data
                .ok_or(MessageHubProtoError::ConversionError)?
                .try_into()?,
        );
    }

    Ok(map)
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use astarte_message_hub_proto::astarte_message::Payload;
    use astarte_message_hub_proto::{AstarteDataTypeIndividual, AstarteMessage};
    use chrono::{DateTime, Utc};

    use crate::{Aggregation, AstarteDeviceDataEvent};

    use super::map_values_to_astarte_data_type_individual;
    use super::AstarteType;
    use super::IndividualData;
    use super::MessageHubProtoError;

    #[test]
    fn proto_astarte_double_into_astarte_device_sdk_type_success() {
        let value: f64 = 15.5;
        let expected_double_value = IndividualData::AstarteDouble(value);
        let astarte_type: AstarteType = expected_double_value.try_into().unwrap();

        if let AstarteType::Double(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_integer_into_astarte_device_sdk_type_success() {
        let value: i32 = 15;
        let expected_integer_value = IndividualData::AstarteInteger(value);
        let astarte_type: AstarteType = expected_integer_value.try_into().unwrap();

        if let AstarteType::Integer(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_boolean_into_astarte_device_sdk_type_success() {
        let value: bool = true;
        let expected_boolean_value = IndividualData::AstarteBoolean(value);
        let astarte_type: AstarteType = expected_boolean_value.try_into().unwrap();

        if let AstarteType::Boolean(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_long_integer_into_astarte_device_sdk_type_success() {
        let value: i64 = 154;
        let expected_long_integer_value = IndividualData::AstarteLongInteger(value);
        let astarte_type: AstarteType = expected_long_integer_value.try_into().unwrap();

        if let AstarteType::LongInteger(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_string_into_astarte_device_sdk_type_success() {
        let value: String = "test".to_owned();
        let expected_string_value = IndividualData::AstarteString(value.clone());
        let astarte_type: AstarteType = expected_string_value.try_into().unwrap();

        if let AstarteType::String(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_binary_blob_into_astarte_device_sdk_type_success() {
        let value: Vec<u8> = vec![10, 34];
        let expected_binary_blob_value = IndividualData::AstarteBinaryBlob(value.clone());
        let astarte_type: AstarteType = expected_binary_blob_value.try_into().unwrap();

        if let AstarteType::BinaryBlob(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_date_time_into_astarte_device_sdk_type_success() {
        let value: DateTime<Utc> = Utc::now();
        let expected_date_time_value = IndividualData::AstarteDateTime(value.into());
        let astarte_type: AstarteType = expected_date_time_value.try_into().unwrap();

        if let AstarteType::DateTime(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_double_array_into_astarte_device_sdk_type_success() {
        let value: Vec<f64> = vec![15.5, 18.7];
        use astarte_message_hub_proto::AstarteDoubleArray;
        let expected_double_array_value = IndividualData::AstarteDoubleArray(AstarteDoubleArray {
            values: value.clone(),
        });
        let astarte_type: AstarteType = expected_double_array_value.try_into().unwrap();

        if let AstarteType::DoubleArray(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_integer_array_into_astarte_device_sdk_type_success() {
        let value: Vec<i32> = vec![15, 18];
        use astarte_message_hub_proto::AstarteIntegerArray;
        let expected_integer_array_value =
            IndividualData::AstarteIntegerArray(AstarteIntegerArray {
                values: value.clone(),
            });
        let astarte_type: AstarteType = expected_integer_array_value.try_into().unwrap();

        if let AstarteType::IntegerArray(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_boolean_array_into_astarte_device_sdk_type_success() {
        let value: Vec<bool> = vec![false, true];
        use astarte_message_hub_proto::AstarteBooleanArray;
        let expected_boolean_array_value =
            IndividualData::AstarteBooleanArray(AstarteBooleanArray {
                values: value.clone(),
            });
        let astarte_type: AstarteType = expected_boolean_array_value.try_into().unwrap();

        if let AstarteType::BooleanArray(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_long_integer_array_into_astarte_device_sdk_type_success() {
        let value: Vec<i64> = vec![1543, 18];
        use astarte_message_hub_proto::AstarteLongIntegerArray;
        let expected_long_integer_array_value =
            IndividualData::AstarteLongIntegerArray(AstarteLongIntegerArray {
                values: value.clone(),
            });
        let astarte_type: AstarteType = expected_long_integer_array_value.try_into().unwrap();

        if let AstarteType::LongIntegerArray(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_string_array_into_astarte_device_sdk_type_success() {
        let value: Vec<String> = vec!["test1".to_owned(), "test2".to_owned()];
        use astarte_message_hub_proto::AstarteStringArray;
        let expected_string_array_value = IndividualData::AstarteStringArray(AstarteStringArray {
            values: value.clone(),
        });
        let astarte_type: AstarteType = expected_string_array_value.try_into().unwrap();

        if let AstarteType::StringArray(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_binary_blob_array_into_astarte_device_sdk_type_success() {
        let value: Vec<Vec<u8>> = vec![vec![11, 201], vec![1, 241]];
        use astarte_message_hub_proto::AstarteBinaryBlobArray;
        let expected_binary_blob_array_value =
            IndividualData::AstarteBinaryBlobArray(AstarteBinaryBlobArray {
                values: value.clone(),
            });
        let astarte_type: AstarteType = expected_binary_blob_array_value.try_into().unwrap();

        if let AstarteType::BinaryBlobArray(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn proto_astarte_date_time_array_into_astarte_device_sdk_type_success() {
        use astarte_message_hub_proto::AstarteDateTimeArray;
        use pbjson_types::Timestamp;

        let value: Vec<DateTime<Utc>> = vec![Utc::now(), Utc::now()];
        let expected_date_time_array_value =
            IndividualData::AstarteDateTimeArray(AstarteDateTimeArray {
                values: value
                    .clone()
                    .into_iter()
                    .map(|it| it.into())
                    .collect::<Vec<Timestamp>>(),
            });
        let astarte_type: AstarteType = expected_date_time_array_value.try_into().unwrap();

        if let AstarteType::DateTimeArray(astarte_value) = astarte_type {
            assert_eq!(value, astarte_value);
        } else {
            panic!();
        }
    }

    #[test]
    fn convert_astarte_message_to_astarte_device_data_event_individual_success() {
        let expected_data: f64 = 15.5;
        let interface_name = "test.name.json".to_string();
        let interface_path = "test".to_string();

        let astarte_type: AstarteType = expected_data.try_into().unwrap();
        let payload: Payload = astarte_type.try_into().unwrap();

        let astarte_message = AstarteMessage {
            interface_name: interface_name.clone(),
            path: interface_path.clone(),
            timestamp: None,
            payload: Some(payload),
        };

        let astarte_device_data_event: AstarteDeviceDataEvent = astarte_message.try_into().unwrap();

        assert_eq!(interface_name, astarte_device_data_event.interface);
        assert_eq!(interface_path, astarte_device_data_event.path);

        match astarte_device_data_event.data {
            Aggregation::Individual(value) => {
                assert_eq!(value, expected_data)
            }
            Aggregation::Object(_) => {
                panic!()
            }
        }
    }

    #[test]
    fn convert_astarte_message_to_astarte_device_data_event_object_success() {
        use astarte_message_hub_proto::AstarteDataTypeIndividual;
        let interface_name = "test.name.json".to_string();
        let interface_path = "test".to_string();

        let expected_data_f64: f64 = 15.5;
        let expected_data_i32: i32 = 15;
        let mut object_map: HashMap<String, AstarteDataTypeIndividual> = HashMap::new();
        object_map.insert("1".to_string(), expected_data_f64.into());
        object_map.insert("2".to_string(), expected_data_i32.into());

        let astarte_message = AstarteMessage {
            interface_name: interface_name.clone(),
            path: interface_path.clone(),
            timestamp: None,
            payload: Some(Payload::AstarteData(object_map.into())),
        };

        let astarte_device_data_event: AstarteDeviceDataEvent = astarte_message.try_into().unwrap();

        assert_eq!(interface_name, astarte_device_data_event.interface);
        assert_eq!(interface_path, astarte_device_data_event.path);

        match astarte_device_data_event.data {
            Aggregation::Individual(_) => {
                panic!()
            }
            Aggregation::Object(object_map) => {
                assert_eq!(
                    object_map.get("1").unwrap().clone(),
                    AstarteType::try_from(expected_data_f64).unwrap()
                );
                assert_eq!(
                    object_map.get("2").unwrap().clone(),
                    AstarteType::from(expected_data_i32)
                );
            }
        }
    }

    #[test]
    fn convert_astarte_message_to_astarte_device_data_event_unset_success() {
        let interface_name = "test.name.json".to_string();
        let interface_path = "test".to_string();

        let astarte_type: AstarteType = AstarteType::Unset;
        let payload: Payload = astarte_type.try_into().unwrap();

        let astarte_message = AstarteMessage {
            interface_name: interface_name.clone(),
            path: interface_path.clone(),
            timestamp: None,
            payload: Some(payload),
        };

        let astarte_device_data_event: AstarteDeviceDataEvent = astarte_message.try_into().unwrap();

        assert_eq!(interface_name, astarte_device_data_event.interface);
        assert_eq!(interface_path, astarte_device_data_event.path);

        match astarte_device_data_event.data {
            Aggregation::Individual(value) => {
                assert_eq!(AstarteType::Unset, value)
            }
            Aggregation::Object(_) => {
                panic!()
            }
        }
    }

    #[test]
    fn convert_map_values_to_astarte_astarte_data_type_individual_success() {
        let expected_data: f64 = 15.5;
        use std::collections::HashMap;
        let astarte_type_map =
            HashMap::from([("key1".to_string(), AstarteType::Double(expected_data))]);

        let conversion_map_result = map_values_to_astarte_data_type_individual(astarte_type_map);
        assert!(conversion_map_result.is_ok());

        let astarte_individual_map = conversion_map_result.unwrap();

        if let IndividualData::AstarteDouble(double_data) = astarte_individual_map
            .get("key1")
            .unwrap()
            .individual_data
            .clone()
            .unwrap()
        {
            assert_eq!(expected_data, double_data)
        } else {
            panic!()
        }
    }

    #[test]
    fn convert_proto_interface_to_astarte_interface() {
        use crate::Interface;

        use astarte_message_hub_proto::types::InterfaceJson;

        const SERV_PROPS_IFACE: &str = r#"
        {
            "interface_name": "org.astarte-platform.test.test",
            "version_major": 1,
            "version_minor": 1,
            "type": "properties",
            "ownership": "server",
            "mappings": [
                {
                    "endpoint": "/button",
                    "type": "boolean",
                    "explicit_timestamp": true
                },
                {
                    "endpoint": "/uptimeSeconds",
                    "type": "integer",
                    "explicit_timestamp": true
                }
            ]
        }
        "#;

        let interface = InterfaceJson(SERV_PROPS_IFACE.into());

        let astarte_interface: Interface = interface.try_into().unwrap();

        assert_eq!(
            astarte_interface.interface_name(),
            "org.astarte-platform.test.test"
        );
        assert_eq!(astarte_interface.version_major(), 1);
    }

    #[tokio::test]
    async fn convert_proto_interface_with_special_chars_to_astarte_interface() {
        use crate::Interface;

        use astarte_message_hub_proto::types::InterfaceJson;

        const IFACE_SPECIAL_CHARS: &str = r#"
        {
            "interface_name": "org.astarte-platform.test.test",
            "version_major": 1,
            "version_minor": 1,
            "type": "properties",
            "ownership": "server",
            "mappings": [
                {
                    "endpoint": "/uptimeSeconds",
                    "type": "integer",
                    "explicit_timestamp": true,
                    "description": "Hello 你好 안녕하세요"
                }
            ]
        }
        "#;

        let interface = InterfaceJson(IFACE_SPECIAL_CHARS.into());

        let astarte_interface: Interface = interface.try_into().unwrap();

        assert_eq!(
            astarte_interface.interface_name(),
            "org.astarte-platform.test.test"
        );
        assert_eq!(astarte_interface.version_major(), 1);
    }

    #[tokio::test]
    async fn convert_bad_proto_interface_to_astarte_interface() {
        use crate::Interface;

        use astarte_message_hub_proto::types::InterfaceJson;

        const IFACE_BAD: &str = r#"{"#;

        let interface = InterfaceJson(IFACE_BAD.into());

        let astarte_interface_bad_result: Result<Interface, crate::error::Error> =
            interface.try_into();

        assert!(astarte_interface_bad_result.is_err());
    }

    #[test]
    fn convert_astarte_type_unset_give_conversion_error() {
        let expected_data = AstarteType::Unset;

        let result: Result<AstarteDataTypeIndividual, MessageHubProtoError> =
            expected_data.try_into();

        assert!(result.is_err());
        assert!(matches!(
            result.err().unwrap(),
            MessageHubProtoError::UnsetConversion
        ));
    }

    #[test]
    fn convert_astarte_device_data_event_unset_to_astarte_message() {
        use astarte_message_hub_proto::astarte_message::Payload;
        let expected_data = AstarteType::Unset;

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Individual(expected_data),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);
        assert_eq!(
            Payload::AstarteUnset(astarte_message_hub_proto::AstarteUnset {}),
            astarte_message.payload.unwrap()
        );
    }

    fn get_individual_data_from_payload(
        payload: Payload,
    ) -> Result<AstarteType, MessageHubProtoError> {
        payload
            .take_data()
            .and_then(astarte_message_hub_proto::AstarteDataType::take_individual)
            .and_then(|data| data.individual_data)
            .ok_or(MessageHubProtoError::ConversionError)?
            .try_into()
    }

    #[test]
    fn convert_astarte_device_data_event_individual_f64_to_astarte_message() {
        let expected_data = AstarteType::Double(10.1);

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Individual(expected_data.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_i32_to_astarte_message() {
        let expected_data = AstarteType::Integer(10);

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Individual(expected_data.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_bool_to_astarte_message() {
        let expected_data = AstarteType::Boolean(true);

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Individual(expected_data.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_i64_to_astarte_message() {
        let expected_data = AstarteType::LongInteger(45);

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Individual(expected_data.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_string_to_astarte_message() {
        let expected_data = AstarteType::String("test".to_owned());

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Individual(expected_data.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_bytes_to_astarte_message() {
        let expected_data = AstarteType::BinaryBlob(vec![12, 48]);

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Individual(expected_data.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_date_time_to_astarte_message() {
        let expected_data = AstarteType::DateTime(Utc::now());

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Individual(expected_data.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_f64_array_to_astarte_message() {
        let expected_data = AstarteType::DoubleArray(vec![13.5, 487.35]);

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Individual(expected_data.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_i32_array_to_astarte_message() {
        let expected_data = AstarteType::IntegerArray(vec![78, 45]);

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Individual(expected_data.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_bool_array_to_astarte_message() {
        let expected_data = AstarteType::BooleanArray(vec![true, false, true]);

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Individual(expected_data.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_i64_array_to_astarte_message() {
        let expected_data = AstarteType::LongIntegerArray(vec![658, 77845, 4444]);

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Individual(expected_data.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_string_array_to_astarte_message() {
        let expected_data =
            AstarteType::StringArray(vec!["test1".to_owned(), "test_098".to_string()]);

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Individual(expected_data.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_bytes_array_to_astarte_message() {
        let expected_data = AstarteType::BinaryBlobArray(vec![vec![12, 48], vec![47, 55], vec![9]]);

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Individual(expected_data.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );
        assert_eq!(astarte_device_data_event.path, astarte_message.path);

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_individual_date_time_array_to_astarte_message() {
        let expected_data = AstarteType::DateTimeArray(vec![Utc::now(), Utc::now()]);

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Individual(expected_data.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );

        let payload = astarte_message.payload.unwrap();
        let astarte_type = get_individual_data_from_payload(payload).unwrap();

        assert_eq!(expected_data, astarte_type);
    }

    #[test]
    fn convert_astarte_device_data_event_object_to_astarte_message() {
        let expected_map = HashMap::from([
            ("Mercury".to_owned(), AstarteType::Double(0.4)),
            ("Venus".to_owned(), AstarteType::Double(0.7)),
            ("Earth".to_owned(), AstarteType::Double(1.0)),
            ("Mars".to_owned(), AstarteType::Double(1.5)),
        ]);

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Object(expected_map.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );

        let astarte_object = astarte_message
            .take_data()
            .and_then(|data| data.take_object())
            .unwrap();

        let object_data = astarte_object.object_data;
        for (k, v) in expected_map.into_iter() {
            let astarte_type: AstarteType = object_data
                .get(&k)
                .and_then(|data| data.individual_data.as_ref())
                .and_then(|data| data.clone().try_into().ok())
                .unwrap();

            assert_eq!(v, astarte_type);
        }
    }

    #[test]
    fn convert_astarte_device_data_event_object2_to_astarte_message() {
        let expected_map = HashMap::from([
            ("M".to_owned(), AstarteType::Double(0.4)),
            (
                "V".to_owned(),
                AstarteType::StringArray(vec!["test1".to_owned(), "test2".to_owned()]),
            ),
            ("R".to_owned(), AstarteType::Integer(112)),
            ("a".to_owned(), AstarteType::Boolean(false)),
        ]);

        let astarte_device_data_event = AstarteDeviceDataEvent {
            interface: "test.name.json".to_owned(),
            path: "test".to_owned(),
            data: Aggregation::Object(expected_map.clone()),
        };

        let astarte_message: AstarteMessage = astarte_device_data_event.clone().try_into().unwrap();
        assert_eq!(
            astarte_device_data_event.interface,
            astarte_message.interface_name
        );

        let object_data = astarte_message
            .take_data()
            .and_then(|data| data.take_object())
            .unwrap()
            .object_data;

        for (k, v) in expected_map.into_iter() {
            let astarte_type: AstarteType = object_data
                .get(&k)
                .and_then(|data| data.individual_data.as_ref())
                .and_then(|data| data.clone().try_into().ok())
                .unwrap();

            assert_eq!(v, astarte_type);
        }
    }

    #[test]
    fn from_sdk_astarte_type_to_astarte_message_payload_success() {
        let expected_double_value: f64 = 15.5;
        let astarte_sdk_type_double = AstarteType::Double(expected_double_value);

        let payload: Payload = astarte_sdk_type_double.try_into().unwrap();

        let double_value = payload
            .take_data()
            .and_then(astarte_message_hub_proto::AstarteDataType::take_individual)
            .and_then(|data| data.individual_data)
            .unwrap();

        assert_eq!(
            IndividualData::AstarteDouble(expected_double_value),
            double_value
        );
    }

    #[test]
    fn from_sdk_astarte_aggregate_to_astarte_message_payload_success() {
        use astarte_message_hub_proto::astarte_message::Payload;

        let expected_data: f64 = 15.5;
        use std::collections::HashMap;
        let astarte_type_map = Aggregation::Object(HashMap::from([(
            "key1".to_string(),
            AstarteType::Double(expected_data),
        )]));

        let payload_result: Result<Payload, MessageHubProtoError> = astarte_type_map.try_into();
        assert!(payload_result.is_ok());

        let double_data = payload_result
            .ok()
            .as_mut()
            .and_then(Payload::data_mut)
            .and_then(astarte_message_hub_proto::AstarteDataType::object_mut)
            .and_then(|data| data.object_data.remove("key1"))
            .and_then(|data| data.individual_data)
            .unwrap();

        assert_eq!(IndividualData::AstarteDouble(expected_data), double_data);
    }
}
