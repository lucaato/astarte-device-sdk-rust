#! /usr/bin/env bash

astartectl appengine devices publish-datastream \
    DayugqhpTPi2RgkELFPj9Q org.astarte-platform.rust.e2etest.ServerAggregate \
    /sensor_0 '{"double_endpoint": 42.5, "integer_endpoint": 42, "boolean_endpoint": true, "longinteger_endpoint": 123456789, "string_endpoint": "test", "binaryblob_endpoint": "aGVsbG8=", "datetime_endpoint": "2024-01-01T00:00:00Z", "doublearray_endpoint": [1.1, 2.2], "integerarray_endpoint": [1, 2], "booleanarray_endpoint": [true, false], "longintegerarray_endpoint": [123, 456], "stringarray_endpoint": ["a", "b"], "binaryblobarray_endpoint": ["aGVsbG8=", "d29ybGQ="], "datetimearray_endpoint": ["2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z"]}' \
    -k ~/Work/SecoMind/Public/edgehog/platform/edgehog/backend/priv/repo/seeds/keys/realm_private.pem
