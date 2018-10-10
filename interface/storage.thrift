/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

namespace cpp vesoft.storage
namespace java vesoft.storage
namespace go vesoft.storage

enum ErrorCode {
    SUCCEEDED = 0,

    // RPC Failure
    E_DISCONNECTED = -1,
    E_FAILED_TO_CONNECT = -2,
    E_RPC_FAILURE = -3,
} (cpp.enum_strict)


// These are all data types supported in the graph properties
enum SupportedType {
    // Simple types
    BOOL = 1,
    INT = 2,
    FLOAT = 3,
    DOUBLE = 4,
    STRING = 5,
    VID = 6,

    // Date time
    TIMESTAMP = 21,
    YEAR = 22,
    YEARMONTH = 23,
    DATE = 24,
    DATETIME = 25,

    // Graph specific
    PATH = 41,

    // Container types
    LIST = 101,
    SET = 102,
    MAP = 103,      // The key type is always a STRING
    STRUCT = 104,
} (cpp.enum_strict)


struct ValueType {
    1: SupportedType type;
    // vtype only exists when the type is a LIST, SET, or MAP
    2: optional ValueType value_type (cpp.ref = true);
    // When the type is STRUCT, schema defines the struct
    3: optional Schema schema (cpp.ref = true);
} (cpp.virtual)


struct ColumnDef {
    1: required string name,
    2: required ValueType type,
}


struct Schema {
    1: list<ColumnDef> columns,
}


struct QueryResponse {
    1: required ErrorCode error_code,
    2: required i32 latency_in_ms,      // Query latency from storage service
    3: optional Schema schema,
    4: optional binary data,
}


service StorageService {
    QueryResponse getOutBound(1: list<i64> ids,
                              2: list<i32> edgeTypes,
                              3: binary filter
                              4: list<string> returnColumns)
}

