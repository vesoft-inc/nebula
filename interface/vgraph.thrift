/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

namespace cpp vesoft.vgraph
namespace java vesoft.vgraph
namespace go vesoft.vgraph


enum ResultCode {
    SUCCEEDED = 0,

    // Authentication error
    E_BAD_USERNAME_PASSWORD = -1,

    // Execution errors
    E_SESSION_INVALID = -2,
    E_SESSION_TIMEOUT = -3,

    E_SYNTAX_ERROR = -4,
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
    DATE = 22,
    TIME = 23,
    DATETIME = 24,
    YEAR = 25,

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
    2: optional ValueType vType (cpp.ref = true);
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


struct ExecutionResponse {
    1: required ResultCode result,
    2: required i32 latencyInMs,
    3: optional string errorMsg,
    4: optional Schema schema,
    // The returned data is encoded and will be decoded when it's read
    5: optional binary data,
    // The query latency on the server side
}


struct AuthResponse {
    1: required ResultCode result,
    2: optional i64 sessionId,
    3: optional string errorMsg,
}


service GraphDbService {
    AuthResponse authenticate(1: string username, 2: string password)

    oneway void signout(1: i64 sessionId)

    ExecutionResponse execute(1: i64 sessionId, 2: string stmt)
}
