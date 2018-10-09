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

    // RPC Failure
    E_DISCONNECTED = -1,
    E_FAIL_TO_CONNECT = -2,
    E_RPC_FAILURE = -3,

    // Authentication error
    E_BAD_USERNAME_PASSWORD = -4,

    // Execution errors
    E_SESSION_INVALID = -5,
    E_SESSION_TIMEOUT = -6,

    E_SYNTAX_ERROR = -7,
} (cpp.enum_strict)


typedef i64 IdType
typedef i64 Timestamp
typedef i16 Year
struct YearMonth {
    1: i16 year;
    2: byte month;
}
struct Date {
    1: i16 year;
    2: byte month;
    3: byte day;
}
struct DateTime {
    1: i16 year;
    2: byte month;
    3: byte day;
    4: byte hour;
    5: byte minute;
    6: byte second;
    7: i16 millisec;
    8: i16 microsec;
}


union ColumnValue {
    // Simple types
    1: bool boolVal,
    2: i64 intVal;
    3: IdType idVal;
    4: float floatVal;
    5: double doubleVal;
    6: binary strVal;

    // Date time types
    7: Timestamp tsVal;
    8: Year yearVal;
    9: YearMonth monthVal;
    10: Date dateVal;
    11: DateTime datetimeVal;

    // Graph specific
//    PATH = 41;

    // Container types
//    LIST = 101;
//    SET = 102;
//    MAP = 103;
//    STRUCT = 104;
}


struct RowType {
    1: list<ColumnValue> row;
}


struct ExecutionResponse {
    1: required ResultCode result;
    2: required i32 latencyInMs;        // Execution time on server
    3: optional string errorMsg;
    4: optional list<binary> colNames;  // Column names
    5: optional list<RowType> data;
}


struct AuthResponse {
    1: required ResultCode result;
    2: optional i64 sessionId;
    3: optional string errorMsg;
}


service GraphDbService {
    AuthResponse authenticate(1: string username, 2: string password)

    oneway void signout(1: i64 sessionId)

    ExecutionResponse execute(1: i64 sessionId, 2: string stmt)
}
