/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

include "common.thrift"

namespace cpp nebula.graph
namespace java com.vesoft.nebula.graph
namespace go nebula.graph

enum ErrorCode {
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
    E_EXECUTION_ERROR = -8,
    // Nothing is executed When command is comment
    E_STATEMENT_EMTPY = -9,

    // User and permission error
    E_USER_NOT_FOUND = -10,
    E_BAD_PERMISSION = -11,
    E_PARTIALLY_FAILED = -12,

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

struct Vertex {
    1: common.VertexID id;
}
struct Edge {
    1: binary type;
    2: common.EdgeRanking ranking;
    3: optional common.VertexID src;
    4: optional common.VertexID dst;
}
union PathEntry {
    1: Vertex vertex;
    2: Edge edge;
}
struct Path {
    1: list<PathEntry> entry_list;
}

union ColumnValue {
    // Simple types
    1: bool bool_val,
    2: i64 integer;
    3: IdType id;
    4: float single_precision;
    5: double double_precision;
    6: binary str;

    // Date time types
    7: Timestamp timestamp;
    8: Year year;
    9: YearMonth month;
    10: Date date;
    11: DateTime datetime;

    // Graph specific
    41: Path path;

    // Container types
    // LIST = 101;
    // SET = 102;
    // MAP = 103;
    // STRUCT = 104;
}


struct RowValue {
    1: list<ColumnValue> columns;
}

struct ExecutionResponse {
    1: required ErrorCode error_code;
    2: required i32 latency_in_us;          // Execution time on server
    3: optional string error_msg;
    4: optional list<binary> column_names;  // Column names
    5: optional list<RowValue> rows;
    6: optional string space_name;
    7: optional string warning_msg;
}


struct AuthResponse {
    1: required ErrorCode error_code;
    2: optional i64 session_id;
    3: optional string error_msg;
}


service GraphService {
    AuthResponse authenticate(1: string username, 2: string password)

    oneway void signout(1: i64 sessionId)

    ExecutionResponse execute(1: i64 sessionId, 2: string stmt)
}
