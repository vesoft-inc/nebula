/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

namespace cpp nebula.graph
namespace java com.vesoft.nebula.graph
namespace go nebula.graph
namespace js nebula.graph
namespace csharp nebula.graph

include "common.thrift"

/*
 *
 *  Note: In order to support multiple languages, all string
 *        have to be defined as **binary** in the thrift file
 *
 */

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
} (cpp.enum_strict)


struct ExecutionResponse {
    1: required ErrorCode               error_code;
    2: required i32                     latency_in_us;  // Execution time on server
    3: optional common.DataSet          data;           // Can return multiple dataset
    4: optional binary                  space_name;
    5: optional binary                  error_msg;
}


struct AuthResponse {
    1: required ErrorCode   error_code;
    2: optional binary      error_msg;
    3: optional i64         session_id;
}


service GraphService {
    AuthResponse authenticate(1: binary username, 2: binary password)

    oneway void signout(1: i64 sessionId)

    ExecutionResponse execute(1: i64 sessionId, 2: binary stmt)

    // Same as execute(), but response will be a json string
    binary executeJson(1: i64 sessionId, 2: binary stmt)
}
