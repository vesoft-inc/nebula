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
namespace py nebula2.graph

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


struct ProfilingStats {
    // How many rows being processed in an executor.
    1: i64  rows;
    // Duration spent in an executor.
    2: i64  duration;
}


struct PlanNodeDescription {
    1: required binary                          name;
    2: required i64                             id;
    3: required binary                          output_var;
    // Each argument of an executor would be tranformed to string.
    4: optional list<binary>                    arguments;
    // If an executor would be executed multi times,
    // the profiling statistics should be multi-versioned.
    5: optional list<ProfilingStats>            profiles;
    // The condition used for selector/loop.
    6: optional bool                            condition;
    7: optional list<i64>                       dependencies;
}


enum PlanFormat {
    ROW = 1,
    DOT = 2,
}


struct PlanDescription {
    1: list<PlanNodeDescription>   plan_node_descs;
    2: PlanFormat                  format = PlanFormat.ROW;
}


struct ExecutionResponse {
    1: required ErrorCode               error_code;
    2: required i32                     latency_in_us;  // Execution time on server
    3: optional common.DataSet          data;           // Can return multiple dataset
    4: optional binary                  space_name;
    5: optional binary                  error_msg;
    6: optional list<PlanDescription>   plan_desc;
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
