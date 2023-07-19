/* vim: ft=proto
 * Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

namespace cpp nebula.graph
namespace java com.vesoft.nebula.graph
namespace go nebula.graph
namespace js nebula.graph
namespace csharp nebula.graph
namespace py nebula3.graph

include "common.thrift"

cpp_include "common/graph/PairOps-inl.h"
cpp_include "common/graph/ProfilingStatsOps-inl.h"
cpp_include "common/graph/PlanNodeBranchInfoOps-inl.h"
cpp_include "common/graph/PlanNodeDescriptionOps-inl.h"
cpp_include "common/graph/PlanDescriptionOps-inl.h"
cpp_include "common/graph/ExecutionResponseOps-inl.h"
cpp_include "common/graph/AuthResponseOps-inl.h"

/*
 *
 *  Note: In order to support multiple languages, all string
 *        have to be defined as **binary** in the thrift file
 *
 */

struct ProfilingStats {
    // How many rows being processed in an executor.
    1: required i64  rows;
    // Duration spent in an executor.
    2: required i64  exec_duration_in_us;
    // Total duration spent in an executor, contains schedule time
    3: required i64  total_duration_in_us;
    // Other profiling stats data map
    4: optional map<binary, binary>
        (cpp.template = "std::unordered_map") other_stats;
} (cpp.type = "nebula::ProfilingStats", cpp.noncopyable)

// The info used for select/loop.
struct PlanNodeBranchInfo {
    // True if loop body or then branch of select
    1: required bool  is_do_branch;
    // select/loop node id
    2: required i64   condition_node_id;
} (cpp.type = "nebula::PlanNodeBranchInfo")

struct Pair {
    1: required binary key;
    2: required binary value;
} (cpp.type = "nebula::Pair")

struct PlanNodeDescription {
    1: required binary                          name;
    2: required i64                             id;
    3: required binary                          output_var;
    // other description of an executor
    4: optional list<Pair>                      description;
    // If an executor would be executed multi times,
    // the profiling statistics should be multi-versioned.
    5: optional list<ProfilingStats>            profiles;
    6: optional PlanNodeBranchInfo              branch_info;
    7: optional list<i64>                       dependencies;
} (cpp.type = "nebula::PlanNodeDescription", cpp.noncopyable)

struct PlanDescription {
    1: required list<PlanNodeDescription>     plan_node_descs;
    // map from node id to index of list
    2: required map<i64, i64>
        (cpp.template = "std::unordered_map") node_index_map;
    // the print format of exec plan, lowercase string like `dot'
    3: required binary                        format;
    // the time optimizer spent
    4: required i32                           optimize_time_in_us;
} (cpp.type = "nebula::PlanDescription", cpp.noncopyable)


struct ExecutionResponse {
    1: required common.ErrorCode        error_code;
    2: required i64                     latency_in_us;  // Execution time on server
    3: optional common.DataSet          data;
    4: optional binary                  space_name;
    5: optional binary                  error_msg;
    6: optional PlanDescription         plan_desc;
    7: optional binary                  comment;        // Supplementary instruction
} (cpp.type = "nebula::ExecutionResponse", cpp.noncopyable)


struct AuthResponse {
    1: required common.ErrorCode   error_code;
    2: optional binary             error_msg;
    3: optional i64                session_id;
    4: optional i32                time_zone_offset_seconds;
    5: optional binary             time_zone_name;
} (cpp.type = "nebula::AuthResponse", cpp.noncopyable)


struct VerifyClientVersionResp {
    1: required common.ErrorCode error_code;
    2: optional binary           error_msg;
}


struct VerifyClientVersionReq {
    1: required binary version = common.version;
}


service GraphService {
    AuthResponse authenticate(1: binary username, 2: binary password)

    oneway void signout(1: i64 sessionId)

    ExecutionResponse execute(1: i64 sessionId, 2: binary stmt)
    ExecutionResponse executeWithParameter(1: i64 sessionId, 2: binary stmt, 3: map<binary, common.Value>(cpp.template = "std::unordered_map") parameterMap)
    // Same as execute(), but response will be a json string
    binary executeJson(1: i64 sessionId, 2: binary stmt)
    binary executeJsonWithParameter(1: i64 sessionId, 2: binary stmt, 3: map<binary, common.Value>(cpp.template = "std::unordered_map") parameterMap)
    
    VerifyClientVersionResp verifyClientVersion(1: VerifyClientVersionReq req)
}
