/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_GRAPH_RESPONSE_H
#define COMMON_GRAPH_RESPONSE_H

#include <algorithm>
#include <memory>
#include <unordered_map>
#include <vector>

#include "common/datatypes/DataSet.h"

namespace nebula {

enum class ErrorCode {
    // for common code
    SUCCEEDED                         = 0,

    E_DISCONNECTED                    = -1,        // RPC Failure
    E_FAIL_TO_CONNECT                 = -2,
    E_RPC_FAILURE                     = -3,
    E_LEADER_CHANGED                  = -4,


    // only unify metad and storaged error code
    E_SPACE_NOT_FOUND                 = -5,
    E_TAG_NOT_FOUND                   = -6,
    E_EDGE_NOT_FOUND                  = -7,
    E_INDEX_NOT_FOUND                 = -8,
    E_EDGE_PROP_NOT_FOUND             = -9,
    E_TAG_PROP_NOT_FOUND              = -10,
    E_ROLE_NOT_FOUND                  = -11,
    E_CONFIG_NOT_FOUND                = -12,
    E_GROUP_NOT_FOUND                 = -13,
    E_ZONE_NOT_FOUND                  = -14,
    E_LISTENER_NOT_FOUND              = -15,
    E_PART_NOT_FOUND                  = -16,
    E_KEY_NOT_FOUND                   = -17,
    E_USER_NOT_FOUND                  = -18,

    // backup failed
    E_BACKUP_FAILED                   = -24,
    E_BACKUP_EMPTY_TABLE              = -25,
    E_BACKUP_TABLE_FAILED             = -26,
    E_PARTIAL_RESULT                  = -27,
    E_REBUILD_INDEX_FAILED            = -28,
    E_INVALID_PASSWORD                = -29,
    E_FAILED_GET_ABS_PATH             = -30,


    // 1xxx for graphd
    E_BAD_USERNAME_PASSWORD           = -1001,     // Authentication error
    E_SESSION_INVALID                 = -1002,     // Execution errors
    E_SESSION_TIMEOUT                 = -1003,
    E_SYNTAX_ERROR                    = -1004,
    E_EXECUTION_ERROR                 = -1005,
    E_STATEMENT_EMPTY                 = -1006,     // Nothing is executed When command is comment

    E_BAD_PERMISSION                  = -1008,
    E_SEMANTIC_ERROR                  = -1009,     // semantic error
    E_TOO_MANY_CONNECTIONS            = -1010,     // Exceeding the maximum number of connections
    E_PARTIAL_SUCCEEDED               = -1011,


    // 2xxx for metad
    E_NO_HOSTS                        = -2001,     // Operation Failure
    E_EXISTED                         = -2002,
    E_INVALID_HOST                    = -2003,
    E_UNSUPPORTED                     = -2004,
    E_NOT_DROP                        = -2005,
    E_BALANCER_RUNNING                = -2006,
    E_CONFIG_IMMUTABLE                = -2007,
    E_CONFLICT                        = -2008,
    E_INVALID_PARM                    = -2009,
    E_WRONGCLUSTER                    = -2010,

    E_STORE_FAILURE                   = -2021,
    E_STORE_SEGMENT_ILLEGAL           = -2022,
    E_BAD_BALANCE_PLAN                = -2023,
    E_BALANCED                        = -2024,
    E_NO_RUNNING_BALANCE_PLAN         = -2025,
    E_NO_VALID_HOST                   = -2026,
    E_CORRUPTTED_BALANCE_PLAN         = -2027,
    E_NO_INVALID_BALANCE_PLAN         = -2028,


    // Authentication Failure
    E_IMPROPER_ROLE                   = -2030,
    E_INVALID_PARTITION_NUM           = -2031,
    E_INVALID_REPLICA_FACTOR          = -2032,
    E_INVALID_CHARSET                 = -2033,
    E_INVALID_COLLATE                 = -2034,
    E_CHARSET_COLLATE_NOT_MATCH       = -2035,

    // Admin Failure
    E_SNAPSHOT_FAILURE                = -2040,
    E_BLOCK_WRITE_FAILURE             = -2041,
    E_REBUILD_INDEX_FAILURE           = -2042,
    E_INDEX_WITH_TTL                  = -2043,
    E_ADD_JOB_FAILURE                 = -2044,
    E_STOP_JOB_FAILURE                = -2045,
    E_SAVE_JOB_FAILURE                = -2046,
    E_BALANCER_FAILURE                = -2047,
    E_JOB_NOT_FINISHED                = -2048,
    E_TASK_REPORT_OUT_DATE            = -2049,
    E_INVALID_JOB                     = -2065,

    // Backup Failure
    E_BACKUP_BUILDING_INDEX           = -2066,
    E_BACKUP_SPACE_NOT_FOUND          = -2067,

    // RESTORE Failure
    E_RESTORE_FAILURE                 = -2068,
    E_SESSION_NOT_FOUND               = -2069,

    // ListClusterInfo Failure
    E_LIST_CLUSTER_FAILURE              = -2070,
    E_LIST_CLUSTER_GET_ABS_PATH_FAILURE = -2071,
    E_GET_META_DIR_FAILURE              = -2072,


    // 3xxx for storaged
    E_CONSENSUS_ERROR                 = -3001,
    E_KEY_HAS_EXISTS                  = -3002,
    E_DATA_TYPE_MISMATCH              = -3003,
    E_INVALID_FIELD_VALUE             = -3004,
    E_INVALID_OPERATION               = -3005,
    E_NOT_NULLABLE                    = -3006,     // Not allowed to be null
    // The field neither can be NULL, nor has a default value
    E_FIELD_UNSET                     = -3007,
    // Value exceeds the range of type
    E_OUT_OF_RANGE                    = -3008,
    // Atomic operation failed
    E_ATOMIC_OP_FAILED                = -3009,
    E_DATA_CONFLICT_ERROR             = -3010,  // data conflict, for index write without toss.

    E_WRITE_STALLED                   = -3011,

    // meta failures
    E_IMPROPER_DATA_TYPE              = -3021,
    E_INVALID_SPACEVIDLEN             = -3022,

    // Invalid request
    E_INVALID_FILTER                  = -3031,
    E_INVALID_UPDATER                 = -3032,
    E_INVALID_STORE                   = -3033,
    E_INVALID_PEER                    = -3034,
    E_RETRY_EXHAUSTED                 = -3035,
    E_TRANSFER_LEADER_FAILED          = -3036,
    E_INVALID_STAT_TYPE               = -3037,
    E_INVALID_VID                     = -3038,
    E_NO_TRANSFORMED                  = -3039,

    // meta client failed
    E_LOAD_META_FAILED                = -3040,

    // checkpoint failed
    E_FAILED_TO_CHECKPOINT            = -3041,
    E_CHECKPOINT_BLOCKED              = -3042,

    // Filter out
    E_FILTER_OUT                      = -3043,
    E_INVALID_DATA                    = -3044,

    E_MUTATE_EDGE_CONFLICT            = -3045,
    E_MUTATE_TAG_CONFLICT             = -3046,

    // transaction
    E_OUTDATED_LOCK                   = -3047,

    // task manager failed
    E_INVALID_TASK_PARA               = -3051,
    E_USER_CANCEL                     = -3052,
    E_TASK_EXECUTION_FAILED           = -3053,

    E_UNKNOWN                         = -8000,
};

template <typename T>
bool inline checkPointer(const T *lhs, const T *rhs) {
    if (lhs == rhs) {
        return true;
    } else if (lhs != nullptr && rhs != nullptr) {
        return *lhs == *rhs;
    } else {
        return false;
    }
}

// TODO(shylock) use optional for optional in thrift instead of pointer

struct AuthResponse {
    void __clear() {
        errorCode = ErrorCode::SUCCEEDED;
        sessionId = nullptr;
        errorMsg = nullptr;
    }

    void clear() {
        __clear();
    }

    bool operator==(const AuthResponse &rhs) const {
        if (errorCode != rhs.errorCode) {
            return false;
        }
        if (!checkPointer(sessionId.get(), rhs.sessionId.get())) {
            return false;
        }
        if (!checkPointer(errorMsg.get(), rhs.errorMsg.get())) {
            return false;
        }
        if (!checkPointer(timeZoneOffsetSeconds.get(), rhs.timeZoneOffsetSeconds.get())) {
            return false;
        }
        return checkPointer(timeZoneName.get(), timeZoneName.get());
    }

    ErrorCode errorCode{ErrorCode::SUCCEEDED};
    std::unique_ptr<int64_t> sessionId{nullptr};
    std::unique_ptr<std::string> errorMsg{nullptr};
    std::unique_ptr<int32_t> timeZoneOffsetSeconds{nullptr};
    std::unique_ptr<std::string> timeZoneName{nullptr};
};


struct ProfilingStats {
    void __clear() {
        rows = 0;
        execDurationInUs = 0;
        totalDurationInUs = 0;
        otherStats = nullptr;
    }

    void clear() {
        __clear();
    }

    bool operator==(const ProfilingStats &rhs) const {
        if (rows != rhs.rows) {
            return false;
        }
        if (execDurationInUs != rhs.execDurationInUs) {
            return false;
        }
        if (totalDurationInUs != rhs.totalDurationInUs) {
            return false;
        }
        return checkPointer(otherStats.get(), rhs.otherStats.get());
    }

    // How many rows being processed in an executor.
    int64_t rows{0};
    // Duration spent in an executor.
    int64_t execDurationInUs{0};
    // Total duration spent in an executor, contains schedule time
    int64_t totalDurationInUs{0};
    // Other profiling stats data map
    std::unique_ptr<std::unordered_map<std::string, std::string>> otherStats;
};

// The info used for select/loop.
struct PlanNodeBranchInfo {
    void __clear() {
        isDoBranch = false;
        conditionNodeId = -1;
    }

    void clear() {
        __clear();
    }

    bool operator==(const PlanNodeBranchInfo &rhs) const {
        return isDoBranch == rhs.isDoBranch && conditionNodeId == rhs.conditionNodeId;
    }

    // True if loop body or then branch of select
    bool  isDoBranch{0};
    // select/loop node id
    int64_t  conditionNodeId{-1};
};

struct Pair {
    void __clear() {
        key.clear();
        value.clear();
    }

    void clear() {
        __clear();
    }

    bool operator==(const Pair &rhs) const {
        return key == rhs.key && value == rhs.value;
    }

    std::string key;
    std::string value;
};

struct PlanNodeDescription {
    void __clear() {
        name.clear();
        id = -1;
        outputVar.clear();
        description = nullptr;
        profiles = nullptr;
        branchInfo = nullptr;
        dependencies = nullptr;
    }

    void clear() {
        __clear();
    }

    bool operator==(const PlanNodeDescription &rhs) const;

    std::string                                   name;
    int64_t                                       id{-1};
    std::string                                   outputVar;
    // other description of an executor
    std::unique_ptr<std::vector<Pair>>            description{nullptr};
    // If an executor would be executed multi times,
    // the profiling statistics should be multi-versioned.
    std::unique_ptr<std::vector<ProfilingStats>>   profiles{nullptr};
    std::unique_ptr<PlanNodeBranchInfo>            branchInfo{nullptr};
    std::unique_ptr<std::vector<int64_t>>          dependencies{nullptr};
};

struct PlanDescription {
    void __clear() {
        planNodeDescs.clear();
        nodeIndexMap.clear();
        format.clear();
        optimize_time_in_us = 0;
    }

    void clear() {
        __clear();
    }

    bool operator==(const PlanDescription &rhs) const {
        return planNodeDescs == rhs.planNodeDescs &&
            nodeIndexMap == rhs.nodeIndexMap &&
            format == rhs.format;
    }

    std::vector<PlanNodeDescription>     planNodeDescs;
    // map from node id to index of list
    std::unordered_map<int64_t, int64_t> nodeIndexMap;
    // the print format of exec plan, lowercase string like `dot'
    std::string                          format;
    // the optimization spent time
    int32_t                              optimize_time_in_us{0};
};

struct ExecutionResponse {
    void __clear() {
        errorCode = ErrorCode::SUCCEEDED;
        latencyInUs = 0;
        data.reset();
        spaceName.reset();
        errorMsg.reset();
        planDesc.reset();
        comment.reset();
    }

    void clear() {
        __clear();
    }

    bool operator==(const ExecutionResponse &rhs) const {
        if (errorCode != rhs.errorCode) {
            return false;
        }
        if (latencyInUs != rhs.latencyInUs) {
            return false;
        }
        if (!checkPointer(data.get(), rhs.data.get())) {
            return false;
        }
        if (!checkPointer(spaceName.get(), rhs.spaceName.get())) {
            return false;
        }
        if (!checkPointer(errorMsg.get(), rhs.errorMsg.get())) {
            return false;
        }
        if (!checkPointer(planDesc.get(), rhs.planDesc.get())) {
            return false;
        }
        if (!checkPointer(comment.get(), rhs.comment.get())) {
            return false;
        }
        return true;
    }

    ErrorCode errorCode{ErrorCode::SUCCEEDED};
    int32_t latencyInUs{0};
    std::unique_ptr<nebula::DataSet> data{nullptr};
    std::unique_ptr<std::string> spaceName{nullptr};
    std::unique_ptr<std::string> errorMsg{nullptr};
    std::unique_ptr<PlanDescription> planDesc{nullptr};
    std::unique_ptr<std::string> comment{nullptr};
};

}   // namespace nebula

#endif  // COMMON_GRAPH_RESPONSE_H
