/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_GRAPH_RESPONSE_H
#define COMMON_GRAPH_RESPONSE_H

#include <algorithm>
#include <memory>
#include <ostream>
#include <unordered_map>
#include <vector>

#include "common/datatypes/DataSet.h"

#define ErrorCodeEnums \
    /* for common code */ \
    X(SUCCEEDED                         , 0)  \
\
    X(E_DISCONNECTED                    , -1)     /* RPC Failure */  \
    X(E_FAIL_TO_CONNECT                 , -2)  \
    X(E_RPC_FAILURE                     , -3)  \
    X(E_LEADER_CHANGED                  , -4)  \
\
    /* only unify metad and storaged error code */ \
    X(E_SPACE_NOT_FOUND                 , -5)  \
    X(E_TAG_NOT_FOUND                   , -6)  \
    X(E_EDGE_NOT_FOUND                  , -7)  \
    X(E_INDEX_NOT_FOUND                 , -8)  \
    X(E_EDGE_PROP_NOT_FOUND             , -9)  \
    X(E_TAG_PROP_NOT_FOUND              , -10)  \
    X(E_ROLE_NOT_FOUND                  , -11)  \
    X(E_CONFIG_NOT_FOUND                , -12)  \
    X(E_GROUP_NOT_FOUND                 , -13)  \
    X(E_ZONE_NOT_FOUND                  , -14)  \
    X(E_LISTENER_NOT_FOUND              , -15)  \
    X(E_PART_NOT_FOUND                  , -16)  \
    X(E_KEY_NOT_FOUND                   , -17)  \
    X(E_USER_NOT_FOUND                  , -18)  \
\
    /* backup failed */ \
    X(E_BACKUP_FAILED                   , -24)  \
    X(E_BACKUP_EMPTY_TABLE              , -25)  \
    X(E_BACKUP_TABLE_FAILED             , -26)  \
    X(E_PARTIAL_RESULT                  , -27)  \
    X(E_REBUILD_INDEX_FAILED            , -28)  \
    X(E_INVALID_PASSWORD                , -29)  \
    X(E_FAILED_GET_ABS_PATH             , -30)  \
\
    /* 1xxx for graphd */ \
    X(E_BAD_USERNAME_PASSWORD           , -1001)  /* Authentication error */  \
    X(E_SESSION_INVALID                 , -1002)    /* Execution errors */  \
    X(E_SESSION_TIMEOUT                 , -1003)  \
    X(E_SYNTAX_ERROR                    , -1004)  \
    X(E_EXECUTION_ERROR                 , -1005)  \
    X(E_STATEMENT_EMPTY                 , -1006)  /* Nothing is executed When command is empty */  \
\
    X(E_BAD_PERMISSION                  , -1008)  \
    X(E_SEMANTIC_ERROR                  , -1009)  /* semantic error */   \
    X(E_TOO_MANY_CONNECTIONS            , -1010)  \
    X(E_PARTIAL_SUCCEEDED               , -1011)  \
\
    /* 2xxx for metad */ \
    X(E_NO_HOSTS                        , -2001)  /* Operation Failure*/   \
    X(E_EXISTED                         , -2002)  \
    X(E_INVALID_HOST                    , -2003)  \
    X(E_UNSUPPORTED                     , -2004)  \
    X(E_NOT_DROP                        , -2005)  \
    X(E_BALANCER_RUNNING                , -2006)  \
    X(E_CONFIG_IMMUTABLE                , -2007)  \
    X(E_CONFLICT                        , -2008)  \
    X(E_INVALID_PARM                    , -2009)  \
    X(E_WRONGCLUSTER                    , -2010)  \
\
    X(E_STORE_FAILURE                   , -2021)  \
    X(E_STORE_SEGMENT_ILLEGAL           , -2022)  \
    X(E_BAD_BALANCE_PLAN                , -2023)  \
    X(E_BALANCED                        , -2024)  \
    X(E_NO_RUNNING_BALANCE_PLAN         , -2025)  \
    X(E_NO_VALID_HOST                   , -2026)  \
    X(E_CORRUPTTED_BALANCE_PLAN         , -2027)  \
    X(E_NO_INVALID_BALANCE_PLAN         , -2028)  \
\
    /* Authentication Failure */ \
    X(E_IMPROPER_ROLE                   , -2030)  \
    X(E_INVALID_PARTITION_NUM           , -2031)  \
    X(E_INVALID_REPLICA_FACTOR          , -2032)  \
    X(E_INVALID_CHARSET                 , -2033)  \
    X(E_INVALID_COLLATE                 , -2034)  \
    X(E_CHARSET_COLLATE_NOT_MATCH       , -2035)  \
\
    /* Admin Failure */ \
    X(E_SNAPSHOT_FAILURE                , -2040)  \
    X(E_BLOCK_WRITE_FAILURE             , -2041)  \
    X(E_REBUILD_INDEX_FAILURE           , -2042)  \
    X(E_INDEX_WITH_TTL                  , -2043)  \
    X(E_ADD_JOB_FAILURE                 , -2044)  \
    X(E_STOP_JOB_FAILURE                , -2045)  \
    X(E_SAVE_JOB_FAILURE                , -2046)  \
    X(E_BALANCER_FAILURE                , -2047)  \
    X(E_JOB_NOT_FINISHED                , -2048)  \
    X(E_TASK_REPORT_OUT_DATE            , -2049)  \
    X(E_INVALID_JOB                     , -2065)  \
\
    /* Backup Failure */ \
    X(E_BACKUP_BUILDING_INDEX           , -2066)  \
    X(E_BACKUP_SPACE_NOT_FOUND          , -2067)  \
\
    /* RESTORE Failure */ \
    X(E_RESTORE_FAILURE                 , -2068)  \
    X(E_SESSION_NOT_FOUND               , -2069)  \
\
    /* ListClusterInfo Failure */ \
    X(E_LIST_CLUSTER_FAILURE              , -2070)  \
    X(E_LIST_CLUSTER_GET_ABS_PATH_FAILURE , -2071)  \
    X(E_GET_META_DIR_FAILURE              , -2072)  \
\
    X(E_QUERY_NOT_FOUND                   , -2073) \
    /* 3xxx for storaged */ \
    X(E_CONSENSUS_ERROR                 , -3001)  \
    X(E_KEY_HAS_EXISTS                  , -3002)  \
    X(E_DATA_TYPE_MISMATCH              , -3003)  \
    X(E_INVALID_FIELD_VALUE             , -3004)  \
    X(E_INVALID_OPERATION               , -3005)  \
    X(E_NOT_NULLABLE                    , -3006)  /* Not allowed to be null */   \
    /* The field neither can be NULL, nor has a default value */ \
    X(E_FIELD_UNSET                     , -3007)  \
    /* Value exceeds the range of type */ \
    X(E_OUT_OF_RANGE                    , -3008)  \
    /* Atomic operation failed */ \
    X(E_ATOMIC_OP_FAILED                , -3009)  \
    /* data conflict, for index write without toss. */  \
    X(E_DATA_CONFLICT_ERROR             , -3010)  \
\
    X(E_WRITE_STALLED                   , -3011)  \
\
    /* meta failures */ \
    X(E_IMPROPER_DATA_TYPE              , -3021)  \
    X(E_INVALID_SPACEVIDLEN             , -3022)  \
\
    /* Invalid request */ \
    X(E_INVALID_FILTER                  , -3031)  \
    X(E_INVALID_UPDATER                 , -3032)  \
    X(E_INVALID_STORE                   , -3033)  \
    X(E_INVALID_PEER                    , -3034)  \
    X(E_RETRY_EXHAUSTED                 , -3035)  \
    X(E_TRANSFER_LEADER_FAILED          , -3036)  \
    X(E_INVALID_STAT_TYPE               , -3037)  \
    X(E_INVALID_VID                     , -3038)  \
    X(E_NO_TRANSFORMED                  , -3039)  \
\
    /* meta client failed */ \
    X(E_LOAD_META_FAILED                , -3040)  \
\
    /* checkpoint failed */ \
    X(E_FAILED_TO_CHECKPOINT            , -3041)  \
    X(E_CHECKPOINT_BLOCKED              , -3042)  \
\
    /* Filter out */ \
    X(E_FILTER_OUT                      , -3043)  \
    X(E_INVALID_DATA                    , -3044)  \
\
    X(E_MUTATE_EDGE_CONFLICT            , -3045)  \
    X(E_MUTATE_TAG_CONFLICT             , -3046)  \
\
    /* transaction */ \
    X(E_OUTDATED_LOCK                   , -3047)  \
\
    /* task manager failed */ \
    X(E_INVALID_TASK_PARA               , -3051)  \
    X(E_USER_CANCEL                     , -3052)  \
    X(E_TASK_EXECUTION_FAILED           , -3053)  \
\
    X(E_UNKNOWN                         , -8000)  \


namespace nebula {

#define X(EnumName, EnumNumber) EnumName = EnumNumber,

enum class ErrorCode {
    ErrorCodeEnums
};

#undef X

const char* errorCode(ErrorCode code);

static inline std::ostream& operator<<(std::ostream &os, ErrorCode code) {
    os << errorCode(code);
    return os;
}

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
