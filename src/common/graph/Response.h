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
    SUCCEEDED = 0,
    E_DISCONNECTED = -1,
    E_FAIL_TO_CONNECT = -2,
    E_RPC_FAILURE = -3,
    E_BAD_USERNAME_PASSWORD = -4,
    E_SESSION_INVALID = -5,
    E_SESSION_TIMEOUT = -6,
    E_SYNTAX_ERROR = -7,
    E_EXECUTION_ERROR = -8,
    E_STATEMENT_EMPTY = -9,
    E_USER_NOT_FOUND = -10,
    E_BAD_PERMISSION = -11,
    E_SEMANTIC_ERROR = -12,
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
    void clear() {
        errorCode = ErrorCode::SUCCEEDED;
        sessionId = nullptr;
        errorMsg = nullptr;
    }

    bool operator==(const AuthResponse &rhs) const {
        if (errorCode != rhs.errorCode) {
            return false;
        }
        if (!checkPointer(sessionId.get(), rhs.sessionId.get())) {
            return false;
        }
        return checkPointer(errorMsg.get(), rhs.errorMsg.get());
    }

    ErrorCode errorCode{ErrorCode::SUCCEEDED};
    std::unique_ptr<int64_t> sessionId{nullptr};
    std::unique_ptr<std::string> errorMsg{nullptr};
};


struct ProfilingStats {
    void clear() {
        rows = 0;
        execDurationInUs = 0;
        totalDurationInUs = 0;
        otherStats = nullptr;
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
    void clear() {
        isDoBranch = false;
        conditionNodeId = -1;
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
    void clear() {
        key.clear();
        value.clear();
    }

    bool operator==(const Pair &rhs) const {
        return key == rhs.key && value == rhs.value;
    }

    std::string key;
    std::string value;
};

struct PlanNodeDescription {
    void clear() {
        name.clear();
        id = -1;
        outputVar.clear();
        description = nullptr;
        profiles = nullptr;
        branchInfo = nullptr;
        dependencies = nullptr;
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
    void clear() {
        planNodeDescs.clear();
        nodeIndexMap.clear();
        format.clear();
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
};

struct ExecutionResponse {
    void clear() {
        errorCode = ErrorCode::SUCCEEDED;
        latencyInUs = 0;
        data = nullptr;
        spaceName = nullptr;
        errorMsg = nullptr;
        planDesc = nullptr;
        comment = nullptr;
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
        return checkPointer(comment.get(), rhs.comment.get());
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
