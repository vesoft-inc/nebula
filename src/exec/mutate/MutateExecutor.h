/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_MUTATE_MUTATEEXECUTOR_H_
#define EXEC_MUTATE_MUTATEEXECUTOR_H_

#include "common/interface/gen-cpp2/storage_types.h"
#include "common/clients/storage/GraphStorageClient.h"

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class MutateExecutor : public Executor {
public:
    MutateExecutor(const std::string &name, const PlanNode *node, QueryContext *qctx)
        : Executor(name, node, qctx) {}

protected:
    using RpcResponse = nebula::storage::StorageRpcResponse<nebula::storage::cpp2::ExecResponse>;
    Status handleResponse(const RpcResponse &resp, const std::string &executorName) {
        auto completeness = resp.completeness();
        Status status;
        if (completeness != 100) {
            const auto& failedCodes = resp.failedParts();
            std::vector<std::string> errorMsgs;
            for (auto it = failedCodes.begin(); it != failedCodes.end(); it++) {
                LOG(ERROR) << executorName << " failed, error "
                           << storage::cpp2::_ErrorCode_VALUES_TO_NAMES.at(it->second)
                           << ", part " << it->first;
                status = handleErrorCode(it->second, it->first);
                if (status.ok()) {
                    continue;
                }
                errorMsgs.emplace_back(status.toString());
            }
            return Status::Error("%s not complete, completeness: %d, error: %s",
                                 executorName.c_str(),
                                 completeness,
                                 folly::join(", ", errorMsgs).c_str());
        }
        return Status::OK();
    }

    Status handleErrorCode(nebula::storage::cpp2::ErrorCode code, PartitionID partId) {
        switch (code) {
            case storage::cpp2::ErrorCode::E_INVALID_VID:
                return Status::Error("Invalid vid.");
            case storage::cpp2::ErrorCode::E_INVALID_FIELD_VALUE: {
                std::string error = "Invalid field value: may be the filed is not NULL "
                             "or without default value or wrong schema.";
                return Status::Error(std::move(error));
            }
            case storage::cpp2::ErrorCode::E_INVALID_FILTER:
                return Status::Error("Invalid filter.");
            case storage::cpp2::ErrorCode::E_INVALID_UPDATER:
                return Status::Error("Invalid Update col or yield col.");
            case storage::cpp2::ErrorCode::E_TAG_NOT_FOUND:
                return Status::Error("Tag not found.");
            case storage::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND:
                return Status::Error("Tag prop not found.");
            case storage::cpp2::ErrorCode::E_EDGE_NOT_FOUND:
                return Status::Error("Edge not found.");
            case storage::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND:
                return Status::Error("Edge prop not found.");
            case storage::cpp2::ErrorCode::E_INVALID_DATA:
                return Status::Error("Invalid data, may be wrong value type.");
            case storage::cpp2::ErrorCode::E_NOT_NULLABLE:
                return Status::Error("The not null field cannot be null.");
            case storage::cpp2::ErrorCode::E_FIELD_UNSET:
                return Status::Error("The not null field doesn't have a default value.");
            case storage::cpp2::ErrorCode::E_OUT_OF_RANGE:
                return Status::Error("Out of range value.");
            case storage::cpp2::ErrorCode::E_ATOMIC_OP_FAILED:
                return Status::Error("Atomic operation failed.");
            case storage::cpp2::ErrorCode::E_FILTER_OUT:
                return Status::OK();
            default:
                auto status = Status::Error("Unknown error, part: %d, error code: %d.",
                                            partId, static_cast<int32_t>(code));
                LOG(ERROR) << status;
                return status;
        }
        return Status::OK();
    }
};
}   // namespace graph
}   // namespace nebula

#endif   // EXEC_MUTATE_MUTATEEXECUTOR_H_
