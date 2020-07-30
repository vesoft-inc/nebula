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
        if (completeness != 100) {
            const auto& failedCodes = resp.failedParts();
            for (auto it = failedCodes.begin(); it != failedCodes.end(); it++) {
                LOG(ERROR) << executorName << " failed, error "
                           << storage::cpp2::_ErrorCode_VALUES_TO_NAMES.at(it->second)
                           << ", part " << it->first;
            }
            return Status::Error("%s not complete, completeness: %d",
                                 executorName.c_str(), completeness);
        }
        return Status::OK();
    }
};
}   // namespace graph
}   // namespace nebula

#endif   // EXEC_MUTATE_MUTATEEXECUTOR_H_
