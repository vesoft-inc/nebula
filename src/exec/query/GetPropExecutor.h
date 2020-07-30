/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef _EXEC_QUERY_GET_PROP_EXECUTOR_H_
#define _EXEC_QUERY_GET_PROP_EXECUTOR_H_

#include "exec/Executor.h"
#include "common/clients/storage/StorageClientBase.h"

namespace nebula {
namespace graph {

class GetPropExecutor : public Executor {
protected:
    GetPropExecutor(const std::string &name, const PlanNode *node, QueryContext *qctx)
        : Executor(name, node, qctx) {}

    Status handleResp(storage::StorageRpcResponse<storage::cpp2::GetPropResponse> &&rpcResp) {
        auto result = handleCompleteness(rpcResp);
        NG_RETURN_IF_ERROR(result);
        auto state = std::move(result).value();
        // Ok, merge DataSets to one
        nebula::DataSet v;
        for (auto &resp : rpcResp.responses()) {
            if (resp.__isset.props) {
                if (UNLIKELY(!v.append(std::move(*resp.get_props())))) {
                    // it's impossible according to the interface
                    LOG(WARNING) << "Heterogeneous props dataset";
                    state = Result::State::kPartialSuccess;
                }
            } else {
                state = Result::State::kPartialSuccess;
            }
        }
        for (auto &colName : v.colNames) {
            std::replace(colName.begin(), colName.end(), ':', '.');
        }
        VLOG(1) << "Resp: " << v;
        return finish(ResultBuilder()
                      .value(std::move(v))
                      .iter(Iterator::Kind::kSequential)
                      .state(state)
                      .finish());
    }

    // TODO(shylock) only used for storage fetch executor, will modify other executors to use it
    template <typename Resp>
    StatusOr<Result::State>
    handleCompleteness(const storage::StorageRpcResponse<Resp> &rpcResp) const {
        auto completeness = rpcResp.completeness();
        if (completeness != 100) {
            const auto &failedCodes = rpcResp.failedParts();
            for (auto it = failedCodes.begin(); it != failedCodes.end(); it++) {
                LOG(ERROR) << name_ << " failed, error "
                           << storage::cpp2::_ErrorCode_VALUES_TO_NAMES.at(it->second) << ", part "
                           << it->first;
            }
            if (completeness == 0) {
                LOG(ERROR) << "Get data from storage failed in executor " << name_;
                return Status::Error(" Get data from storage failed in executor.");
            }
            return Result::State::kPartialSuccess;
        }
        return Result::State::kSuccess;
    }
};

}   // namespace graph
}   // namespace nebula

#endif  // _EXEC_QUERY_GET_PROP_EXECUTOR_H_
