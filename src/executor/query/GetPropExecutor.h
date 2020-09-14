/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef _EXEC_QUERY_GET_PROP_EXECUTOR_H_
#define _EXEC_QUERY_GET_PROP_EXECUTOR_H_

#include "executor/QueryStorageExecutor.h"
#include "common/clients/storage/StorageClientBase.h"

namespace nebula {
namespace graph {

class GetPropExecutor : public QueryStorageExecutor {
protected:
    GetPropExecutor(const std::string &name, const PlanNode *node, QueryContext *qctx)
        : QueryStorageExecutor(name, node, qctx) {}

    Status handleResp(storage::StorageRpcResponse<storage::cpp2::GetPropResponse> &&rpcResp,
                      const std::vector<std::string> &colNames) {
        auto result = handleCompleteness(rpcResp, false);
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
        if (!colNames.empty()) {
            DCHECK_EQ(colNames.size(), v.colSize());
            v.colNames = colNames;
        } else {
            for (auto &colName : v.colNames) {
                std::replace(colName.begin(), colName.end(), ':', '.');
            }
        }
        VLOG(1) << "Resp: " << v;
        return finish(ResultBuilder()
                      .value(std::move(v))
                      .iter(Iterator::Kind::kProp)
                      .state(state)
                      .finish());
    }
};

}   // namespace graph
}   // namespace nebula

#endif  // _EXEC_QUERY_GET_PROP_EXECUTOR_H_
