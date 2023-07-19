/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef _EXEC_QUERY_GET_PROP_EXECUTOR_H_
#define _EXEC_QUERY_GET_PROP_EXECUTOR_H_

#include "clients/storage/StorageClientBase.h"
#include "graph/executor/StorageAccessExecutor.h"
#include "graph/service/GraphFlags.h"

namespace nebula {
namespace graph {

class GetPropExecutor : public StorageAccessExecutor {
 protected:
  GetPropExecutor(const std::string &name, const PlanNode *node, QueryContext *qctx)
      : StorageAccessExecutor(name, node, qctx) {}

  template <typename Response>
  Status handleResp(storage::StorageRpcResponse<Response> &&rpcResp,
                    const std::vector<std::string> &colNames) {
    auto result = handleCompleteness(rpcResp, FLAGS_accept_partial_success);
    NG_RETURN_IF_ERROR(result);
    auto state = std::move(result).value();
    // Ok, merge DataSets to one
    nebula::DataSet v;
    for (auto &resp : rpcResp.responses()) {
      if (resp.props_ref().has_value()) {
        if (UNLIKELY(!v.append(std::move(*resp.props_ref())))) {
          // it's impossible according to the interface
          LOG(ERROR) << "Heterogeneous props dataset";
          state = Result::State::kPartialSuccess;
        }
      } else {
        state = Result::State::kPartialSuccess;
      }
    }
    if (!colNames.empty()) {
      DCHECK_EQ(colNames.size(), v.colSize());
      v.colNames = colNames;
    }
    return finish(
        ResultBuilder().value(std::move(v)).iter(Iterator::Kind::kProp).state(state).build());
  }
};

}  // namespace graph
}  // namespace nebula

#endif  // _EXEC_QUERY_GET_PROP_EXECUTOR_H_
