// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_FULLTEXTINDEXSCANEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_FULLTEXTINDEXSCANEXECUTOR_H_

#include "common/plugin/fulltext/elasticsearch/ESAdapter.h"
#include "graph/executor/StorageAccessExecutor.h"
#include "graph/service/GraphFlags.h"

namespace nebula::graph {
class FulltextIndexScan;
class FulltextIndexScanExecutor final : public StorageAccessExecutor {
 public:
  FulltextIndexScanExecutor(const PlanNode* node, QueryContext* qctx)
      : StorageAccessExecutor("FulltextIndexScanExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  StatusOr<plugin::ESQueryResult> accessFulltextIndex(const std::string& index,
                                                      TextSearchExpression* expr);
  folly::Future<Status> getTags(const FulltextIndexScan* scan, DataSet&& dataset);
  folly::Future<Status> getEdges(const FulltextIndexScan* scan, DataSet&& dataset);
  plugin::ESAdapter esAdapter_;

  template <typename Response>
  Status handleResp(storage::StorageRpcResponse<Response>&& rpcResp,
                    const std::vector<std::string>& colNames) {
    auto result = handleCompleteness(rpcResp, FLAGS_accept_partial_success);
    NG_RETURN_IF_ERROR(result);
    auto state = std::move(result).value();
    // Ok, merge DataSets to one
    nebula::DataSet v;
    for (auto& resp : rpcResp.responses()) {
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

}  // namespace nebula::graph

#endif
