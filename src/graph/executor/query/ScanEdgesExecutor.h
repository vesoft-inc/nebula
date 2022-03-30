// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/GetPropExecutor.h"
// full table scan edge, but must have limit parameter
namespace nebula {
namespace graph {
class ScanEdgesExecutor final : public GetPropExecutor {
 public:
  ScanEdgesExecutor(const PlanNode *node, QueryContext *qctx)
      : GetPropExecutor("ScanEdgesExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  folly::Future<Status> scanEdges();
};

}  // namespace graph
}  // namespace nebula
