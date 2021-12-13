/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include "graph/executor/query/GetPropExecutor.h"
#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {

class ScanVerticesExecutor final : public GetPropExecutor {
 public:
  ScanVerticesExecutor(const PlanNode *node, QueryContext *qctx)
      : GetPropExecutor("ScanVerticesExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  folly::Future<Status> scanVertices();
};

}  // namespace graph
}  // namespace nebula
