/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/futures/Future.h>  // for Future

#include <memory>  // for allocator

#include "graph/executor/query/GetPropExecutor.h"  // for GetPropExecutor

namespace nebula {
class Status;
namespace graph {
class PlanNode;
class QueryContext;
}  // namespace graph

class Status;

namespace graph {
class PlanNode;
class QueryContext;

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
