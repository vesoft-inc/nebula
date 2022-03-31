// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_INTERSECTEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_INTERSECTEXECUTOR_H_

#include "graph/executor/query/SetExecutor.h"
// get the intersection of left & right sets
namespace nebula {
namespace graph {

class IntersectExecutor : public SetExecutor {
 public:
  IntersectExecutor(const PlanNode *node, QueryContext *qctx)
      : SetExecutor("IntersectExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_INTERSECTEXECUTOR_H_
