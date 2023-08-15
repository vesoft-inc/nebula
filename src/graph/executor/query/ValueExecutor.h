// Copyright (c) 2023 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#pragma once

#include "graph/executor/Executor.h"

// delete the corresponding iterator when the row in the dataset does not meet the conditions
// and save the filtered iterator to the result
namespace nebula {
namespace graph {

class ValueExecutor final : public Executor {
 public:
  ValueExecutor(const PlanNode *node, QueryContext *qctx) : Executor("ValueExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

}  // namespace graph
}  // namespace nebula
