// Copyright (c) 2023 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/ValueExecutor.h"

#include "graph/context/Result.h"
#include "graph/planner/plan/Query.h"
#include "graph/service/GraphFlags.h"

namespace nebula {
namespace graph {

folly::Future<Status> ValueExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto value = asNode<ValueNode>(node())->value();
  return finish(ResultBuilder().value(std::move(value)).build());
}

}  // namespace graph
}  // namespace nebula
