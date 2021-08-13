/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/executor/logic/StartExecutor.h"

#include "graph/planner/plan/PlanNode.h"
#include "graph/util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> StartExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  return start();
}

}  // namespace graph
}  // namespace nebula
