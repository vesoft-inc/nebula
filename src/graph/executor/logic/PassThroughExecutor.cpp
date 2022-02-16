/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/logic/PassThroughExecutor.h"

#include <algorithm>  // for max
#include <string>     // for string, basic_string
#include <utility>    // for move
#include <vector>     // for vector

#include "common/base/Status.h"              // for Status
#include "common/datatypes/DataSet.h"        // for DataSet
#include "common/datatypes/Value.h"          // for Value
#include "common/time/ScopedTimer.h"         // for SCOPED_TIMER
#include "graph/context/ExecutionContext.h"  // for ExecutionContext
#include "graph/context/Iterator.h"          // for Iterator
#include "graph/context/Result.h"            // for ResultBuilder, Result
#include "graph/planner/plan/PlanNode.h"     // for PlanNode

namespace nebula {
namespace graph {

folly::Future<Status> PassThroughExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  const auto &result = ectx_->getResult(node()->outputVar());
  auto iter = result.iter();
  if (!iter->isDefaultIter() && !iter->empty()) {
    // Return directly if this pass through output result is not empty
    return Status::OK();
  }

  DataSet ds;
  ds.colNames = node()->colNames();
  return finish(ResultBuilder().value(Value(std::move(ds))).build());
}

}  // namespace graph
}  // namespace nebula
