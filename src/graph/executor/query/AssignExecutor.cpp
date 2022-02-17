/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/query/AssignExecutor.h"

#include <string>   // for operator<<, char_t...
#include <utility>  // for move, pair
#include <vector>   // for vector

#include "common/base/Logging.h"                   // for COMPACT_GOOGLE_LOG...
#include "common/base/Status.h"                    // for Status
#include "common/datatypes/Value.h"                // for operator<<, Value
#include "common/expression/Expression.h"          // for Expression
#include "common/time/ScopedTimer.h"               // for SCOPED_TIMER
#include "graph/context/ExecutionContext.h"        // for ExecutionContext
#include "graph/context/QueryExpressionContext.h"  // for QueryExpressionCon...
#include "graph/context/Result.h"                  // for ResultBuilder
#include "graph/planner/plan/Query.h"              // for Assign

namespace nebula {
namespace graph {

folly::Future<Status> AssignExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto* assign = asNode<Assign>(node());
  auto& items = assign->items();
  QueryExpressionContext ctx(ectx_);

  for (const auto& item : items) {
    auto varName = item.first;
    auto& valueExpr = item.second;
    auto value = valueExpr->eval(ctx);
    VLOG(1) << "VarName is: " << varName << " value is : " << value;
    if (value.isDataSet()) {
      ectx_->setResult(varName, ResultBuilder().value(std::move(value)).build());
    } else {
      ectx_->setValue(varName, std::move(value));
    }
  }
  return Status::OK();
}

}  // namespace graph
}  // namespace nebula
