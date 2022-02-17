/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/query/ProjectExecutor.h"

#include <algorithm>  // for max
#include <string>     // for string, operator<<
#include <utility>    // for move
#include <vector>     // for vector

#include "common/base/Logging.h"                   // for COMPACT_GOOGLE_LOG...
#include "common/base/Status.h"                    // for Status
#include "common/datatypes/DataSet.h"              // for Row, DataSet, oper...
#include "common/datatypes/Value.h"                // for Value
#include "common/expression/Expression.h"          // for Expression
#include "common/time/ScopedTimer.h"               // for SCOPED_TIMER
#include "graph/context/ExecutionContext.h"        // for ExecutionContext
#include "graph/context/Iterator.h"                // for Iterator
#include "graph/context/QueryExpressionContext.h"  // for QueryExpressionCon...
#include "graph/context/Result.h"                  // for ResultBuilder, Result
#include "graph/planner/plan/PlanNode.h"           // for PlanNode
#include "graph/planner/plan/Query.h"              // for Project
#include "parser/Clauses.h"                        // for YieldColumn, Yield...

namespace nebula {
namespace graph {

folly::Future<Status> ProjectExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto* project = asNode<Project>(node());
  auto columns = project->columns()->columns();
  auto iter = ectx_->getResult(project->inputVar()).iter();
  DCHECK(!!iter);
  QueryExpressionContext ctx(ectx_);

  VLOG(1) << "input: " << project->inputVar();
  DataSet ds;
  ds.colNames = project->colNames();
  ds.rows.reserve(!iter->isGetNeighborsIter() ? iter->size() : 0);
  for (; iter->valid(); iter->next()) {
    Row row;
    for (auto& col : columns) {
      Value val = col->expr()->eval(ctx(iter.get()));
      row.values.emplace_back(std::move(val));
    }
    ds.rows.emplace_back(std::move(row));
  }
  VLOG(1) << node()->outputVar() << ":" << ds;
  return finish(ResultBuilder().value(Value(std::move(ds))).build());
}

}  // namespace graph
}  // namespace nebula
