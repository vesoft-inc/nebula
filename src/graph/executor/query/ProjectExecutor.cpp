// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/ProjectExecutor.h"
#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {

folly::Future<Status> ProjectExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto* project = asNode<Project>(node());
  auto columns = project->columns()->columns();
  auto iter = ectx_->getResult(project->inputVar()).iter();
  DCHECK(!!iter);
  QueryExpressionContext ctx(ectx_);

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
  return finish(ResultBuilder().value(Value(std::move(ds))).build());
}

}  // namespace graph
}  // namespace nebula
