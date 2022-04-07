// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/logic/ArgumentExecutor.h"

#include "graph/planner/plan/Logic.h"

namespace nebula {
namespace graph {

ArgumentExecutor::ArgumentExecutor(const PlanNode *node, QueryContext *qctx)
    : Executor("ArgumentExecutor", node, qctx) {}

folly::Future<Status> ArgumentExecutor::execute() {
  auto *argNode = asNode<Argument>(node());
  auto &alias = argNode->getAlias();
  auto iter = ectx_->getResult(argNode->inputVar()).iter();
  DCHECK(iter != nullptr);

  DataSet ds;
  ds.colNames = argNode->colNames();
  ds.rows.reserve(iter->size());
  std::unordered_set<Value> unique;
  for (; iter->valid(); iter->next()) {
    auto val = iter->getColumn(alias);
    if (!val.isVertex()) {
      continue;
    }
    if (unique.emplace(val.getVertex().vid).second) {
      Row row;
      row.values.emplace_back(std::move(val));
      ds.rows.emplace_back(std::move(row));
    }
  }
  return finish(ResultBuilder().value(Value(std::move(ds))).build());
}
}  // namespace graph
}  // namespace nebula
