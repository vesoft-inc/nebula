// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/algo/CartesianProductExecutor.h"

#include "graph/planner/plan/Algo.h"
#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {

folly::Future<Status> CartesianProductExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto* cartesianProduct = asNode<CartesianProduct>(node());
  auto vars = cartesianProduct->inputVars();
  if (vars.size() < 2) {
    return Status::Error("vars's size : %zu, must be greater than 2", vars.size());
  }
  DataSet result;
  auto* lds = const_cast<DataSet*>(&ectx_->getResult(vars[0]).value().getDataSet());
  for (size_t i = 1; i < vars.size(); ++i) {
    const auto& rds = ectx_->getResult(vars[i]).value().getDataSet();
    DataSet ds;
    doCartesianProduct(*lds, rds, ds);
    result = std::move(ds);
    lds = &result;
  }
  for (auto& cols : cartesianProduct->allColNames()) {
    result.colNames.reserve(result.colNames.size() + cols.size());
    result.colNames.insert(result.colNames.end(),
                           std::make_move_iterator(cols.begin()),
                           std::make_move_iterator(cols.end()));
  }
  return finish(ResultBuilder().value(Value(std::move(result))).build());
}

void CartesianProductExecutor::doCartesianProduct(const DataSet& lds,
                                                  const DataSet& rds,
                                                  DataSet& result) {
  result.rows.reserve(lds.size() * rds.size());
  for (auto i = lds.begin(); i < lds.end(); ++i) {
    for (auto j = rds.begin(); j < rds.end(); ++j) {
      Row row;
      row.reserve(i->size() + j->size());
      row.values.insert(row.values.end(), i->values.begin(), i->values.end());
      row.values.insert(row.values.end(), j->values.begin(), j->values.end());
      result.rows.emplace_back(std::move(row));
    }
  }
}

CrossJoinExecutor::CrossJoinExecutor(const PlanNode* node, QueryContext* qctx)
    : CartesianProductExecutor(node, qctx) {
  name_ = "CrossJoinExecutor";
}

folly::Future<Status> CrossJoinExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto* cj = asNode<CrossJoin>(node());
  const auto& lds = ectx_->getResult(cj->leftInputVar()).value().getDataSet();
  const auto& rds = ectx_->getResult(cj->rightInputVar()).value().getDataSet();
  DataSet result;
  doCartesianProduct(lds, rds, result);
  result.colNames = cj->colNames();
  return finish(ResultBuilder().value(Value(std::move(result))).build());
}
}  // namespace graph
}  // namespace nebula
