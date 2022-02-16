/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/algo/CartesianProductExecutor.h"

#include <stddef.h>  // for size_t

#include <algorithm>  // for max
#include <iterator>   // for move_iterator, make_move...
#include <ostream>    // for operator<<, basic_ostream
#include <utility>    // for move

#include "common/base/Logging.h"             // for COMPACT_GOOGLE_LOG_INFO
#include "common/base/Status.h"              // for Status
#include "common/datatypes/DataSet.h"        // for Row, DataSet, operator<<
#include "common/datatypes/List.h"           // for List
#include "common/datatypes/Value.h"          // for Value
#include "common/time/ScopedTimer.h"         // for SCOPED_TIMER
#include "graph/context/ExecutionContext.h"  // for ExecutionContext
#include "graph/context/Result.h"            // for ResultBuilder, Result
#include "graph/planner/plan/Algo.h"         // for BiCartesianProduct, Cart...

namespace nebula {
namespace graph {
class PlanNode;
class QueryContext;

class PlanNode;
class QueryContext;

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
  VLOG(1) << "Cartesian Product is : " << result;
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

BiCartesianProductExecutor::BiCartesianProductExecutor(const PlanNode* node, QueryContext* qctx)
    : CartesianProductExecutor(node, qctx) {
  name_ = "BiCartesianProductExecutor";
}

folly::Future<Status> BiCartesianProductExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto* BiCP = asNode<BiCartesianProduct>(node());
  const auto& lds = ectx_->getResult(BiCP->leftInputVar()).value().getDataSet();
  const auto& rds = ectx_->getResult(BiCP->rightInputVar()).value().getDataSet();
  DataSet result;
  doCartesianProduct(lds, rds, result);
  result.colNames = BiCP->colNames();
  VLOG(1) << "Cartesian Product is : " << result;
  return finish(ResultBuilder().value(Value(std::move(result))).build());
}
}  // namespace graph
}  // namespace nebula
