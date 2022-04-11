// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/UnionExecutor.h"

#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {

folly::Future<Status> UnionExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  NG_RETURN_IF_ERROR(checkInputDataSets());
  auto left = getLeftInputDataIter();
  auto right = getRightInputDataIter();

  DataSet ds;
  ds.colNames = std::move(colNames_);

  DCHECK(left->isSequentialIter());
  auto leftIter = static_cast<SequentialIter*>(left.get());
  ds.rows.insert(ds.rows.end(),
                 std::make_move_iterator(leftIter->begin()),
                 std::make_move_iterator(leftIter->end()));

  DCHECK(right->isSequentialIter());
  auto rightIter = static_cast<SequentialIter*>(right.get());
  ds.rows.insert(ds.rows.end(),
                 std::make_move_iterator(rightIter->begin()),
                 std::make_move_iterator(rightIter->end()));

  return finish(ResultBuilder().value(Value(std::move(ds))).build());
}

}  // namespace graph
}  // namespace nebula
