// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/MinusExecutor.h"
#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {

folly::Future<Status> MinusExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  NG_RETURN_IF_ERROR(checkInputDataSets());

  auto left = getLeftInputData();
  auto right = getRightInputData();

  std::unordered_set<const Row*> hashSet;
  for (; right.iterRef()->valid(); right.iterRef()->next()) {
    hashSet.insert(right.iterRef()->row());
    // TODO: should test duplicate rows
  }

  auto* lIter = left.iterRef();
  if (!hashSet.empty()) {
    while (lIter->valid()) {
      auto iter = hashSet.find(lIter->row());
      if (iter == hashSet.end()) {
        lIter->next();
      } else {
        lIter->unstableErase();
      }
    }
  }

  ResultBuilder builder;
  builder.value(left.valuePtr()).iter(std::move(left).iter());
  return finish(builder.build());
}

}  // namespace graph
}  // namespace nebula
