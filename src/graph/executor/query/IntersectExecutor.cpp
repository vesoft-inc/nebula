/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/query/IntersectExecutor.h"

#include <algorithm>      // for max
#include <string>         // for string, basic_string
#include <type_traits>    // for remove_reference<>::type
#include <unordered_set>  // for unordered_set, operator==
#include <utility>        // for move
#include <vector>         // for vector

#include "common/base/Status.h"        // for Status, NG_RETURN_IF_ERROR
#include "common/datatypes/DataSet.h"  // for DataSet, Row
#include "common/datatypes/Value.h"    // for Value
#include "common/time/ScopedTimer.h"   // for SCOPED_TIMER
#include "graph/context/Iterator.h"    // for Iterator, equal_to, hash
#include "graph/context/Result.h"      // for ResultBuilder, Result

namespace nebula {
namespace graph {

folly::Future<Status> IntersectExecutor::execute() {
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
  ResultBuilder builder;
  if (hashSet.empty()) {
    auto value = lIter->valuePtr();
    DataSet ds;
    ds.colNames = value->getDataSet().colNames;
    builder.value(Value(std::move(ds))).iter(Iterator::Kind::kSequential);
    return finish(builder.build());
  }

  while (lIter->valid()) {
    auto iter = hashSet.find(lIter->row());
    if (iter == hashSet.end()) {
      lIter->unstableErase();
    } else {
      lIter->next();
    }
  }

  builder.value(left.valuePtr()).iter(std::move(left).iter());
  return finish(builder.build());
}

}  // namespace graph
}  // namespace nebula
