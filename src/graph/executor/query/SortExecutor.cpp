/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/query/SortExecutor.h"

#include <folly/Likely.h>  // for UNLIKELY

#include <algorithm>    // for sort
#include <string>       // for operator<<, char_traits
#include <type_traits>  // for remove_reference<>::type
#include <utility>      // for move, pair
#include <vector>       // for vector

#include "common/base/Logging.h"             // for LOG, LogMessage, _LOG_ERROR
#include "common/base/Status.h"              // for Status
#include "common/datatypes/DataSet.h"        // for Row
#include "common/datatypes/Value.h"          // for Value, operator<, operat...
#include "common/time/ScopedTimer.h"         // for SCOPED_TIMER
#include "graph/context/ExecutionContext.h"  // for ExecutionContext
#include "graph/context/Iterator.h"          // for SequentialIter, operator<<
#include "graph/context/Result.h"            // for ResultBuilder, Result
#include "graph/planner/plan/Query.h"        // for Sort
#include "parser/TraverseSentences.h"        // for OrderFactor, OrderFactor...

namespace nebula {
namespace graph {

folly::Future<Status> SortExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto *sort = asNode<Sort>(node());
  Result result = ectx_->getResult(sort->inputVar());
  auto *iter = result.iterRef();
  if (UNLIKELY(iter == nullptr)) {
    return Status::Error("Internal error: nullptr iterator in sort executor");
  }
  if (UNLIKELY(!iter->isSequentialIter())) {
    std::stringstream ss;
    ss << "Internal error: Sort executor does not supported " << iter->kind();
    LOG(ERROR) << ss.str();
    return Status::Error(ss.str());
  }

  auto &factors = sort->factors();
  auto comparator = [&factors](const Row &lhs, const Row &rhs) {
    for (auto &item : factors) {
      auto index = item.first;
      auto orderType = item.second;
      if (lhs[index] == rhs[index]) {
        continue;
      }

      if (orderType == OrderFactor::OrderType::ASCEND) {
        return lhs[index] < rhs[index];
      } else if (orderType == OrderFactor::OrderType::DESCEND) {
        return lhs[index] > rhs[index];
      }
    }
    return false;
  };

  auto seqIter = static_cast<SequentialIter *>(iter);
  std::sort(seqIter->begin(), seqIter->end(), comparator);
  return finish(ResultBuilder().value(result.valuePtr()).iter(std::move(result).iter()).build());
}

}  // namespace graph
}  // namespace nebula
