/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/query/TopNExecutor.h"

#include <folly/Likely.h>  // for UNLIKELY
#include <stddef.h>        // for size_t

#include <algorithm>    // for max
#include <ostream>      // for stringstream, operator<<
#include <queue>        // for make_heap, pop_heap, pus...
#include <string>       // for operator<<, char_traits
#include <type_traits>  // for remove_reference<>::type
#include <utility>      // for move, pair
#include <vector>       // for vector

#include "common/base/Logging.h"             // for LOG, LogMessage, _LOG_ERROR
#include "common/base/Status.h"              // for Status
#include "common/datatypes/Value.h"          // for Value, operator<, operat...
#include "common/time/ScopedTimer.h"         // for SCOPED_TIMER
#include "graph/context/ExecutionContext.h"  // for ExecutionContext
#include "graph/context/Iterator.h"          // for SequentialIter, Iterator
#include "graph/context/Result.h"            // for ResultBuilder, Result
#include "graph/planner/plan/Query.h"        // for TopN
#include "parser/TraverseSentences.h"        // for OrderFactor, OrderFactor...

namespace nebula {
namespace graph {

folly::Future<Status> TopNExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto *topn = asNode<TopN>(node());
  Result result = ectx_->getResult(topn->inputVar());
  auto *iter = result.iterRef();
  if (UNLIKELY(iter == nullptr)) {
    return Status::Error("Internal error: nullptr iterator in topn executor");
  }
  if (UNLIKELY(!result.iter()->isSequentialIter())) {
    std::stringstream ss;
    ss << "Internal error: Sort executor does not supported " << iter->kind();
    LOG(ERROR) << ss.str();
    return Status::Error(ss.str());
  }

  auto &factors = topn->factors();
  comparator_ = [&factors](const Row &lhs, const Row &rhs) {
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

  offset_ = topn->offset();
  auto count = topn->count();
  auto size = iter->size();
  maxCount_ = count;
  heapSize_ = 0;
  if (size <= static_cast<size_t>(offset_)) {
    maxCount_ = 0;
  } else if (size > static_cast<size_t>(offset_ + count)) {
    heapSize_ = offset_ + count;
  } else {
    maxCount_ = size - offset_;
    heapSize_ = size;
  }
  if (heapSize_ == 0) {
    iter->clear();
    return finish(ResultBuilder().value(result.valuePtr()).iter(std::move(result).iter()).build());
  }

  executeTopN<SequentialIter>(iter);
  iter->eraseRange(maxCount_, size);
  return finish(ResultBuilder().value(result.valuePtr()).iter(std::move(result).iter()).build());
}

template <typename U>
void TopNExecutor::executeTopN(Iterator *iter) {
  auto uIter = static_cast<U *>(iter);
  std::vector<Row> heap(uIter->begin(), uIter->begin() + heapSize_);
  std::make_heap(heap.begin(), heap.end(), comparator_);
  auto it = uIter->begin() + heapSize_;
  while (it != uIter->end()) {
    if (comparator_(*it, heap[0])) {
      std::pop_heap(heap.begin(), heap.end(), comparator_);
      heap.pop_back();
      heap.push_back(*it);
      std::push_heap(heap.begin(), heap.end(), comparator_);
    }
    ++it;
  }
  std::sort_heap(heap.begin(), heap.end(), comparator_);

  auto beg = uIter->begin();
  for (int i = 0; i < maxCount_; ++i) {
    beg[i] = heap[offset_ + i];
  }
}

}  // namespace graph
}  // namespace nebula
