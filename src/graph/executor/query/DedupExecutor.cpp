/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/query/DedupExecutor.h"

#include <folly/Likely.h>  // for UNLIKELY
#include <stdint.h>        // for uint16_t

#include <algorithm>      // for max
#include <string>         // for string
#include <unordered_set>  // for unordered_set
#include <utility>        // for move, pair

#include "common/base/Logging.h"             // for COMPACT_GOOGLE_LOG_FATAL
#include "common/base/Status.h"              // for Status, operator<<
#include "common/datatypes/DataSet.h"        // for Row
#include "common/time/ScopedTimer.h"         // for SCOPED_TIMER
#include "graph/context/ExecutionContext.h"  // for ExecutionContext
#include "graph/context/Iterator.h"          // for Iterator, equal_to, hash
#include "graph/context/Result.h"            // for Result
#include "graph/planner/plan/Query.h"        // for Dedup

namespace nebula {
namespace graph {
folly::Future<Status> DedupExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto* dedup = asNode<Dedup>(node());
  DCHECK(!dedup->inputVar().empty());
  Result result = ectx_->getResult(dedup->inputVar());
  auto* iter = result.iterRef();

  if (UNLIKELY(iter == nullptr)) {
    return Status::Error("Internal Error: iterator is nullptr");
  }

  if (UNLIKELY(iter->isGetNeighborsIter() || iter->isDefaultIter())) {
    auto e = Status::Error("Invalid iterator kind, %d", static_cast<uint16_t>(iter->kind()));
    LOG(ERROR) << e;
    return e;
  }
  std::unordered_set<const Row*> unique;
  unique.reserve(iter->size());
  while (iter->valid()) {
    if (!unique.emplace(iter->row()).second) {
      iter->unstableErase();
    } else {
      iter->next();
    }
  }
  iter->reset();
  return finish(std::move(result));
}

}  // namespace graph
}  // namespace nebula
