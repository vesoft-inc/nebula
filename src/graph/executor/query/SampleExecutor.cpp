/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/query/SampleExecutor.h"

#include <cstddef>      // for size_t
#include <type_traits>  // for remove_reference<>...
#include <utility>      // for move

#include "common/base/Logging.h"                   // for GetReferenceableValue
#include "common/base/Status.h"                    // for Status
#include "common/time/ScopedTimer.h"               // for SCOPED_TIMER
#include "graph/context/ExecutionContext.h"        // for ExecutionContext
#include "graph/context/Iterator.h"                // for Iterator, Iterator...
#include "graph/context/QueryExpressionContext.h"  // for QueryExpressionCon...
#include "graph/context/Result.h"                  // for ResultBuilder, Result
#include "graph/planner/plan/Query.h"              // for Sample

namespace nebula {
namespace graph {

folly::Future<Status> SampleExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto* sample = asNode<Sample>(node());
  Result result = ectx_->getResult(sample->inputVar());
  auto* iter = result.iterRef();
  DCHECK_NE(iter->kind(), Iterator::Kind::kDefault);
  ResultBuilder builder;
  builder.value(result.valuePtr());
  QueryExpressionContext qec(ectx_);
  auto count = sample->count(qec);
  if (iter->kind() == Iterator::Kind::kGetNeighbors ||
      iter->size() > static_cast<std::size_t>(count)) {
    // Sampling
    iter->sample(count);
  }
  builder.iter(std::move(result).iter());
  return finish(builder.build());
}

}  // namespace graph
}  // namespace nebula
