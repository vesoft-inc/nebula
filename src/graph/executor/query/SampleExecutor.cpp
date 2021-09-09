/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/executor/query/SampleExecutor.h"

#include "graph/planner/plan/Query.h"
#include "graph/util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> SampleExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto* sample = asNode<Sample>(node());
  Result result = ectx_->getResult(sample->inputVar());
  auto* iter = result.iterRef();
  DCHECK_NE(iter->kind(), Iterator::Kind::kDefault);
  DCHECK_NE(iter->kind(), Iterator::Kind::kGetNeighbors);
  ResultBuilder builder;
  builder.value(result.valuePtr());
  auto count = sample->count();
  auto size = iter->size();
  if (size > static_cast<std::size_t>(count)) {
    // Sampling
    iter->sample(count);
  }
  builder.iter(std::move(result).iter());
  return finish(builder.build());
}

}  // namespace graph
}  // namespace nebula
