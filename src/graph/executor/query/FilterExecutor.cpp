/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/executor/query/FilterExecutor.h"

#include "graph/context/QueryExpressionContext.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> FilterExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto* filter = asNode<Filter>(node());
  Result result = ectx_->getResult(filter->inputVar());
  auto* iter = result.iterRef();
  if (iter == nullptr || iter->isDefaultIter()) {
    auto status = Status::Error("iterator is nullptr or DefaultIter");
    LOG(ERROR) << status;
    return status;
  }

  VLOG(2) << "Get input var: " << filter->inputVar()
          << ", iterator type: " << static_cast<int16_t>(iter->kind())
          << ", input data size: " << iter->size();

  ResultBuilder builder;
  builder.value(result.valuePtr());
  QueryExpressionContext ctx(ectx_);
  auto condition = filter->condition();
  while (iter->valid()) {
    auto val = condition->eval(ctx(iter));
    if (val.isBadNull() || (!val.empty() && !val.isBool() && !val.isNull())) {
      return Status::Error("Wrong type result, the type should be NULL, EMPTY or BOOL");
    }
    if (val.empty() || val.isNull() || !val.getBool()) {
      if (UNLIKELY(filter->needStableFilter())) {
        iter->erase();
      } else {
        iter->unstableErase();
      }
    } else {
      iter->next();
    }
  }

  iter->reset();
  builder.iter(std::move(result).iter());
  return finish(builder.build());
}

}  // namespace graph
}  // namespace nebula
