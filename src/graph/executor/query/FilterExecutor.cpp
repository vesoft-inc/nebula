/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/query/FilterExecutor.h"

#include <folly/Likely.h>  // for UNLIKELY
#include <stdint.h>        // for int16_t

#include <string>       // for operator<<, char_t...
#include <type_traits>  // for remove_reference<>...
#include <utility>      // for move

#include "common/base/Logging.h"                   // for LogMessage, COMPAC...
#include "common/base/Status.h"                    // for Status, operator<<
#include "common/datatypes/Value.h"                // for Value
#include "common/expression/Expression.h"          // for Expression
#include "common/time/ScopedTimer.h"               // for SCOPED_TIMER
#include "graph/context/ExecutionContext.h"        // for ExecutionContext
#include "graph/context/Iterator.h"                // for Iterator
#include "graph/context/QueryExpressionContext.h"  // for QueryExpressionCon...
#include "graph/context/Result.h"                  // for ResultBuilder, Result
#include "graph/planner/plan/Query.h"              // for Filter

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
          << ", iterator type: " << static_cast<int16_t>(iter->kind());

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
