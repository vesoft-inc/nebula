/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/LimitExecutor.h"
#include "planner/plan/Query.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> LimitExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto* limit = asNode<Limit>(node());
    Result result = ectx_->getResult(limit->inputVar());
    auto* iter = result.iterRef();
    ResultBuilder builder;
    builder.value(result.valuePtr());
    auto offset = limit->offset();
    auto count = limit->count();
    auto size = iter->size();
    if (size <= static_cast<size_t>(offset)) {
        iter->clear();
    } else if (size > static_cast<size_t>(offset + count)) {
        iter->eraseRange(0, offset);
        iter->eraseRange(count, size - offset);
    } else if (size > static_cast<size_t>(offset) &&
               size <= static_cast<size_t>(offset + count)) {
        iter->eraseRange(0, offset);
    }
    builder.iter(std::move(result).iter());
    return finish(builder.finish());
}

}   // namespace graph
}   // namespace nebula
