/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/LimitExecutor.h"
#include "context/ExpressionContextImpl.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

folly::Future<Status> LimitExecutor::execute() {
    dumpLog();
    auto* limit = asNode<Limit>(node());
    auto iter = ectx_->getResult(limit->inputVar()).iter();
    auto result = ExecResult::buildDefault(iter->valuePtr());
    ExpressionContextImpl ctx(ectx_, iter.get());
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
    result.setIter(std::move(iter));
    return finish(std::move(result));
}

}   // namespace graph
}   // namespace nebula
