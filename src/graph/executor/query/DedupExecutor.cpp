/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/DedupExecutor.h"
#include "planner/plan/Query.h"
#include "context/QueryExpressionContext.h"
#include "util/ScopedTimer.h"

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
        auto e = Status::Error("Invalid iterator kind, %d",
                               static_cast<uint16_t>(iter->kind()));
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

}   // namespace graph
}   // namespace nebula
