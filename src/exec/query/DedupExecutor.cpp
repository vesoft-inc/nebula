/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/DedupExecutor.h"
#include "planner/Query.h"
#include "context/ExpressionContextImpl.h"

namespace nebula {
namespace graph {
folly::Future<Status> DedupExecutor::execute() {
    dumpLog();
    auto* dedup = asNode<Dedup>(node());
    auto iter = ectx_->getResult(dedup->inputVar()).iter();

    if (iter == nullptr) {
        return Status::Error("Internal Error: iterator is nullptr");
    }

    if (iter->isGetNeighborsIter()) {
        LOG(INFO) << "Invalid iterator kind: " << static_cast<uint16_t>(iter->kind());
        return Status::Error("Invalid iterator kind, %d", static_cast<uint16_t>(iter->kind()));
    }
    auto result = ExecResult::buildDefault(iter->valuePtr());
    ExpressionContextImpl ctx(ectx_, iter.get());
    std::unordered_set<const Row *> unique;
    while (iter->valid()) {
        if (unique.find(iter->row()) != unique.end()) {
            iter->erase();
        } else {
            unique.emplace(iter->row());
            iter->next();
        }
    }
    iter->reset();
    result.setIter(std::move(iter));
    return finish(std::move(result));
}

}   // namespace graph
}   // namespace nebula
