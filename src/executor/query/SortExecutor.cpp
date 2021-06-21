/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/SortExecutor.h"
#include "planner/plan/Query.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> SortExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto* sort = asNode<Sort>(node());
    Result result = ectx_->getResult(sort->inputVar());
    auto* iter = result.iterRef();
    if (UNLIKELY(iter == nullptr)) {
        return Status::Error("Internal error: nullptr iterator in sort executor");
    }
    if (UNLIKELY(!iter->isSequentialIter())) {
        std::stringstream ss;
        ss << "Internal error: Sort executor does not supported " << iter->kind();
        LOG(ERROR) << ss.str();
        return Status::Error(ss.str());
    }

    auto &factors = sort->factors();
    auto comparator = [&factors] (const Row &lhs, const Row &rhs) {
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

    auto seqIter = static_cast<SequentialIter*>(iter);
    std::sort(seqIter->begin(), seqIter->end(), comparator);
    return finish(ResultBuilder().value(result.valuePtr()).iter(std::move(result).iter()).finish());
}

}   // namespace graph
}   // namespace nebula
