/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/TopNExecutor.h"
#include "planner/plan/Query.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> TopNExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto* topn = asNode<TopN>(node());
    Result result = ectx_->getResult(topn->inputVar());
    auto* iter = result.iterRef();
    if (UNLIKELY(iter == nullptr)) {
        return Status::Error("Internal error: nullptr iterator in topn executor");
    }
    if (UNLIKELY(!result.iter()->isSequentialIter())) {
        std::stringstream ss;
        ss << "Internal error: Sort executor does not supported " << iter->kind();
        LOG(ERROR) << ss.str();
        return Status::Error(ss.str());
    }

    auto &factors = topn->factors();
    comparator_ = [&factors] (const Row &lhs, const Row &rhs) {
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

    offset_ = topn->offset();
    auto count = topn->count();
    auto size = iter->size();
    maxCount_ = count;
    heapSize_ = 0;
    if (size <= static_cast<size_t>(offset_)) {
        maxCount_ = 0;
    } else if (size > static_cast<size_t>(offset_ + count)) {
        heapSize_ = offset_ + count;
    } else {
        maxCount_ = size - offset_;
        heapSize_ = size;
    }
    if (heapSize_ == 0) {
        iter->clear();
        return finish(ResultBuilder()
            .value(result.valuePtr()).iter(std::move(result).iter()).finish());
    }

    executeTopN<SequentialIter>(iter);
    iter->eraseRange(maxCount_, size);
    return finish(ResultBuilder().value(result.valuePtr()).iter(std::move(result).iter()).finish());
}

template<typename U>
void TopNExecutor::executeTopN(Iterator *iter) {
    auto uIter = static_cast<U*>(iter);
    std::vector<Row> heap(uIter->begin(), uIter->begin()+heapSize_);
    std::make_heap(heap.begin(), heap.end(), comparator_);
    auto it = uIter->begin() + heapSize_;
    while (it != uIter->end()) {
        if (comparator_(*it, heap[0])) {
            std::pop_heap(heap.begin(), heap.end(), comparator_);
            heap.pop_back();
            heap.push_back(*it);
            std::push_heap(heap.begin(), heap.end(), comparator_);
        }
        ++it;
    }
    std::sort_heap(heap.begin(), heap.end(), comparator_);

    auto beg = uIter->begin();
    for (int i = 0; i < maxCount_; ++i) {
        beg[i] = heap[offset_+i];
    }
}

}   // namespace graph
}   // namespace nebula
