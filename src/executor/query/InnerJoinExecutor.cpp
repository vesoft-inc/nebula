/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/InnerJoinExecutor.h"

#include "context/Iterator.h"
#include "context/QueryExpressionContext.h"
#include "planner/Query.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {
folly::Future<Status> InnerJoinExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    NG_RETURN_IF_ERROR(checkInputDataSets());
    return join();
}

Status InnerJoinExecutor::close() {
    exchange_ = false;
    hashTable_.clear();
    return Executor::close();
}

folly::Future<Status> InnerJoinExecutor::join() {
    auto* join = asNode<Join>(node());
    auto lhsIter = ectx_->getVersionedResult(join->leftVar().first, join->leftVar().second).iter();
    auto rhsIter =
        ectx_->getVersionedResult(join->rightVar().first, join->rightVar().second).iter();

    auto resultIter = std::make_unique<JoinIter>(join->colNames());
    resultIter->joinIndex(lhsIter.get(), rhsIter.get());
    auto bucketSize = lhsIter->size() > rhsIter->size() ? rhsIter->size() : lhsIter->size();
    hashTable_.reserve(bucketSize);
    resultIter->reserve(lhsIter->size() > rhsIter->size() ? lhsIter->size() : rhsIter->size());

    if (!(lhsIter->empty() || rhsIter->empty())) {
        if (lhsIter->size() < rhsIter->size()) {
            buildHashTable(join->hashKeys(), lhsIter.get());
            probe(join->probeKeys(), rhsIter.get(), resultIter.get());
        } else {
            exchange_ = true;
            buildHashTable(join->probeKeys(), rhsIter.get());
            probe(join->hashKeys(), lhsIter.get(), resultIter.get());
        }
    }
    return finish(ResultBuilder().iter(std::move(resultIter)).finish());
}

void InnerJoinExecutor::probe(const std::vector<Expression*>& probeKeys,
                              Iterator* probeIter,
                              JoinIter* resultIter) {
    QueryExpressionContext ctx(ectx_);
    for (; probeIter->valid(); probeIter->next()) {
        List list;
        list.values.reserve(probeKeys.size());
        for (auto& col : probeKeys) {
            Value val = col->eval(ctx(probeIter));
            list.values.emplace_back(std::move(val));
        }

        auto range = hashTable_.find(list);
        if (range == hashTable_.end()) {
            continue;
        }
        for (auto* row : range->second) {
            std::vector<const Row*> values;
            auto& lSegs = row->segments();
            auto& rSegs = probeIter->row()->segments();
            values.reserve(lSegs.size() + rSegs.size());
            if (exchange_) {
                values.insert(values.end(), rSegs.begin(), rSegs.end());
                values.insert(values.end(), lSegs.begin(), lSegs.end());
            } else {
                values.insert(values.end(), lSegs.begin(), lSegs.end());
                values.insert(values.end(), rSegs.begin(), rSegs.end());
            }
            size_t size = row->size() + probeIter->row()->size();
            JoinIter::JoinLogicalRow newRow(
                std::move(values), size, &resultIter->getColIdxIndices());
            VLOG(1) << node()->outputVar() << " : " << newRow;
            resultIter->addRow(std::move(newRow));
        }
    }
}
}   // namespace graph
}   // namespace nebula
