/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/LeftJoinExecutor.h"

#include "context/Iterator.h"
#include "context/QueryExpressionContext.h"
#include "planner/Query.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {
folly::Future<Status> LeftJoinExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    NG_RETURN_IF_ERROR(checkInputDataSets());
    return join();
}

Status LeftJoinExecutor::close() {
    hashTable_.clear();
    return Executor::close();
}

folly::Future<Status> LeftJoinExecutor::join() {
    auto* join = asNode<Join>(node());
    auto lhsIter = ectx_->getVersionedResult(join->leftVar().first, join->leftVar().second).iter();
    auto& rhsResult = ectx_->getVersionedResult(join->rightVar().first, join->rightVar().second);
    rightColSize_ = rhsResult.valuePtr()->getDataSet().colNames.size();
    auto rhsIter = rhsResult.iter();

    auto resultIter = std::make_unique<JoinIter>(join->colNames());
    resultIter->joinIndex(lhsIter.get(), rhsIter.get());
    hashTable_.reserve(rhsIter->size() == 0 ? 1 : rhsIter->size());
    resultIter->reserve(lhsIter->size());
    if (!lhsIter->empty()) {
        buildHashTable(join->probeKeys(), rhsIter.get());
        probe(join->hashKeys(), lhsIter.get(), resultIter.get());
    }
    return finish(ResultBuilder().iter(std::move(resultIter)).finish());
}

void LeftJoinExecutor::probe(const std::vector<Expression*>& probeKeys,
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
            std::vector<const Row*> values;
            auto& lSegs = probeIter->row()->segments();
            values.reserve(lSegs.size() + 1);
            values.insert(values.end(), lSegs.begin(), lSegs.end());
            Row* emptyRow = qctx_->objPool()->add(new List(std::vector<Value>(rightColSize_)));
            values.insert(values.end(), emptyRow);
            size_t size = probeIter->row()->size() + rightColSize_;
            JoinIter::JoinLogicalRow newRow(
                std::move(values), size, &resultIter->getColIdxIndices());
            VLOG(1) << node()->outputVar() << " : " << newRow;
            resultIter->addRow(std::move(newRow));
        } else {
            for (auto* row : range->second) {
                std::vector<const Row*> values;
                auto& lSegs = probeIter->row()->segments();
                auto& rSegs = row->segments();
                values.reserve(lSegs.size() + rSegs.size());
                values.insert(values.end(), lSegs.begin(), lSegs.end());
                values.insert(values.end(), rSegs.begin(), rSegs.end());
                size_t size = row->size() + probeIter->row()->size();
                JoinIter::JoinLogicalRow newRow(
                    std::move(values), size, &resultIter->getColIdxIndices());
                VLOG(1) << node()->outputVar() << " : " << newRow;
                resultIter->addRow(std::move(newRow));
            }
        }
    }
}
}   // namespace graph
}   // namespace nebula
