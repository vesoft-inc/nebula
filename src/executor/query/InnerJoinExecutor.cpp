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
    auto bucketSize = lhsIter_->size() > rhsIter_->size() ? rhsIter_->size() : lhsIter_->size();
    hashTable_.reserve(bucketSize);
    DataSet result;
    if (!(lhsIter_->empty() || rhsIter_->empty())) {
        if (lhsIter_->size() < rhsIter_->size()) {
            buildHashTable(join->hashKeys(), lhsIter_.get());
            result = probe(join->probeKeys(), rhsIter_.get());
        } else {
            exchange_ = true;
            buildHashTable(join->probeKeys(), rhsIter_.get());
            result = probe(join->hashKeys(), lhsIter_.get());
        }
    }
    result.colNames = join->colNames();
    VLOG(1) << result;
    return finish(ResultBuilder().value(Value(std::move(result))).finish());
}

DataSet InnerJoinExecutor::probe(const std::vector<Expression*>& probeKeys,
                             Iterator* probeIter) {
    DataSet ds;
    QueryExpressionContext ctx(ectx_);
    for (; probeIter->valid(); probeIter->next()) {
        List list;
        list.values.reserve(probeKeys.size());
        for (auto& col : probeKeys) {
            Value val = col->eval(ctx(probeIter));
            list.values.emplace_back(std::move(val));
        }

        const auto& range = hashTable_.find(list);
        if (range == hashTable_.end()) {
            continue;
        }
        for (auto* row : range->second) {
            auto& lRow = *row;
            auto& rRow = *probeIter->row();
            VLOG(1) << lRow << rRow;
            Row newRow;
            newRow.reserve(lRow.size() + rRow.size());
            auto& values = newRow.values;
            if (exchange_) {
                values.insert(values.end(), rRow.values.begin(), rRow.values.end());
                values.insert(values.end(), lRow.values.begin(), lRow.values.end());
            } else {
                values.insert(values.end(), lRow.values.begin(), lRow.values.end());
                values.insert(values.end(), rRow.values.begin(), rRow.values.end());
            }
            VLOG(1) << "Row: " << newRow;
            ds.rows.emplace_back(std::move(newRow));
        }
    }
    return ds;
}
}   // namespace graph
}   // namespace nebula
