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
    auto& rhsResult = ectx_->getVersionedResult(join->rightVar().first, join->rightVar().second);
    rightColSize_ = rhsResult.valuePtr()->getDataSet().colNames.size();

    hashTable_.reserve(rhsIter_->size() == 0 ? 1 : rhsIter_->size());
    DataSet result;
    if (!lhsIter_->empty()) {
        buildHashTable(join->probeKeys(), rhsIter_.get());
        result = probe(join->hashKeys(), lhsIter_.get());
    }
    result.colNames = join->colNames();
    return finish(ResultBuilder().value(Value(std::move(result))).finish());
}

DataSet LeftJoinExecutor::probe(const std::vector<Expression*>& probeKeys,
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

        auto range = hashTable_.find(list);
        if (range == hashTable_.end()) {
            auto& lRow = *probeIter->row();
            auto lRowSize = lRow.size();
            Row newRow;
            newRow.reserve(colSize_);
            auto& values = newRow.values;
            values.insert(values.end(), lRow.values.begin(), lRow.values.end());
            values.insert(values.end(), colSize_ - lRowSize, Value::kEmpty);
            ds.rows.emplace_back(std::move(newRow));
        } else {
            for (auto* row : range->second) {
                auto& lRow = *probeIter->row();
                auto& rRow = *row;
                Row newRow;
                auto& values = newRow.values;
                values.reserve(lRow.size() + rRow.size());
                values.insert(values.end(), lRow.values.begin(), lRow.values.end());
                values.insert(values.end(), rRow.values.begin(), rRow.values.end());
                ds.rows.emplace_back(std::move(newRow));
            }
        }
    }
    return ds;
}
}   // namespace graph
}   // namespace nebula
