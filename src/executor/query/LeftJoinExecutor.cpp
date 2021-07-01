/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/LeftJoinExecutor.h"

#include "context/Iterator.h"
#include "context/QueryExpressionContext.h"
#include "planner/plan/Query.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {
folly::Future<Status> LeftJoinExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    NG_RETURN_IF_ERROR(checkInputDataSets());
    return join();
}

Status LeftJoinExecutor::close() {
    return Executor::close();
}

folly::Future<Status> LeftJoinExecutor::join() {
    auto* join = asNode<Join>(node());
    auto& rhsResult = ectx_->getVersionedResult(join->rightVar().first, join->rightVar().second);
    rightColSize_ = rhsResult.valuePtr()->getDataSet().colNames.size();

    auto& hashKeys = join->hashKeys();
    auto& probeKeys = join->probeKeys();
    DCHECK_EQ(hashKeys.size(), probeKeys.size());
    DataSet result;

    if (hashKeys.size() == 1 && probeKeys.size() == 1) {
        std::unordered_map<Value, std::vector<const Row*>> hashTable;
        hashTable.reserve(rhsIter_->size() == 0 ? 1 : rhsIter_->size());
        if (!lhsIter_->empty()) {
            buildSingleKeyHashTable(join->probeKeys().front(), rhsIter_.get(), hashTable);
            result = singleKeyProbe(join->hashKeys().front(), lhsIter_.get(), hashTable);
        }
    } else {
        std::unordered_map<List, std::vector<const Row*>> hashTable;
        hashTable.reserve(rhsIter_->size() == 0 ? 1 : rhsIter_->size());
        if (!lhsIter_->empty()) {
            buildHashTable(join->probeKeys(), rhsIter_.get(), hashTable);
            result = probe(join->hashKeys(), lhsIter_.get(), hashTable);
        }
    }

    result.colNames = join->colNames();
    VLOG(2) << node_->toString() << ", result: " << result;
    return finish(ResultBuilder().value(Value(std::move(result))).finish());
}

DataSet LeftJoinExecutor::probe(
    const std::vector<Expression*>& probeKeys,
    Iterator* probeIter,
    const std::unordered_map<List, std::vector<const Row*>>& hashTable) const {
    DataSet ds;
    ds.rows.reserve(probeIter->size());
    QueryExpressionContext ctx(ectx_);
    for (; probeIter->valid(); probeIter->next()) {
        List list;
        list.values.reserve(probeKeys.size());
        for (auto& col : probeKeys) {
            Value val = col->eval(ctx(probeIter));
            list.values.emplace_back(std::move(val));
        }

        buildNewRow<List>(hashTable, list, *probeIter->row(), ds);
    }
    return ds;
}

DataSet LeftJoinExecutor::singleKeyProbe(
    Expression* probeKey,
    Iterator* probeIter,
    const std::unordered_map<Value, std::vector<const Row*>>& hashTable) const {
    DataSet ds;
    ds.rows.reserve(probeIter->size());
    QueryExpressionContext ctx(ectx_);
    for (; probeIter->valid(); probeIter->next()) {
        auto& val = probeKey->eval(ctx(probeIter));
        buildNewRow<Value>(hashTable, val, *probeIter->row(), ds);
    }
    return ds;
}

template <class T>
void LeftJoinExecutor::buildNewRow(const std::unordered_map<T, std::vector<const Row*>>& hashTable,
                                   const T& val,
                                   const Row& lRow,
                                   DataSet& ds) const {
    auto range = hashTable.find(val);
    if (range == hashTable.end()) {
        auto lRowSize = lRow.size();
        Row newRow;
        newRow.reserve(colSize_);
        auto& values = newRow.values;
        values.insert(values.end(),
                std::make_move_iterator(lRow.values.begin()),
                std::make_move_iterator(lRow.values.end()));
        values.insert(values.end(), colSize_ - lRowSize, Value::kEmpty);
        ds.rows.emplace_back(std::move(newRow));
    } else {
        for (auto* row : range->second) {
            auto& rRow = *row;
            Row newRow;
            auto& values = newRow.values;
            values.reserve(lRow.size() + rRow.size());
            values.insert(values.end(),
                    std::make_move_iterator(lRow.values.begin()),
                    std::make_move_iterator(lRow.values.end()));
            values.insert(values.end(), rRow.values.begin(), rRow.values.end());
            ds.rows.emplace_back(std::move(newRow));
        }
    }
}

}   // namespace graph
}   // namespace nebula
