/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/InnerJoinExecutor.h"

#include "context/Iterator.h"
#include "context/QueryExpressionContext.h"
#include "planner/plan/Query.h"
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
    return Executor::close();
}

folly::Future<Status> InnerJoinExecutor::join() {
    auto* join = asNode<Join>(node());
    auto bucketSize = lhsIter_->size() > rhsIter_->size() ? rhsIter_->size() : lhsIter_->size();

    auto& hashKeys = join->hashKeys();
    auto& probeKeys = join->probeKeys();
    DCHECK_EQ(hashKeys.size(), probeKeys.size());
    DataSet result;

    if (lhsIter_->empty() || rhsIter_->empty()) {
        result.colNames = join->colNames();
        return finish(ResultBuilder().value(Value(std::move(result))).finish());
    }

    if (hashKeys.size() == 1 && probeKeys.size() == 1) {
        std::unordered_map<Value, std::vector<const Row*>> hashTable;
        hashTable.reserve(bucketSize);
        if (lhsIter_->size() < rhsIter_->size()) {
            buildSingleKeyHashTable(hashKeys.front(), lhsIter_.get(), hashTable);
            result = singleKeyProbe(probeKeys.front(), rhsIter_.get(), hashTable);
        } else {
            exchange_ = true;
            buildSingleKeyHashTable(probeKeys.front(), rhsIter_.get(), hashTable);
            result = singleKeyProbe(hashKeys.front(), lhsIter_.get(), hashTable);
        }
    } else {
        std::unordered_map<List, std::vector<const Row*>> hashTable;
        hashTable.reserve(bucketSize);
        if (lhsIter_->size() < rhsIter_->size()) {
            buildHashTable(join->hashKeys(), lhsIter_.get(), hashTable);
            result = probe(join->probeKeys(), rhsIter_.get(), hashTable);
        } else {
            exchange_ = true;
            buildHashTable(join->probeKeys(), rhsIter_.get(), hashTable);
            result = probe(join->hashKeys(), lhsIter_.get(), hashTable);
        }
    }
    result.colNames = join->colNames();
    return finish(ResultBuilder().value(Value(std::move(result))).finish());
}

DataSet InnerJoinExecutor::probe(
    const std::vector<Expression*>& probeKeys,
    Iterator* probeIter,
    const std::unordered_map<List, std::vector<const Row*>>& hashTable) const {
    DataSet ds;
    QueryExpressionContext ctx(ectx_);
    ds.rows.reserve(probeIter->size());
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

DataSet InnerJoinExecutor::singleKeyProbe(
    Expression* probeKey,
    Iterator* probeIter,
    const std::unordered_map<Value, std::vector<const Row*>>& hashTable) const {
    DataSet ds;
    QueryExpressionContext ctx(ectx_);
    for (; probeIter->valid(); probeIter->next()) {
        auto& val = probeKey->eval(ctx(probeIter));
        buildNewRow<Value>(hashTable, val, *probeIter->row(), ds);
    }
    return ds;
}

template <class T>
void InnerJoinExecutor::buildNewRow(const std::unordered_map<T, std::vector<const Row*>>& hashTable,
                                    const T& val,
                                    const Row& rRow,
                                    DataSet& ds) const {
    const auto& range = hashTable.find(val);
    if (range == hashTable.end()) {
        return;
    }
    for (auto* row : range->second) {
        auto& lRow = *row;
        Row newRow;
        newRow.reserve(lRow.size() + rRow.size());
        auto& values = newRow.values;
        if (exchange_) {
            values.insert(values.end(),
                    std::make_move_iterator(rRow.values.begin()),
                    std::make_move_iterator(rRow.values.end()));
            values.insert(values.end(), lRow.values.begin(), lRow.values.end());
        } else {
            values.insert(values.end(), lRow.values.begin(), lRow.values.end());
            values.insert(values.end(),
                    std::make_move_iterator(rRow.values.begin()),
                    std::make_move_iterator(rRow.values.end()));
        }
        ds.rows.emplace_back(std::move(newRow));
    }
}
}   // namespace graph
}   // namespace nebula
