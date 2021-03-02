/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/DataJoinExecutor.h"

#include "planner/Query.h"
#include "context/QueryExpressionContext.h"
#include "context/Iterator.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {
folly::Future<Status> DataJoinExecutor::execute() {
    return doInnerJoin();
}

Status DataJoinExecutor::close() {
    exchange_ = false;
    hashTable_.clear();
    return Executor::close();
}

folly::Future<Status> DataJoinExecutor::doInnerJoin() {
    SCOPED_TIMER(&execTime_);

    auto* dataJoin = asNode<DataJoin>(node());
    auto colNames = dataJoin->colNames();
    auto lhsIter = ectx_
                       ->getVersionedResult(dataJoin->leftVar().first,
                                            dataJoin->leftVar().second)
                       .iter();
    DCHECK(!!lhsIter);
    if (lhsIter->isGetNeighborsIter() || lhsIter->isDefaultIter()) {
        std::stringstream ss;
        ss << "Join executor does not support " << lhsIter->kind();
        return error(Status::Error(ss.str()));
    }
    auto rhsIter = ectx_
                       ->getVersionedResult(dataJoin->rightVar().first,
                                            dataJoin->rightVar().second)
                       .iter();
    DCHECK(!!rhsIter);
    if (lhsIter->isGetNeighborsIter() || lhsIter->isDefaultIter()) {
        std::stringstream ss;
        ss << "Join executor does not support " << lhsIter->kind();
        return error(Status::Error(ss.str()));
    }

    auto resultIter = std::make_unique<JoinIter>(std::move(colNames));
    resultIter->joinIndex(lhsIter.get(), rhsIter.get());
    auto bucketSize =
        lhsIter->size() > rhsIter->size() ? rhsIter->size() : lhsIter->size();
    hashTable_.reserve(bucketSize);
    resultIter->reserve(lhsIter->size() > rhsIter->size() ? lhsIter->size() : rhsIter->size());

    if (!(lhsIter->empty() || rhsIter->empty())) {
        if (lhsIter->size() < rhsIter->size()) {
            buildHashTable(dataJoin->hashKeys(), lhsIter.get());
            probe(dataJoin->probeKeys(), rhsIter.get(), resultIter.get());
        } else {
            exchange_ = true;
            buildHashTable(dataJoin->probeKeys(), rhsIter.get());
            probe(dataJoin->hashKeys(), lhsIter.get(), resultIter.get());
        }
    }
    return finish(ResultBuilder().iter(std::move(resultIter)).finish());
}

void DataJoinExecutor::buildHashTable(const std::vector<Expression*>& hashKeys,
                                      Iterator* iter) {
    QueryExpressionContext ctx(ectx_);
    for (; iter->valid(); iter->next()) {
        List list;
        list.values.reserve(hashKeys.size());
        for (auto& col : hashKeys) {
            Value val = col->eval(ctx(iter));
            list.values.emplace_back(std::move(val));
        }

        auto& vals = hashTable_[list];
        vals.emplace_back(iter->row());
    }
}

void DataJoinExecutor::probe(const std::vector<Expression*>& probeKeys,
                             Iterator* probeIter, JoinIter* resultIter) {
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
            JoinIter::JoinLogicalRow newRow(std::move(values), size,
                                        &resultIter->getColIdxIndices());
            resultIter->addRow(std::move(newRow));
        }
    }
}
}  // namespace graph
}  // namespace nebula
