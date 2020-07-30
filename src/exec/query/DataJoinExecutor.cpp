/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/DataJoinExecutor.h"

#include "planner/Query.h"
#include "context/QueryExpressionContext.h"
#include "context/Iterator.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {
folly::Future<Status> DataJoinExecutor::execute() {
    return doInnerJoin().ensure([this]() {
        exchange_ = false;
    });
}

folly::Future<Status> DataJoinExecutor::doInnerJoin() {
    SCOPED_TIMER(&execTime_);

    auto* dataJoin = asNode<DataJoin>(node());

    VLOG(1) << "lhs hist: " << ectx_->getHistory(dataJoin->leftVar().first).size();
    VLOG(1) << "rhs hist: " << ectx_->getHistory(dataJoin->rightVar().first).size();
    auto lhsIter = ectx_
                       ->getVersionedResult(dataJoin->leftVar().first,
                                            dataJoin->leftVar().second)
                       .iter();
    DCHECK(!!lhsIter);
    VLOG(1) << "lhs: " << dataJoin->leftVar().first << " " << lhsIter->size();
    if (!lhsIter->isSequentialIter() && !lhsIter->isJoinIter()) {
        std::stringstream ss;
        ss << "Join executor does not support " << lhsIter->kind();
        return error(Status::Error(ss.str()));
    }
    auto rhsIter = ectx_
                       ->getVersionedResult(dataJoin->rightVar().first,
                                            dataJoin->rightVar().second)
                       .iter();
    DCHECK(!!rhsIter);
    VLOG(1) << "rhs: " << dataJoin->rightVar().first << " " << rhsIter->size();
    if (!rhsIter->isSequentialIter() && !rhsIter->isJoinIter()) {
        std::stringstream ss;
        ss << "Join executor does not support " << lhsIter->kind();
        return error(Status::Error(ss.str()));
    }

    auto resultIter = std::make_unique<JoinIter>();
    resultIter->joinIndex(lhsIter.get(), rhsIter.get());
    auto bucketSize =
        lhsIter->size() > rhsIter->size() ? rhsIter->size() : lhsIter->size();
    hashTable_ = std::make_unique<HashTable>(bucketSize);

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
    QueryExpressionContext ctx(ectx_, iter);
    for (; iter->valid(); iter->next()) {
        List list;
        list.values.reserve(hashKeys.size());
        for (auto& col : hashKeys) {
            Value val = col->eval(ctx);
            list.values.emplace_back(std::move(val));
        }

        VLOG(1) << "key: " << list;
        hashTable_->add(std::move(list), iter->row());
    }
}

void DataJoinExecutor::probe(const std::vector<Expression*>& probeKeys,
                             Iterator* probeIter, JoinIter* resultIter) {
    QueryExpressionContext ctx(ectx_, probeIter);
    for (; probeIter->valid(); probeIter->next()) {
        List list;
        list.values.reserve(probeKeys.size());
        for (auto& col : probeKeys) {
            Value val = col->eval(ctx);
            list.values.emplace_back(std::move(val));
        }

        VLOG(1) << "probe: " << list;
        auto range = hashTable_->get(list);
        for (auto i = range.first; i != range.second; ++i) {
            auto row = i->second;
            std::vector<const Row*> values;
            auto lSegs = row->segments();
            auto rSegs = probeIter->row()->segments();
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
            VLOG(1) << node()->varName() << " : " << newRow;
            resultIter->addRow(std::move(newRow));
        }
    }
}
}  // namespace graph
}  // namespace nebula
