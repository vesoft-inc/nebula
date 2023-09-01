// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/LeftJoinExecutor.h"

#include <algorithm>

#include "graph/planner/plan/Query.h"
#include "graph/service/GraphFlags.h"

namespace nebula {
namespace graph {
folly::Future<Status> LeftJoinExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto* joinNode = asNode<Join>(node());
  auto& rhsResult =
      ectx_->getVersionedResult(joinNode->rightVar().first, joinNode->rightVar().second);
  rightColSize_ = rhsResult.valuePtr()->getDataSet().colNames.size();
  NG_RETURN_IF_ERROR(checkInputDataSets());
  if (FLAGS_max_job_size <= 1) {
    return join(joinNode->hashKeys(), joinNode->probeKeys(), joinNode->colNames());
  } else {
    return joinMultiJobs(joinNode->hashKeys(), joinNode->probeKeys(), joinNode->colNames());
  }
}

Status LeftJoinExecutor::close() {
  return Executor::close();
}

folly::Future<Status> LeftJoinExecutor::join(const std::vector<Expression*>& hashKeys,
                                             const std::vector<Expression*>& probeKeys,
                                             const std::vector<std::string>& colNames) {
  DCHECK_EQ(hashKeys.size(), probeKeys.size());
  DataSet result;
  if (hashKeys.size() == 1 && probeKeys.size() == 1) {
    std::unordered_map<Value, std::vector<const Row*>> hashTable;
    hashTable.reserve(rhsIter_->empty() ? 1 : rhsIter_->size());
    if (!lhsIter_->empty()) {
      buildSingleKeyHashTable(probeKeys.front(), rhsIter_.get(), hashTable);
      mv_ = movable(node()->inputVars()[0]);
      result = singleKeyProbe(hashKeys.front(), lhsIter_.get(), hashTable);
    }
  } else {
    std::unordered_map<List, std::vector<const Row*>> hashTable;
    hashTable.reserve(rhsIter_->empty() ? 1 : rhsIter_->size());
    if (!lhsIter_->empty()) {
      buildHashTable(probeKeys, rhsIter_.get(), hashTable);
      mv_ = movable(node()->inputVars()[0]);
      result = probe(hashKeys, lhsIter_.get(), hashTable);
    }
  }

  result.colNames = colNames;
  return finish(ResultBuilder().value(Value(std::move(result))).build());
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

    if (mv_) {
      // Probe row only match key in HashTable once, so we could move it directly,
      // key/value in HashTable will be matched multiple times, so we can't move it.
      buildNewRow<List>(hashTable, list, probeIter->moveRow(), ds);
    } else {
      buildNewRow<List>(hashTable, list, *probeIter->row(), ds);
    }
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
    if (mv_) {
      // Probe row only match key in HashTable once, so we could move it directly,
      // key/value in HashTable will be matched multiple times, so we can't move it.
      buildNewRow<Value>(hashTable, val, probeIter->moveRow(), ds);
    } else {
      buildNewRow<Value>(hashTable, val, *probeIter->row(), ds);
    }
  }
  return ds;
}

folly::Future<Status> LeftJoinExecutor::joinMultiJobs(const std::vector<Expression*>& hashKeys,
                                                      const std::vector<Expression*>& probeKeys,
                                                      const std::vector<std::string>& colNames) {
  DCHECK_EQ(hashKeys.size(), probeKeys.size());
  DataSet result;
  if (hashKeys.size() == 1 && probeKeys.size() == 1) {
    hashTable_.reserve(rhsIter_->empty() ? 1 : rhsIter_->size());
    if (!lhsIter_->empty()) {
      buildSingleKeyHashTable(probeKeys.front(), rhsIter_.get(), hashTable_);
      return singleKeyProbe(hashKeys.front(), lhsIter_.get());
    }
  } else {
    listHashTable_.reserve(rhsIter_->empty() ? 1 : rhsIter_->size());
    if (!lhsIter_->empty()) {
      buildHashTable(probeKeys, rhsIter_.get(), listHashTable_);
      return probe(hashKeys, lhsIter_.get());
    }
  }

  result.colNames = colNames;
  return finish(ResultBuilder().value(Value(std::move(result))).build());
}

folly::Future<Status> LeftJoinExecutor::probe(const std::vector<Expression*>& probeKeys,
                                              Iterator* probeIter) {
  auto scatter = [this, probeKeys = probeKeys](
                     size_t begin, size_t end, Iterator* tmpIter) -> StatusOr<DataSet> {
    std::vector<Expression*> tmpProbeKeys;
    std::for_each(probeKeys.begin(), probeKeys.end(), [&tmpProbeKeys](auto& e) {
      tmpProbeKeys.emplace_back(e->clone());
    });
    DataSet ds;
    QueryExpressionContext ctx(ectx_);
    ds.rows.reserve(end - begin);
    for (; tmpIter->valid() && begin++ < end; tmpIter->next()) {
      List list;
      list.values.reserve(tmpProbeKeys.size());
      for (auto& col : tmpProbeKeys) {
        Value val = col->eval(ctx(tmpIter));
        list.values.emplace_back(std::move(val));
      }

      buildNewRow<List>(listHashTable_, list, *tmpIter->row(), ds);
    }
    return ds;
  };

  auto gather = [this](std::vector<folly::Try<StatusOr<DataSet>>>&& results) mutable -> Status {
    memory::MemoryCheckGuard guard;
    DataSet result;
    auto* joinNode = asNode<Join>(node());
    result.colNames = joinNode->colNames();
    for (auto& respVal : results) {
      if (respVal.hasException()) {
        auto ex = respVal.exception().get_exception<std::bad_alloc>();
        if (ex) {
          throw std::bad_alloc();
        } else {
          throw std::runtime_error(respVal.exception().what().c_str());
        }
      }
      auto res = std::move(respVal).value();
      auto&& rows = std::move(res).value();
      result.rows.insert(result.rows.end(),
                         std::make_move_iterator(rows.begin()),
                         std::make_move_iterator(rows.end()));
    }
    return finish(ResultBuilder().value(Value(std::move(result))).build());
  };

  return runMultiJobs(std::move(scatter), std::move(gather), probeIter);
}

folly::Future<Status> LeftJoinExecutor::singleKeyProbe(Expression* probeKey, Iterator* probeIter) {
  auto scatter = [this, probeKey](
                     size_t begin, size_t end, Iterator* tmpIter) -> StatusOr<DataSet> {
    auto tmpProbeKey = probeKey->clone();
    DataSet ds;
    QueryExpressionContext ctx(ectx_);
    ds.rows.reserve(end - begin);
    for (; tmpIter->valid() && begin++ < end; tmpIter->next()) {
      auto& val = tmpProbeKey->eval(ctx(tmpIter));
      buildNewRow<Value>(hashTable_, val, *tmpIter->row(), ds);
    }
    return ds;
  };

  auto gather = [this](std::vector<folly::Try<StatusOr<DataSet>>>&& results) mutable -> Status {
    memory::MemoryCheckGuard guard;
    DataSet result;
    auto* joinNode = asNode<Join>(node());
    result.colNames = joinNode->colNames();
    for (auto& respVal : results) {
      if (respVal.hasException()) {
        auto ex = respVal.exception().get_exception<std::bad_alloc>();
        if (ex) {
          throw std::bad_alloc();
        } else {
          throw std::runtime_error(respVal.exception().what().c_str());
        }
      }
      auto res = std::move(respVal).value();
      auto&& rows = std::move(res).value();
      result.rows.insert(result.rows.end(),
                         std::make_move_iterator(rows.begin()),
                         std::make_move_iterator(rows.end()));
    }
    finish(ResultBuilder().value(Value(std::move(result))).build());
    return Status::OK();
  };

  return runMultiJobs(std::move(scatter), std::move(gather), probeIter);
}

template <class T>
void LeftJoinExecutor::buildNewRow(const std::unordered_map<T, std::vector<const Row*>>& hashTable,
                                   const T& val,
                                   Row lRow,
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
    values.insert(values.end(), colSize_ - lRowSize, Value::kNullValue);
    ds.rows.emplace_back(std::move(newRow));
  } else {
    for (std::size_t i = 0; i < (range->second.size() - 1); ++i) {
      ds.rows.emplace_back(newRow(lRow, *range->second[i]));
    }
    // Move probe row in last new row creating
    ds.rows.emplace_back(newRow(std::move(lRow), *range->second.back()));
  }
}

HashLeftJoinExecutor::HashLeftJoinExecutor(const PlanNode* node, QueryContext* qctx)
    : LeftJoinExecutor(node, qctx) {
  name_ = "HashLeftJoinExecutor";
}

folly::Future<Status> HashLeftJoinExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto* joinNode = asNode<HashJoin>(node());
  auto& rhsResult = ectx_->getResult(joinNode->rightInputVar());
  rightColSize_ = rhsResult.valuePtr()->getDataSet().colNames.size();
  NG_RETURN_IF_ERROR(checkBiInputDataSets());
  return join(joinNode->hashKeys(), joinNode->probeKeys(), joinNode->colNames());
}
}  // namespace graph
}  // namespace nebula
