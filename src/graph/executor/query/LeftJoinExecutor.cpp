// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/LeftJoinExecutor.h"

#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {
folly::Future<Status> LeftJoinExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto* joinNode = asNode<Join>(node());
  auto& rhsResult =
      ectx_->getVersionedResult(joinNode->rightVar().first, joinNode->rightVar().second);
  rightColSize_ = rhsResult.valuePtr()->getDataSet().colNames.size();
  NG_RETURN_IF_ERROR(checkInputDataSets());
  return join(joinNode->hashKeys(), joinNode->probeKeys(), joinNode->colNames());
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
    hashTable_.reserve(rhsIter_->empty() ? 1 : rhsIter_->size());
    if (!lhsIter_->empty()) {
      buildSingleKeyHashTable(probeKeys.front(), rhsIter_.get(), hashTable_);
      return singleKeyProbe(hashKeys.front(), lhsIter_.get(), hashTable_);
    }
  } else {
    hashTable_.reserve(rhsIter_->empty() ? 1 : rhsIter_->size());
    if (!lhsIter_->empty()) {
      buildHashTable(probeKeys, rhsIter_.get(), hashTable_);
      return probe(hashKeys, lhsIter_.get(), hashTable_);
    }
  }

  result.colNames = colNames;
  return finish(ResultBuilder().value(Value(std::move(result))).build());
}

folly::Future<Status> LeftJoinExecutor::probe(
    const std::vector<Expression*>& probeKeys,
    Iterator* probeIter,
    const std::unordered_map<Value, std::vector<const Row*>>& hashTable) {
  auto scatter = [this, tmpProbeKeys = probeKeys, tmpHashTable = &hashTable](
                     size_t begin, size_t end, Iterator* tmpIter) -> StatusOr<DataSet> {
    // Iterates to the begin pos
    size_t tmp = 0;
    for (; tmpIter->valid() && tmp < begin; ++tmp) {
      tmpIter->next();
    }

    DataSet ds;
    QueryExpressionContext ctx(ectx_);
    ds.rows.reserve(end - begin);
    for (; tmpIter->valid() && tmp++ < end; tmpIter->next()) {
      List list;
      list.values.reserve(tmpProbeKeys.size());
      for (auto& col : tmpProbeKeys) {
        Value val = col->eval(ctx(tmpIter));
        list.values.emplace_back(std::move(val));
      }

      buildNewRow<Value>(*tmpHashTable, list, *tmpIter->row(), ds);
    }
    return ds;
  };

  auto gather = [this](auto&& results) mutable -> Status {
    DataSet result;
    auto* joinNode = asNode<Join>(node());
    result.colNames = joinNode->colNames();
    for (auto& r : results) {
      auto&& rows = std::move(r).value();
      result.rows.insert(result.rows.end(),
                         std::make_move_iterator(rows.begin()),
                         std::make_move_iterator(rows.end()));
    }
    return finish(ResultBuilder().value(Value(std::move(result))).build());
  };

  return runMultiJobs(std::move(scatter), std::move(gather), probeIter);
}

folly::Future<Status> LeftJoinExecutor::singleKeyProbe(
    Expression* probeKey,
    Iterator* probeIter,
    const std::unordered_map<Value, std::vector<const Row*>>& hashTable) {
  auto scatter = [this, probeKey, tmpHashTable = &hashTable](
                     size_t begin, size_t end, Iterator* tmpIter) -> StatusOr<DataSet> {
    // Iterates to the begin pos
    size_t tmp = 0;
    for (; tmpIter->valid() && tmp < begin; ++tmp) {
      tmpIter->next();
    }

    DataSet ds;
    QueryExpressionContext ctx(ectx_);
    ds.rows.reserve(end - begin);
    for (; tmpIter->valid() && tmp++ < end; tmpIter->next()) {
      auto& val = probeKey->eval(ctx(tmpIter));
      buildNewRow<Value>(*tmpHashTable, val, *tmpIter->row(), ds);
    }
    return ds;
  };

  auto gather = [this](auto&& results) mutable -> Status {
    DataSet result;
    auto* joinNode = asNode<Join>(node());
    result.colNames = joinNode->colNames();
    for (auto& r : results) {
      auto&& rows = std::move(r).value();
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
    values.insert(values.end(), colSize_ - lRowSize, Value::kNullValue);
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

BiLeftJoinExecutor::BiLeftJoinExecutor(const PlanNode* node, QueryContext* qctx)
    : LeftJoinExecutor(node, qctx) {
  name_ = "BiLeftJoinExecutor";
}
folly::Future<Status> BiLeftJoinExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto* joinNode = asNode<BiJoin>(node());
  auto& rhsResult = ectx_->getResult(joinNode->rightInputVar());
  rightColSize_ = rhsResult.valuePtr()->getDataSet().colNames.size();
  NG_RETURN_IF_ERROR(checkBiInputDataSets());
  return join(joinNode->hashKeys(), joinNode->probeKeys(), joinNode->colNames());
}
}  // namespace graph
}  // namespace nebula
