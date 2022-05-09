// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/InnerJoinExecutor.h"

#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {
folly::Future<Status> InnerJoinExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto* joinNode = asNode<Join>(node());
  NG_RETURN_IF_ERROR(checkInputDataSets());
  return join(joinNode->hashKeys(), joinNode->probeKeys(), joinNode->colNames());
}

Status InnerJoinExecutor::close() {
  exchange_ = false;
  return Executor::close();
}

folly::Future<Status> InnerJoinExecutor::join(const std::vector<Expression*>& hashKeys,
                                              const std::vector<Expression*>& probeKeys,
                                              const std::vector<std::string>& colNames) {
  auto bucketSize = lhsIter_->size() > rhsIter_->size() ? rhsIter_->size() : lhsIter_->size();

  DCHECK_EQ(hashKeys.size(), probeKeys.size());

  if (lhsIter_->empty() || rhsIter_->empty()) {
    DataSet result;
    result.colNames = colNames;
    return finish(ResultBuilder().value(Value(std::move(result))).build());
  }

  if (hashKeys.size() == 1 && probeKeys.size() == 1) {
    hashTable_.reserve(bucketSize);
    if (lhsIter_->size() < rhsIter_->size()) {
      buildSingleKeyHashTable(hashKeys.front(), lhsIter_.get());
      return singleKeyProbe(probeKeys.front(), rhsIter_.get());
    } else {
      exchange_ = true;
      buildSingleKeyHashTable(probeKeys.front(), rhsIter_.get());
      return singleKeyProbe(hashKeys.front(), lhsIter_.get());
    }
  } else {
    listHashTable_.reserve(bucketSize);
    if (lhsIter_->size() < rhsIter_->size()) {
      buildHashTable(hashKeys, lhsIter_.get());
      return probe(probeKeys, rhsIter_.get());
    } else {
      exchange_ = true;
      buildHashTable(probeKeys, rhsIter_.get());
      return probe(hashKeys, lhsIter_.get());
    }
  }
}

folly::Future<Status> InnerJoinExecutor::probe(const std::vector<Expression*>& probeKeys,
                                               Iterator* probeIter) {
  std::vector<Expression*> tmpProbeKeys;
  std::for_each(probeKeys.begin(), probeKeys.end(), [&tmpProbeKeys](auto& e) {
    tmpProbeKeys.emplace_back(e->clone());
  });
  auto scatter = [this, tmpProbeKeys = std::move(tmpProbeKeys)](
                     size_t begin, size_t end, Iterator* tmpIter) -> StatusOr<DataSet> {
    LOG(ERROR) << "begin: " << begin << " end: " << end;
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
      buildNewRow<List>(listHashTable_, list, *tmpIter->row(), ds);
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

folly::Future<Status> InnerJoinExecutor::singleKeyProbe(Expression* probeKey, Iterator* probeIter) {
  auto scatter = [this, probeKey](
                     size_t begin, size_t end, Iterator* tmpIter) -> StatusOr<DataSet> {
    auto tmpProbeKey = probeKey->clone();
    // Iterates to the begin pos
    size_t tmp = 0;
    for (; tmpIter->valid() && tmp < begin; ++tmp) {
      tmpIter->next();
    }

    DataSet ds;
    QueryExpressionContext ctx(ectx_);
    ds.rows.reserve(end - begin);
    for (; tmpIter->valid() && tmp++ < end; tmpIter->next()) {
      auto& val = tmpProbeKey->eval(ctx(tmpIter));
      buildNewRow<Value>(hashTable_, val, *tmpIter->row(), ds);
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

BiInnerJoinExecutor::BiInnerJoinExecutor(const PlanNode* node, QueryContext* qctx)
    : InnerJoinExecutor(node, qctx) {
  name_ = "BiInnerJoinExecutor";
}

folly::Future<Status> BiInnerJoinExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto* joinNode = asNode<BiJoin>(node());
  NG_RETURN_IF_ERROR(checkBiInputDataSets());
  return join(joinNode->hashKeys(), joinNode->probeKeys(), joinNode->colNames());
}
}  // namespace graph
}  // namespace nebula
