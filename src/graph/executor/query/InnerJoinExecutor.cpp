// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/InnerJoinExecutor.h"

#include "graph/planner/plan/Query.h"
#include "graph/service/GraphFlags.h"

namespace nebula {
namespace graph {
folly::Future<Status> InnerJoinExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto* joinNode = asNode<Join>(node());
  NG_RETURN_IF_ERROR(checkInputDataSets());
  if (FLAGS_max_job_size <= 1) {
    return join(joinNode->hashKeys(), joinNode->probeKeys(), joinNode->colNames());
  } else {
    return joinMultiJobs(joinNode->hashKeys(), joinNode->probeKeys(), joinNode->colNames());
  }
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
  DataSet result;

  if (lhsIter_->empty() || rhsIter_->empty()) {
    result.colNames = colNames;
    return finish(ResultBuilder().value(Value(std::move(result))).build());
  }

  if (hashKeys.size() == 1 && probeKeys.size() == 1) {
    std::unordered_map<Value, std::vector<const Row*>> hashTable;
    hashTable.reserve(bucketSize);
    if (lhsIter_->size() < rhsIter_->size()) {
      buildSingleKeyHashTable(hashKeys.front(), lhsIter_.get(), hashTable);
      mv_ = movable(rightVar());
      result = singleKeyProbe(probeKeys.front(), rhsIter_.get(), hashTable);
    } else {
      exchange_ = true;
      buildSingleKeyHashTable(probeKeys.front(), rhsIter_.get(), hashTable);
      mv_ = movable(leftVar());
      result = singleKeyProbe(hashKeys.front(), lhsIter_.get(), hashTable);
    }
  } else {
    std::unordered_map<List, std::vector<const Row*>> hashTable;
    hashTable.reserve(bucketSize);
    if (lhsIter_->size() < rhsIter_->size()) {
      buildHashTable(hashKeys, lhsIter_.get(), hashTable);
      mv_ = movable(rightVar());
      result = probe(probeKeys, rhsIter_.get(), hashTable);
    } else {
      exchange_ = true;
      buildHashTable(probeKeys, rhsIter_.get(), hashTable);
      mv_ = movable(leftVar());
      result = probe(hashKeys, lhsIter_.get(), hashTable);
    }
  }
  result.colNames = colNames;
  return finish(ResultBuilder().value(Value(std::move(result))).build());
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

DataSet InnerJoinExecutor::singleKeyProbe(
    Expression* probeKey,
    Iterator* probeIter,
    const std::unordered_map<Value, std::vector<const Row*>>& hashTable) const {
  DataSet ds;
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

folly::Future<Status> InnerJoinExecutor::joinMultiJobs(const std::vector<Expression*>& hashKeys,
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
      buildSingleKeyHashTable(hashKeys.front(), lhsIter_.get(), hashTable_);
      return singleKeyProbe(probeKeys.front(), rhsIter_.get());
    } else {
      exchange_ = true;
      buildSingleKeyHashTable(probeKeys.front(), rhsIter_.get(), hashTable_);
      return singleKeyProbe(hashKeys.front(), lhsIter_.get());
    }
  } else {
    listHashTable_.reserve(bucketSize);
    if (lhsIter_->size() < rhsIter_->size()) {
      buildHashTable(hashKeys, lhsIter_.get(), listHashTable_);
      return probe(probeKeys, rhsIter_.get());
    } else {
      exchange_ = true;
      buildHashTable(probeKeys, rhsIter_.get(), listHashTable_);
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
    finish(ResultBuilder().value(Value(std::move(result))).build());
    return Status::OK();
  };

  return runMultiJobs(std::move(scatter), std::move(gather), probeIter);
}

folly::Future<Status> InnerJoinExecutor::singleKeyProbe(Expression* probeKey, Iterator* probeIter) {
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
void InnerJoinExecutor::buildNewRow(const std::unordered_map<T, std::vector<const Row*>>& hashTable,
                                    const T& val,
                                    Row rRow,
                                    DataSet& ds) const {
  const auto& range = hashTable.find(val);
  if (range == hashTable.end()) {
    return;
  }
  for (std::size_t i = 0, e = range->second.size() - 1; i < e; ++i) {
    if (exchange_) {
      ds.rows.emplace_back(newRow(rRow, *range->second[i]));
    } else {
      ds.rows.emplace_back(newRow(*range->second[i], rRow));
    }
  }
  // Move probe row in last new row creating
  if (exchange_) {
    ds.rows.emplace_back(newRow(std::move(rRow), *range->second.back()));
  } else {
    ds.rows.emplace_back(newRow(*range->second.back(), std::move(rRow)));
  }
}

const std::string& InnerJoinExecutor::leftVar() const {
  if (node_->kind() == PlanNode::Kind::kHashInnerJoin) {
    return node_->asNode<HashJoin>()->leftInputVar();
  } else {
    return node_->asNode<Join>()->leftVar().first;
  }
}

const std::string& InnerJoinExecutor::rightVar() const {
  if (node_->kind() == PlanNode::Kind::kHashInnerJoin) {
    return node_->asNode<HashJoin>()->rightInputVar();
  } else {
    return node_->asNode<Join>()->rightVar().first;
  }
}

HashInnerJoinExecutor::HashInnerJoinExecutor(const PlanNode* node, QueryContext* qctx)
    : InnerJoinExecutor(node, qctx) {
  name_ = "HashInnerJoinExecutor";
}

folly::Future<Status> HashInnerJoinExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto* joinNode = asNode<HashJoin>(node());
  NG_RETURN_IF_ERROR(checkBiInputDataSets());
  return join(joinNode->hashKeys(), joinNode->probeKeys(), joinNode->colNames());
}

}  // namespace graph
}  // namespace nebula
