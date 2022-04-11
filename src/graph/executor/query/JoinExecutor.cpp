// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/query/JoinExecutor.h"

#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {

Status JoinExecutor::checkInputDataSets() {
  auto* join = asNode<Join>(node());
  lhsIter_ = ectx_->getVersionedResult(join->leftVar().first, join->leftVar().second).iter();
  DCHECK(!!lhsIter_);
  if (lhsIter_->isGetNeighborsIter() || lhsIter_->isDefaultIter()) {
    std::stringstream ss;
    ss << "Join executor does not support " << lhsIter_->kind();
    return Status::Error(ss.str());
  }
  rhsIter_ = ectx_->getVersionedResult(join->rightVar().first, join->rightVar().second).iter();
  DCHECK(!!rhsIter_);
  if (rhsIter_->isGetNeighborsIter() || rhsIter_->isDefaultIter()) {
    std::stringstream ss;
    ss << "Join executor does not support " << rhsIter_->kind();
    return Status::Error(ss.str());
  }
  colSize_ = join->colNames().size();
  return Status::OK();
}

Status JoinExecutor::checkBiInputDataSets() {
  auto* join = asNode<BiJoin>(node());
  lhsIter_ = ectx_->getResult(join->leftInputVar()).iter();
  DCHECK(!!lhsIter_);
  if (lhsIter_->isGetNeighborsIter() || lhsIter_->isDefaultIter()) {
    std::stringstream ss;
    ss << "Join executor does not support " << lhsIter_->kind();
    return Status::Error(ss.str());
  }
  rhsIter_ = ectx_->getResult(join->rightInputVar()).iter();
  DCHECK(!!rhsIter_);
  if (rhsIter_->isGetNeighborsIter() || rhsIter_->isDefaultIter()) {
    std::stringstream ss;
    ss << "Join executor does not support " << rhsIter_->kind();
    return Status::Error(ss.str());
  }
  colSize_ = join->colNames().size();
  return Status::OK();
}

void JoinExecutor::buildHashTable(
    const std::vector<Expression*>& hashKeys,
    Iterator* iter,
    std::unordered_map<List, std::vector<const Row*>>& hashTable) const {
  QueryExpressionContext ctx(ectx_);
  for (; iter->valid(); iter->next()) {
    List list;
    list.values.reserve(hashKeys.size());
    for (auto& col : hashKeys) {
      Value val = col->eval(ctx(iter));
      list.values.emplace_back(std::move(val));
    }

    auto& vals = hashTable[list];
    vals.emplace_back(iter->row());
  }
}

void JoinExecutor::buildSingleKeyHashTable(
    Expression* hashKey,
    Iterator* iter,
    std::unordered_map<Value, std::vector<const Row*>>& hashTable) const {
  QueryExpressionContext ctx(ectx_);
  for (; iter->valid(); iter->next()) {
    auto& val = hashKey->eval(ctx(iter));

    auto& vals = hashTable[val];
    vals.emplace_back(iter->row());
  }
}

}  // namespace graph
}  // namespace nebula
