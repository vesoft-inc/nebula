/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/query/JoinExecutor.h"

#include <algorithm>  // for max
#include <ostream>    // for operator<<, basic_...
#include <utility>    // for pair, move

#include "common/base/Logging.h"                   // for COMPACT_GOOGLE_LOG...
#include "common/datatypes/List.h"                 // for List, hash
#include "common/datatypes/Value.h"                // for Value, hash, opera...
#include "common/expression/Expression.h"          // for Expression
#include "graph/context/ExecutionContext.h"        // for ExecutionContext
#include "graph/context/Iterator.h"                // for Iterator, operator<<
#include "graph/context/QueryExpressionContext.h"  // for QueryExpressionCon...
#include "graph/context/Result.h"                  // for Result
#include "graph/planner/plan/Query.h"              // for BiJoin, Join

namespace nebula {
namespace graph {

Status JoinExecutor::checkInputDataSets() {
  auto* join = asNode<Join>(node());
  lhsIter_ = ectx_->getVersionedResult(join->leftVar().first, join->leftVar().second).iter();
  DCHECK(!!lhsIter_);
  VLOG(1) << "lhs: " << join->leftVar().first << " " << lhsIter_->size();
  if (lhsIter_->isGetNeighborsIter() || lhsIter_->isDefaultIter()) {
    std::stringstream ss;
    ss << "Join executor does not support " << lhsIter_->kind();
    return Status::Error(ss.str());
  }
  rhsIter_ = ectx_->getVersionedResult(join->rightVar().first, join->rightVar().second).iter();
  DCHECK(!!rhsIter_);
  VLOG(1) << "rhs: " << join->rightVar().first << " " << rhsIter_->size();
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
  VLOG(1) << "lhs: " << join->leftInputVar() << " " << lhsIter_->size();
  if (lhsIter_->isGetNeighborsIter() || lhsIter_->isDefaultIter()) {
    std::stringstream ss;
    ss << "Join executor does not support " << lhsIter_->kind();
    return Status::Error(ss.str());
  }
  rhsIter_ = ectx_->getResult(join->rightInputVar()).iter();
  DCHECK(!!rhsIter_);
  VLOG(1) << "rhs: " << join->rightInputVar() << " " << rhsIter_->size();
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
