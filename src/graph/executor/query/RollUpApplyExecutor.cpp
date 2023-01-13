/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/query/RollUpApplyExecutor.h"

#include "graph/context/Iterator.h"
#include "graph/context/QueryExpressionContext.h"
#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {

folly::Future<Status> RollUpApplyExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  return rollUpApply();
}

Status RollUpApplyExecutor::checkBiInputDataSets() {
  auto* rollUpApply = asNode<RollUpApply>(node());
  lhsIter_ = ectx_->getResult(rollUpApply->leftInputVar()).iter();
  DCHECK(!!lhsIter_);
  if (lhsIter_->isGetNeighborsIter() || lhsIter_->isDefaultIter()) {
    std::stringstream ss;
    ss << "RollUpApply executor does not support " << lhsIter_->kind();
    return Status::Error(ss.str());
  }
  rhsIter_ = ectx_->getResult(rollUpApply->rightInputVar()).iter();
  DCHECK(!!rhsIter_);
  if (rhsIter_->isGetNeighborsIter() || rhsIter_->isDefaultIter()) {
    std::stringstream ss;
    ss << "RollUpApply executor does not support " << rhsIter_->kind();
    return Status::Error(ss.str());
  }
  colSize_ = rollUpApply->colNames().size();
  return Status::OK();
}

void RollUpApplyExecutor::buildHashTable(
    const std::vector<std::pair<Expression*, bool>>& compareCols,
    const InputPropertyExpression* collectCol,
    Iterator* iter,
    std::unordered_map<ListWrapper, List, WrapperHash, WrapperEqual>& hashTable) const {
  QueryExpressionContext ctx(ectx_);

  for (; iter->valid(); iter->next()) {
    ListWrapper listWrapper;
    for (auto& pair : compareCols) {
      Value val = pair.first->eval(ctx(iter));
      listWrapper.emplace(val, pair.second);
    }
    DLOG(ERROR) << "Hash Key : " << listWrapper.toString();

    auto& vals = hashTable[listWrapper];
    vals.emplace_back(const_cast<InputPropertyExpression*>(collectCol)->eval(ctx(iter)));
  }
}

void RollUpApplyExecutor::buildSingleKeyHashTable(
    const std::pair<Expression*, bool>& compareCol,
    const InputPropertyExpression* collectCol,
    Iterator* iter,
    std::unordered_map<ListWrapper, List, WrapperHash, WrapperEqual>& hashTable) const {
  QueryExpressionContext ctx(ectx_);
  for (; iter->valid(); iter->next()) {
    auto& val = compareCol.first->eval(ctx(iter));
    ListWrapper listWrapper;
    listWrapper.emplace(std::move(val), compareCol.second);
    auto& vals = hashTable[listWrapper];
    vals.emplace_back(const_cast<InputPropertyExpression*>(collectCol)->eval(ctx(iter)));
  }
}

void RollUpApplyExecutor::buildZeroKeyHashTable(const InputPropertyExpression* collectCol,
                                                Iterator* iter,
                                                List& hashTable) const {
  QueryExpressionContext ctx(ectx_);
  hashTable.reserve(iter->size());
  for (; iter->valid(); iter->next()) {
    hashTable.emplace_back(const_cast<InputPropertyExpression*>(collectCol)->eval(ctx(iter)));
  }
}

DataSet RollUpApplyExecutor::probeZeroKey(Iterator* probeIter, const List& hashTable) {
  DataSet ds;
  ds.rows.reserve(probeIter->size());
  QueryExpressionContext ctx(ectx_);
  for (; probeIter->valid(); probeIter->next()) {
    Row row = mv_ ? probeIter->moveRow() : *probeIter->row();
    row.emplace_back(std::move(hashTable));
    ds.rows.emplace_back(std::move(row));
  }
  return ds;
}

DataSet RollUpApplyExecutor::probeSingleKey(
    const std::pair<Expression*, bool>& probeKey,
    Iterator* probeIter,
    const std::unordered_map<ListWrapper, List, WrapperHash, WrapperEqual>& hashTable) {
  DataSet ds;
  ds.rows.reserve(probeIter->size());
  QueryExpressionContext ctx(ectx_);
  for (; probeIter->valid(); probeIter->next()) {
    auto& val = probeKey.first->eval(ctx(probeIter));
    ListWrapper listWrapper;
    listWrapper.emplace(std::move(val), probeKey.second);
    List vals;
    auto found = hashTable.find(listWrapper);
    if (found != hashTable.end()) {
      vals = found->second;
    }
    Row row = mv_ ? probeIter->moveRow() : *probeIter->row();
    row.emplace_back(std::move(vals));
    ds.rows.emplace_back(std::move(row));
  }
  return ds;
}

DataSet RollUpApplyExecutor::probe(
    std::vector<std::pair<Expression*, bool>> probeKeys,
    Iterator* probeIter,
    const std::unordered_map<ListWrapper, List, WrapperHash, WrapperEqual>& hashTable) {
  DataSet ds;
  ds.rows.reserve(probeIter->size());
  QueryExpressionContext ctx(ectx_);
  for (; probeIter->valid(); probeIter->next()) {
    ListWrapper listWrapper;
    for (auto& pair : probeKeys) {
      Value val = pair.first->eval(ctx(probeIter));
      listWrapper.emplace(std::move(val), pair.second);
    }

    List vals;
    DLOG(ERROR) << "Hash Find : " << listWrapper.toString();
    auto found = hashTable.find(listWrapper);
    if (found != hashTable.end()) {
      vals = found->second;
      DLOG(ERROR) << " FOUND !!! ";
    }
    Row row = mv_ ? probeIter->moveRow() : *probeIter->row();
    row.emplace_back(std::move(vals));
    ds.rows.emplace_back(std::move(row));
  }
  return ds;
}

folly::Future<Status> RollUpApplyExecutor::rollUpApply() {
  auto* rollUpApplyNode = asNode<RollUpApply>(node());
  NG_RETURN_IF_ERROR(checkBiInputDataSets());
  DataSet result;
  mv_ = movable(node()->inputVars()[0]);

  auto compareCols = rollUpApplyNode->compareCols();

  if (compareCols.size() == 0) {
    List hashTable;
    buildZeroKeyHashTable(rollUpApplyNode->collectCol(), rhsIter_.get(), hashTable);
    result = probeZeroKey(lhsIter_.get(), hashTable);
  } else if (compareCols.size() == 1) {
    std::unordered_map<ListWrapper, List, WrapperHash, WrapperEqual> hashTable;
    std::pair<Expression*, bool> pair(compareCols[0].first->clone(), compareCols[0].second);
    buildSingleKeyHashTable(pair, rollUpApplyNode->collectCol(), rhsIter_.get(), hashTable);
    std::pair<Expression*, bool> newPair(compareCols[0].first->clone(), compareCols[0].second);
    result = probeSingleKey(newPair, lhsIter_.get(), hashTable);
  } else {
    // Copy the compareCols to make sure the propIndex_ is not cached in the expr
    auto cloneExpr = [](std::vector<std::pair<Expression*, bool>> pairs) {
      std::vector<std::pair<Expression*, bool>> collectColsCopy;
      collectColsCopy.reserve(pairs.size());
      for (auto& pair : pairs) {
        collectColsCopy.emplace_back(std::make_pair(pair.first->clone(), pair.second));
      }
      return collectColsCopy;
    };

    std::unordered_map<ListWrapper, List, WrapperHash, WrapperEqual> hashTable;
    buildHashTable(
        cloneExpr(compareCols), rollUpApplyNode->collectCol(), rhsIter_.get(), hashTable);

    result = probe(cloneExpr(compareCols), lhsIter_.get(), hashTable);
  }
  result.colNames = rollUpApplyNode->colNames();
  DLOG(ERROR) << "Rollup result " << result.toString();
  return finish(ResultBuilder().value(Value(std::move(result))).build());
}

}  // namespace graph
}  // namespace nebula
