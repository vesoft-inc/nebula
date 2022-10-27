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

void RollUpApplyExecutor::buildHashTable(const std::vector<Expression*>& compareCols,
                                         const InputPropertyExpression* collectCol,
                                         Iterator* iter,
                                         std::unordered_map<List, List>& hashTable) const {
  QueryExpressionContext ctx(ectx_);

  for (; iter->valid(); iter->next()) {
    List list;
    list.values.reserve(compareCols.size());
    for (auto& col : compareCols) {
      Value val = col->eval(ctx(iter));
      list.values.emplace_back(std::move(val));
    }

    auto& vals = hashTable[list];
    vals.emplace_back(const_cast<InputPropertyExpression*>(collectCol)->eval(ctx(iter)));
  }
}

void RollUpApplyExecutor::buildSingleKeyHashTable(
    Expression* compareCol,
    const InputPropertyExpression* collectCol,
    Iterator* iter,
    std::unordered_map<Value, List>& hashTable) const {
  QueryExpressionContext ctx(ectx_);
  for (; iter->valid(); iter->next()) {
    auto& val = compareCol->eval(ctx(iter));

    auto& vals = hashTable[val];
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

DataSet RollUpApplyExecutor::probeSingleKey(Expression* probeKey,
                                            Iterator* probeIter,
                                            const std::unordered_map<Value, List>& hashTable) {
  DataSet ds;
  ds.rows.reserve(probeIter->size());
  QueryExpressionContext ctx(ectx_);
  for (; probeIter->valid(); probeIter->next()) {
    auto& val = probeKey->eval(ctx(probeIter));
    List vals;
    auto found = hashTable.find(val);
    if (found != hashTable.end()) {
      vals = found->second;
    }
    Row row = mv_ ? probeIter->moveRow() : *probeIter->row();
    row.emplace_back(std::move(vals));
    ds.rows.emplace_back(std::move(row));
  }
  return ds;
}

DataSet RollUpApplyExecutor::probe(std::vector<Expression*> probeKeys,
                                   Iterator* probeIter,
                                   const std::unordered_map<List, List>& hashTable) {
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

    List vals;
    auto found = hashTable.find(list);
    if (found != hashTable.end()) {
      vals = found->second;
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
    std::unordered_map<Value, List> hashTable;
    buildSingleKeyHashTable(
        compareCols[0]->clone(), rollUpApplyNode->collectCol(), rhsIter_.get(), hashTable);
    result = probeSingleKey(compareCols[0]->clone(), lhsIter_.get(), hashTable);
  } else {
    // Copy the compareCols to make sure the propIndex_ is not cached in the expr
    auto cloneExpr = [](std::vector<Expression*> exprs) {
      std::vector<Expression*> collectColsCopy;
      collectColsCopy.reserve(exprs.size());
      for (auto& expr : exprs) {
        collectColsCopy.emplace_back(expr->clone());
      }
      return collectColsCopy;
    };

    std::unordered_map<List, List> hashTable;
    buildHashTable(
        cloneExpr(compareCols), rollUpApplyNode->collectCol(), rhsIter_.get(), hashTable);

    result = probe(cloneExpr(compareCols), lhsIter_.get(), hashTable);
  }
  result.colNames = rollUpApplyNode->colNames();
  return finish(ResultBuilder().value(Value(std::move(result))).build());
}

}  // namespace graph
}  // namespace nebula
