/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/query/PatternApplyExecutor.h"

#include "graph/context/Iterator.h"
#include "graph/context/QueryExpressionContext.h"
#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {

folly::Future<Status> PatternApplyExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  return patternApply();
}

Status PatternApplyExecutor::checkBiInputDataSets() {
  auto* patternApply = asNode<PatternApply>(node());
  lhsIter_ = ectx_->getResult(patternApply->leftInputVar()).iter();
  DCHECK(!!lhsIter_);
  if (lhsIter_->isGetNeighborsIter() || lhsIter_->isDefaultIter()) {
    std::stringstream ss;
    ss << "PatternApply executor does not support " << lhsIter_->kind();
    return Status::Error(ss.str());
  }
  rhsIter_ = ectx_->getResult(patternApply->rightInputVar()).iter();
  DCHECK(!!rhsIter_);
  if (rhsIter_->isGetNeighborsIter() || rhsIter_->isDefaultIter()) {
    std::stringstream ss;
    ss << "PatternApply executor does not support " << rhsIter_->kind();
    return Status::Error(ss.str());
  }
  isAntiPred_ = patternApply->isAntiPredicate();

  return Status::OK();
}

void PatternApplyExecutor::collectValidKeys(const std::vector<Expression*>& keyCols,
                                            Iterator* iter,
                                            std::unordered_set<List>& validKeys) const {
  QueryExpressionContext ctx(ectx_);
  for (; iter->valid(); iter->next()) {
    List list;
    list.values.reserve(keyCols.size());
    for (auto& col : keyCols) {
      Value val = col->eval(ctx(iter));
      list.values.emplace_back(std::move(val));
    }
    validKeys.emplace(std::move(list));
  }
}

void PatternApplyExecutor::collectValidKey(Expression* keyCol,
                                           Iterator* iter,
                                           std::unordered_set<Value>& validKey) const {
  QueryExpressionContext ctx(ectx_);
  for (; iter->valid(); iter->next()) {
    auto& val = keyCol->eval(ctx(iter));
    validKey.emplace(val);
  }
}

DataSet PatternApplyExecutor::applyZeroKey(Iterator* appliedIter, const bool allValid) {
  DataSet ds;
  ds.rows.reserve(appliedIter->size());
  QueryExpressionContext ctx(ectx_);
  for (; appliedIter->valid(); appliedIter->next()) {
    Row row = mv_ ? appliedIter->moveRow() : *appliedIter->row();
    if (allValid) {
      ds.rows.emplace_back(std::move(row));
    }
  }
  return ds;
}

DataSet PatternApplyExecutor::applySingleKey(Expression* appliedKey,
                                             Iterator* appliedIter,
                                             const std::unordered_set<Value>& validKey) {
  DataSet ds;
  ds.rows.reserve(appliedIter->size());
  QueryExpressionContext ctx(ectx_);
  for (; appliedIter->valid(); appliedIter->next()) {
    auto& val = appliedKey->eval(ctx(appliedIter));
    bool applyFlag = (validKey.find(val) != validKey.end()) ^ isAntiPred_;
    if (applyFlag) {
      Row row = mv_ ? appliedIter->moveRow() : *appliedIter->row();
      ds.rows.emplace_back(std::move(row));
    }
  }
  return ds;
}

DataSet PatternApplyExecutor::applyMultiKey(std::vector<Expression*> appliedKeys,
                                            Iterator* appliedIter,
                                            const std::unordered_set<List>& validKeys) {
  DataSet ds;
  ds.rows.reserve(appliedIter->size());
  QueryExpressionContext ctx(ectx_);
  for (; appliedIter->valid(); appliedIter->next()) {
    List list;
    list.values.reserve(appliedKeys.size());
    for (auto& col : appliedKeys) {
      Value val = col->eval(ctx(appliedIter));
      list.values.emplace_back(std::move(val));
    }

    bool applyFlag = (validKeys.find(list) != validKeys.end()) ^ isAntiPred_;
    if (applyFlag) {
      Row row = mv_ ? appliedIter->moveRow() : *appliedIter->row();
      ds.rows.emplace_back(std::move(row));
    }
  }
  return ds;
}

folly::Future<Status> PatternApplyExecutor::patternApply() {
  auto* patternApplyNode = asNode<PatternApply>(node());
  NG_RETURN_IF_ERROR(checkBiInputDataSets());

  DataSet result;
  mv_ = movable(node()->inputVars()[0]);
  auto keyCols = patternApplyNode->keyCols();
  if (keyCols.size() == 0) {
    // Reverse the valid flag if the pattern predicate is an anti-predicate
    applyZeroKey(lhsIter_.get(), (rhsIter_->size() > 0) ^ isAntiPred_);
  } else if (keyCols.size() == 1) {
    std::unordered_set<Value> validKey;
    collectValidKey(keyCols[0]->clone(), rhsIter_.get(), validKey);
    result = applySingleKey(keyCols[0]->clone(), lhsIter_.get(), validKey);
  } else {
    // Copy the keyCols to refresh the inside propIndex_ cache
    auto cloneExpr = [](std::vector<Expression*> exprs) {
      std::vector<Expression*> applyColsCopy;
      applyColsCopy.reserve(exprs.size());
      for (auto& expr : exprs) {
        applyColsCopy.emplace_back(expr->clone());
      }
      return applyColsCopy;
    };

    std::unordered_set<List> validKeys;
    collectValidKeys(cloneExpr(keyCols), rhsIter_.get(), validKeys);
    result = applyMultiKey(cloneExpr(keyCols), lhsIter_.get(), validKeys);
  }

  result.colNames = patternApplyNode->colNames();
  return finish(ResultBuilder().value(Value(std::move(result))).build());
}

}  // namespace graph
}  // namespace nebula
