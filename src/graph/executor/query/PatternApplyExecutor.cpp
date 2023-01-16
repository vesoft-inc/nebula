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

void PatternApplyExecutor::collectValidKeys(const std::vector<std::pair<Expression*, bool>>& pairs,
                                            Iterator* iter,
                                            std::unordered_set<ListWrapper>& validKeys) const {
  QueryExpressionContext ctx(ectx_);
  for (; iter->valid(); iter->next()) {
    ListWrapper listWrapper;
    for (auto& pair : pairs) {
      Value val = pair.first->eval(ctx(iter));
      listWrapper.emplace(std::move(val), pair.second);
    }
    validKeys.emplace(std::move(listWrapper));
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

DataSet PatternApplyExecutor::applyMultiKey(std::vector<std::pair<Expression*, bool>> pairs,
                                            Iterator* appliedIter,
                                            const std::unordered_set<ListWrapper>& validKeys) {
  DataSet ds;
  ds.rows.reserve(appliedIter->size());
  QueryExpressionContext ctx(ectx_);
  for (; appliedIter->valid(); appliedIter->next()) {
    ListWrapper listWrapper;
    for (auto& pair : pairs) {
      Value val = pair.first->eval(ctx(appliedIter));
      listWrapper.emplace(std::move(val), pair.second);
    }

    bool applyFlag = (validKeys.find(listWrapper) != validKeys.end()) ^ isAntiPred_;
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
    collectValidKey(keyCols[0].first->clone(), rhsIter_.get(), validKey);
    result = applySingleKey(keyCols[0].first->clone(), lhsIter_.get(), validKey);
  } else {
    // Copy the keyCols to refresh the inside propIndex_ cache
    auto cloneExpr = [](std::vector<std::pair<Expression*, bool>> pairs) {
      std::vector<std::pair<Expression*, bool>> applyColsCopy;
      applyColsCopy.reserve(pairs.size());
      for (auto& pair : pairs) {
        applyColsCopy.emplace_back(std::make_pair(pair.first->clone(), pair.second));
      }
      return applyColsCopy;
    };

    std::unordered_set<ListWrapper> validKeys;
    collectValidKeys(cloneExpr(keyCols), rhsIter_.get(), validKeys);
    result = applyMultiKey(cloneExpr(keyCols), lhsIter_.get(), validKeys);
  }

  result.colNames = patternApplyNode->colNames();
  return finish(ResultBuilder().value(Value(std::move(result))).build());
}

}  // namespace graph
}  // namespace nebula
