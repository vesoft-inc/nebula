/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class PatternApplyExecutor : public Executor {
 public:
  PatternApplyExecutor(const PlanNode* node, QueryContext* qctx)
      : Executor("PatternApplyExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 protected:
  Status checkBiInputDataSets();

  void collectValidKeys(const std::vector<Expression*>& keyCols,
                        Iterator* iter,
                        std::unordered_set<List>& validKeys) const;

  void collectValidKey(Expression* keyCol,
                       Iterator* iter,
                       std::unordered_set<Value>& validKey) const;

  DataSet applyZeroKey(Iterator* appliedIter, const bool allValid);

  DataSet applySingleKey(Expression* appliedCol,
                         Iterator* appliedIter,
                         const std::unordered_set<Value>& validKey);

  DataSet applyMultiKey(std::vector<Expression*> appliedKeys,
                        Iterator* appliedIter,
                        const std::unordered_set<List>& validKeys);

  folly::Future<Status> patternApply();
  std::unique_ptr<Iterator> lhsIter_;
  std::unique_ptr<Iterator> rhsIter_;

  // Should apply the reverse when the pattern is an anti-predicate
  bool isAntiPred_{false};
  // Check if the apply side dataset movable
  bool mv_{false};
};

}  // namespace graph
}  // namespace nebula
