/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include <robin_hood.h>

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

template <typename K, typename V>
using RhFlatMap = robin_hood::unordered_flat_map<K, V, std::hash<K>>;

class RollUpApplyExecutor : public Executor {
 public:
  RollUpApplyExecutor(const PlanNode* node, QueryContext* qctx)
      : Executor("RollUpApplyExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 protected:
  void buildHashTable(const std::vector<Expression*>& compareCols, Iterator* iter);

  Status checkBiInputDataSets();

  void buildHashTable(const std::vector<Expression*>& compareCols,
                      const InputPropertyExpression* collectCol,
                      Iterator* iter,
                      RhFlatMap<List, List>& hashTable) const;

  void buildSingleKeyHashTable(Expression* compareCol,
                               const InputPropertyExpression* collectCol,
                               Iterator* iter,
                               RhFlatMap<Value, List>& hashTable) const;

  void buildZeroKeyHashTable(const InputPropertyExpression* collectCol,
                             Iterator* iter,
                             List& hashTable) const;

  DataSet probeZeroKey(Iterator* probeIter, const List& hashTable);

  DataSet probeSingleKey(Expression* probeKey,
                         Iterator* probeIter,
                         const RhFlatMap<Value, List>& hashTable);

  DataSet probe(std::vector<Expression*> probeKeys,
                Iterator* probeIter,
                const RhFlatMap<List, List>& hashTable);

  folly::Future<Status> rollUpApply();

  std::unique_ptr<Iterator> lhsIter_;
  std::unique_ptr<Iterator> rhsIter_;
  size_t colSize_{0};
  // Does the probe result movable?
  bool mv_{false};
};

}  // namespace graph
}  // namespace nebula
