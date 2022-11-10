// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_JOINEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_JOINEXECUTOR_H_

#include <robin_hood.h>

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

template <typename K, typename V>
using RhFlatMap = robin_hood::unordered_flat_map<K, V, std::hash<K>>;

template <typename K>
using RhFlatSet = robin_hood::unordered_flat_set<K, std::hash<K>>;

class JoinExecutor : public Executor {
 public:
  JoinExecutor(const std::string& name, const PlanNode* node, QueryContext* qctx)
      : Executor(name, node, qctx) {}

 protected:
  Status checkInputDataSets();

  Status checkBiInputDataSets();

  void buildHashTable(const std::vector<Expression*>& hashKeys,
                      Iterator* iter,
                      RhFlatMap<List, std::vector<const Row*>>& hashTable);

  void buildSingleKeyHashTable(Expression* hashKey,
                               Iterator* iter,
                               RhFlatMap<Value, std::vector<const Row*>>& hashTable);

  // concat rows
  Row newRow(Row left, Row right) const;

  std::unique_ptr<Iterator> lhsIter_;
  std::unique_ptr<Iterator> rhsIter_;
  size_t colSize_{0};
  RhFlatMap<Value, std::vector<const Row*>> hashTable_;
  RhFlatMap<List, std::vector<const Row*>> listHashTable_;
};
}  // namespace graph
}  // namespace nebula
#endif
