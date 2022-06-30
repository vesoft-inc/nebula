// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_JOINEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_JOINEXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class JoinExecutor : public Executor {
 public:
  JoinExecutor(const std::string& name, const PlanNode* node, QueryContext* qctx)
      : Executor(name, node, qctx) {}

 protected:
  Status checkInputDataSets();

  Status checkBiInputDataSets();

  void buildHashTable(const std::vector<Expression*>& hashKeys,
                      Iterator* iter,
                      std::unordered_map<List, std::vector<const Row*>>& hashTable);

  void buildSingleKeyHashTable(Expression* hashKey,
                               Iterator* iter,
                               std::unordered_map<Value, std::vector<const Row*>>& hashTable);

  // concat rows
  Row newRow(Row left, Row right) const;

  std::unique_ptr<Iterator> lhsIter_;
  std::unique_ptr<Iterator> rhsIter_;
  size_t colSize_{0};
  std::unordered_map<Value, std::vector<const Row*>> hashTable_;
  std::unordered_map<List, std::vector<const Row*>> listHashTable_;
};
}  // namespace graph
}  // namespace nebula
#endif
