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
  // colSize_ is the size of the output columns of join executor.
  // If the join is natural join, which means the left and right columns have duplicated names,
  // the output columns will be a intersection of the left and right columns.
  size_t colSize_{0};
  // If the join is natural join, rhsOutputColIdxs_ will be used to record the output column index
  // of the right. If not, rhsOutputColIdxs_ will be empty.
  std::optional<std::vector<size_t>> rhsOutputColIdxs_;
  std::unordered_map<Value, std::vector<const Row*>> hashTable_;
  std::unordered_map<List, std::vector<const Row*>> listHashTable_;
};
}  // namespace graph
}  // namespace nebula
#endif
