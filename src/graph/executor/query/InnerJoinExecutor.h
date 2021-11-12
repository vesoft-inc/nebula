/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_QUERY_INNERJOINEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_INNERJOINEXECUTOR_H_

#include "graph/executor/query/JoinExecutor.h"

namespace nebula {
namespace graph {

class InnerJoinExecutor final : public JoinExecutor {
 public:
  InnerJoinExecutor(const PlanNode* node, QueryContext* qctx)
      : JoinExecutor("InnerJoinExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

  Status close() override;

 private:
  folly::Future<Status> join();

  DataSet probe(const std::vector<Expression*>& probeKeys,
                Iterator* probeIter,
                const std::unordered_map<List, std::vector<const Row*>>& hashTable) const;

  DataSet singleKeyProbe(Expression* probeKey,
                         Iterator* probeIter,
                         const std::unordered_map<Value, std::vector<const Row*>>& hashTable) const;

  template <class T>
  void buildNewRow(const std::unordered_map<T, std::vector<const Row*>>& hashTable,
                   const T& val,
                   const Row& rRow,
                   DataSet& ds) const;

 private:
  bool exchange_{false};
};
}  // namespace graph
}  // namespace nebula
#endif
