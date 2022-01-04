/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_QUERY_LEFTJOINEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_LEFTJOINEXECUTOR_H_

#include "graph/executor/query/JoinExecutor.h"

namespace nebula {
namespace graph {

class LeftJoinExecutor : public JoinExecutor {
 public:
  LeftJoinExecutor(const PlanNode* node, QueryContext* qctx)
      : JoinExecutor("LeftJoinExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

  Status close() override;

 protected:
  folly::Future<Status> join(const std::vector<Expression*>& hashKeys,
                             const std::vector<Expression*>& probeKeys,
                             const std::vector<std::string>& colNames);

  DataSet probe(const std::vector<Expression*>& probeKeys,
                Iterator* probeIter,
                const std::unordered_map<List, std::vector<const Row*>>& hashTable) const;

  DataSet singleKeyProbe(Expression* probeKey,
                         Iterator* probeIter,
                         const std::unordered_map<Value, std::vector<const Row*>>& hashTable) const;

  template <class T>
  void buildNewRow(const std::unordered_map<T, std::vector<const Row*>>& hashTable,
                   const T& val,
                   const Row& lRow,
                   DataSet& ds) const;

  size_t rightColSize_{0};
};

/**
 * No diffrence with left join in processing data, but the dependencies would be executed in
 * paralell.
 */
class BiLeftJoinExecutor final : public LeftJoinExecutor {
 public:
  BiLeftJoinExecutor(const PlanNode* node, QueryContext* qctx);

  folly::Future<Status> execute() override;
};
}  // namespace graph
}  // namespace nebula
#endif
