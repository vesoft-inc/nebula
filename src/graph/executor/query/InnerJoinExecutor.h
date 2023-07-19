// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_INNERJOINEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_INNERJOINEXECUTOR_H_

#include "graph/executor/query/JoinExecutor.h"
namespace nebula {
namespace graph {

class InnerJoinExecutor : public JoinExecutor {
 public:
  InnerJoinExecutor(const PlanNode* node, QueryContext* qctx)
      : JoinExecutor("InnerJoinExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

  Status close() override;

 protected:
  // join/probe/singleKeyProbe implemented for single job.
  folly::Future<Status> join(const std::vector<Expression*>& hashKeys,
                             const std::vector<Expression*>& probeKeys,
                             const std::vector<std::string>& colNames);

  DataSet probe(const std::vector<Expression*>& probeKeys,
                Iterator* probeIter,
                const std::unordered_map<List, std::vector<const Row*>>& hashTable) const;

  DataSet singleKeyProbe(Expression* probeKey,
                         Iterator* probeIter,
                         const std::unordered_map<Value, std::vector<const Row*>>& hashTable) const;

  // joinMultiJobs/probe/singleKeyProbe implemented for multi jobs.
  // For now, the InnerJoin implementation only implement the parallel processing on probe side.
  folly::Future<Status> joinMultiJobs(const std::vector<Expression*>& hashKeys,
                                      const std::vector<Expression*>& probeKeys,
                                      const std::vector<std::string>& colNames);

  folly::Future<Status> probe(const std::vector<Expression*>& probeKeys, Iterator* probeIter);

  folly::Future<Status> singleKeyProbe(Expression* probeKey, Iterator* probeIter);

  template <class T>
  void buildNewRow(const std::unordered_map<T, std::vector<const Row*>>& hashTable,
                   const T& val,
                   Row rRow,
                   DataSet& ds) const;

  const std::string& leftVar() const;

  const std::string& rightVar() const;

 private:
  bool exchange_{false};
  // Does the probe result movable?
  bool mv_{false};
};

// No difference with inner join in processing data, but the dependencies would be executed in
// parallel.
class HashInnerJoinExecutor final : public InnerJoinExecutor {
 public:
  HashInnerJoinExecutor(const PlanNode* node, QueryContext* qctx);

  folly::Future<Status> execute() override;
};
}  // namespace graph
}  // namespace nebula
#endif
