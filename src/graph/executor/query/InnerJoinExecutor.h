/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_QUERY_INNERJOINEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_INNERJOINEXECUTOR_H_

#include <folly/futures/Future.h>  // for Future

#include <string>         // for allocator, string
#include <unordered_map>  // for unordered_map
#include <vector>         // for vector

#include "common/base/Status.h"                 // for Status
#include "common/datatypes/DataSet.h"           // for Row, DataSet
#include "graph/executor/query/JoinExecutor.h"  // for JoinExecutor

namespace nebula {
class Expression;
namespace graph {
class Iterator;
class PlanNode;
class QueryContext;
}  // namespace graph
struct List;
struct Value;

class Expression;
struct List;
struct Value;

namespace graph {
class Iterator;
class PlanNode;
class QueryContext;

class InnerJoinExecutor : public JoinExecutor {
 public:
  InnerJoinExecutor(const PlanNode* node, QueryContext* qctx)
      : JoinExecutor("InnerJoinExecutor", node, qctx) {}

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
                   const Row& rRow,
                   DataSet& ds) const;

 private:
  bool exchange_{false};
};

/**
 * No diffrence with inner join in processing data, but the dependencies would be executed in
 * paralell.
 */
class BiInnerJoinExecutor final : public InnerJoinExecutor {
 public:
  BiInnerJoinExecutor(const PlanNode* node, QueryContext* qctx);

  folly::Future<Status> execute() override;
};
}  // namespace graph
}  // namespace nebula
#endif
