/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_QUERY_JOINEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_JOINEXECUTOR_H_

#include <stddef.h>  // for size_t

#include <memory>         // for unique_ptr
#include <string>         // for string
#include <unordered_map>  // for unordered_map
#include <vector>         // for vector

#include "common/base/Status.h"        // for Status
#include "common/datatypes/DataSet.h"  // for Row
#include "graph/context/Iterator.h"    // for Iterator
#include "graph/executor/Executor.h"   // for Executor

namespace nebula {
class Expression;
namespace graph {
class PlanNode;
class QueryContext;
}  // namespace graph
struct List;
struct Value;

class Expression;
struct List;
struct Value;

namespace graph {
class PlanNode;
class QueryContext;

class JoinExecutor : public Executor {
 public:
  JoinExecutor(const std::string& name, const PlanNode* node, QueryContext* qctx)
      : Executor(name, node, qctx) {}

 protected:
  Status checkInputDataSets();

  void buildHashTable(const std::vector<Expression*>& hashKeys, Iterator* iter);

  Status checkBiInputDataSets();

  void buildHashTable(const std::vector<Expression*>& hashKeys,
                      Iterator* iter,
                      std::unordered_map<List, std::vector<const Row*>>& hashTable) const;

  void buildSingleKeyHashTable(Expression* hashKey,
                               Iterator* iter,
                               std::unordered_map<Value, std::vector<const Row*>>& hashTable) const;

  std::unique_ptr<Iterator> lhsIter_;
  std::unique_ptr<Iterator> rhsIter_;
  size_t colSize_{0};
};
}  // namespace graph
}  // namespace nebula
#endif
