/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_ALGO_CARTESIANPRODUCTEXECUTOR_H_
#define GRAPH_EXECUTOR_ALGO_CARTESIANPRODUCTEXECUTOR_H_

#include <folly/futures/Future.h>  // for Future

#include <string>  // for string, allocator, basic_...
#include <vector>  // for vector

#include "graph/executor/Executor.h"  // for Executor

namespace nebula {
class Status;
namespace graph {
class PlanNode;
class QueryContext;
}  // namespace graph
struct DataSet;

class Status;
struct DataSet;

namespace graph {
class PlanNode;
class QueryContext;

class CartesianProductExecutor : public Executor {
 public:
  CartesianProductExecutor(const PlanNode* node, QueryContext* qctx)
      : Executor("CartesianProductExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 protected:
  void doCartesianProduct(const DataSet& lds, const DataSet& rds, DataSet& result);

 private:
  std::vector<std::vector<std::string>> colNames_;
};

class BiCartesianProductExecutor : public CartesianProductExecutor {
 public:
  BiCartesianProductExecutor(const PlanNode* node, QueryContext* qctx);

  folly::Future<Status> execute() override;
};

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_EXECUTOR_ALGO_CARTESIANPRODUCTEXECUTOR_H_
