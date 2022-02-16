/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_MUTATE_UPDATEEXECUTOR_H_
#define GRAPH_EXECUTOR_MUTATE_UPDATEEXECUTOR_H_

#include <folly/futures/Future.h>  // for Future

#include <string>  // for string, allocator
#include <vector>  // for vector

#include "common/base/StatusOr.h"                  // for StatusOr
#include "graph/executor/StorageAccessExecutor.h"  // for StorageAccessExecutor

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

class UpdateBaseExecutor : public StorageAccessExecutor {
 public:
  UpdateBaseExecutor(const std::string &execName, const PlanNode *node, QueryContext *qctx)
      : StorageAccessExecutor(execName, node, qctx) {}

  virtual ~UpdateBaseExecutor() {}

 protected:
  StatusOr<DataSet> handleResult(DataSet &&data);

 protected:
  std::vector<std::string> yieldNames_;
};

class UpdateVertexExecutor final : public UpdateBaseExecutor {
 public:
  UpdateVertexExecutor(const PlanNode *node, QueryContext *qctx)
      : UpdateBaseExecutor("UpdateVertexExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class UpdateEdgeExecutor final : public UpdateBaseExecutor {
 public:
  UpdateEdgeExecutor(const PlanNode *node, QueryContext *qctx)
      : UpdateBaseExecutor("UpdateEdgeExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_MUTATE_UPDATEEXECUTOR_H_
