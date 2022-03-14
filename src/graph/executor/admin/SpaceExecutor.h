/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_ADMIN_SPACESEXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_SPACESEXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class CreateSpaceExecutor final : public Executor {
 public:
  CreateSpaceExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("CreateSpaceExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class CreateSpaceAsExecutor final : public Executor {
 public:
  CreateSpaceAsExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("CreateSpaceAsExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class DescSpaceExecutor final : public Executor {
 public:
  DescSpaceExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("DescSpaceExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class DropSpaceExecutor final : public Executor {
 public:
  DropSpaceExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("DropSpaceExecutor", node, qctx) {}

  void unRegisterSpaceLevelMetrics(const std::string &spaceName);

  folly::Future<Status> execute() override;
};

class ClearSpaceExecutor final : public Executor {
 public:
  ClearSpaceExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("ClearSpaceExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class ShowSpacesExecutor final : public Executor {
 public:
  ShowSpacesExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("ShowSpacesExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class ShowCreateSpaceExecutor final : public Executor {
 public:
  ShowCreateSpaceExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("ShowCreateSpaceExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

class AlterSpaceExecutor final : public Executor {
 public:
  AlterSpaceExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("AlterSpaceExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};
}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_SPACESEXECUTOR_H_
