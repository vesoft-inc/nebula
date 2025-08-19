// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_MAINTAIN_TAGANNINDEXEXECUTOR_H_
#define GRAPH_EXECUTOR_MAINTAIN_TAGANNINDEXEXECUTOR_H_

#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class CreateTagAnnIndexExecutor final : public Executor {
 public:
  CreateTagAnnIndexExecutor(const PlanNode *node, QueryContext *qctx)
      : Executor("CreateTagAnnIndexExecutor", node, qctx) {}

  folly::Future<Status> execute() override;
};

// class DropTagIndexExecutor final : public Executor {
//  public:
//   DropTagIndexExecutor(const PlanNode *node, QueryContext *qctx)
//       : Executor("DropTagIndexExecutor", node, qctx) {}

//   folly::Future<Status> execute() override;
// };

// class DescTagIndexExecutor final : public Executor {
//  public:
//   DescTagIndexExecutor(const PlanNode *node, QueryContext *qctx)
//       : Executor("DescTagIndexExecutor", node, qctx) {}

//   folly::Future<Status> execute() override;
// };

// class ShowCreateTagIndexExecutor final : public Executor {
//  public:
//   ShowCreateTagIndexExecutor(const PlanNode *node, QueryContext *qctx)
//       : Executor("ShowCreateTagIndexExecutor", node, qctx) {}

//   folly::Future<Status> execute() override;
// };

// class ShowTagIndexesExecutor final : public Executor {
//  public:
//   ShowTagIndexesExecutor(const PlanNode *node, QueryContext *qctx)
//       : Executor("ShowTagIndexesExecutor", node, qctx) {}

//   folly::Future<Status> execute() override;
// };

// class ShowTagIndexStatusExecutor final : public Executor {
//  public:
//   ShowTagIndexStatusExecutor(const PlanNode *node, QueryContext *qctx)
//       : Executor("ShowTagIndexStatusExecutor", node, qctx) {}

//   folly::Future<Status> execute() override;
// };

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_MAINTAIN_TAGANNINDEXEXECUTOR_H_
