/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_SCHEDULER_SCHEDULER_H_
#define GRAPH_SCHEDULER_SCHEDULER_H_

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "graph/executor/Executor.h"
#include "graph/planner/plan/PlanNode.h"

namespace nebula {
namespace graph {
class Scheduler : private cpp::NonCopyable, private cpp::NonMovable {
 public:
  Scheduler() = default;
  virtual ~Scheduler() = default;

  virtual folly::Future<Status> schedule() = 0;

  static void analyzeLifetime(const PlanNode *node, bool inLoop = false);
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_SCHEDULER_SCHEDULER_H_
