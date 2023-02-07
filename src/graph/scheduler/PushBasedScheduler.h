/* Copyright (c) 2023 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_SCHEDULER_PUSHBASEDSCHEDULER_H_
#define GRAPH_SCHEDULER_PUSHBASEDSCHEDULER_H_

#include "graph/scheduler/Scheduler.h"

namespace nebula {
namespace graph {

class PushBasedScheduler final : public Scheduler {
 public:
  explicit PushBasedScheduler(QueryContext* qctx);

  folly::Future<Status> schedule() override;
  void waitFinish() override {}

 private:
  void buildDepMap(Executor* exec);
  folly::Future<Status> runExecutor();
  folly::Future<Status> runExecutor(Executor* exec);

  std::unordered_map<std::string, std::vector<Executor*>> argumentMap_;
  std::unordered_set<Executor*> leafExecutors_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_SCHEDULER_PUSHBASEDSCHEDULER_H_
