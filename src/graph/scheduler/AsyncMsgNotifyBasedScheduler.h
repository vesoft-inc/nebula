/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_SCHEDULER_ASYNCMSGNOTIFYBASEDSCHEDULER_H_
#define GRAPH_SCHEDULER_ASYNCMSGNOTIFYBASEDSCHEDULER_H_

#include "graph/executor/logic/LoopExecutor.h"
#include "graph/executor/logic/SelectExecutor.h"
#include "graph/scheduler/Scheduler.h"

namespace nebula {
namespace graph {
/**
 * This is an scheduler implementation based on asynchronous message
 * notification and bread first search. Each node in execution plan would be
 * triggered to run when the node itself receives all the messages that send by
 * its dependencies. And once the node is done running, it will send a message
 * to the nodes who is depend on it. A bread first search would be applied to
 * traverse the whole execution plan, and build the message notifiers according
 * to the previously described mechanism.
 */
class AsyncMsgNotifyBasedScheduler final : public Scheduler {
 public:
  explicit AsyncMsgNotifyBasedScheduler(QueryContext* qctx);

  folly::Future<Status> schedule() override;

  void waitFinish() override;

 private:
  folly::Future<Status> doSchedule(Executor* root) const;

  /**
   * futures: current executor will be triggered when all the futures are
   * notified. exe: current executor runner: a thread-pool promises: the
   * promises will be set a value which triggers the other executors if current
   * executor is done working.
   */
  folly::Future<Status> scheduleExecutor(std::vector<folly::Future<Status>>&& futures,
                                         Executor* exe,
                                         folly::Executor* runner) const;

  folly::Future<Status> runSelect(std::vector<folly::Future<Status>>&& futures,
                                  SelectExecutor* select,
                                  folly::Executor* runner) const;

  folly::Future<Status> runExecutor(std::vector<folly::Future<Status>>&& futures,
                                    Executor* exe,
                                    folly::Executor* runner) const;

  folly::Future<Status> runLeafExecutor(Executor* exe, folly::Executor* runner) const;

  folly::Future<Status> runLoop(std::vector<folly::Future<Status>>&& futures,
                                LoopExecutor* loop,
                                folly::Executor* runner) const;

  Status checkStatus(std::vector<Status>&& status) const;

  void notifyOK(std::vector<folly::Promise<Status>>& promises) const;

  void notifyError(std::vector<folly::Promise<Status>>& promises, Status status) const;

  folly::Future<Status> execute(Executor* executor) const;

  folly::Future<Status> runExecute(Executor* executor) const;

  void addExecuting(Executor* executor) const;

  void removeExecuting(Executor* executor) const;

  void setFailStatus(Status status) const;

  bool hasFailStatus() const;

  static std::string formatPrettyId(Executor* executor);

  static std::string formatPrettyDependencyTree(Executor* root);

  static void appendExecutor(size_t tabs, Executor* executor, std::stringstream& ss);

 private:
  // set if some Executor failed, then other Executors no need to do Executor::execute() further
  mutable std::mutex smtx_;
  mutable std::optional<Status> failedStatus_;
  mutable std::mutex emtx_;
  mutable std::condition_variable cv_;
  mutable size_t executing_{0};
  QueryContext* qctx_{nullptr};
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_SCHEDULER_ASYNCMSGNOTIFYBASEDSCHEDULER_H_
