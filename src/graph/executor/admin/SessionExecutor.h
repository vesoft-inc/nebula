// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_ADMIN_SESSIONEXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_SESSIONEXECUTOR_H_

#include "common/time/TimeUtils.h"
#include "graph/executor/Executor.h"

namespace nebula {
namespace graph {

class ShowSessionsExecutor final : public Executor {
 public:
  ShowSessionsExecutor(const PlanNode *node, QueryContext *ectx)
      : Executor("ShowSessionsExecutor", node, ectx) {}

  folly::Future<Status> execute() override;

 private:
  // List sessions in the cluster
  folly::Future<Status> listSessions();
  // List sessions in the current graph node
  folly::Future<Status> listLocalSessions();

  folly::Future<Status> getSession(SessionID sessionId);
  // Add session info into dataset
  void addSessions(const meta::cpp2::Session &session, DataSet &dataSet) const;

  DateTime microSecToDateTime(const int64_t microSec) const {
    auto dateTime = time::TimeConversion::unixSecondsToDateTime(microSec / 1000000);
    dateTime.microsec = microSec % 1000000;
    return dateTime;
  }
};

class UpdateSessionExecutor final : public Executor {
 public:
  UpdateSessionExecutor(const PlanNode *node, QueryContext *ectx)
      : Executor("UpdateSessionExecutor", node, ectx) {}

  folly::Future<Status> execute() override;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_ADMIN_SESSIONEXECUTOR_H_
