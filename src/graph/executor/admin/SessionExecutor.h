/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_EXECUTOR_ADMIN_SESSIONEXECUTOR_H_
#define GRAPH_EXECUTOR_ADMIN_SESSIONEXECUTOR_H_

#include <folly/futures/Future.h>  // for Future
#include <stdint.h>                // for int64_t

#include <memory>  // for allocator

#include "common/datatypes/Date.h"       // for DateTime, DateTime::(anonymo...
#include "common/thrift/ThriftTypes.h"   // for SessionID
#include "common/time/TimeConversion.h"  // for TimeConversion
#include "common/time/TimeUtils.h"
#include "graph/executor/Executor.h"  // for Executor
#include "graph/service/RequestContext.h"

namespace nebula {
class Status;
namespace graph {
class PlanNode;
class QueryContext;
}  // namespace graph
namespace meta {
namespace cpp2 {
class Session;
}  // namespace cpp2
}  // namespace meta
struct DataSet;

class Status;
namespace meta {
namespace cpp2 {
class Session;
}  // namespace cpp2
}  // namespace meta
struct DataSet;

namespace graph {
class PlanNode;
class QueryContext;

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
