// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/admin/ShowMetaLeaderExecutor.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "common/time/TimeUtils.h"
#include "graph/planner/plan/Admin.h"

namespace nebula {
namespace graph {

folly::Future<Status> ShowMetaLeaderExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto metaLeader = qctx()->getMetaClient()->getMetaLeader();
  auto now = time::WallClock::fastNowInMilliSec();
  auto lastHeartBeatTime = qctx()->getMetaClient()->HeartbeatTime();
  auto intervalMs = now - lastHeartBeatTime;
  DataSet ds({"Meta Leader", "secs from last heart beat"});
  auto strLeader = folly::sformat("{}:{}", metaLeader.host, metaLeader.port);
  nebula::Row r({std::move(strLeader), intervalMs / 1000});
  ds.emplace_back(std::move(r));
  finish(ds);
  return Status::OK();
}

}  // namespace graph
}  // namespace nebula
