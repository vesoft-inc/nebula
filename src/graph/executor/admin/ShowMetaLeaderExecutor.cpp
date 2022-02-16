/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/admin/ShowMetaLeaderExecutor.h"

#include <folly/Format.h>

#include <algorithm>
#include <string>
#include <utility>

#include "clients/meta/MetaClient.h"
#include "common/base/Status.h"
#include "common/datatypes/DataSet.h"
#include "common/datatypes/HostAddr.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Value.h"
#include "common/time/ScopedTimer.h"
#include "common/time/WallClock.h"
#include "graph/context/QueryContext.h"

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
