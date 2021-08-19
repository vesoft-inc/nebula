/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/executor/admin/ShowMetaLeaderExecutor.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "graph/context/QueryContext.h"
#include "graph/planner/plan/Admin.h"
#include "graph/util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> ShowMetaLeaderExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto metaLeader = qctx()->getMetaClient()->getMetaLeader();
  LOG(INFO) << "messi Meta Leader: " << metaLeader.toString();
  DataSet ds({"Meta Leader"});
  nebula::Row r({metaLeader.toString()});
  ds.emplace_back(std::move(r));
  finish(ds);
  return Status::OK();
}

}  // namespace graph
}  // namespace nebula
