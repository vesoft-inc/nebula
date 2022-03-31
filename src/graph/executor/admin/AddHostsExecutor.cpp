// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/admin/AddHostsExecutor.h"

#include "graph/planner/plan/Admin.h"

namespace nebula {
namespace graph {

folly::Future<Status> AddHostsExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto *ahNode = asNode<AddHosts>(node());
  return qctx()
      ->getMetaClient()
      ->addHosts(ahNode->getHosts())
      .via(runner())
      .thenValue([this](StatusOr<bool> resp) {
        SCOPED_TIMER(&execTime_);
        NG_RETURN_IF_ERROR(resp);
        if (!resp.value()) {
          return Status::Error("Add Hosts failed!");
        }
        return Status::OK();
      });
}

}  // namespace graph
}  // namespace nebula
