/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/admin/BalanceDiskAttachExecutor.h"

#include "graph/planner/plan/Admin.h"

namespace nebula {
namespace graph {

folly::Future<Status> BalanceDiskAttachExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto *bdaNode = asNode<BalanceDiskAttach>(node());
  return qctx()
      ->getMetaClient()
      ->balanceDisk(bdaNode->targetHost(), bdaNode->paths(), false)
      .via(runner())
      .thenValue([this](StatusOr<int64_t> resp) {
        SCOPED_TIMER(&execTime_);
        if (!resp.ok()) {
          LOG(ERROR) << resp.status();
          return resp.status();
        }
        DataSet v({"ID"});
        v.emplace_back(Row({resp.value()}));
        return finish(std::move(v));
      });
}

}  // namespace graph
}  // namespace nebula
