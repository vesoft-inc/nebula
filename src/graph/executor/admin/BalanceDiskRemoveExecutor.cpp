/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/executor/admin/BalanceDiskRemoveExecutor.h"

#include "graph/planner/plan/Admin.h"

namespace nebula {
namespace graph {

folly::Future<Status> BalanceDiskRemoveExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto *bdrNode = asNode<BalanceDiskRemove>(node());
  return qctx()
      ->getMetaClient()
      ->balanceDisk(bdrNode->targetHost(), bdrNode->paths(), true)
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
