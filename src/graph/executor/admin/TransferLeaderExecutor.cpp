//
// Created by fujie on 24-10-21.
//

#include "TransferLeaderExecutor.h"

#include "clients/meta/MetaClient.h"
#include "common/time/ScopedTimer.h"
#include "graph/context/QueryContext.h"
#include "graph/planner/plan/Admin.h"
#include "graph/planner/plan/Query.h"
#include "graph/service/PermissionManager.h"
#include "interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace graph {

folly::Future<Status> TransferLeaderExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  return transferLeader();
}

folly::Future<Status> TransferLeaderExecutor::transferLeader() {
  auto *transferLeaderNode = asNode<TransferLeader>(node());
  auto spaceName = transferLeaderNode->spaceName();
  auto spaceIdResult = qctx()->getMetaClient()->getSpaceIdByNameFromCache(spaceName);
  NG_RETURN_IF_ERROR(spaceIdResult);
  auto spaceId = spaceIdResult.value();
  auto *session = qctx_->rctx()->session();
  NG_RETURN_IF_ERROR(PermissionManager::canWriteSpace(session));

  return qctx()
      ->getMetaClient()
      ->transferLeader(transferLeaderNode->address(), spaceId, transferLeaderNode->concurrency())
      .via(runner())
      .thenValue([this](StatusOr<bool> resp) {
        SCOPED_TIMER(&execTime_);
        NG_RETURN_IF_ERROR(resp);
        if (!resp.value()) {
          return Status::Error("Transfer leader failed!");
        }
        return Status::OK();
      });
}

}  // namespace graph
}  // namespace nebula