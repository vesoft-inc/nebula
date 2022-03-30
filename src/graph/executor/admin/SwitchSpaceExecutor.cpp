// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/admin/SwitchSpaceExecutor.h"

#include "clients/meta/MetaClient.h"
#include "graph/planner/plan/Query.h"
#include "graph/service/PermissionManager.h"
#include "interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace graph {

folly::Future<Status> SwitchSpaceExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto *spaceToNode = asNode<SwitchSpace>(node());
  auto spaceName = spaceToNode->getSpaceName();
  return qctx()->getMetaClient()->getSpace(spaceName).via(runner()).thenValue(
      [spaceName, this](StatusOr<meta::cpp2::SpaceItem> resp) {
        if (!resp.ok()) {
          LOG(WARNING) << "Switch space :`" << spaceName << "' fail: " << resp.status();
          return resp.status();
        }

        auto spaceId = resp.value().get_space_id();
        auto *session = qctx_->rctx()->session();
        NG_RETURN_IF_ERROR(PermissionManager::canReadSpace(session, spaceId));
        const auto &properties = resp.value().get_properties();

        SpaceInfo spaceInfo;
        spaceInfo.id = spaceId;
        spaceInfo.name = spaceName;
        spaceInfo.spaceDesc = std::move(properties);
        qctx_->rctx()->session()->setSpace(std::move(spaceInfo));
        LOG(INFO) << "Graph switched to `" << spaceName << "', space id: " << spaceId;
        return Status::OK();
      });
}

}  // namespace graph
}  // namespace nebula
