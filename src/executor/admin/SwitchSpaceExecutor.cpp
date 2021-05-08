/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/admin/SwitchSpaceExecutor.h"

#include "service/PermissionManager.h"
#include "planner/plan/Query.h"
#include "common/clients/meta/MetaClient.h"
#include "common/interface/gen-cpp2/meta_types.h"
#include "context/QueryContext.h"
#include "planner/plan/Query.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> SwitchSpaceExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *spaceToNode = asNode<SwitchSpace>(node());
    auto spaceName = spaceToNode->getSpaceName();
    return qctx()->getMetaClient()->getSpace(spaceName)
            .via(runner())
            .thenValue([spaceName, this](StatusOr<meta::cpp2::SpaceItem> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }

                auto spaceId = resp.value().get_space_id();
                auto *session = qctx_->rctx()->session();
                NG_RETURN_IF_ERROR(PermissionManager::canReadSpace(session, spaceId));
                const auto& properties = resp.value().get_properties();

                SpaceInfo spaceInfo;
                spaceInfo.id = spaceId;
                spaceInfo.name = spaceName;
                spaceInfo.spaceDesc = std::move(properties);
                qctx_->rctx()->session()->setSpace(std::move(spaceInfo));
                LOG(INFO) << "Graph switched to `" << spaceName
                          << "', space id: " << spaceId;
                return Status::OK();
            });
}

}   // namespace graph
}   // namespace nebula
