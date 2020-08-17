/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/admin/SwitchSpaceExecutor.h"

#include "planner/Query.h"
#include "context/QueryContext.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> SwitchSpaceExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *spaceToNode = asNode<SwitchSpace>(node());
    auto spaceName = spaceToNode->getSpaceName();
    return qctx()->getMetaClient()->getSpace(spaceName)
            .via(runner())
            .then([spaceName, this](StatusOr<meta::cpp2::SpaceItem> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }
                auto spaceId = resp.value().get_space_id();
                qctx_->rctx()->session()->setSpace(spaceName, spaceId);
                LOG(INFO) << "Graph space switched to `" << spaceName
                          << "', space id: " << spaceId;
                return Status::OK();
            });
}

}   // namespace graph
}   // namespace nebula
