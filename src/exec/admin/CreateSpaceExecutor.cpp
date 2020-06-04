/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/admin/CreateSpaceExecutor.h"
#include "planner/Admin.h"
#include "context/QueryContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> CreateSpaceExecutor::execute() {
    return createSpace().ensure([this]() { UNUSED(this); });
}

folly::Future<Status> CreateSpaceExecutor::createSpace() {
    dumpLog();

    auto *csNode = asNode<CreateSpace>(node());
    return qctx()->getMetaClient()->createSpace(csNode->getSpaceDesc(), csNode->getIfNotExists())
        .via(runner())
        .then([](StatusOr<bool> resp) {
            if (!resp.ok()) {
                LOG(ERROR) << resp.status();
                return resp.status();
            }
            return Status::OK();
        });
}

}   // namespace graph
}   // namespace nebula
