/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/maintain/CreateTagExecutor.h"
#include "planner/Maintain.h"
#include "context/QueryContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> CreateTagExecutor::execute() {
    return createTag().ensure([this]() { UNUSED(this); });
}

folly::Future<Status> CreateTagExecutor::createTag() {
    dumpLog();

    auto space = qctx_->rctx()->session()->space();
    auto *ctNode = asNode<CreateTag>(node());
    return qctx()->getMetaClient()->createTagSchema(space,
            ctNode->getName(), ctNode->getSchema(), ctNode->getIfNotExists())
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
