/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/maintain/CreateTagExecutor.h"
#include "planner/Maintain.h"
#include "service/ExecutionContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> CreateTagExecutor::execute() {
    return createTag().ensure([this]() { UNUSED(this); });
}

folly::Future<Status> CreateTagExecutor::createTag() {
    dumpLog();

    auto *ctNode = asNode<CreateTag>(node());
    return ectx()->getMetaClient()->createTagSchema(ctNode->space(),
            ctNode->getName(), ctNode->getSchema(), ctNode->getIfNotExists())
        .via(runner())
        .then([this](StatusOr<bool> resp) {
            if (!resp.ok()) {
                LOG(ERROR) << resp.status();
                return resp.status();
            }
            return Status::OK();
        });
}

}   // namespace graph
}   // namespace nebula
