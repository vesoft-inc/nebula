/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/update/InsertVerticesExecutor.h"
#include "planner/Maintain.h"
#include "service/QueryContext.h"

// common
#include "clients/meta/MetaClient.h"

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
        .then([this](StatusOr<bool> resp) {
            if (!resp.ok()) return resp.status();
            nebula::Value value;
            finish(std::move(value));
            return Status::OK();
        });
}

}   // namespace graph
}   // namespace nebula
