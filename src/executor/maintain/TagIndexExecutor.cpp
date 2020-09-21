/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/maintain/TagIndexExecutor.h"
#include "planner/Maintain.h"

namespace nebula {
namespace graph {

folly::Future<Status> CreateTagIndexExecutor::execute() {
    auto *ctiNode = asNode<CreateTagIndex>(node());
    auto spaceId = qctx()->rctx()->session()->space().id;
    return qctx()->getMetaClient()->createTagIndex(spaceId,
            ctiNode->getIndexName(), ctiNode->getSchemaName(),
            ctiNode->getFields(), ctiNode->getIfNotExists())
            .via(runner())
            .then([ctiNode, spaceId](StatusOr<IndexID> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << "SpaceId: " << spaceId
                               << ", Create index`" << ctiNode->getIndexName()
                               << " at tag: " << ctiNode->getSchemaName()
                               << "' failed: " << resp.status();
                    return resp.status();
                }
                return Status::OK();
            });
}

folly::Future<Status> DropTagIndexExecutor::execute() {
    return Status::OK();
}

folly::Future<Status> DescTagIndexExecutor::execute() {
    return Status::OK();
}

folly::Future<Status> RebuildTagIndexExecutor::execute() {
    return Status::OK();
}

}   // namespace graph
}   // namespace nebula
