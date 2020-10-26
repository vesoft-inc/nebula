/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/maintain/EdgeIndexExecutor.h"
#include "planner/Maintain.h"

namespace nebula {
namespace graph {

folly::Future<Status> CreateEdgeIndexExecutor::execute() {
    auto *ceiNode = asNode<CreateEdgeIndex>(node());
    auto spaceId = qctx()->rctx()->session()->space().id;
    return qctx()->getMetaClient()->createEdgeIndex(spaceId,
            ceiNode->getIndexName(), ceiNode->getSchemaName(),
            ceiNode->getFields(), ceiNode->getIfNotExists())
            .via(runner())
            .then([ceiNode, spaceId](StatusOr<IndexID> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << "SpaceId: " << spaceId
                               << ", Create index`" << ceiNode->getIndexName()
                               << " at edge: " << ceiNode->getSchemaName()
                               << "' failed: " << resp.status();
                    return resp.status();
                }
                return Status::OK();
            });
}

folly::Future<Status> DropEdgeIndexExecutor::execute() {
    // auto *deiNode = asNode<DropEdgeIndexNode>(node());
    // auto spaceId = qctx()->rctx()->session()->space();
    return Status::OK();
}

folly::Future<Status> DescEdgeIndexExecutor::execute() {
    // auto *deiNode = asNode<DescEdgeIndexNode>(node());
    // auto spaceId = qctx()->rctx()->session()->space();
    return Status::OK();
}

}   // namespace graph
}   // namespace nebula
