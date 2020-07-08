/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "exec/maintain/AlterSchemaExecutor.h"
#include "planner/Maintain.h"
#include "context/QueryContext.h"

namespace nebula {
namespace graph {

folly::Future<Status> AlterTagExecutor::execute() {
    dumpLog();

    auto *aeNode = asNode<AlterTag>(node());
    return qctx()->getMetaClient()->alterTagSchema(aeNode->space(),
                                                   aeNode->getName(),
                                                   aeNode->getSchemaItems(),
                                                   aeNode->getSchemaProp())
            .via(runner())
            .then([](StatusOr<TagID> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }
                return Status::OK();
            });
}

folly::Future<Status> AlterEdgeExecutor::execute() {
    dumpLog();

    auto *aeNode = asNode<AlterEdge>(node());
    return qctx()->getMetaClient()->alterEdgeSchema(aeNode->space(),
                                                    aeNode->getName(),
                                                    aeNode->getSchemaItems(),
                                                    aeNode->getSchemaProp())
            .via(runner())
            .then([](StatusOr<EdgeType> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }
                return Status::OK();
            });
}

}   // namespace graph
}   // namespace nebula
