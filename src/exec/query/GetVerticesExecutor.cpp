/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/GetVerticesExecutor.h"

// common
#include "clients/storage/GraphStorageClient.h"
// graph
#include "planner/Query.h"
#include "service/ExecutionContext.h"

using nebula::storage::GraphStorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetPropResponse;

namespace nebula {
namespace graph {

folly::Future<Status> GetVerticesExecutor::execute() {
    return SingleInputExecutor::execute().then(cb([this](Status s) {
        if (!s.ok()) return error(std::move(s));
        return getVertices().ensure([this]() {
            // TODO(yee): some cleanup or stats actions
            UNUSED(this);
        });
    }));
}

folly::Future<Status> GetVerticesExecutor::getVertices() {
    dumpLog();

    // auto *gv = asNode<GetVertices>(node());
    // Expression *srcExpr = gv->src();
    // UNUSED(srcExpr);

    // std::vector<VertexID> vertices;
    // // TODO(yee): compute vertices by evaluate expression

    // GraphStorageClient *storageClient_ = ectx()->getStorageClient();

    // return storageClient_->getVertexProps(gv->space(), vertices, gv->props(), gv->filter())
    //     .via(runner())
    //     .then([this](StorageRpcResponse<VertexPropResponse> resp) {
    //         if (!resp.succeeded()) return Status::PartNotFound();
    //         resp.responses();
    //         nebula::Value value;
    //         finish(std::move(value));
    //         return Status::OK();
    //     });
    return start();
}

}   // namespace graph
}   // namespace nebula
