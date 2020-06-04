/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "exec/query/GetVerticesExecutor.h"

#include "common/clients/storage/GraphStorageClient.h"

#include "planner/Query.h"
#include "context/QueryContext.h"

using nebula::storage::GraphStorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetPropResponse;

namespace nebula {
namespace graph {

folly::Future<Status> GetVerticesExecutor::execute() {
    return getVertices().ensure([this]() {
        // TODO(yee): some cleanup or stats actions
        UNUSED(this);
    });
}

folly::Future<Status> GetVerticesExecutor::getVertices() {
    dumpLog();

    // auto *gv = asNode<GetVertices>(node());
    // Expression *srcExpr = gv->src();
    // UNUSED(srcExpr);

    // std::vector<VertexID> vertices;
    // // TODO(yee): compute vertices by evaluate expression

    // GraphStorageClient *storageClient_ = ectx_->getStorageClient();

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
