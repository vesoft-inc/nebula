/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_GRAPHSTORAGESERVICEHANDLER_H_
#define STORAGE_GRAPHSTORAGESERVICEHANDLER_H_

#include "common/base/Base.h"
#include "common/interface/gen-cpp2/GraphStorageService.h"
#include <folly/executors/IOThreadPoolExecutor.h>
#include "storage/CommonUtils.h"
#include "storage/StorageFlags.h"

namespace nebula {
namespace storage {

class StorageEnv;

class GraphStorageServiceHandler final : public cpp2::GraphStorageServiceSvIf {
public:
    explicit GraphStorageServiceHandler(StorageEnv* env);

    // Vertice section
    folly::Future<cpp2::ExecResponse>
    future_addVertices(const cpp2::AddVerticesRequest& req) override;

    folly::Future<cpp2::ExecResponse>
    future_deleteVertices(const cpp2::DeleteVerticesRequest& req) override;

    folly::Future<cpp2::UpdateResponse>
    future_updateVertex(const cpp2::UpdateVertexRequest& req) override;

    // Edge section
    folly::Future<cpp2::ExecResponse>
    future_addEdges(const cpp2::AddEdgesRequest& req) override;

    folly::Future<cpp2::ExecResponse>
    future_deleteEdges(const cpp2::DeleteEdgesRequest& req) override;

    folly::Future<cpp2::UpdateResponse>
    future_updateEdge(const cpp2::UpdateEdgeRequest& req) override;

    folly::Future<cpp2::GetNeighborsResponse>
    future_getNeighbors(const cpp2::GetNeighborsRequest& req) override;

    folly::Future<cpp2::GetPropResponse>
    future_getProps(const cpp2::GetPropRequest& req) override;

    folly::Future<cpp2::LookupIndexResp>
    future_lookupIndex(const cpp2::LookupIndexRequest& req) override;

    folly::Future<cpp2::ExecResponse>
    future_addEdgesAtomic(const cpp2::AddEdgesRequest& req) override;

    folly::Future<cpp2::ScanVertexResponse>
    future_scanVertex(const cpp2::ScanVertexRequest& req) override;

    folly::Future<cpp2::ScanEdgeResponse>
    future_scanEdge(const cpp2::ScanEdgeRequest& req) override;

    folly::Future<cpp2::GetUUIDResp>
    future_getUUID(const cpp2::GetUUIDReq& req) override;

private:
    StorageEnv*                                     env_{nullptr};
    std::shared_ptr<folly::Executor>                readerPool_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_GRAPHSTORAGESERVICEHANDLER_H_
