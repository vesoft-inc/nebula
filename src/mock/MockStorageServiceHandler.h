/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#ifndef EXECUTOR_MOCKSTORAGESERVICEHANDLER_H_
#define EXECUTOR_MOCKSTORAGESERVICEHANDLER_H_

#include "common/base/Base.h"
#include "common/interface/gen-cpp2/GraphStorageService.h"
#include "mock/StorageCache.h"
#include <folly/futures/Promise.h>
#include <folly/futures/Future.h>

namespace nebula {
namespace graph {

class MockStorageServiceHandler final : public storage::cpp2::GraphStorageServiceSvIf {
public:
    explicit MockStorageServiceHandler(uint16_t metaPort) {
        storageCache_ = std::make_unique<StorageCache>(metaPort);
    }

    ~MockStorageServiceHandler() = default;

    folly::Future<storage::cpp2::GetNeighborsResponse>
    future_getNeighbors(const storage::cpp2::GetNeighborsRequest& req) override;

    folly::Future<storage::cpp2::GetPropResponse>
    future_getProps(const storage::cpp2::GetPropRequest& req) override;

    folly::Future<storage::cpp2::ExecResponse>
    future_addVertices(const storage::cpp2::AddVerticesRequest& req) override;

    folly::Future<storage::cpp2::ExecResponse>
    future_addEdges(const storage::cpp2::AddEdgesRequest& req) override;

    folly::Future<storage::cpp2::ExecResponse>
    future_deleteEdges(const storage::cpp2::DeleteEdgesRequest& req) override;

    folly::Future<storage::cpp2::ExecResponse>
    future_deleteVertices(const storage::cpp2::DeleteVerticesRequest& req) override;

    folly::Future<storage::cpp2::UpdateResponse>
    future_updateVertex(const storage::cpp2::UpdateVertexRequest& req) override;

    folly::Future<storage::cpp2::UpdateResponse>
    future_updateEdge(const storage::cpp2::UpdateEdgeRequest& req) override;

    folly::Future<storage::cpp2::LookupIndexResp>
    future_lookupIndex(const storage::cpp2::LookupIndexRequest& req) override;

private:
    std::unique_ptr<StorageCache>       storageCache_;
};
}  // namespace graph
}  // namespace nebula
#endif  // EXECUTOR_MOCKSTORAGESERVICEHANDLER_H_
