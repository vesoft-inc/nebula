/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "MockStorageServiceHandler.h"


namespace nebula {
namespace graph {

#define GENERATE_ERROR_COMMON() \
    do { \
        storage::cpp2::ResponseCommon result; \
        std::vector<storage::cpp2::PartitionResult> partsCode; \
        storage::cpp2::PartitionResult partitionResult; \
        partitionResult.set_code(storage::cpp2::ErrorCode::E_UNKNOWN); \
        partsCode.emplace_back(std::move(partitionResult)); \
        result.set_failed_parts(partsCode); \
        resp.set_result(std::move(result)); \
    } while (false)

folly::Future<storage::cpp2::GetNeighborsResponse>
MockStorageServiceHandler::future_getNeighbors(const storage::cpp2::GetNeighborsRequest& req) {
    UNUSED(req);
    folly::Promise<storage::cpp2::GetNeighborsResponse> promise;
    auto future = promise.getFuture();
    return future;
}

folly::Future<storage::cpp2::GetPropResponse>
MockStorageServiceHandler::future_getProps(const storage::cpp2::GetPropRequest& req) {
    UNUSED(req);
    folly::Promise<storage::cpp2::GetPropResponse> promise;
    auto future = promise.getFuture();
    return future;
}

folly::Future<storage::cpp2::ExecResponse>
MockStorageServiceHandler::future_addVertices(const storage::cpp2::AddVerticesRequest& req) {
    folly::Promise<storage::cpp2::ExecResponse> promise;
    auto future = promise.getFuture();
    storage::cpp2::ExecResponse resp;
    auto status = storageCache_->addVertices(req);
    if (!status.ok()) {
        GENERATE_ERROR_COMMON();
    }
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<storage::cpp2::ExecResponse>
MockStorageServiceHandler::future_addEdges(const storage::cpp2::AddEdgesRequest& req) {
    folly::Promise<storage::cpp2::ExecResponse> promise;
    auto future = promise.getFuture();
    storage::cpp2::ExecResponse resp;
    auto status = storageCache_->addEdges(req);
    if (!status.ok()) {
        GENERATE_ERROR_COMMON();
    }
    promise.setValue(std::move(resp));
    return future;
}

folly::Future<storage::cpp2::ExecResponse>
MockStorageServiceHandler::future_deleteEdges(const storage::cpp2::DeleteEdgesRequest& req) {
    UNUSED(req);
    folly::Promise<storage::cpp2::ExecResponse> promise;
    auto future = promise.getFuture();
    return future;
}

folly::Future<storage::cpp2::ExecResponse>
MockStorageServiceHandler::future_deleteVertices(const storage::cpp2::DeleteVerticesRequest& req) {
    UNUSED(req);
    folly::Promise<storage::cpp2::ExecResponse> promise;
    auto future = promise.getFuture();
    return future;
}

folly::Future<storage::cpp2::UpdateResponse>
MockStorageServiceHandler::future_updateVertex(const storage::cpp2::UpdateVertexRequest& req) {
    UNUSED(req);
    folly::Promise<storage::cpp2::UpdateResponse> promise;
    auto future = promise.getFuture();
    return future;
}

folly::Future<storage::cpp2::UpdateResponse>
MockStorageServiceHandler::future_updateEdge(const storage::cpp2::UpdateEdgeRequest& req) {
    UNUSED(req);
    folly::Promise<storage::cpp2::UpdateResponse> promise;
    auto future = promise.getFuture();
    return future;
}

folly::Future<storage::cpp2::LookUpIndexResp>
MockStorageServiceHandler::future_lookUpVertexIndex(const storage::cpp2::LookUpIndexRequest& req) {
    UNUSED(req);
    folly::Promise<storage::cpp2::LookUpIndexResp> promise;
    auto future = promise.getFuture();
    return future;
}

folly::Future<storage::cpp2::LookUpIndexResp>
MockStorageServiceHandler::future_lookUpEdgeIndex(const storage::cpp2::LookUpIndexRequest& req) {
    UNUSED(req);
    folly::Promise<storage::cpp2::LookUpIndexResp> promise;
    auto future = promise.getFuture();
    return future;
}
}  // namespace graph
}  // namespace nebula

