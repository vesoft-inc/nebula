/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <unistd.h>

#include "common/base/Base.h"
#include "storage/GraphStorageLocalServer.h"

#define LOCAL_RETURN_FUTURE(threadManager, respType, callFunc) \
  UNUSED(request);                                             \
  auto promise = std::make_shared<folly::Promise<respType>>(); \
  respType dummyResp;                                          \
  auto f = promise->getFuture();                               \
  promise->setValue(std::move(dummyResp));                     \
  return f;

namespace nebula::storage {

std::mutex mutex_;
std::shared_ptr<GraphStorageLocalServer> instance_ = nullptr;

void GraphStorageLocalServer::setThreadManager(
    std::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager) {
  threadManager_ = threadManager;
}

void GraphStorageLocalServer::setInterface(
    std::shared_ptr<apache::thrift::ServerInterface> handler) {
  handler_ = handler;
}

folly::Future<cpp2::GetNeighborsResponse> GraphStorageLocalServer::future_getNeighbors(
    const cpp2::GetNeighborsRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::GetNeighborsResponse, future_getNeighbors);
}

folly::Future<cpp2::GetDstBySrcResponse> GraphStorageLocalServer::future_getDstBySrc(
    const cpp2::GetDstBySrcRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::GetDstBySrcResponse, future_getDstBySrc);
}

folly::Future<cpp2::ExecResponse> GraphStorageLocalServer::future_addVertices(
    const cpp2::AddVerticesRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::ExecResponse, future_addVertices);
}

folly::Future<cpp2::ExecResponse> GraphStorageLocalServer::future_chainAddEdges(
    const cpp2::AddEdgesRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::ExecResponse, future_chainAddEdges);
}

folly::Future<cpp2::ExecResponse> GraphStorageLocalServer::future_addEdges(
    const cpp2::AddEdgesRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::ExecResponse, future_addEdges);
}

folly::Future<cpp2::GetPropResponse> GraphStorageLocalServer::future_getProps(
    const cpp2::GetPropRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::GetPropResponse, future_getProps);
}

folly::Future<cpp2::ExecResponse> GraphStorageLocalServer::future_deleteEdges(
    const cpp2::DeleteEdgesRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::ExecResponse, future_deleteEdges);
}

folly::Future<cpp2::ExecResponse> GraphStorageLocalServer::future_chainDeleteEdges(
    const cpp2::DeleteEdgesRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::ExecResponse, future_chainDeleteEdges);
}

folly::Future<cpp2::ExecResponse> GraphStorageLocalServer::future_deleteVertices(
    const cpp2::DeleteVerticesRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::ExecResponse, future_deleteVertices);
}

folly::Future<cpp2::ExecResponse> GraphStorageLocalServer::future_deleteTags(
    const cpp2::DeleteTagsRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::ExecResponse, future_deleteTags);
}

folly::Future<cpp2::UpdateResponse> GraphStorageLocalServer::future_updateVertex(
    const cpp2::UpdateVertexRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::UpdateResponse, future_updateVertex);
}

folly::Future<cpp2::UpdateResponse> GraphStorageLocalServer::future_chainUpdateEdge(
    const cpp2::UpdateEdgeRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::UpdateResponse, future_chainUpdateEdge);
}

folly::Future<cpp2::UpdateResponse> GraphStorageLocalServer::future_updateEdge(
    const cpp2::UpdateEdgeRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::UpdateResponse, future_updateEdge);
}

folly::Future<cpp2::GetUUIDResp> GraphStorageLocalServer::future_getUUID(
    const cpp2::GetUUIDReq& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::GetUUIDResp, future_getUUID);
}

folly::Future<cpp2::LookupIndexResp> GraphStorageLocalServer::future_lookupIndex(
    const cpp2::LookupIndexRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::LookupIndexResp, future_lookupIndex);
}

folly::Future<cpp2::GetNeighborsResponse> GraphStorageLocalServer::future_lookupAndTraverse(
    const cpp2::LookupAndTraverseRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::GetNeighborsResponse, future_lookupAndTraverse);
}

folly::Future<cpp2::ScanResponse> GraphStorageLocalServer::future_scanVertex(
    const cpp2::ScanVertexRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::ScanResponse, future_scanVertex);
}

folly::Future<cpp2::ScanResponse> GraphStorageLocalServer::future_scanEdge(
    const cpp2::ScanEdgeRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::ScanResponse, future_scanEdge);
}

folly::Future<cpp2::KVGetResponse> GraphStorageLocalServer::future_get(
    const cpp2::KVGetRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::KVGetResponse, future_get);
}

folly::Future<cpp2::ExecResponse> GraphStorageLocalServer::future_put(
    const cpp2::KVPutRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::ExecResponse, future_put);
}

folly::Future<cpp2::ExecResponse> GraphStorageLocalServer::future_remove(
    const cpp2::KVRemoveRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::ExecResponse, future_remove);
}

}  // namespace nebula::storage
