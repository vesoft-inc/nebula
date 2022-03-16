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
  auto f = promise->getSemiFuture();                           \
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

void GraphStorageLocalServer::serve() {
  if (serving_) {
    LOG(WARNING) << "Server already serving";
    return;
  }
  // do nothing, wait stop
  serving_ = true;
  sem_.wait();
}

void GraphStorageLocalServer::stop() {
  if (!serving_) {
    LOG(WARNING) << "Can't stop server not serving";
    return;
  }
  sem_.signal();
  serving_ = false;
}

folly::SemiFuture<cpp2::GetNeighborsResponse> GraphStorageLocalServer::semifuture_getNeighbors(
    const cpp2::GetNeighborsRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::GetNeighborsResponse, semifuture_getNeighbors);
}

folly::SemiFuture<cpp2::ExecResponse> GraphStorageLocalServer::semifuture_addVertices(
    const cpp2::AddVerticesRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::ExecResponse, semifuture_addVertices);
}

folly::SemiFuture<cpp2::ExecResponse> GraphStorageLocalServer::semifuture_chainAddEdges(
    const cpp2::AddEdgesRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::ExecResponse, semifuture_chainAddEdges);
}

folly::SemiFuture<cpp2::ExecResponse> GraphStorageLocalServer::semifuture_addEdges(
    const cpp2::AddEdgesRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::ExecResponse, semifuture_addEdges);
}

folly::SemiFuture<cpp2::GetPropResponse> GraphStorageLocalServer::semifuture_getProps(
    const cpp2::GetPropRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::GetPropResponse, semifuture_getProps);
}

folly::SemiFuture<cpp2::ExecResponse> GraphStorageLocalServer::semifuture_deleteEdges(
    const cpp2::DeleteEdgesRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::ExecResponse, semifuture_deleteEdges);
}

folly::SemiFuture<cpp2::ExecResponse> GraphStorageLocalServer::semifuture_chainDeleteEdges(
    const cpp2::DeleteEdgesRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::ExecResponse, semifuture_chainDeleteEdges);
}

folly::SemiFuture<cpp2::ExecResponse> GraphStorageLocalServer::semifuture_deleteVertices(
    const cpp2::DeleteVerticesRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::ExecResponse, semifuture_deleteVertices);
}

folly::SemiFuture<cpp2::ExecResponse> GraphStorageLocalServer::semifuture_deleteTags(
    const cpp2::DeleteTagsRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::ExecResponse, semifuture_deleteTags);
}

folly::SemiFuture<cpp2::UpdateResponse> GraphStorageLocalServer::semifuture_updateVertex(
    const cpp2::UpdateVertexRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::UpdateResponse, semifuture_updateVertex);
}

folly::SemiFuture<cpp2::UpdateResponse> GraphStorageLocalServer::semifuture_chainUpdateEdge(
    const cpp2::UpdateEdgeRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::UpdateResponse, semifuture_chainUpdateEdge);
}

folly::SemiFuture<cpp2::UpdateResponse> GraphStorageLocalServer::semifuture_updateEdge(
    const cpp2::UpdateEdgeRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::UpdateResponse, semifuture_updateEdge);
}

folly::SemiFuture<cpp2::GetUUIDResp> GraphStorageLocalServer::semifuture_getUUID(
    const cpp2::GetUUIDReq& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::GetUUIDResp, semifuture_getUUID);
}

folly::SemiFuture<cpp2::LookupIndexResp> GraphStorageLocalServer::semifuture_lookupIndex(
    const cpp2::LookupIndexRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::LookupIndexResp, semifuture_lookupIndex);
}

folly::SemiFuture<cpp2::GetNeighborsResponse> GraphStorageLocalServer::semifuture_lookupAndTraverse(
    const cpp2::LookupAndTraverseRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::GetNeighborsResponse, semifuture_lookupAndTraverse);
}

folly::SemiFuture<cpp2::ScanResponse> GraphStorageLocalServer::semifuture_scanVertex(
    const cpp2::ScanVertexRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::ScanResponse, semifuture_scanVertex);
}

folly::SemiFuture<cpp2::ScanResponse> GraphStorageLocalServer::semifuture_scanEdge(
    const cpp2::ScanEdgeRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::ScanResponse, semifuture_scanEdge);
}

folly::SemiFuture<cpp2::KVGetResponse> GraphStorageLocalServer::semifuture_get(
    const cpp2::KVGetRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::KVGetResponse, semifuture_get);
}

folly::SemiFuture<cpp2::ExecResponse> GraphStorageLocalServer::semifuture_put(
    const cpp2::KVPutRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::ExecResponse, semifuture_put);
}

folly::SemiFuture<cpp2::ExecResponse> GraphStorageLocalServer::semifuture_remove(
    const cpp2::KVRemoveRequest& request) {
  LOCAL_RETURN_FUTURE(threadManager_, cpp2::ExecResponse, semifuture_remove);
}

}  // namespace nebula::storage
