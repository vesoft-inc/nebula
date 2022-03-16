/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "GraphStorageLocalServer.h"

#include <folly/ExceptionWrapper.h>
#include <unistd.h>

#include "common/base/Base.h"
#include "storage/GraphStorageServiceHandler.h"

using folly::exception_wrapper;

#define LOCAL_RETURN_FUTURE(RespType, callFunc)                                                  \
  auto promise = std::make_shared<folly::Promise<RespType>>();                                   \
  auto f = promise->getSemiFuture();                                                             \
  threadManager_->add([this, promise, request] {                                                 \
    std::dynamic_pointer_cast<GraphStorageServiceHandler>(handler_)                              \
        ->callFunc(std::move(request))                                                           \
        .thenValue([promise](RespType&& resp) { promise->setValue(std::move(resp)); })           \
        .thenError([promise](exception_wrapper&& ex) { promise->setException(std::move(ex)); }); \
  });                                                                                            \
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
  LOCAL_RETURN_FUTURE(cpp2::GetNeighborsResponse, future_getNeighbors);
}

folly::SemiFuture<cpp2::ExecResponse> GraphStorageLocalServer::semifuture_addVertices(
    const cpp2::AddVerticesRequest& request) {
  LOCAL_RETURN_FUTURE(cpp2::ExecResponse, future_addVertices);
}

folly::SemiFuture<cpp2::ExecResponse> GraphStorageLocalServer::semifuture_chainAddEdges(
    const cpp2::AddEdgesRequest& request) {
  LOCAL_RETURN_FUTURE(cpp2::ExecResponse, future_chainAddEdges);
}

folly::SemiFuture<cpp2::ExecResponse> GraphStorageLocalServer::semifuture_addEdges(
    const cpp2::AddEdgesRequest& request) {
  LOCAL_RETURN_FUTURE(cpp2::ExecResponse, future_addEdges);
}

folly::SemiFuture<cpp2::GetPropResponse> GraphStorageLocalServer::semifuture_getProps(
    const cpp2::GetPropRequest& request) {
  LOCAL_RETURN_FUTURE(cpp2::GetPropResponse, future_getProps);
}

folly::SemiFuture<cpp2::ExecResponse> GraphStorageLocalServer::semifuture_deleteEdges(
    const cpp2::DeleteEdgesRequest& request) {
  LOCAL_RETURN_FUTURE(cpp2::ExecResponse, future_deleteEdges);
}

folly::SemiFuture<cpp2::ExecResponse> GraphStorageLocalServer::semifuture_chainDeleteEdges(
    const cpp2::DeleteEdgesRequest& request) {
  LOCAL_RETURN_FUTURE(cpp2::ExecResponse, future_chainDeleteEdges);
}

folly::SemiFuture<cpp2::ExecResponse> GraphStorageLocalServer::semifuture_deleteVertices(
    const cpp2::DeleteVerticesRequest& request) {
  LOCAL_RETURN_FUTURE(cpp2::ExecResponse, future_deleteVertices);
}

folly::SemiFuture<cpp2::ExecResponse> GraphStorageLocalServer::semifuture_deleteTags(
    const cpp2::DeleteTagsRequest& request) {
  LOCAL_RETURN_FUTURE(cpp2::ExecResponse, future_deleteTags);
}

folly::SemiFuture<cpp2::UpdateResponse> GraphStorageLocalServer::semifuture_updateVertex(
    const cpp2::UpdateVertexRequest& request) {
  LOCAL_RETURN_FUTURE(cpp2::UpdateResponse, future_updateVertex);
}

folly::SemiFuture<cpp2::UpdateResponse> GraphStorageLocalServer::semifuture_chainUpdateEdge(
    const cpp2::UpdateEdgeRequest& request) {
  LOCAL_RETURN_FUTURE(cpp2::UpdateResponse, future_chainUpdateEdge);
}

folly::SemiFuture<cpp2::UpdateResponse> GraphStorageLocalServer::semifuture_updateEdge(
    const cpp2::UpdateEdgeRequest& request) {
  LOCAL_RETURN_FUTURE(cpp2::UpdateResponse, future_updateEdge);
}

folly::SemiFuture<cpp2::GetUUIDResp> GraphStorageLocalServer::semifuture_getUUID(
    const cpp2::GetUUIDReq& request) {
  LOCAL_RETURN_FUTURE(cpp2::GetUUIDResp, future_getUUID);
}

folly::SemiFuture<cpp2::LookupIndexResp> GraphStorageLocalServer::semifuture_lookupIndex(
    const cpp2::LookupIndexRequest& request) {
  LOCAL_RETURN_FUTURE(cpp2::LookupIndexResp, future_lookupIndex);
}

folly::SemiFuture<cpp2::GetNeighborsResponse> GraphStorageLocalServer::semifuture_lookupAndTraverse(
    const cpp2::LookupAndTraverseRequest& request) {
  LOCAL_RETURN_FUTURE(cpp2::GetNeighborsResponse, future_lookupAndTraverse);
}

folly::SemiFuture<cpp2::ScanResponse> GraphStorageLocalServer::semifuture_scanVertex(
    const cpp2::ScanVertexRequest& request) {
  LOCAL_RETURN_FUTURE(cpp2::ScanResponse, future_scanVertex);
}

folly::SemiFuture<cpp2::ScanResponse> GraphStorageLocalServer::semifuture_scanEdge(
    const cpp2::ScanEdgeRequest& request) {
  LOCAL_RETURN_FUTURE(cpp2::ScanResponse, future_scanEdge);
}

folly::SemiFuture<cpp2::KVGetResponse> GraphStorageLocalServer::semifuture_get(
    const cpp2::KVGetRequest& request) {
  LOCAL_RETURN_FUTURE(cpp2::KVGetResponse, future_get);
}

folly::SemiFuture<cpp2::ExecResponse> GraphStorageLocalServer::semifuture_put(
    const cpp2::KVPutRequest& request) {
  LOCAL_RETURN_FUTURE(cpp2::ExecResponse, future_put);
}

folly::SemiFuture<cpp2::ExecResponse> GraphStorageLocalServer::semifuture_remove(
    const cpp2::KVRemoveRequest& request) {
  LOCAL_RETURN_FUTURE(cpp2::ExecResponse, future_remove);
}

}  // namespace nebula::storage
