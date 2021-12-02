/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "GraphStorageLocalServer.h"

#include <unistd.h>

#include "common/base/Base.h"

namespace nebula::storage {

std::mutex mutex_;
std::shared_ptr<GraphStorageLocalServer> instance_ = nullptr;

void GraphStorageLocalServer::setThreadManager(
    std::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager) {
  // lock?
  threadManager_ = threadManager;
}

void GraphStorageLocalServer::setInterface(std::shared_ptr<GraphStorageServiceHandler> handler) {
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

folly::Future<::nebula::storage::cpp2::GetNeighborsResponse>
GraphStorageLocalServer::future_getNeighbors(
    const ::nebula::storage::cpp2::GetNeighborsRequest& request) {
  return folly::via(threadManager_.get(),
                    [this, &request]() { return handler_->future_getNeighbors(request); });
}

folly::Future<::nebula::storage::cpp2::ExecResponse> GraphStorageLocalServer::future_addVertices(
    const ::nebula::storage::cpp2::AddVerticesRequest& request) {
  return folly::via(threadManager_.get(),
                    [this, &request]() { return handler_->future_addVertices(request); });
}

folly::Future<::nebula::storage::cpp2::ExecResponse> GraphStorageLocalServer::future_chainAddEdges(
    const ::nebula::storage::cpp2::AddEdgesRequest& request) {
  return folly::via(threadManager_.get(),
                    [this, &request]() { return handler_->future_chainAddEdges(request); });
}

folly::Future<::nebula::storage::cpp2::ExecResponse> GraphStorageLocalServer::future_addEdges(
    const ::nebula::storage::cpp2::AddEdgesRequest& request) {
  return folly::via(threadManager_.get(),
                    [this, &request]() { return handler_->future_addEdges(request); });
}

folly::Future<::nebula::storage::cpp2::GetPropResponse> GraphStorageLocalServer::future_getProps(
    const ::nebula::storage::cpp2::GetPropRequest& request) {
  return folly::via(threadManager_.get(),
                    [this, &request]() { return handler_->future_getProps(request); });
}

folly::Future<::nebula::storage::cpp2::ExecResponse> GraphStorageLocalServer::future_deleteEdges(
    const ::nebula::storage::cpp2::DeleteEdgesRequest& request) {
  return folly::via(threadManager_.get(),
                    [this, &request]() { return handler_->future_deleteEdges(request); });
}

folly::Future<::nebula::storage::cpp2::ExecResponse> GraphStorageLocalServer::future_deleteVertices(
    const ::nebula::storage::cpp2::DeleteVerticesRequest& request) {
  return folly::via(threadManager_.get(),
                    [this, &request]() { return handler_->future_deleteVertices(request); });
}

folly::Future<::nebula::storage::cpp2::ExecResponse> GraphStorageLocalServer::future_deleteTags(
    const ::nebula::storage::cpp2::DeleteTagsRequest& request) {
  return folly::via(threadManager_.get(),
                    [this, &request]() { return handler_->future_deleteTags(request); });
}

folly::Future<::nebula::storage::cpp2::UpdateResponse> GraphStorageLocalServer::future_updateVertex(
    const ::nebula::storage::cpp2::UpdateVertexRequest& request) {
  return folly::via(threadManager_.get(),
                    [this, &request]() { return handler_->future_updateVertex(request); });
}

folly::Future<::nebula::storage::cpp2::UpdateResponse>
GraphStorageLocalServer::future_chainUpdateEdge(
    const ::nebula::storage::cpp2::UpdateEdgeRequest& request) {
  return folly::via(threadManager_.get(),
                    [this, &request]() { return handler_->future_chainUpdateEdge(request); });
}

folly::Future<::nebula::storage::cpp2::UpdateResponse> GraphStorageLocalServer::future_updateEdge(
    const ::nebula::storage::cpp2::UpdateEdgeRequest& request) {
  return folly::via(threadManager_.get(),
                    [this, &request]() { return handler_->future_updateEdge(request); });
}

folly::Future<::nebula::storage::cpp2::GetUUIDResp> GraphStorageLocalServer::future_getUUID(
    const ::nebula::storage::cpp2::GetUUIDReq& request) {
  return folly::via(threadManager_.get(),
                    [this, &request]() { return handler_->future_getUUID(request); });
}

folly::Future<::nebula::storage::cpp2::LookupIndexResp> GraphStorageLocalServer::future_lookupIndex(
    const ::nebula::storage::cpp2::LookupIndexRequest& request) {
  return folly::via(threadManager_.get(),
                    [this, &request]() { return handler_->future_lookupIndex(request); });
}

folly::Future<::nebula::storage::cpp2::GetNeighborsResponse>
GraphStorageLocalServer::future_lookupAndTraverse(
    const ::nebula::storage::cpp2::LookupAndTraverseRequest& request) {
  return folly::via(threadManager_.get(),
                    [this, &request]() { return handler_->future_lookupAndTraverse(request); });
}

folly::Future<::nebula::storage::cpp2::ScanVertexResponse>
GraphStorageLocalServer::future_scanVertex(
    const ::nebula::storage::cpp2::ScanVertexRequest& request) {
  return folly::via(threadManager_.get(),
                    [this, &request]() { return handler_->future_scanVertex(request); });
}

folly::Future<::nebula::storage::cpp2::ScanEdgeResponse> GraphStorageLocalServer::future_scanEdge(
    const ::nebula::storage::cpp2::ScanEdgeRequest& request) {
  return folly::via(threadManager_.get(),
                    [this, &request]() { return handler_->future_scanEdge(request); });
}

folly::Future<::nebula::storage::cpp2::KVGetResponse> GraphStorageLocalServer::future_get(
    const ::nebula::storage::cpp2::KVGetRequest& request) {
  return folly::via(threadManager_.get(),
                    [this, &request]() { return handler_->future_get(request); });
}

folly::Future<::nebula::storage::cpp2::ExecResponse> GraphStorageLocalServer::future_put(
    const ::nebula::storage::cpp2::KVPutRequest& request) {
  return folly::via(threadManager_.get(),
                    [this, &request]() { return handler_->future_put(request); });
}

folly::Future<::nebula::storage::cpp2::ExecResponse> GraphStorageLocalServer::future_remove(
    const ::nebula::storage::cpp2::KVRemoveRequest& request) {
  return folly::via(threadManager_.get(),
                    [this, &request]() { return handler_->future_remove(request); });
}

}  // namespace nebula::storage
