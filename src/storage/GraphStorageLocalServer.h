/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include <folly/fibers/Semaphore.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>

#include <memory>

#include "common/base/Base.h"
#include "folly/fibers/Semaphore.h"
#include "interface/gen-cpp2/GraphStorageServiceAsyncClient.h"
#include "storage/GraphStorageServiceHandler.h"

namespace nebula::storage {
class GraphStorageLocalServer final : public nebula::cpp::NonCopyable,
                                      public nebula::cpp::NonMovable {
 public:
  static std::shared_ptr<GraphStorageLocalServer> getInstance() {
    static std::shared_ptr<GraphStorageLocalServer> instance{new GraphStorageLocalServer()};
    return instance;
  }
  void setThreadManager(std::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager);
  void setInterface(std::shared_ptr<GraphStorageServiceHandler> iface);
  void stop();
  void serve();

 public:
  folly::Future<::nebula::storage::cpp2::GetNeighborsResponse> future_getNeighbors(
      const ::nebula::storage::cpp2::GetNeighborsRequest& request);
  folly::Future<::nebula::storage::cpp2::ExecResponse> future_addVertices(
      const ::nebula::storage::cpp2::AddVerticesRequest& request);
  folly::Future<::nebula::storage::cpp2::ExecResponse> future_chainAddEdges(
      const ::nebula::storage::cpp2::AddEdgesRequest& request);
  folly::Future<::nebula::storage::cpp2::ExecResponse> future_addEdges(
      const ::nebula::storage::cpp2::AddEdgesRequest& request);
  folly::Future<::nebula::storage::cpp2::GetPropResponse> future_getProps(
      const ::nebula::storage::cpp2::GetPropRequest& request);
  folly::Future<::nebula::storage::cpp2::ExecResponse> future_deleteEdges(
      const ::nebula::storage::cpp2::DeleteEdgesRequest& request);
  folly::Future<::nebula::storage::cpp2::ExecResponse> future_deleteVertices(
      const ::nebula::storage::cpp2::DeleteVerticesRequest& request);
  folly::Future<::nebula::storage::cpp2::ExecResponse> future_deleteTags(
      const ::nebula::storage::cpp2::DeleteTagsRequest& request);
  folly::Future<::nebula::storage::cpp2::UpdateResponse> future_updateVertex(
      const ::nebula::storage::cpp2::UpdateVertexRequest& request);
  folly::Future<::nebula::storage::cpp2::UpdateResponse> future_chainUpdateEdge(
      const ::nebula::storage::cpp2::UpdateEdgeRequest& request);
  folly::Future<::nebula::storage::cpp2::UpdateResponse> future_updateEdge(
      const ::nebula::storage::cpp2::UpdateEdgeRequest& request);
  folly::Future<::nebula::storage::cpp2::GetUUIDResp> future_getUUID(
      const ::nebula::storage::cpp2::GetUUIDReq& request);
  folly::Future<::nebula::storage::cpp2::LookupIndexResp> future_lookupIndex(
      const ::nebula::storage::cpp2::LookupIndexRequest& request);
  folly::Future<::nebula::storage::cpp2::GetNeighborsResponse> future_lookupAndTraverse(
      const ::nebula::storage::cpp2::LookupAndTraverseRequest& request);
  folly::Future<::nebula::storage::cpp2::ScanVertexResponse> future_scanVertex(
      const ::nebula::storage::cpp2::ScanVertexRequest& request);
  folly::Future<::nebula::storage::cpp2::ScanEdgeResponse> future_scanEdge(
      const ::nebula::storage::cpp2::ScanEdgeRequest& request);

 private:
  GraphStorageLocalServer() = default;

 private:
  std::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager_;
  std::shared_ptr<GraphStorageServiceHandler> handler_;
  folly::fibers::Semaphore sem_{0};
  static std::mutex mutex_;
  bool serving_ = {false};
};
}  // namespace nebula::storage
