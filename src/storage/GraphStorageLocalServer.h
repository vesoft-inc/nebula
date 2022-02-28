/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_GRAPHSTORAGELOCALSERVER_H
#define STORAGE_GRAPHSTORAGELOCALSERVER_H

#include <folly/fibers/Semaphore.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>

#include <boost/core/noncopyable.hpp>
#include <memory>

#include "common/base/Base.h"
#include "common/cpp/helpers.h"
#include "folly/fibers/Semaphore.h"
#include "interface/gen-cpp2/GraphStorageServiceAsyncClient.h"
namespace nebula::storage {
class GraphStorageLocalServer final : public boost::noncopyable, public nebula::cpp::NonMovable {
 public:
  static std::shared_ptr<GraphStorageLocalServer> getInstance() {
    static std::shared_ptr<GraphStorageLocalServer> instance{new GraphStorageLocalServer()};
    return instance;
  }
  void setThreadManager(std::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager);
  void setInterface(std::shared_ptr<apache::thrift::ServerInterface> handler);
  void stop();
  void serve();

 public:
  folly::Future<cpp2::GetNeighborsResponse> future_getNeighbors(
      const cpp2::GetNeighborsRequest& request);
  folly::Future<cpp2::ExecResponse> future_addVertices(const cpp2::AddVerticesRequest& request);
  folly::Future<cpp2::ExecResponse> future_chainAddEdges(const cpp2::AddEdgesRequest& request);
  folly::Future<cpp2::ExecResponse> future_addEdges(const cpp2::AddEdgesRequest& request);
  folly::Future<cpp2::GetPropResponse> future_getProps(const cpp2::GetPropRequest& request);
  folly::Future<cpp2::ExecResponse> future_deleteEdges(const cpp2::DeleteEdgesRequest& request);
  folly::Future<cpp2::ExecResponse> future_chainDeleteEdges(
      const cpp2::DeleteEdgesRequest& request);
  folly::Future<cpp2::ExecResponse> future_deleteVertices(
      const cpp2::DeleteVerticesRequest& request);
  folly::Future<cpp2::ExecResponse> future_deleteTags(const cpp2::DeleteTagsRequest& request);
  folly::Future<cpp2::UpdateResponse> future_updateVertex(const cpp2::UpdateVertexRequest& request);
  folly::Future<cpp2::UpdateResponse> future_chainUpdateEdge(
      const cpp2::UpdateEdgeRequest& request);
  folly::Future<cpp2::UpdateResponse> future_updateEdge(const cpp2::UpdateEdgeRequest& request);
  folly::Future<cpp2::GetUUIDResp> future_getUUID(const cpp2::GetUUIDReq& request);
  folly::Future<cpp2::LookupIndexResp> future_lookupIndex(const cpp2::LookupIndexRequest& request);
  folly::Future<cpp2::GetNeighborsResponse> future_lookupAndTraverse(
      const cpp2::LookupAndTraverseRequest& request);
  folly::Future<cpp2::ScanResponse> future_scanVertex(const cpp2::ScanVertexRequest& request);
  folly::Future<cpp2::ScanResponse> future_scanEdge(const cpp2::ScanEdgeRequest& request);
  folly::Future<cpp2::KVGetResponse> future_get(const cpp2::KVGetRequest& request);
  folly::Future<cpp2::ExecResponse> future_put(const cpp2::KVPutRequest& request);
  folly::Future<cpp2::ExecResponse> future_remove(const cpp2::KVRemoveRequest& request);

 private:
  GraphStorageLocalServer() = default;

 private:
  std::shared_ptr<apache::thrift::concurrency::ThreadManager> threadManager_;
  std::shared_ptr<apache::thrift::ServerInterface> handler_;
  folly::fibers::Semaphore sem_{0};
  static std::mutex mutex_;
  bool serving_ = {false};
};
}  // namespace nebula::storage
#endif
