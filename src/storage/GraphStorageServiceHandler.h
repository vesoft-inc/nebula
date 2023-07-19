/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_GRAPHSTORAGESERVICEHANDLER_H_
#define STORAGE_GRAPHSTORAGESERVICEHANDLER_H_

#include <folly/executors/IOThreadPoolExecutor.h>

#include "common/base/Base.h"
#include "interface/gen-cpp2/GraphStorageService.h"
#include "storage/CommonUtils.h"
#include "storage/StorageFlags.h"

namespace nebula {
namespace storage {

class StorageEnv;

class GraphStorageServiceHandler final : public cpp2::GraphStorageServiceSvIf {
 public:
  explicit GraphStorageServiceHandler(StorageEnv* env);
  // Vertice section
  folly::Future<cpp2::ExecResponse> future_addVertices(
      const cpp2::AddVerticesRequest& req) override;

  folly::Future<cpp2::ExecResponse> future_deleteVertices(
      const cpp2::DeleteVerticesRequest& req) override;

  folly::Future<cpp2::ExecResponse> future_deleteTags(const cpp2::DeleteTagsRequest& req) override;

  folly::Future<cpp2::UpdateResponse> future_updateVertex(
      const cpp2::UpdateVertexRequest& req) override;

  // Edge section
  folly::Future<cpp2::ExecResponse> future_addEdges(const cpp2::AddEdgesRequest& req) override;

  folly::Future<cpp2::ExecResponse> future_deleteEdges(
      const cpp2::DeleteEdgesRequest& req) override;

  folly::Future<cpp2::UpdateResponse> future_updateEdge(
      const cpp2::UpdateEdgeRequest& req) override;

  folly::Future<cpp2::GetNeighborsResponse> future_getNeighbors(
      const cpp2::GetNeighborsRequest& req) override;

  folly::Future<cpp2::GetDstBySrcResponse> future_getDstBySrc(
      const cpp2::GetDstBySrcRequest& req) override;

  folly::Future<cpp2::GetPropResponse> future_getProps(const cpp2::GetPropRequest& req) override;

  folly::Future<cpp2::LookupIndexResp> future_lookupIndex(
      const cpp2::LookupIndexRequest& req) override;

  folly::Future<cpp2::UpdateResponse> future_chainUpdateEdge(
      const cpp2::UpdateEdgeRequest& req) override;

  folly::Future<cpp2::ExecResponse> future_chainAddEdges(const cpp2::AddEdgesRequest& req) override;

  folly::Future<cpp2::ScanResponse> future_scanVertex(const cpp2::ScanVertexRequest& req) override;

  folly::Future<cpp2::ExecResponse> future_chainDeleteEdges(
      const cpp2::DeleteEdgesRequest& req) override;

  folly::Future<cpp2::ScanResponse> future_scanEdge(const cpp2::ScanEdgeRequest& req) override;

  folly::Future<cpp2::GetUUIDResp> future_getUUID(const cpp2::GetUUIDReq& req) override;

  folly::Future<cpp2::ExecResponse> future_put(const cpp2::KVPutRequest& req) override;

  folly::Future<cpp2::KVGetResponse> future_get(const cpp2::KVGetRequest& req) override;

  folly::Future<cpp2::ExecResponse> future_remove(const cpp2::KVRemoveRequest& req) override;

 private:
  StorageEnv* env_{nullptr};
  std::shared_ptr<folly::Executor> readerPool_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_GRAPHSTORAGESERVICEHANDLER_H_
