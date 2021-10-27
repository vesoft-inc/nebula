/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CLIENTS_STORAGE_GRAPHSTORAGECLIENT_H_
#define CLIENTS_STORAGE_GRAPHSTORAGECLIENT_H_

#include <gtest/gtest_prod.h>

#include "clients/storage/StorageClientBase.h"
#include "common/base/Base.h"
#include "interface/gen-cpp2/GraphStorageServiceAsyncClient.h"

namespace nebula {
namespace storage {

template <typename T>
using StorageRpcRespFuture = folly::SemiFuture<StorageRpcResponse<T>>;

/**
 * A wrapper class for GraphStorageServiceAsyncClient thrift API
 *
 * The class is NOT reentrant
 */
class GraphStorageClient : public StorageClientBase<cpp2::GraphStorageServiceAsyncClient> {
  FRIEND_TEST(StorageClientTest, LeaderChangeTest);

 public:
  struct CommonRequestParam {
    GraphSpaceID space;
    SessionID session;
    ExecutionPlanID plan;
    bool profile{false};
    bool useExperimentalFeature{false};
    folly::EventBase* evb{nullptr};

    CommonRequestParam(GraphSpaceID space_,
                       SessionID sess,
                       ExecutionPlanID plan_,
                       bool profile_ = false,
                       bool experimental = false,
                       folly::EventBase* evb_ = nullptr);

    cpp2::RequestCommon toReqCommon() const;
  };

  GraphStorageClient(std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool,
                     meta::MetaClient* metaClient)
      : StorageClientBase<cpp2::GraphStorageServiceAsyncClient>(ioThreadPool, metaClient) {}
  virtual ~GraphStorageClient() {}

  StorageRpcRespFuture<cpp2::GetNeighborsResponse> getNeighbors(
      const CommonRequestParam& param,
      std::vector<std::string> colNames,
      // The first column has to be the VertexID
      const std::vector<Row>& vertices,
      const std::vector<EdgeType>& edgeTypes,
      cpp2::EdgeDirection edgeDirection,
      const std::vector<cpp2::StatProp>* statProps,
      const std::vector<cpp2::VertexProp>* vertexProps,
      const std::vector<cpp2::EdgeProp>* edgeProps,
      const std::vector<cpp2::Expr>* expressions,
      bool dedup = false,
      bool random = false,
      const std::vector<cpp2::OrderBy>& orderBy = std::vector<cpp2::OrderBy>(),
      int64_t limit = std::numeric_limits<int64_t>::max(),
      const Expression* filter = nullptr);

  StorageRpcRespFuture<cpp2::GetPropResponse> getProps(
      const CommonRequestParam& param,
      const DataSet& input,
      const std::vector<cpp2::VertexProp>* vertexProps,
      const std::vector<cpp2::EdgeProp>* edgeProps,
      const std::vector<cpp2::Expr>* expressions,
      bool dedup = false,
      const std::vector<cpp2::OrderBy>& orderBy = std::vector<cpp2::OrderBy>(),
      int64_t limit = std::numeric_limits<int64_t>::max(),
      const Expression* filter = nullptr);

  StorageRpcRespFuture<cpp2::ExecResponse> addVertices(
      const CommonRequestParam& param,
      std::vector<cpp2::NewVertex> vertices,
      std::unordered_map<TagID, std::vector<std::string>> propNames,
      bool ifNotExists);

  StorageRpcRespFuture<cpp2::ExecResponse> addEdges(const CommonRequestParam& param,
                                                    std::vector<cpp2::NewEdge> edges,
                                                    std::vector<std::string> propNames,
                                                    bool ifNotExists);

  StorageRpcRespFuture<cpp2::ExecResponse> deleteEdges(const CommonRequestParam& param,
                                                       std::vector<storage::cpp2::EdgeKey> edges);

  StorageRpcRespFuture<cpp2::ExecResponse> deleteVertices(const CommonRequestParam& param,
                                                          std::vector<Value> ids);

  StorageRpcRespFuture<cpp2::ExecResponse> deleteTags(const CommonRequestParam& param,
                                                      std::vector<cpp2::DelTags> delTags);

  folly::Future<StatusOr<storage::cpp2::UpdateResponse>> updateVertex(
      const CommonRequestParam& param,
      Value vertexId,
      TagID tagId,
      std::vector<cpp2::UpdatedProp> updatedProps,
      bool insertable,
      std::vector<std::string> returnProps,
      std::string condition);

  folly::Future<StatusOr<storage::cpp2::UpdateResponse>> updateEdge(
      const CommonRequestParam& param,
      storage::cpp2::EdgeKey edgeKey,
      std::vector<cpp2::UpdatedProp> updatedProps,
      bool insertable,
      std::vector<std::string> returnProps,
      std::string condition);

  folly::Future<StatusOr<cpp2::GetUUIDResp>> getUUID(GraphSpaceID space,
                                                     const std::string& name,
                                                     folly::EventBase* evb = nullptr);

  StorageRpcRespFuture<cpp2::LookupIndexResp> lookupIndex(
      const CommonRequestParam& param,
      const std::vector<storage::cpp2::IndexQueryContext>& contexts,
      bool isEdge,
      int32_t tagOrEdge,
      const std::vector<std::string>& returnCols,
      int64_t limit);

  StorageRpcRespFuture<cpp2::GetNeighborsResponse> lookupAndTraverse(
      const CommonRequestParam& param, cpp2::IndexSpec indexSpec, cpp2::TraverseSpec traverseSpec);

  folly::Future<StatusOr<cpp2::ScanEdgeResponse>> scanEdge(cpp2::ScanEdgeRequest req,
                                                           folly::EventBase* evb = nullptr);

  folly::Future<StatusOr<cpp2::ScanVertexResponse>> scanVertex(cpp2::ScanVertexRequest req,
                                                               folly::EventBase* evb = nullptr);

 private:
  StatusOr<std::function<const VertexID&(const Row&)>> getIdFromRow(GraphSpaceID space,
                                                                    bool isEdgeProps) const;

  StatusOr<std::function<const VertexID&(const cpp2::NewVertex&)>> getIdFromNewVertex(
      GraphSpaceID space) const;

  StatusOr<std::function<const VertexID&(const cpp2::NewEdge&)>> getIdFromNewEdge(
      GraphSpaceID space) const;

  StatusOr<std::function<const VertexID&(const cpp2::EdgeKey&)>> getIdFromEdgeKey(
      GraphSpaceID space) const;

  StatusOr<std::function<const VertexID&(const Value&)>> getIdFromValue(GraphSpaceID space) const;

  StatusOr<std::function<const VertexID&(const cpp2::DelTags&)>> getIdFromDelTags(
      GraphSpaceID space) const;
};

}  // namespace storage
}  // namespace nebula

#endif  // CLIENTS_STORAGE_GRAPHSTORAGECLIENT_H_
