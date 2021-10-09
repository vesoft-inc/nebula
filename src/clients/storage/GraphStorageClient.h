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

/**
 * A wrapper class for GraphStorageServiceAsyncClient thrift API
 *
 * The class is NOT reentrant
 */
class GraphStorageClient : public StorageClientBase<cpp2::GraphStorageServiceAsyncClient> {
  FRIEND_TEST(StorageClientTest, LeaderChangeTest);

  using Parent = StorageClientBase<cpp2::GraphStorageServiceAsyncClient>;

 public:
  GraphStorageClient(std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool,
                     meta::MetaClient* metaClient)
      : Parent(ioThreadPool, metaClient) {}
  virtual ~GraphStorageClient() {}

  folly::SemiFuture<StorageRpcResponse<cpp2::GetNeighborsResponse>> getNeighbors(
      GraphSpaceID space,
      SessionID session,
      ExecutionPlanID plan,
      bool profile,
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
      const Expression* filter = nullptr,
      folly::EventBase* evb = nullptr);

  folly::SemiFuture<StorageRpcResponse<cpp2::GetPropResponse>> getProps(
      GraphSpaceID space,
      SessionID session,
      ExecutionPlanID plan,
      const DataSet& input,
      const std::vector<cpp2::VertexProp>* vertexProps,
      const std::vector<cpp2::EdgeProp>* edgeProps,
      const std::vector<cpp2::Expr>* expressions,
      bool dedup = false,
      const std::vector<cpp2::OrderBy>& orderBy = std::vector<cpp2::OrderBy>(),
      int64_t limit = std::numeric_limits<int64_t>::max(),
      const Expression* filter = nullptr,
      folly::EventBase* evb = nullptr);

  folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> addVertices(
      GraphSpaceID space,
      SessionID session,
      ExecutionPlanID plan,
      std::vector<cpp2::NewVertex> vertices,
      std::unordered_map<TagID, std::vector<std::string>> propNames,
      bool ifNotExists,
      folly::EventBase* evb = nullptr);

  folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> addEdges(
      GraphSpaceID space,
      SessionID session,
      ExecutionPlanID plan,
      std::vector<cpp2::NewEdge> edges,
      std::vector<std::string> propNames,
      bool ifNotExists,
      folly::EventBase* evb = nullptr,
      bool useToss = false);

  folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> deleteEdges(
      GraphSpaceID space,
      SessionID session,
      ExecutionPlanID plan,
      std::vector<storage::cpp2::EdgeKey> edges,
      folly::EventBase* evb = nullptr);

  folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> deleteVertices(
      GraphSpaceID space,
      SessionID session,
      ExecutionPlanID plan,
      std::vector<Value> ids,
      folly::EventBase* evb = nullptr);

  folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> deleteTags(
      GraphSpaceID space,
      SessionID session,
      ExecutionPlanID plan,
      std::vector<cpp2::DelTags> delTags,
      folly::EventBase* evb = nullptr);

  folly::Future<StatusOr<storage::cpp2::UpdateResponse>> updateVertex(
      GraphSpaceID space,
      SessionID session,
      ExecutionPlanID plan,
      Value vertexId,
      TagID tagId,
      std::vector<cpp2::UpdatedProp> updatedProps,
      bool insertable,
      std::vector<std::string> returnProps,
      std::string condition,
      folly::EventBase* evb = nullptr);

  folly::Future<StatusOr<storage::cpp2::UpdateResponse>> updateEdge(
      GraphSpaceID space,
      SessionID session,
      ExecutionPlanID plan,
      storage::cpp2::EdgeKey edgeKey,
      std::vector<cpp2::UpdatedProp> updatedProps,
      bool insertable,
      std::vector<std::string> returnProps,
      std::string condition,
      folly::EventBase* evb = nullptr,
      bool useExperimentalFeature = false);

  folly::Future<StatusOr<cpp2::GetUUIDResp>> getUUID(GraphSpaceID space,
                                                     const std::string& name,
                                                     folly::EventBase* evb = nullptr);

  folly::SemiFuture<StorageRpcResponse<cpp2::LookupIndexResp>> lookupIndex(
      GraphSpaceID space,
      SessionID session,
      ExecutionPlanID plan,
      bool profile,
      const std::vector<storage::cpp2::IndexQueryContext>& contexts,
      bool isEdge,
      int32_t tagOrEdge,
      const std::vector<std::string>& returnCols,
      int64_t limit,
      folly::EventBase* evb = nullptr);

  folly::SemiFuture<StorageRpcResponse<cpp2::GetNeighborsResponse>> lookupAndTraverse(
      GraphSpaceID space,
      SessionID session,
      ExecutionPlanID plan,
      cpp2::IndexSpec indexSpec,
      cpp2::TraverseSpec traverseSpec,
      folly::EventBase* evb = nullptr);

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

  cpp2::RequestCommon makeRequestCommon(SessionID sessionId,
                                        ExecutionPlanID planId,
                                        bool profile = false);
};

}  // namespace storage
}  // namespace nebula

#endif  // CLIENTS_STORAGE_GRAPHSTORAGECLIENT_H_
