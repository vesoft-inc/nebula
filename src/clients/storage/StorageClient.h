/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef CLIENTS_STORAGE_STORAGECLIENT_H
#define CLIENTS_STORAGE_STORAGECLIENT_H

#include "clients/storage/StorageClientBase.h"
#include "common/base/Base.h"
#include "common/thrift/ThriftClientManager.h"
#include "common/thrift/ThriftLocalClientManager.h"
#include "interface/gen-cpp2/GraphStorageServiceAsyncClient.h"
#include "storage/GraphStorageLocalServer.h"

namespace nebula {
namespace storage {

template <typename T>
using StorageRpcRespFuture = folly::SemiFuture<StorageRpcResponse<T>>;

/**
 * A wrapper class for GraphStorageServiceAsyncClient thrift API
 *
 * The class is NOT reentrant
 */
#ifndef BUILD_STANDALONE
using ThriftClientType = cpp2::GraphStorageServiceAsyncClient;
template <typename T>
using ThriftClientManType = thrift::ThriftClientManager<T>;
#else
using ThriftClientType = GraphStorageLocalServer;
template <typename T>
using ThriftClientManType = thrift::LocalClientManager<T>;

#endif
class StorageClient
    : public StorageClientBase<ThriftClientType, ThriftClientManType<ThriftClientType>> {
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

  StorageClient(std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool,
                meta::MetaClient* metaClient)
      : StorageClientBase<ThriftClientType, ThriftClientManType<ThriftClientType>>(ioThreadPool,
                                                                                   metaClient) {}
  virtual ~StorageClient() {}

  StorageRpcRespFuture<cpp2::GetNeighborsResponse> getNeighbors(
      const CommonRequestParam& param,
      std::vector<std::string> colNames,
      // The first column has to be the VertexID
      const std::vector<Value>& vids,
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
      const Expression* tagFilter = nullptr);

  StorageRpcRespFuture<cpp2::GetDstBySrcResponse> getDstBySrc(
      const CommonRequestParam& param,
      const std::vector<Value>& vertices,
      const std::vector<EdgeType>& edgeTypes);

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
      bool ifNotExists,
      bool ignoreExistedIndex);

  StorageRpcRespFuture<cpp2::ExecResponse> addEdges(const CommonRequestParam& param,
                                                    std::vector<cpp2::NewEdge> edges,
                                                    std::vector<std::string> propNames,
                                                    bool ifNotExists,
                                                    bool ignoreExistedIndex);

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
      std::vector<storage::cpp2::OrderBy> orderBy,
      int64_t limit);

  StorageRpcRespFuture<cpp2::GetNeighborsResponse> lookupAndTraverse(
      const CommonRequestParam& param, cpp2::IndexSpec indexSpec, cpp2::TraverseSpec traverseSpec);

  StorageRpcRespFuture<cpp2::ScanResponse> scanEdge(const CommonRequestParam& param,
                                                    const std::vector<cpp2::EdgeProp>& vertexProp,
                                                    int64_t limit,
                                                    const Expression* filter);

  StorageRpcRespFuture<cpp2::ScanResponse> scanVertex(
      const CommonRequestParam& param,
      const std::vector<cpp2::VertexProp>& vertexProp,
      int64_t limit,
      const Expression* filter);

  folly::SemiFuture<StorageRpcResponse<cpp2::KVGetResponse>> get(GraphSpaceID space,
                                                                 std::vector<std::string>&& keys,
                                                                 bool returnPartly = false,
                                                                 folly::EventBase* evb = nullptr);

  folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> put(GraphSpaceID space,
                                                                std::vector<KeyValue> kvs,
                                                                folly::EventBase* evb = nullptr);

  folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> remove(GraphSpaceID space,
                                                                   std::vector<std::string> keys,
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
#endif
