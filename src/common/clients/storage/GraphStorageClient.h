/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_CLIENTS_STORAGE_GRAPHSTORAGECLIENT_H_
#define COMMON_CLIENTS_STORAGE_GRAPHSTORAGECLIENT_H_

#include "common/base/Base.h"
#include <gtest/gtest_prod.h>
#include "common/interface/gen-cpp2/GraphStorageServiceAsyncClient.h"
#include "common/clients/storage/StorageClientBase.h"


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
        std::string filter = std::string(),
        folly::EventBase* evb = nullptr);

    folly::SemiFuture<StorageRpcResponse<cpp2::GetPropResponse>> getProps(
        GraphSpaceID space,
        const DataSet& input,
        const std::vector<cpp2::VertexProp>* vertexProps,
        const std::vector<cpp2::EdgeProp>* edgeProps,
        const std::vector<cpp2::Expr>* expressions,
        bool dedup = false,
        const std::vector<cpp2::OrderBy>& orderBy = std::vector<cpp2::OrderBy>(),
        int64_t limit = std::numeric_limits<int64_t>::max(),
        std::string filter = std::string(),
        folly::EventBase* evb = nullptr);

    folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> addVertices(
        GraphSpaceID space,
        std::vector<cpp2::NewVertex> vertices,
        std::unordered_map<TagID, std::vector<std::string>> propNames,
        bool overwritable,
        folly::EventBase* evb = nullptr);

    folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> addEdges(
        GraphSpaceID space,
        std::vector<cpp2::NewEdge> edges,
        std::vector<std::string> propNames,
        bool overwritable,
        folly::EventBase* evb = nullptr);

    folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> deleteEdges(
        GraphSpaceID space,
        std::vector<storage::cpp2::EdgeKey> edges,
        folly::EventBase* evb = nullptr);

    folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> deleteVertices(
        GraphSpaceID space,
        std::vector<VertexID> ids,
        folly::EventBase* evb = nullptr);

    folly::Future<StatusOr<storage::cpp2::UpdateResponse>> updateVertex(
        GraphSpaceID space,
        VertexID vertexId,
        TagID tagId,
        std::vector<cpp2::UpdatedProp> updatedProps,
        bool insertable,
        std::vector<std::string> returnProps,
        std::string condition,
        folly::EventBase* evb = nullptr);

    folly::Future<StatusOr<storage::cpp2::UpdateResponse>> updateEdge(
        GraphSpaceID space,
        storage::cpp2::EdgeKey edgeKey,
        std::vector<cpp2::UpdatedProp> updatedProps,
        bool insertable,
        std::vector<std::string> returnProps,
        std::string condition,
        folly::EventBase* evb = nullptr);

    folly::Future<StatusOr<cpp2::GetUUIDResp>> getUUID(
        GraphSpaceID space,
        const std::string& name,
        folly::EventBase* evb = nullptr);

    folly::SemiFuture<StorageRpcResponse<cpp2::LookupIndexResp>> lookupIndex(
        GraphSpaceID space,
        std::vector<storage::cpp2::IndexQueryContext> contexts,
        bool isEdge,
        int32_t tagOrEdge,
        std::vector<std::string> returnCols,
        folly::EventBase *evb = nullptr);

    folly::SemiFuture<StorageRpcResponse<cpp2::GetNeighborsResponse>> lookupAndTraverse(
        GraphSpaceID space,
        cpp2::IndexSpec indexSpec,
        cpp2::TraverseSpec traverseSpec,
        folly::EventBase* evb = nullptr);
};

}   // namespace storage
}   // namespace nebula

#endif  // COMMON_CLIENTS_STORAGE_GRAPHSTORAGECLIENT_H_
