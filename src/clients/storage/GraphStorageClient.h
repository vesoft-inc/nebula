/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CLIENTS_STORAGE_GRAPHSTORAGECLIENT_H_
#define CLIENTS_STORAGE_GRAPHSTORAGECLIENT_H_

#include "base/Base.h"
#include <gtest/gtest_prod.h>
#include "interface/gen-cpp2/GraphStorageServiceAsyncClient.h"
#include "clients/storage/StorageClientBase.h"


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
        const std::vector<std::string>* vertexProps,
        const std::vector<std::string>* edgeProps,
        bool dedup = false,
        const std::vector<cpp2::OrderBy>& orderBy = std::vector<cpp2::OrderBy>(),
        int64_t limit = std::numeric_limits<int64_t>::max(),
        std::string filter = std::string(),
        folly::EventBase* evb = nullptr);

    folly::SemiFuture<StorageRpcResponse<cpp2::GetPropResponse>> getProps(
        GraphSpaceID space,
        std::vector<std::string> colNames,
        const std::vector<Row>& input,
        const std::vector<std::string>& props,
        bool dedup = false,
        const std::vector<cpp2::OrderBy>& orderBy = std::vector<cpp2::OrderBy>(),
        int64_t limit = std::numeric_limits<int64_t>::max(),
        std::string filter = std::string(),
        folly::EventBase* evb = nullptr);

    folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> addVertices(
        GraphSpaceID space,
        std::vector<cpp2::NewVertex> vertices,
        bool overwritable,
        folly::EventBase* evb = nullptr);

    folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> addEdges(
        GraphSpaceID space,
        std::vector<cpp2::NewEdge> edges,
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
        std::vector<cpp2::UpdatedVertexProp> updatedProps,
        bool insertable,
        std::vector<std::string> returnProps,
        std::string condition,
        folly::EventBase* evb = nullptr);

    folly::Future<StatusOr<storage::cpp2::UpdateResponse>> updateEdge(
        GraphSpaceID space,
        storage::cpp2::EdgeKey edgeKey,
        std::vector<cpp2::UpdatedEdgeProp> updatedProps,
        bool insertable,
        std::vector<std::string> returnProps,
        std::string condition,
        folly::EventBase* evb = nullptr);

    folly::Future<StatusOr<cpp2::GetUUIDResp>> getUUID(
        GraphSpaceID space,
        const std::string& name,
        folly::EventBase* evb = nullptr);

    folly::SemiFuture<StorageRpcResponse<cpp2::LookUpIndexResp>>
    lookUpVertexIndex(
        GraphSpaceID space,
        IndexID indexId,
        std::string filter,
        std::vector<std::string> returnCols,
        folly::EventBase *evb = nullptr);

    folly::SemiFuture<StorageRpcResponse<cpp2::LookUpIndexResp>>
    lookUpEdgeIndex(
        GraphSpaceID space,
        IndexID indexId,
        std::string filter,
        std::vector<std::string> returnCols,
        folly::EventBase *evb = nullptr);
};

}   // namespace storage
}   // namespace nebula

#endif  // CLIENTS_STORAGE_GRAPHSTORAGECLIENT_H_
