/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CLIENTS_STORAGE_GRAPHSTORAGECLIENT_H_
#define CLIENTS_STORAGE_GRAPHSTORAGECLIENT_H_

#include "base/Base.h"
#include <gtest/gtest_prod.h>
#include "gen-cpp2/GraphStorageServiceAsyncClient.h"
#include "clients/storage/StorageClientBase.h"

namespace nebula {
namespace storage {

/**
 * A wrapper class for GraphStorageServiceAsyncClient thrift API
 *
 * The class is NOT reenterable
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
        std::vector<VertexID> vertices,
        std::vector<EdgeType> edgeTypes,
        std::vector<cpp2::VertexProp> vertexProps,
        std::vector<cpp2::EdgeProp> edgeProps,
        std::vector<cpp2::StatProp> statProps,
        std::string filter,
        folly::EventBase* evb = nullptr);

    folly::SemiFuture<StorageRpcResponse<cpp2::VertexPropResponse>> getVertexProps(
        GraphSpaceID space,
        std::vector<VertexID> vertices,
        std::vector<cpp2::VertexProp> props,
        std::string filter,
        folly::EventBase* evb = nullptr);

    folly::SemiFuture<StorageRpcResponse<cpp2::EdgePropResponse>> getEdgeProps(
        GraphSpaceID space,
        std::vector<cpp2::EdgeKey> edges,
        std::vector<cpp2::EdgeProp> props,
        std::string filter,
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
        std::vector<cpp2::VertexProp> returnProps,
        std::string condition,
        folly::EventBase* evb = nullptr);

    folly::Future<StatusOr<storage::cpp2::UpdateResponse>> updateEdge(
        GraphSpaceID space,
        storage::cpp2::EdgeKey edgeKey,
        std::vector<cpp2::UpdatedEdgeProp> updatedProps,
        bool insertable,
        std::vector<cpp2::EdgeProp> returnProps,
        std::string condition,
        folly::EventBase* evb = nullptr);

    folly::Future<StatusOr<cpp2::GetUUIDResp>> getUUID(
        GraphSpaceID space,
        const std::string& name,
        folly::EventBase* evb = nullptr);
};

}   // namespace storage
}   // namespace nebula

#endif  // CLIENTS_STORAGE_GRAPHSTORAGECLIENT_H_
