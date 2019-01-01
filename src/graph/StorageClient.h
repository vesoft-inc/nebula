/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef GRAPH_STORAGECLIENT_H_
#define GRAPH_STORAGECLIENT_H_

#include "base/Base.h"
#include <folly/futures/Future.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include "gen-cpp2/StorageServiceAsyncClient.h"

namespace nebula {
namespace graph {

/**
 * A wrapper class for storage thrift API
 *
 * The class is NOT re-entriable
 */
class StorageClient final {
public:
    StorageClient(std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool);

    folly::SemiFuture<storage::cpp2::ExecResponse> addVertices(
        GraphSpaceID space,
        std::vector<storage::cpp2::Vertex> vertices,
        bool overwritable,
        folly::EventBase* evb = nullptr);

    folly::SemiFuture<storage::cpp2::ExecResponse> addEdges(
        GraphSpaceID space,
        std::vector<storage::cpp2::Edge> edges,
        bool overwritable,
        folly::EventBase* evb = nullptr);

    folly::SemiFuture<storage::cpp2::QueryResponse> getNeighbors(
        GraphSpaceID space,
        std::vector<VertexID> vertices,
        EdgeType edgeType,
        bool isOutBound,
        std::string filter,
        std::vector<storage::cpp2::PropDef> returnCols,
        folly::EventBase* evb = nullptr);

    folly::SemiFuture<storage::cpp2::QueryResponse> neighborStats(
        GraphSpaceID space,
        std::vector<VertexID> vertices,
        EdgeType edgeType,
        bool isOutBound,
        std::string filter,
        std::vector<storage::cpp2::PropStat> returnCols,
        folly::EventBase* evb = nullptr);

    folly::SemiFuture<storage::cpp2::QueryResponse> getVertexProps(
        GraphSpaceID space,
        std::vector<VertexID> vertices,
        std::vector<storage::cpp2::PropDef> returnCols,
        folly::EventBase* evb = nullptr);

    folly::SemiFuture<storage::cpp2::QueryResponse> getEdgeProps(
        GraphSpaceID space,
        std::vector<storage::cpp2::EdgeKey> edges,
        std::vector<storage::cpp2::PropDef> returnCols,
        folly::EventBase* evb = nullptr);


private:
    std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool_;

    template<class Request,
             class Context,
             class SingleResponseHandler,
             class AllDoneHandler>
    folly::SemiFuture<typename Context::ResponseType> collectResponse(
        folly::EventBase* evb,
        std::shared_ptr<Context> context,
        std::unordered_map<HostAddr, Request> requests,
        SingleResponseHandler&& singleRespHandler,
        AllDoneHandler&& allDoneHandler);

    template<class Request, class RemoteFunc, class RequestPartAccessor>
    folly::SemiFuture<storage::cpp2::ExecResponse> collectExecResponse(
        folly::EventBase* evb,
        std::unordered_map<HostAddr, Request> requests,
        RemoteFunc&& remoteFunc,
        RequestPartAccessor&& partAccessor);

    template<class Request, class RemoteFunc, class RequestPartAccessor>
    folly::SemiFuture<storage::cpp2::QueryResponse> collectQueryResponse(
        folly::EventBase* evb,
        std::unordered_map<HostAddr, Request> requests,
        RemoteFunc&& remoteFunc,
        RequestPartAccessor&& partAccessor);
};

}   // namespace graph
}   // namespace nebula

#include "graph/StorageClient.inl"

#endif  // GRAPH_STORAGECLIENT_H_

