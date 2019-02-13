/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef STORAGE_CLIENT_STORAGECLIENT_H_
#define STORAGE_CLIENT_STORAGECLIENT_H_

#include "base/Base.h"
#include <folly/futures/Future.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include "gen-cpp2/StorageServiceAsyncClient.h"

namespace nebula {
namespace storage {

template<class Response>
class StorageRpcResponse final {
public:
    enum class Result {
        ALL_SUCCEEDED = 0,
        PARTIAL_SUCCEEDED = 1,
    };

    explicit StorageRpcResponse(size_t partsSent) : totalPartsSent_(partsSent) {}

    bool succeeded() const {
        return result_ == Result::ALL_SUCCEEDED;
    }

    int32_t maxLatency() const {
        return maxLatency_;
    }

    void setLatency(int32_t latency) {
        if (latency > maxLatency_) {
            maxLatency_ = latency;
        }
    }

    void markFailure() {
        result_ = Result::PARTIAL_SUCCEEDED;
    }

    // A value between [0, 100], representing a precentage
    int32_t completeness() const {
        return (totalPartsSent_ - failedParts_.size()) * 100 / totalPartsSent_;
    }

    std::unordered_map<PartitionID, storage::cpp2::ErrorCode>& failedParts() {
        return failedParts_;
    }

    std::vector<Response>& responses() {
        return responses_;
    }


private:
    const size_t totalPartsSent_;

    Result result_{Result::ALL_SUCCEEDED};
    std::unordered_map<PartitionID, storage::cpp2::ErrorCode> failedParts_;
    int32_t maxLatency_{0};
    std::vector<Response> responses_;
};


/**
 * A wrapper class for storage thrift API
 *
 * The class is NOT re-entriable
 */
class StorageClient final {
public:
    explicit StorageClient(std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool);

    folly::SemiFuture<StorageRpcResponse<storage::cpp2::ExecResponse>> addVertices(
        GraphSpaceID space,
        std::vector<storage::cpp2::Vertex> vertices,
        bool overwritable,
        folly::EventBase* evb = nullptr);

    folly::SemiFuture<StorageRpcResponse<storage::cpp2::ExecResponse>> addEdges(
        GraphSpaceID space,
        std::vector<storage::cpp2::Edge> edges,
        bool overwritable,
        folly::EventBase* evb = nullptr);

    folly::SemiFuture<StorageRpcResponse<storage::cpp2::QueryResponse>> getNeighbors(
        GraphSpaceID space,
        std::vector<VertexID> vertices,
        EdgeType edgeType,
        bool isOutBound,
        std::string filter,
        std::vector<storage::cpp2::PropDef> returnCols,
        folly::EventBase* evb = nullptr);

    folly::SemiFuture<StorageRpcResponse<storage::cpp2::QueryStatsResponse>> neighborStats(
        GraphSpaceID space,
        std::vector<VertexID> vertices,
        EdgeType edgeType,
        bool isOutBound,
        std::string filter,
        std::vector<storage::cpp2::PropDef> returnCols,
        folly::EventBase* evb = nullptr);

    folly::SemiFuture<StorageRpcResponse<storage::cpp2::QueryResponse>> getVertexProps(
        GraphSpaceID space,
        std::vector<VertexID> vertices,
        std::vector<storage::cpp2::PropDef> returnCols,
        folly::EventBase* evb = nullptr);

    folly::SemiFuture<StorageRpcResponse<storage::cpp2::EdgePropResponse>> getEdgeProps(
        GraphSpaceID space,
        std::vector<storage::cpp2::EdgeKey> edges,
        std::vector<storage::cpp2::PropDef> returnCols,
        folly::EventBase* evb = nullptr);


private:
    std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool_;

    template<class Request,
             class RemoteFunc,
             class Response =
                typename std::result_of<
                    RemoteFunc(storage::cpp2::StorageServiceAsyncClient*, const Request&)
                >::type::value_type
            >
    folly::SemiFuture<StorageRpcResponse<Response>> collectResponse(
        folly::EventBase* evb,
        std::unordered_map<HostAddr, Request> requests,
        RemoteFunc&& remoteFunc);
};

}   // namespace storage
}   // namespace nebula

#include "storage/client/StorageClient.inl"

#endif  // STORAGE_CLIENT_STORAGECLIENT_H_

