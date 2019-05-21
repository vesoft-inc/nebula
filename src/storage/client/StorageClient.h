/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_CLIENT_STORAGECLIENT_H_
#define STORAGE_CLIENT_STORAGECLIENT_H_

#include "base/Base.h"
#include <folly/futures/Future.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include "gen-cpp2/StorageServiceAsyncClient.h"
#include "meta/client/MetaClient.h"
#include "thrift/ThriftClientManager.h"

namespace nebula {
namespace storage {

template<class Response>
class StorageRpcResponse final {
public:
    enum class Result {
        ALL_SUCCEEDED = 0,
        PARTIAL_SUCCEEDED = 1,
    };

    explicit StorageRpcResponse(size_t reqsSent) : totalReqsSent_(reqsSent) {}

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
        ++failedReqs_;
    }

    // A value between [0, 100], representing a precentage
    int32_t completeness() const {
        return (totalReqsSent_ - failedReqs_) * 100 / totalReqsSent_;
    }

    std::unordered_map<PartitionID, storage::cpp2::ErrorCode>& failedParts() {
        return failedParts_;
    }

    std::vector<Response>& responses() {
        return responses_;
    }


private:
    const size_t totalReqsSent_;
    size_t failedReqs_{0};

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
    explicit StorageClient(std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool,
                           meta::MetaClient *client = nullptr);
    ~StorageClient();

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
    meta::MetaClient *client_{nullptr};
    std::unique_ptr<thrift::ThriftClientManager<
                        storage::cpp2::StorageServiceAsyncClient>> clientsMan_;

private:
    // Calculate the partition id for the given vertex id
    PartitionID partId(GraphSpaceID spaceId, int64_t id) const;

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

    // Cluster given ids into the host they belong to
    // The method returns a map
    //  host_addr (A host, but in most case, the leader will be chosen)
    //      => (partition -> [ids that belong to the shard])
    template<class Container, class GetIdFunc>
    std::unordered_map<HostAddr,
                       std::unordered_map<PartitionID,
                                          std::vector<typename Container::value_type>
                                         >
                      >
    clusterIdsToHosts(GraphSpaceID spaceId, Container ids, GetIdFunc f) const {
        std::unordered_map<HostAddr,
                           std::unordered_map<PartitionID,
                                              std::vector<typename Container::value_type>
                                             >
                          > clusters;
        for (auto& id : ids) {
            PartitionID part = partId(spaceId, f(id));
            auto partMeta = client_->getPartMetaFromCache(spaceId, part);
            CHECK_GT(partMeta.peers_.size(), 0U);
            // TODO We need to use the leader here
            clusters[partMeta.peers_.front()][part].push_back(std::move(id));
        }
        return clusters;
    }
};

}   // namespace storage
}   // namespace nebula

#include "storage/client/StorageClient.inl"

#endif  // STORAGE_CLIENT_STORAGECLIENT_H_

