/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_CLIENT_STORAGECLIENT_H_
#define STORAGE_CLIENT_STORAGECLIENT_H_

#include "base/Base.h"
#include <gtest/gtest_prod.h>
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
class StorageClient {
    FRIEND_TEST(StorageClientTest, LeaderChangeTest);

public:
    StorageClient(std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool,
                  meta::MetaClient *client);
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

protected:
    // Calculate the partition id for the given vertex id
    PartitionID partId(GraphSpaceID spaceId, int64_t id) const;

    const HostAddr& leader(const PartMeta& partMeta) const {
        {
            folly::RWSpinLock::ReadHolder rh(leadersLock_);
            auto it = leaders_.find(std::make_pair(partMeta.spaceId_, partMeta.partId_));
            if (it != leaders_.end()) {
                return it->second;
            }
        }
        VLOG(1) << "No leader exists. Choose one random.";
        return partMeta.peers_[folly::Random::rand32(partMeta.peers_.size())];
    }

    void updateLeader(GraphSpaceID spaceId, PartitionID partId, const HostAddr& leader) {
        LOG(INFO) << "Update leader for " << spaceId << ", " << partId << " to " << leader;
        folly::RWSpinLock::WriteHolder wh(leadersLock_);
        leaders_[std::make_pair(spaceId, partId)] = leader;
    }

    void invalidLeader(GraphSpaceID spaceId, PartitionID partId) {
        folly::RWSpinLock::WriteHolder wh(leadersLock_);
        auto it = leaders_.find(std::make_pair(spaceId, partId));
        if (it != leaders_.end()) {
            leaders_.erase(it);
        }
    }

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
            auto partMeta = getPartMeta(spaceId, part);
            CHECK_GT(partMeta.peers_.size(), 0U);
            const auto& leader = this->leader(partMeta);
            clusters[leader][part].emplace_back(std::move(id));
        }
        return clusters;
    }

    virtual int32_t partsNum(GraphSpaceID spaceId) const {
        CHECK(client_ != nullptr);
        return client_->partsNum(spaceId);
    }

    virtual PartMeta getPartMeta(GraphSpaceID spaceId, PartitionID partId) const {
        CHECK(client_ != nullptr);
        return client_->getPartMetaFromCache(spaceId, partId);
    }

private:
    std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool_;
    meta::MetaClient *client_{nullptr};
    std::unique_ptr<thrift::ThriftClientManager<
                        storage::cpp2::StorageServiceAsyncClient>> clientsMan_;
    mutable folly::RWSpinLock leadersLock_;
    std::unordered_map<std::pair<GraphSpaceID, PartitionID>, HostAddr> leaders_;
};

}   // namespace storage
}   // namespace nebula

#include "storage/client/StorageClient.inl"

#endif  // STORAGE_CLIENT_STORAGECLIENT_H_

