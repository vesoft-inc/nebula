/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CLIENTS_STORAGE_STORAGECLIENTBASE_H_
#define CLIENTS_STORAGE_STORAGECLIENTBASE_H_

#include "base/Base.h"
#include <folly/futures/Future.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include "base/StatusOr.h"
#include "meta/Common.h"
#include "thrift/ThriftClientManager.h"
#include "clients/meta/MetaClient.h"

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
 * A base class for all storage clients
 */
template<typename ClientType>
class StorageClientBase {
protected:
    StorageClientBase(std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool,
                      meta::MetaClient* metaClient);
    virtual ~StorageClientBase();

    const HostAddr getLeader(const meta::PartHosts& partHosts) const;
    void updateLeader(GraphSpaceID spaceId, PartitionID partId, const HostAddr& leader);
    void invalidLeader(GraphSpaceID spaceId, PartitionID partId);

    template<class Request,
             class RemoteFunc,
             class Response =
                typename std::result_of<RemoteFunc(ClientType*, const Request&)>
                    ::type::value_type
            >
    folly::SemiFuture<StorageRpcResponse<Response>> collectResponse(
        folly::EventBase* evb,
        std::unordered_map<HostAddr, Request> requests,
        RemoteFunc&& remoteFunc);

    template<class Request,
             class RemoteFunc,
             class Response =
                typename std::result_of<RemoteFunc(ClientType* client, const Request&)>
                    ::type::value_type
            >
    folly::Future<StatusOr<Response>> getResponse(
            folly::EventBase* evb,
            std::pair<HostAddr, Request> request,
            RemoteFunc remoteFunc);

    // Cluster given ids into the host they belong to
    // The method returns a map
    //  host_addr (A host, but in most case, the leader will be chosen)
    //      => (partition -> [ids that belong to the shard])
    template<class Container, class GetIdFunc>
    StatusOr<
        std::unordered_map<
            HostAddr,
            std::unordered_map<
                PartitionID,
                std::vector<typename Container::value_type>
            >
        >
    >
    clusterIdsToHosts(GraphSpaceID spaceId, Container&& ids, GetIdFunc f) const;

    // Calculate the partition id for the given vertex id
    StatusOr<PartitionID> partId(GraphSpaceID spaceId, int64_t id) const;

    virtual StatusOr<meta::PartHosts> getPartHosts(GraphSpaceID spaceId,
                                                   PartitionID partId) const {
        CHECK(metaClient_ != nullptr);
        return metaClient_->getPartHostsFromCache(spaceId, partId);
    }

private:
    std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool_;
    std::unique_ptr<thrift::ThriftClientManager<ClientType>> clientsMan_;

    meta::MetaClient* metaClient_{nullptr};

    mutable folly::RWSpinLock leadersLock_;
    mutable std::unordered_map<std::pair<GraphSpaceID, PartitionID>, HostAddr> leaders_;
};

}   // namespace storage
}   // namespace nebula

#include "clients/storage/StorageClientBase.inl"

#endif  // CLIENTS_STORAGE_STORAGECLIENTBASE_H_
