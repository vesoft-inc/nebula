/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_CLIENTS_STORAGE_STORAGECLIENTBASE_H_
#define COMMON_CLIENTS_STORAGE_STORAGECLIENTBASE_H_

#include "common/base/Base.h"
#include <folly/futures/Future.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include "common/base/StatusOr.h"
#include "common/meta/Common.h"
#include "common/thrift/ThriftClientManager.h"
#include "common/clients/meta/MetaClient.h"
#include "common/interface/gen-cpp2/storage_types.h"

DECLARE_int32(storage_client_timeout_ms);
DECLARE_uint32(storage_client_retry_interval_ms);

constexpr int32_t kInternalPortOffset = -2;

namespace nebula {
namespace storage {

template<class Response>
class StorageRpcResponse final {
public:
    enum class Result {
        ALL_SUCCEEDED = 0,
        PARTIAL_SUCCEEDED = 1,
    };

    explicit StorageRpcResponse(size_t reqsSent) : totalReqsSent_(reqsSent) {
        lock_ = std::make_unique<std::mutex>();
    }

    bool succeeded() const {
        std::lock_guard<std::mutex> g(*lock_);
        return result_ == Result::ALL_SUCCEEDED;
    }

    int32_t maxLatency() const {
        std::lock_guard<std::mutex> g(*lock_);
        return maxLatency_;
    }

    void setLatency(HostAddr host, int32_t latency, int32_t e2eLatency) {
        std::lock_guard<std::mutex> g(*lock_);
        if (latency > maxLatency_) {
            maxLatency_ = latency;
        }
        hostLatency_.emplace_back(std::make_tuple(host, latency, e2eLatency));
    }

    void markFailure() {
        std::lock_guard<std::mutex> g(*lock_);
        result_ = Result::PARTIAL_SUCCEEDED;
        ++failedReqs_;
    }

    // A value between [0, 100], representing a precentage
    int32_t completeness() const {
        std::lock_guard<std::mutex> g(*lock_);
        DCHECK_NE(totalReqsSent_, 0);
        return totalReqsSent_ == 0 ? 0 : (totalReqsSent_ - failedReqs_) * 100 / totalReqsSent_;
    }

    void emplaceFailedPart(PartitionID partId, nebula::cpp2::ErrorCode errorCode) {
        std::lock_guard<std::mutex> g(*lock_);
        failedParts_.emplace(partId, errorCode);
    }

    void appendFailedParts(const std::vector<PartitionID> &partsId,
                           nebula::cpp2::ErrorCode errorCode) {
        std::lock_guard<std::mutex> g(*lock_);
        failedParts_.reserve(failedParts_.size() + partsId.size());
        for (const auto &partId : partsId) {
            failedParts_.emplace(partId, errorCode);
        }
    }

    void addResponse(Response&& resp) {
        std::lock_guard<std::mutex> g(*lock_);
        responses_.emplace_back(std::move(resp));
    }

    // Not thread-safe.
    const std::unordered_map<PartitionID, nebula::cpp2::ErrorCode>& failedParts() const {
        return failedParts_;
    }

    // Not thread-safe.
    std::vector<Response>& responses() {
        return responses_;
    }

    // Not thread-safe.
    const std::vector<std::tuple<HostAddr, int32_t, int32_t>>& hostLatency() const {
        return hostLatency_;
    }

private:
    std::unique_ptr<std::mutex> lock_;
    const size_t totalReqsSent_;
    size_t failedReqs_{0};

    Result result_{Result::ALL_SUCCEEDED};
    std::unordered_map<PartitionID, nebula::cpp2::ErrorCode> failedParts_;
    int32_t maxLatency_{0};
    std::vector<Response> responses_;
    std::vector<std::tuple<HostAddr, int32_t, int32_t>> hostLatency_;
};


/**
 * A base class for all storage clients
 */
template<typename ClientType>
class StorageClientBase {
public:
    StatusOr<HostAddr> getLeader(GraphSpaceID spaceId, PartitionID partId) const;

protected:
    StorageClientBase(std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool,
                      meta::MetaClient* metaClient);
    virtual ~StorageClientBase();

    void updateLeader(GraphSpaceID spaceId, PartitionID partId, const HostAddr& leader);
    void invalidLeader(GraphSpaceID spaceId, PartitionID partId);
    void invalidLeader(GraphSpaceID spaceId, std::vector<PartitionID> &partsId);

    template<class Request,
             class RemoteFunc,
             class Response =
                typename std::result_of<
                    RemoteFunc(ClientType*, const Request&)
                >::type::value_type
            >
    folly::SemiFuture<StorageRpcResponse<Response>> collectResponse(
        folly::EventBase* evb,
        std::unordered_map<HostAddr, Request> requests,
        RemoteFunc&& remoteFunc);

    template<class Request,
             class RemoteFunc,
             class Response =
                typename std::result_of<
                    RemoteFunc(ClientType* client, const Request&)
                >::type::value_type
            >
    folly::Future<StatusOr<Response>> getResponse(
            folly::EventBase* evb,
            std::pair<HostAddr, Request>&& request,
            RemoteFunc&& remoteFunc,
            folly::Promise<StatusOr<Response>> pro = folly::Promise<StatusOr<Response>>());

    template<class Request,
             class RemoteFunc,
             class Response =
                typename std::result_of<
                    RemoteFunc(ClientType* client, const Request&)
                >::type::value_type
            >
    void getResponseImpl(
            folly::EventBase* evb,
            std::pair<HostAddr, Request> request,
            RemoteFunc remoteFunc,
            folly::Promise<StatusOr<Response>> pro);

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
    clusterIdsToHosts(GraphSpaceID spaceId, const Container& ids, GetIdFunc f) const;

    virtual StatusOr<meta::PartHosts> getPartHosts(GraphSpaceID spaceId,
                                                   PartitionID partId) const {
        CHECK(metaClient_ != nullptr);
        return metaClient_->getPartHostsFromCache(spaceId, partId);
    }

    virtual StatusOr<std::unordered_map<HostAddr, std::vector<PartitionID>>>
    getHostParts(GraphSpaceID spaceId) const;

    // from map
    template <typename K>
    std::vector<PartitionID> getReqPartsIdFromContainer(
        const std::unordered_map<PartitionID, K> &t) const {
        std::vector<PartitionID> partsId;
        partsId.reserve(t.size());
        for (const auto &part : t) {
            partsId.emplace_back(part.first);
        }
        return partsId;
    }

    // from list
    std::vector<PartitionID> getReqPartsIdFromContainer(const std::vector<PartitionID> &t) const {
        return t;
    }

    template <typename Request>
    std::vector<PartitionID> getReqPartsId(const Request &req) const {
        return getReqPartsIdFromContainer(req.get_parts());
    }

    std::vector<PartitionID> getReqPartsId(const cpp2::UpdateVertexRequest &req) const {
        return {req.get_part_id()};
    }

    std::vector<PartitionID> getReqPartsId(const cpp2::UpdateEdgeRequest &req) const {
        return {req.get_part_id()};
    }

    std::vector<PartitionID> getReqPartsId(const cpp2::GetUUIDReq &req) const {
        return {req.get_part_id()};
    }

    std::vector<PartitionID> getReqPartsId(const cpp2::InternalTxnRequest &req) const {
        return {req.get_part_id()};
    }

    std::vector<PartitionID> getReqPartsId(const cpp2::GetValueRequest &req) const {
        return {req.get_part_id()};
    }

    std::vector<PartitionID> getReqPartsId(const cpp2::ScanEdgeRequest &req) const {
        return {req.get_part_id()};
    }

    std::vector<PartitionID> getReqPartsId(const cpp2::ScanVertexRequest &req) const {
        return {req.get_part_id()};
    }

    bool isValidHostPtr(const HostAddr* addr) {
        return addr != nullptr && !addr->host.empty() && addr->port != 0;
    }

protected:
    meta::MetaClient* metaClient_{nullptr};

private:
    std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool_;
    std::unique_ptr<thrift::ThriftClientManager<ClientType>> clientsMan_;
};

}   // namespace storage
}   // namespace nebula

#include "common/clients/storage/StorageClientBase.inl"

#endif  // COMMON_CLIENTS_STORAGE_STORAGECLIENTBASE_H_
