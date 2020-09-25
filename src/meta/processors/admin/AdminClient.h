/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_PROCESSORS_ADMIN_STORAGEADMINCLIENT_H_
#define META_PROCESSORS_ADMIN_STORAGEADMINCLIENT_H_

#include <folly/executors/IOThreadPoolExecutor.h>
#include "base/Base.h"
#include "base/Status.h"
#include "gen-cpp2/StorageServiceAsyncClient.h"
#include "kvstore/KVStore.h"
#include "thrift/ThriftClientManager.h"

namespace nebula {
namespace meta {

using HostLeaderMap =
    std::unordered_map<network::InetAddress,
                       std::unordered_map<GraphSpaceID, std::vector<PartitionID>>>;

class FaultInjector {
public:
    virtual ~FaultInjector() = default;
    virtual folly::Future<Status> transLeader() = 0;
    virtual folly::Future<Status> addPart() = 0;
    virtual folly::Future<Status> addLearner() = 0;
    virtual folly::Future<Status> waitingForCatchUpData() = 0;
    virtual folly::Future<Status> memberChange() = 0;
    virtual folly::Future<Status> updateMeta() = 0;
    virtual folly::Future<Status> removePart() = 0;
    virtual folly::Future<Status> checkPeers() = 0;
    virtual folly::Future<Status> getLeaderDist(HostLeaderMap* hostLeaderMap) = 0;
    virtual folly::Future<Status> createSnapshot() = 0;
    virtual folly::Future<Status> dropSnapshot() = 0;
    virtual folly::Future<Status> blockingWrites() = 0;
    virtual folly::Future<Status> rebuildTagIndex() = 0;
    virtual folly::Future<Status> rebuildEdgeIndex() = 0;
};

static const network::InetAddress kRandomPeer(0, 0);

class AdminClient {
    FRIEND_TEST(AdminClientTest, RetryTest);

public:
    AdminClient() = default;

    explicit AdminClient(kvstore::KVStore* kv) : kv_(kv) {
        ioThreadPool_ = std::make_unique<folly::IOThreadPoolExecutor>(10);
        clientsMan_ = std::make_unique<
            thrift::ThriftClientManager<storage::cpp2::StorageServiceAsyncClient>>();
    }

    explicit AdminClient(std::unique_ptr<FaultInjector> injector)
        : injector_(std::move(injector)) {}

    ~AdminClient() = default;

    folly::Future<Status> transLeader(GraphSpaceID spaceId,
                                      PartitionID partId,
                                      const network::InetAddress& leader,
                                      const network::InetAddress& dst = kRandomPeer);

    folly::Future<Status> addPart(GraphSpaceID spaceId,
                                  PartitionID partId,
                                  const network::InetAddress& host,
                                  bool asLearner);

    folly::Future<Status> addLearner(GraphSpaceID spaceId,
                                     PartitionID partId,
                                     const network::InetAddress& learner);

    folly::Future<Status> waitingForCatchUpData(GraphSpaceID spaceId,
                                                PartitionID partId,
                                                const network::InetAddress& target);

    /**
     * Add/Remove one peer for raft group (spaceId, partId).
     * "added" should be true if we want to add one peer, otherwise it is false.
     * */
    folly::Future<Status> memberChange(GraphSpaceID spaceId,
                                       PartitionID partId,
                                       const network::InetAddress& peer,
                                       bool added);

    folly::Future<Status> updateMeta(GraphSpaceID spaceId,
                                     PartitionID partId,
                                     const network::InetAddress& leader,
                                     const network::InetAddress& dst);

    folly::Future<Status> removePart(GraphSpaceID spaceId,
                                     PartitionID partId,
                                     const network::InetAddress& host);

    folly::Future<Status> checkPeers(GraphSpaceID spaceId, PartitionID partId);

    folly::Future<Status> getLeaderDist(HostLeaderMap* result);

    folly::Future<Status> createSnapshot(GraphSpaceID spaceId,
                                         const std::string& name,
                                         const network::InetAddress& host);

    folly::Future<Status> dropSnapshot(GraphSpaceID spaceId,
                                       const std::string& name,
                                       const network::InetAddress& host);

    folly::Future<Status> blockingWrites(GraphSpaceID spaceId,
                                         storage::cpp2::EngineSignType sign,
                                         const network::InetAddress& host);

    folly::Future<Status> rebuildTagIndex(const network::InetAddress& address,
                                          GraphSpaceID spaceId,
                                          IndexID indexID,
                                          std::vector<PartitionID> parts,
                                          bool isOffline);

    folly::Future<Status> rebuildEdgeIndex(const network::InetAddress& address,
                                           GraphSpaceID spaceId,
                                           IndexID indexID,
                                           std::vector<PartitionID> parts,
                                           bool isOffline);

    FaultInjector* faultInjector() {
        return injector_.get();
    }

private:
    template <class Request, class RemoteFunc, class RespGenerator>
    folly::Future<Status> getResponse(const network::InetAddress& host,
                                      Request req,
                                      RemoteFunc remoteFunc,
                                      RespGenerator respGen);

    template <typename Request, typename RemoteFunc>
    void getResponse(std::vector<network::InetAddress> hosts,
                     int32_t index,
                     Request req,
                     RemoteFunc remoteFunc,
                     int32_t retry,
                     folly::Promise<Status> pro,
                     int32_t retryLimit);

    void getLeaderDist(const network::InetAddress& host,
                       folly::Promise<StatusOr<storage::cpp2::GetLeaderResp>>&& pro,
                       int32_t retry,
                       int32_t retryLimit);

    Status handleResponse(const storage::cpp2::AdminExecResp& resp);

    nebula::cpp2::HostAddr toThriftHost(const network::InetAddress& addr);

    StatusOr<std::vector<network::InetAddress>> getPeers(GraphSpaceID spaceId, PartitionID partId);

private:
    std::unique_ptr<FaultInjector> injector_{nullptr};
    kvstore::KVStore* kv_ = nullptr;
    std::unique_ptr<folly::IOThreadPoolExecutor> ioThreadPool_{nullptr};
    std::unique_ptr<thrift::ThriftClientManager<storage::cpp2::StorageServiceAsyncClient>>
        clientsMan_;
};
}   // namespace meta
}   // namespace nebula

#endif   // META_PROCESSORS_ADMIN_STORAGEADMINCLIENT_H_
