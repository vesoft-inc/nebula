/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_PROCESSORS_ADMIN_STORAGEADMINCLIENT_H_
#define META_PROCESSORS_ADMIN_STORAGEADMINCLIENT_H_

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/thrift/ThriftClientManager.h"
#include "common/interface/gen-cpp2/StorageAdminServiceAsyncClient.h"
#include "kvstore/KVStore.h"
#include <folly/executors/IOThreadPoolExecutor.h>

namespace nebula {
namespace meta {

using HostLeaderMap = std::unordered_map<HostAddr,
                                         std::unordered_map<GraphSpaceID,
                                                            std::vector<PartitionID>>>;
using HandleResultOpt = folly::Optional<std::function<void(storage::cpp2::AdminExecResp&&)>>;

static const HostAddr kRandomPeer("", 0);

class AdminClient {
    FRIEND_TEST(AdminClientTest, RetryTest);

public:
    AdminClient() = default;

    explicit AdminClient(kvstore::KVStore* kv)
        : kv_(kv) {
        ioThreadPool_ = std::make_unique<folly::IOThreadPoolExecutor>(10);
        clientsMan_ = std::make_unique<
            thrift::ThriftClientManager<storage::cpp2::StorageAdminServiceAsyncClient>>();
    }

    virtual ~AdminClient() = default;

    folly::Executor* executor() const {
        return ioThreadPool_.get();
    }

    virtual folly::Future<Status> transLeader(GraphSpaceID spaceId,
                                              PartitionID partId,
                                              const HostAddr& leader,
                                              const HostAddr& dst = kRandomPeer);

    virtual folly::Future<Status> addPart(GraphSpaceID spaceId,
                                          PartitionID partId,
                                          const HostAddr& host,
                                          bool asLearner);

    virtual folly::Future<Status> addLearner(GraphSpaceID spaceId,
                                             PartitionID partId,
                                             const HostAddr& learner);

    virtual folly::Future<Status> waitingForCatchUpData(GraphSpaceID spaceId,
                                                        PartitionID partId,
                                                        const HostAddr& target);

    /**
     * Add/Remove one peer for raft group (spaceId, partId).
     * "added" should be true if we want to add one peer, otherwise it is false.
     * */
    virtual folly::Future<Status> memberChange(GraphSpaceID spaceId,
                                               PartitionID partId,
                                               const HostAddr& peer,
                                               bool added);

    virtual folly::Future<Status> updateMeta(GraphSpaceID spaceId,
                                             PartitionID partId,
                                             const HostAddr& leader,
                                             const HostAddr& dst);

    virtual folly::Future<Status> removePart(GraphSpaceID spaceId,
                                             PartitionID partId,
                                             const HostAddr& host);

    virtual folly::Future<Status> checkPeers(GraphSpaceID spaceId, PartitionID partId);

    virtual folly::Future<Status> getLeaderDist(HostLeaderMap* result);

    virtual folly::Future<StatusOr<cpp2::BackupInfo>> createSnapshot(GraphSpaceID spaceId,
                                                                     const std::string& name,
                                                                     const HostAddr& host);

    virtual folly::Future<Status> dropSnapshot(GraphSpaceID spaceId,
                                               const std::string& name,
                                               const HostAddr& host);

    virtual folly::Future<Status> blockingWrites(GraphSpaceID spaceId,
                                                 storage::cpp2::EngineSignType sign,
                                                 const HostAddr& host);

    virtual folly::Future<Status> addTask(cpp2::AdminCmd cmd,
                                          int32_t jobId,
                                          int32_t taskId,
                                          GraphSpaceID spaceId,
                                          const std::vector<HostAddr>& specificHosts,
                                          const std::vector<std::string>& taskSpecficParas,
                                          std::vector<PartitionID> parts,
                                          int concurrency,
                                          cpp2::StatisItem* statisResult = nullptr);

    virtual folly::Future<Status> stopTask(const std::vector<HostAddr>& target,
                                           int32_t jobId,
                                           int32_t taskId);

    virtual folly::Future<StatusOr<nebula::cpp2::DirInfo>> listClusterInfo(const HostAddr& host);

private:
    template<class Request,
             class RemoteFunc,
             class RespGenerator>
    folly::Future<Status> getResponse(const HostAddr& host,
                                      Request req,
                                      RemoteFunc remoteFunc,
                                      RespGenerator respGen);

    template<typename Request,
             typename RemoteFunc>
    void getResponse(std::vector<HostAddr> hosts,
                     int32_t index,
                     Request req,
                     RemoteFunc remoteFunc,
                     int32_t retry,
                     folly::Promise<Status> pro,
                     int32_t retryLimit,
                     HandleResultOpt respGen = folly::none);

    void getLeaderDist(const HostAddr& host,
                       folly::Promise<StatusOr<storage::cpp2::GetLeaderPartsResp>>&& pro,
                       int32_t retry,
                       int32_t retryLimit);

    Status handleResponse(const storage::cpp2::AdminExecResp& resp);

    ErrorOr<nebula::cpp2::ErrorCode, std::vector<HostAddr>>
    getPeers(GraphSpaceID spaceId, PartitionID partId);

    std::vector<HostAddr> getAdminAddrFromPeers(const std::vector<HostAddr> &peers);

private:
    kvstore::KVStore* kv_{nullptr};
    std::unique_ptr<folly::IOThreadPoolExecutor> ioThreadPool_{nullptr};
    std::unique_ptr<thrift::ThriftClientManager<storage::cpp2::StorageAdminServiceAsyncClient>>
    clientsMan_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_PROCESSORS_ADMIN_STORAGEADMINCLIENT_H_
