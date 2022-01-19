/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_PROCESSORS_ADMIN_STORAGEADMINCLIENT_H_
#define META_PROCESSORS_ADMIN_STORAGEADMINCLIENT_H_

#include <folly/executors/IOThreadPoolExecutor.h>

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/ssl/SSLConfig.h"
#include "common/thrift/ThriftClientManager.h"
#include "interface/gen-cpp2/StorageAdminServiceAsyncClient.h"
#include "kvstore/KVStore.h"

namespace nebula {
namespace meta {

using HostLeaderMap =
    std::unordered_map<HostAddr, std::unordered_map<GraphSpaceID, std::vector<PartitionID>>>;
using HandleResultOpt = folly::Optional<std::function<void(storage::cpp2::AdminExecResp&&)>>;

static const HostAddr kRandomPeer("", 0);

class AdminClient {
  FRIEND_TEST(AdminClientTest, RetryTest);

 public:
  AdminClient() = default;

  explicit AdminClient(kvstore::KVStore* kv) : kv_(kv) {
    ioThreadPool_ = std::make_unique<folly::IOThreadPoolExecutor>(10);
    clientsMan_ = std::make_unique<
        thrift::ThriftClientManager<storage::cpp2::StorageAdminServiceAsyncClient>>(
        FLAGS_enable_ssl);
  }

  virtual ~AdminClient() = default;

  folly::Executor* executor() const {
    return ioThreadPool_.get();
  }

  /**
   * @brief Transfer given partition's leader from now to dst
   *
   * @param spaceId
   * @param partId
   * @param leader
   * @param dst
   * @return folly::Future<Status>
   */
  virtual folly::Future<Status> transLeader(GraphSpaceID spaceId,
                                            PartitionID partId,
                                            const HostAddr& leader,
                                            const HostAddr& dst = kRandomPeer);

  /**
   * @brief Add a peer for given partition in specified host. The rpc
   *        will be sent to the new partition peer, letting it know the
   *        other peers.
   *
   * @param spaceId
   * @param partId
   * @param host
   * @param asLearner Add an peer only as raft learner
   * @return folly::Future<Status>
   */
  virtual folly::Future<Status> addPart(GraphSpaceID spaceId,
                                        PartitionID partId,
                                        const HostAddr& host,
                                        bool asLearner);

  /**
   * @brief Add a learner for given partition. The rpc will be sent to
   *        the partition leader, writting the add event as a log.
   *
   * @param spaceId
   * @param partId
   * @param learner
   * @return folly::Future<Status>
   */
  virtual folly::Future<Status> addLearner(GraphSpaceID spaceId,
                                           PartitionID partId,
                                           const HostAddr& learner);

  /**
   * @brief Waiting for give partition peer catching data
   *
   * @param spaceId
   * @param partId
   * @param target partition peer address
   * @return folly::Future<Status>
   */
  virtual folly::Future<Status> waitingForCatchUpData(GraphSpaceID spaceId,
                                                      PartitionID partId,
                                                      const HostAddr& target);

  /**
   * @brief Add/Remove one peer for partition (spaceId, partId).
   *        "added" should be true if we want to add one peer, otherwise it is false.
   * @param spaceId
   * @param partId
   * @param peer
   * @param added
   * @return folly::Future<Status>
   */
  virtual folly::Future<Status> memberChange(GraphSpaceID spaceId,
                                             PartitionID partId,
                                             const HostAddr& peer,
                                             bool added);

  /**
   * @brief Update partition peers info in meta kvstore, remove peer 'src', add peer 'dst'
   *
   * @param spaceId
   * @param partId
   * @param src
   * @param dst
   * @return folly::Future<Status>
   */
  virtual folly::Future<Status> updateMeta(GraphSpaceID spaceId,
                                           PartitionID partId,
                                           const HostAddr& src,
                                           const HostAddr& dst);

  /**
   * @brief Remove partition peer in given storage host
   *
   * @param spaceId
   * @param partId
   * @param host storage admin service address
   * @return folly::Future<Status>
   */
  virtual folly::Future<Status> removePart(GraphSpaceID spaceId,
                                           PartitionID partId,
                                           const HostAddr& host);

  /**
   * @brief Check and adjust(add/remove) each peer's peers info according to meta kv store
   *
   * @param spaceId
   * @param partId
   * @return folly::Future<Status>
   */
  virtual folly::Future<Status> checkPeers(GraphSpaceID spaceId, PartitionID partId);

  /**
   * @brief Get the all partitions' leader distribution
   *
   * @param result
   * @return folly::Future<Status>
   */
  virtual folly::Future<Status> getLeaderDist(HostLeaderMap* result);

  // used for snapshot and backup
  /**
   * @brief Create snapshots for given spaces in given host with specified snapshot name
   *
   * @param spaceIds spaces to create snapshot
   * @param name snapshot name
   * @param host storage host
   * @return folly::Future<StatusOr<cpp2::HostBackupInfo>>
   */
  virtual folly::Future<StatusOr<cpp2::HostBackupInfo>> createSnapshot(
      const std::set<GraphSpaceID>& spaceIds, const std::string& name, const HostAddr& host);

  /**
   * @brief Drop snapshots of given spaces in given host with specified snapshot name
   *
   * @param spaceIds spaces to drop
   * @param name snapshot name
   * @param host storage host
   * @return folly::Future<Status>
   */
  virtual folly::Future<Status> dropSnapshot(const std::set<GraphSpaceID>& spaceIds,
                                             const std::string& name,
                                             const HostAddr& host);

  /**
   * @brief Blocking/Allowing writings to given spaces in specified storage host
   *
   * @param spaceIds
   * @param sign BLOCK_ON: blocking, BLOCK_OFF: allowing
   * @param host
   * @return folly::Future<Status>
   */
  virtual folly::Future<Status> blockingWrites(const std::set<GraphSpaceID>& spaceIds,
                                               storage::cpp2::EngineSignType sign,
                                               const HostAddr& host);

  /**
   * @brief Add storage admin task to given storage host
   *
   * @param cmd
   * @param jobId
   * @param taskId
   * @param spaceId
   * @param specificHosts if hosts are empty, will send request to all ative storage hosts
   * @param taskSpecficParas
   * @param parts
   * @param concurrency
   * @param statsResult
   * @return folly::Future<Status>
   */
  virtual folly::Future<Status> addTask(cpp2::AdminCmd cmd,
                                        int32_t jobId,
                                        int32_t taskId,
                                        GraphSpaceID spaceId,
                                        const std::vector<HostAddr>& specificHosts,
                                        const std::vector<std::string>& taskSpecficParas,
                                        std::vector<PartitionID> parts,
                                        int concurrency,
                                        cpp2::StatsItem* statsResult = nullptr);

  /**
   * @brief Stop stoarge admin task in given storage host
   *
   * @param target if target hosts are emtpy, will send request to all active storage hosts
   * @param jobId
   * @param taskId
   * @return folly::Future<Status>
   */
  virtual folly::Future<Status> stopTask(const std::vector<HostAddr>& target,
                                         int32_t jobId,
                                         int32_t taskId);

 private:
  template <class Request, class RemoteFunc, class RespGenerator>
  folly::Future<Status> getResponse(const HostAddr& host,
                                    Request req,
                                    RemoteFunc remoteFunc,
                                    RespGenerator respGen);

  template <typename Request, typename RemoteFunc>
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

  ErrorOr<nebula::cpp2::ErrorCode, std::vector<HostAddr>> getPeers(GraphSpaceID spaceId,
                                                                   PartitionID partId);

  std::vector<HostAddr> getAdminAddrFromPeers(const std::vector<HostAddr>& peers);

 private:
  kvstore::KVStore* kv_{nullptr};
  std::unique_ptr<folly::IOThreadPoolExecutor> ioThreadPool_{nullptr};
  std::unique_ptr<thrift::ThriftClientManager<storage::cpp2::StorageAdminServiceAsyncClient>>
      clientsMan_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_PROCESSORS_ADMIN_STORAGEADMINCLIENT_H_
