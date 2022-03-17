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
   * @brief If it is a partition with only one replica, return succeed if src is the only host.
   *        If it is a partition with multiple replicas:
   *            1. If src is not the leader, return succeed
   *            2. If src is the leader, try to transfer to another peer specified by dst
   *
   * @param spaceId
   * @param partId
   * @param leader
   * @param dst
   * @return folly::Future<Status>
   */
  virtual folly::Future<Status> transLeader(GraphSpaceID spaceId,
                                            PartitionID partId,
                                            const HostAddr& src,
                                            const HostAddr& dst = kRandomPeer);

  /**
   * @brief Open a new partition on specified host. The rpc will be sent to the new partition peer,
   *        letting it know the other peers.
   *
   * @param spaceId
   * @param partId
   * @param host
   * @param asLearner Whether the partition start only as raft learner
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
   * @brief Add/Remove one peer for partition (spaceId, partId). The rpc will be sent to the
   *        partition leader. "added" should be true if we want to add one peer, otherwise it is
   *        false.
   *
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
   * @brief Clear space data in all corresponding storage hosts.
   *
   * @param spaceId space which will be cleared
   * @param hosts storage admin service addresses
   * @return folly::Future<nebula::cpp2::ErrorCode>
   */
  virtual folly::Future<nebula::cpp2::ErrorCode> clearSpace(GraphSpaceID spaceId,
                                                            const std::vector<HostAddr>& hosts);

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
   * @return folly::Future<StatusOr<bool>> Return true if succeed, else return an error status
   */
  virtual folly::Future<StatusOr<bool>> dropSnapshot(const std::set<GraphSpaceID>& spaceIds,
                                                     const std::string& name,
                                                     const HostAddr& host);

  /**
   * @brief Blocking/Allowing writings to given spaces in specified storage host
   *
   * @param spaceIds
   * @param sign BLOCK_ON: blocking, BLOCK_OFF: allowing
   * @param host
   * @return folly::Future<StatusOr<bool>> Return true if succeed, else return an error status
   */
  virtual folly::Future<StatusOr<bool>> blockingWrites(const std::set<GraphSpaceID>& spaceIds,
                                                       storage::cpp2::EngineSignType sign,
                                                       const HostAddr& host);

  /**
   * @brief Add storage admin task to given storage host
   *
   * @param jobType
   * @param jobId
   * @param taskId
   * @param spaceId
   * @param host Target host to add task
   * @param taskSpecficParas
   * @param parts
   * @return folly::Future<StatusOr<bool>> Return true if succeed, else return an error status
   */
  virtual folly::Future<StatusOr<bool>> addTask(cpp2::JobType jobType,
                                                int32_t jobId,
                                                int32_t taskId,
                                                GraphSpaceID spaceId,
                                                const HostAddr& host,
                                                const std::vector<std::string>& taskSpecficParas,
                                                std::vector<PartitionID> parts);

  /**
   * @brief Stop stoarge admin task in given storage host
   *
   * @param host Target storage host to stop job
   * @param jobId
   * @param taskId
   * @return folly::Future<StatusOr<bool>> Return true if succeed, else return an error status
   */
  virtual folly::Future<StatusOr<bool>> stopTask(const HostAddr& host,
                                                 int32_t jobId,
                                                 int32_t taskId);

 private:
  /**
   * @brief Send the rpc request to a storage node, the operation is only related to one partition
   *
   * @tparam Request RPC request type
   * @tparam RemoteFunc Client's RPC function
   * @tparam RespGenerator The result generator based on RPC response
   * @param host Admin ip:port of storage node
   * @param req RPC request
   * @param remoteFunc Client's RPC function
   * @param respGen The result generator based on RPC response
   * @return folly::Future<Status>
   */
  template <class Request, class RemoteFunc, class RespGenerator>
  folly::Future<Status> getResponseFromPart(const HostAddr& host,
                                            Request req,
                                            RemoteFunc remoteFunc,
                                            RespGenerator respGen);

  /**
   * @brief Send the rpc request to a storage node, the operation is only realted to the spaces,
   *        does not have affect on a partition. It may also return extra infomations, so return
   *        StatusOr<Response> is necessary
   *
   * @tparam Request RPC request type
   * @tparam RemoteFunc Client's RPC function
   * @tparam RespGenerator The result generator based on RPC response
   * @tparam RpcResponse RPC response
   * @tparam Response The result type
   * @param host Admin ip:port of storage node
   * @param req RPC request
   * @param remoteFunc Client's RPC function
   * @param respGen The result generator based on RPC response
   * @param pro The promise of result
   */
  template <class Request,
            class RemoteFunc,
            class RespGenerator,
            class RpcResponse = typename std::result_of<
                RemoteFunc(std::shared_ptr<storage::cpp2::StorageAdminServiceAsyncClient>,
                           Request)>::type::value_type,
            class Response = typename std::result_of<RespGenerator(RpcResponse)>::type>
  void getResponseFromHost(const HostAddr& host,
                           Request req,
                           RemoteFunc remoteFunc,
                           RespGenerator respGen,
                           folly::Promise<StatusOr<Response>> pro);

  /**
   * @brief Send the rpc request to a storage node, the operation is only related to one partition
   *        and **it need to be exuecuted on leader**
   *
   * @tparam Request RPC request type
   * @tparam RemoteFunc Client's RPC function
   * @param host Admin ip:port of storage nodes which has the partition
   * @param index The index of storage node in `hosts`, the rpc will be sent to the node
   * @param req RPC request
   * @param remoteFunc Client's RPC function
   * @param retry Current retry times
   * @param pro Promise of result
   * @param retryLimit Max retry times
   */
  template <typename Request, typename RemoteFunc>
  void getResponseFromLeader(std::vector<HostAddr> hosts,
                             int32_t index,
                             Request req,
                             RemoteFunc remoteFunc,
                             int32_t retry,
                             folly::Promise<Status> pro,
                             int32_t retryLimit);

  folly::Future<StatusOr<std::unordered_map<GraphSpaceID, std::vector<PartitionID>>>> getLeaderDist(
      const HostAddr& host);

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
