/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef KVSTORE_PARTMANAGER_H_
#define KVSTORE_PARTMANAGER_H_

#include <gtest/gtest_prod.h>

#include "clients/meta/MetaClient.h"
#include "common/base/Base.h"
#include "common/meta/Common.h"
#include "kvstore/DiskManager.h"

namespace nebula {
namespace kvstore {

/**
 * @brief Handler when found space/part info changed, called from part manager
 */
class Handler {
 public:
  virtual ~Handler() = default;

  /**
   * @brief Add a space
   *
   * @param spaceId
   * @param isListener Whether the space is listener
   */
  virtual void addSpace(GraphSpaceID spaceId, bool isListener = false) = 0;

  /**
   * @brief Add a partition
   *
   * @param spaceId
   * @param partId
   * @param asLearner Whether start partition as learner
   * @param peers Raft peers
   */
  virtual void addPart(GraphSpaceID spaceId,
                       PartitionID partId,
                       bool asLearner,
                       const std::vector<HostAddr>& peers) = 0;

  /**
   * @brief Update space specific options
   *
   * @param spaceId
   * @param options Options map
   * @param isDbOption
   */
  virtual void updateSpaceOption(GraphSpaceID spaceId,
                                 const std::unordered_map<std::string, std::string>& options,
                                 bool isDbOption) = 0;

  /**
   * @brief Remove a space
   *
   * @param spaceId
   * @param isListener Whether the space is listener
   */
  virtual void removeSpace(GraphSpaceID spaceId, bool isListener = false) = 0;

  /**
   * @brief clear space data, but not remove the data dirs.
   *
   * @param spaceId space which will be cleared.
   * @return
   */
  virtual nebula::cpp2::ErrorCode clearSpace(GraphSpaceID spaceId) = 0;

  /**
   * @brief Remove a partition
   *
   * @param spaceId
   * @param partId
   */
  virtual void removePart(GraphSpaceID spaceId, PartitionID partId, bool needLock = true) = 0;

  /**
   * @brief Add a partition as listener
   *
   * @param spaceId
   * @param partId
   * @param type Listener type
   * @param peers Raft peers of listener
   */
  virtual void addListener(GraphSpaceID spaceId,
                           PartitionID partId,
                           meta::cpp2::ListenerType type,
                           const std::vector<HostAddr>& peers) = 0;

  /**
   * @brief Remove a listener partition
   *
   * @param spaceId
   * @param partId
   * @param type Listener type
   */
  virtual void removeListener(GraphSpaceID spaceId,
                              PartitionID partId,
                              meta::cpp2::ListenerType type) = 0;

  /**
   * @brief Check if the partition's listener state has changed, add/remove if necessary
   *
   * @param spaceId
   * @param partId
   * @param remoteListeners The given partition's remote listener list
   */
  virtual void checkRemoteListeners(GraphSpaceID spaceId,
                                    PartitionID partId,
                                    const std::vector<HostAddr>& remoteListeners) = 0;

  /**
   * @brief Retrive the leader distribution
   *
   * @param leaderIds The leader address of all partitions
   * @return int32_t The leader count of all spaces
   */
  virtual int32_t allLeader(
      std::unordered_map<GraphSpaceID, std::vector<meta::cpp2::LeaderInfo>>& leaderIds) = 0;

  /**
   * @brief Get all partitions grouped by data path and spaceId
   *
   * @param diskParts Get all space data path and all partition in the path
   */
  virtual void fetchDiskParts(SpaceDiskPartsMap& diskParts) = 0;
};

/**
 * @brief This class manages all meta information one storage host needed.
 */
class PartManager {
 public:
  PartManager() = default;

  virtual ~PartManager() = default;

  /**
   * @brief return part allocation for a host
   *
   * @param host
   * @return meta::PartsMap Data part allocation of all spaces on the host
   */
  virtual meta::PartsMap parts(const HostAddr& host) = 0;

  /**
   * @brief Return all peers of a given partition
   *
   * @param spaceId
   * @param partId
   * @return StatusOr<meta::PartHosts> Return peers of a partition if succeeded, else return a error
   * status
   */
  virtual StatusOr<meta::PartHosts> partMeta(GraphSpaceID spaceId, PartitionID partId) = 0;

  /**
   * @brief Check current part exist or not on host.
   *
   * @param host
   * @param spaceId
   * @param partId
   * @return Status
   */
  virtual Status partExist(const HostAddr& host, GraphSpaceID spaceId, PartitionID partId) = 0;

  /**
   * @brief Check host has the space exist or not.
   *
   * @param host
   * @param spaceId
   * @return Status
   */
  virtual Status spaceExist(const HostAddr& host, GraphSpaceID spaceId) = 0;

  /**
   * @brief Return the listener allocation of a host
   *
   * @param host
   * @return meta::ListenersMap Listener allocation of all spaces on the host
   */
  virtual meta::ListenersMap listeners(const HostAddr& host) = 0;

  /**
   * @brief Return remote listener info if given partition has any listener
   *
   * @param spaceId
   * @param partId
   * @return StatusOr<std::vector<meta::RemoteListenerInfo>> Remote listener infomations
   */
  virtual StatusOr<std::vector<meta::RemoteListenerInfo>> listenerPeerExist(GraphSpaceID spaceId,
                                                                            PartitionID partId) = 0;

  /**
   * @brief Register a handler to part mananger, e.g. NebulaStore
   *
   * @param handler
   */
  void registerHandler(Handler* handler) {
    handler_ = handler;
  }

 protected:
  Handler* handler_ = nullptr;
};

/**
 * @brief Memory based PartManager, it is used in UTs now.
 */
class MemPartManager final : public PartManager {
  FRIEND_TEST(NebulaStoreTest, SimpleTest);
  FRIEND_TEST(NebulaStoreTest, MultiPathTest);
  FRIEND_TEST(NebulaStoreTest, PartsTest);
  FRIEND_TEST(NebulaStoreTest, PersistPeersTest);
  FRIEND_TEST(NebulaStoreTest, ThreeCopiesTest);
  FRIEND_TEST(NebulaStoreTest, TransLeaderTest);
  FRIEND_TEST(NebulaStoreTest, CheckpointTest);
  FRIEND_TEST(NebulaStoreTest, ThreeCopiesCheckpointTest);
  FRIEND_TEST(NebulaStoreTest, ReadSnapshotTest);
  FRIEND_TEST(NebulaStoreTest, AtomicOpBatchTest);
  FRIEND_TEST(NebulaStoreTest, RemoveInvalidSpaceTest);
  FRIEND_TEST(NebulaStoreTest, BackupRestoreTest);
  friend class ListenerBasicTest;

 public:
  MemPartManager() = default;

  ~MemPartManager() = default;

  /**
   * @brief return part allocation for a host
   *
   * @param host
   * @return meta::PartsMap Data part allocation of all spaces on the host
   */
  meta::PartsMap parts(const HostAddr& host) override;

  /**
   * @brief Return all peers of a given partition
   *
   * @param spaceId
   * @param partId
   * @return StatusOr<meta::PartHosts> Return peers of a partition if succeeded, else return a error
   * status
   */
  StatusOr<meta::PartHosts> partMeta(GraphSpaceID spaceId, PartitionID partId) override;

  /**
   * @brief Add a partition with its peers
   *
   * @param spaceId
   * @param partId
   * @param peers
   */
  void addPart(GraphSpaceID spaceId, PartitionID partId, std::vector<HostAddr> peers = {}) {
    bool noSpace = partsMap_.find(spaceId) == partsMap_.end();
    auto& p = partsMap_[spaceId];
    bool noPart = p.find(partId) == p.end();
    p[partId] = meta::PartHosts();
    auto& pm = p[partId];
    pm.spaceId_ = spaceId;
    pm.partId_ = partId;
    pm.hosts_ = std::move(peers);
    if (noSpace && handler_) {
      handler_->addSpace(spaceId);
    }
    if (noPart && handler_) {
      handler_->addPart(spaceId, partId, false, peers);
    }
  }

  /**
   * @brief Remove a partition from part manager
   *
   * @param spaceId
   * @param partId
   */
  void removePart(GraphSpaceID spaceId, PartitionID partId) {
    auto it = partsMap_.find(spaceId);
    CHECK(it != partsMap_.end());
    if (it->second.find(partId) != it->second.end()) {
      it->second.erase(partId);
      if (handler_) {
        handler_->removePart(spaceId, partId);
        if (it->second.empty()) {
          handler_->removeSpace(spaceId);
        }
      }
    }
  }

  /**
   * @brief Check current part exist or not on host.
   *
   * @param host
   * @param spaceId
   * @param partId
   * @return Status
   */
  Status partExist(const HostAddr& host, GraphSpaceID spaceId, PartitionID partId) override;

  /**
   * @brief Check host has the space exist or not.
   *
   * @param host
   * @param spaceId
   * @return Status
   */
  Status spaceExist(const HostAddr&, GraphSpaceID spaceId) override {
    if (partsMap_.find(spaceId) != partsMap_.end()) {
      return Status::OK();
    } else {
      return Status::SpaceNotFound();
    }
  }

  /**
   * @brief Return the part allocation
   *
   * @return meta::PartsMap&
   */
  meta::PartsMap& partsMap() {
    return partsMap_;
  }

  /**
   * @brief Return the listener allocation of a host
   *
   * @param host
   * @return meta::ListenersMap Listener allocation of all spaces on the host
   */
  meta::ListenersMap listeners(const HostAddr& host) override;

  /**
   * @brief Return remote listener info if given partition has any listener
   *
   * @param spaceId
   * @param partId
   * @return StatusOr<std::vector<meta::RemoteListenerInfo>> Remote listener infomations
   */
  StatusOr<std::vector<meta::RemoteListenerInfo>> listenerPeerExist(GraphSpaceID spaceId,
                                                                    PartitionID partId) override;

 private:
  meta::PartsMap partsMap_;
  meta::ListenersMap listenersMap_;
  meta::RemoteListeners remoteListeners_;
};

/**
 * @brief Part mananger based on meta client and server, all interfaces will read from meta client
 * cache or meta server
 */
class MetaServerBasedPartManager : public PartManager, public meta::MetaChangedListener {
 public:
  /**
   * @brief Construct a new part mananger based on meta
   *
   * @param host Local address
   * @param client Meta client
   */
  explicit MetaServerBasedPartManager(HostAddr host, meta::MetaClient* client = nullptr);

  ~MetaServerBasedPartManager();

  /**
   * @brief return part allocation for a host
   *
   * @param host
   * @return meta::PartsMap Data part allocation of all spaces on the host
   */
  meta::PartsMap parts(const HostAddr& host) override;

  /**
   * @brief Return all peers of a given partition
   *
   * @param spaceId
   * @param partId
   * @return StatusOr<meta::PartHosts> Return peers of a partition if succeeded, else return a error
   * status
   */
  StatusOr<meta::PartHosts> partMeta(GraphSpaceID spaceId, PartitionID partId) override;

  /**
   * @brief Check current part exist or not on host.
   *
   * @param host
   * @param spaceId
   * @param partId
   * @return Status
   */
  Status partExist(const HostAddr& host, GraphSpaceID spaceId, PartitionID partId) override;

  /**
   * @brief Check host has the space exist or not.
   *
   * @param host
   * @param spaceId
   * @return Status
   */
  Status spaceExist(const HostAddr& host, GraphSpaceID spaceId) override;

  /**
   * @brief Return the listener allocation of a host
   *
   * @param host
   * @return meta::ListenersMap Listener allocation of all spaces on the host
   */
  meta::ListenersMap listeners(const HostAddr& host) override;

  /**
   * @brief Return remote listener info if given partition has any listener
   *
   * @param spaceId
   * @param partId
   * @return StatusOr<std::vector<meta::RemoteListenerInfo>> Remote listener infomations
   */
  StatusOr<std::vector<meta::RemoteListenerInfo>> listenerPeerExist(GraphSpaceID spaceId,
                                                                    PartitionID partId) override;
  // Folloing methods implement the interfaces in MetaChangedListener
  /**
   * @brief Found a new space, call handler's method
   *
   * @param spaceId
   * @param isListener Whether the space is a listener
   */
  void onSpaceAdded(GraphSpaceID spaceId, bool isListener) override;

  /**
   * @brief Found a removed space, call handler's method
   *
   * @param spaceId
   * @param isListener Whether the space is a listener
   */
  void onSpaceRemoved(GraphSpaceID spaceId, bool isListener) override;

  /**
   * @brief Found space option updated, call handler's methos
   *
   * @param spaceId
   * @param options Options map
   */
  void onSpaceOptionUpdated(GraphSpaceID spaceId,
                            const std::unordered_map<std::string, std::string>& options) override;

  /**
   * @brief Found a new part, call handler's method
   *
   * @param partMeta Partition's id and peers
   */
  void onPartAdded(const meta::PartHosts& partMeta) override;

  /**
   * @brief Found a removed part, call handler's method
   *
   * @param spaceId
   * @param partId
   */
  void onPartRemoved(GraphSpaceID spaceId, PartitionID partId) override;

  /**
   * @brief Found a part updated, call handler's method
   *
   * @param partMeta Partition's id and peers
   */
  void onPartUpdated(const meta::PartHosts& partMeta) override;

  /**
   * @brief Fetch leader distribution from handler
   *
   * @param leaderParts Leader distribution
   */
  void fetchLeaderInfo(
      std::unordered_map<GraphSpaceID, std::vector<meta::cpp2::LeaderInfo>>& leaderParts) override;

  /**
   * @brief Fetch disk and partition relation
   *
   * @param diskParts Partition allocation grouped by disk
   */
  void fetchDiskParts(SpaceDiskPartsMap& diskParts) override;

  /**
   * @brief Found a new listener, call handler's method
   *
   * @param spaceId
   * @param partId
   * @param listenerHosts Listener's peer
   */
  void onListenerAdded(GraphSpaceID spaceId,
                       PartitionID partId,
                       const meta::ListenerHosts& listenerHosts) override;

  /**
   * @brief Found a removed listener, call handler's method
   *
   * @param spaceId
   * @param partId
   * @param type Listener type
   */
  void onListenerRemoved(GraphSpaceID spaceId,
                         PartitionID partId,
                         meta::cpp2::ListenerType type) override;

  /**
   * @brief Check if a parition has remote listeners, add or remove if necessary
   *
   * @param spaceId
   * @param partId
   * @param remoteListeners Remote listener infos
   */
  void onCheckRemoteListeners(GraphSpaceID spaceId,
                              PartitionID partId,
                              const std::vector<HostAddr>& remoteListeners) override;

 private:
  meta::MetaClient* client_{nullptr};
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_PARTMANAGER_H_
