/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef KVSTORE_NEBULASTORE_H_
#define KVSTORE_NEBULASTORE_H_

#include <folly/RWSpinLock.h>
#include <folly/concurrency/ConcurrentHashMap.h>
#include <gtest/gtest_prod.h>

#include "common/base/Base.h"
#include "common/ssl/SSLConfig.h"
#include "common/utils/Utils.h"
#include "interface/gen-cpp2/RaftexServiceAsyncClient.h"
#include "kvstore/DiskManager.h"
#include "kvstore/KVEngine.h"
#include "kvstore/KVStore.h"
#include "kvstore/Part.h"
#include "kvstore/PartManager.h"
#include "kvstore/listener/Listener.h"
#include "kvstore/raftex/RaftexService.h"
#include "kvstore/raftex/SnapshotManager.h"

namespace nebula {
namespace kvstore {

using ListenerMap = std::unordered_map<meta::cpp2::ListenerType, std::shared_ptr<Listener>>;

struct SpacePartInfo {
  ~SpacePartInfo() {
    parts_.clear();
    engines_.clear();
  }

  std::unordered_map<PartitionID, std::shared_ptr<Part>> parts_;
  std::vector<std::unique_ptr<KVEngine>> engines_;
};

struct SpaceListenerInfo {
  std::unordered_map<PartitionID, ListenerMap> listeners_;
};

/**
 * @brief A derived class of KVStore, interfaces to manipulate data
 */
class NebulaStore : public KVStore, public Handler {
  FRIEND_TEST(NebulaStoreTest, SimpleTest);
  FRIEND_TEST(NebulaStoreTest, MultiPathTest);
  FRIEND_TEST(NebulaStoreTest, PartsTest);
  FRIEND_TEST(NebulaStoreTest, PersistPeersTest);
  FRIEND_TEST(NebulaStoreTest, ThreeCopiesTest);
  FRIEND_TEST(NebulaStoreTest, TransLeaderTest);
  FRIEND_TEST(NebulaStoreTest, CheckpointTest);
  FRIEND_TEST(NebulaStoreTest, ThreeCopiesCheckpointTest);
  FRIEND_TEST(NebulaStoreTest, RemoveInvalidSpaceTest);
  friend class ListenerBasicTest;

 public:
  /**
   * @brief Construct a new NebulaStore object
   *
   * @param options
   * @param ioPool IOThreadPool
   * @param serviceAddr Address of NebulaStore, used in raft
   * @param workers Worker thread
   */
  NebulaStore(KVOptions options,
              std::shared_ptr<folly::IOThreadPoolExecutor> ioPool,
              HostAddr serviceAddr,
              std::shared_ptr<folly::Executor> workers)
      : ioPool_(ioPool),
        storeSvcAddr_(serviceAddr),
        workers_(workers),
        raftAddr_(getRaftAddr(serviceAddr)),
        options_(std::move(options)) {
    CHECK_NOTNULL(options_.partMan_);
    clientMan_ =
        std::make_shared<thrift::ThriftClientManager<raftex::cpp2::RaftexServiceAsyncClient>>(
            FLAGS_enable_ssl);
  }

  ~NebulaStore();

  /**
   * @brief Calculate the raft service address based on the storage service address
   *
   * @param srvcAddr Storage service address
   * @return HostAddr Raft service address
   */
  static HostAddr getRaftAddr(const HostAddr& srvcAddr) {
    return Utils::getRaftAddrFromStoreAddr(srvcAddr);
  }

  /**
   * @brief Calculate the storage service address based on the raft service address
   *
   * @param raftAddr Raft service address
   * @return HostAddr Storage service address
   */
  static HostAddr getStoreAddr(const HostAddr& raftAddr) {
    return Utils::getStoreAddrFromRaftAddr(raftAddr);
  }

  /**
   * @brief Pull meta information from the PartManager and initiate the current store instance
   *
   * @return True if succeed. False if failed.
   */
  bool init();

  /**
   * @brief Stop the engine
   */
  void stop() override;

  /**
   * @brief Return bit-wise capability, not used
   */
  uint32_t capability() const override {
    return 0;
  }

  /**
   * @brief Return storage service address
   */
  HostAddr address() const {
    return storeSvcAddr_;
  }

  /**
   * @brief Get the IOThreadPool
   */
  std::shared_ptr<folly::IOThreadPoolExecutor> getIoPool() const {
    return ioPool_;
  }

  /**
   * @brief Get the Background workers
   */
  std::shared_ptr<thread::GenericThreadPool> getBgWorkers() const {
    return bgWorkers_;
  }

  /**
   * @brief Get the worker executors
   */
  std::shared_ptr<folly::Executor> getExecutors() const {
    return workers_;
  }

  /**
   * @brief Return the current leader
   *
   * @param spaceId
   * @param partId
   * @return ErrorOr<nebula::cpp2::ErrorCode, HostAddr> Get the leader address of given partition if
   * succeed, else return ErrorCode
   */
  ErrorOr<nebula::cpp2::ErrorCode, HostAddr> partLeader(GraphSpaceID spaceId,
                                                        PartitionID partId) override;

  /**
   * @brief Return pointer of part manager
   *
   * @return PartManager*
   */
  PartManager* partManager() const override {
    return options_.partMan_.get();
  }

  /**
   * @brief Return the NebulaStore is started as listener
   */
  bool isListener() const {
    return !options_.listenerPath_.empty();
  }

  /**
   * @brief Get the data paths passed from configuration
   *
   * @return std::vector<std::string> Data paths
   */
  std::vector<std::string> getDataRoot() const override {
    return options_.dataPaths_;
  }

  /**
   * @brief Get the Snapshot from engine.
   *
   * @param spaceId
   * @param partID
   * @return const void* Snapshot pointer.
   */
  const void* GetSnapshot(GraphSpaceID spaceId, PartitionID partID) override;

  /**
   * @brief Release snapshot from engine.
   *
   * @param spaceId
   * @param partId
   * @param snapshot
   */
  void ReleaseSnapshot(GraphSpaceID spaceId, PartitionID partId, const void* snapshot) override;

  /**
   * @brief Read a single key
   *
   * @param spaceId
   * @param partId
   * @param key
   * @param value
   * @param canReadFromFollower Whether check if current kvstore is leader of given partition
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode get(GraphSpaceID spaceId,
                              PartitionID partId,
                              const std::string& key,
                              std::string* value,
                              bool canReadFromFollower = false,
                              const void* snapshot = nullptr) override;

  /**
   * @brief Read a list of keys
   *
   * @param spaceId
   * @param partId
   * @param keys Keys to read
   * @param values Pointers of value
   * @param canReadFromFollower Whether check if current kvstore is leader of given partition
   * @return Return std::vector<Status> when succeeded: Result status of each key, if key[i] does
   * not exist, the i-th value in return value would be Status::KeyNotFound. Return ErrorCode when
   * failed
   */
  std::pair<nebula::cpp2::ErrorCode, std::vector<Status>> multiGet(
      GraphSpaceID spaceId,
      PartitionID partId,
      const std::vector<std::string>& keys,
      std::vector<std::string>* values,
      bool canReadFromFollower = false) override;

  /**
   * @brief Get all results in range [start, end)
   *
   * @param spaceId
   * @param partId
   * @param start Start key, inclusive
   * @param end End key, exclusive
   * @param iter Iterator in range [start, end), returns by kv engine
   * @param canReadFromFollower Whether check if current kvstore is leader of given partition
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode range(GraphSpaceID spaceId,
                                PartitionID partId,
                                const std::string& start,
                                const std::string& end,
                                std::unique_ptr<KVIterator>* iter,
                                bool canReadFromFollower = false) override;

  /**
   * @brief To forbid to pass rvalue via the 'range' parameter.
   */
  nebula::cpp2::ErrorCode range(GraphSpaceID spaceId,
                                PartitionID partId,
                                std::string&& start,
                                std::string&& end,
                                std::unique_ptr<KVIterator>* iter,
                                bool canReadFromFollower = false) override = delete;

  /**
   * @brief Get all results with 'prefix' str as prefix.
   *
   * @param spaceId
   * @param partId
   * @param prefix Key of prefix to seek
   * @param iter Iterator of keys starts with 'prefix', returns by kv engine
   * @param canReadFromFollower Whether check if current kvstore is leader of given partition
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode prefix(GraphSpaceID spaceId,
                                 PartitionID partId,
                                 const std::string& prefix,
                                 std::unique_ptr<KVIterator>* iter,
                                 bool canReadFromFollower = false,
                                 const void* snapshot = nullptr) override;

  /**
   * @brief To forbid to pass rvalue via the 'prefix' parameter.
   */
  nebula::cpp2::ErrorCode prefix(GraphSpaceID spaceId,
                                 PartitionID partId,
                                 std::string&& prefix,
                                 std::unique_ptr<KVIterator>* iter,
                                 bool canReadFromFollower = false,
                                 const void* snapshot = nullptr) override = delete;

  /**
   * @brief Get all results with 'prefix' str as prefix starting form 'start'
   *
   * @param spaceId
   * @param partId
   * @param start Start key, inclusive
   * @param prefix The prefix of keys to iterate
   * @param iter Iterator of keys starts with 'prefix' beginning from 'start', returns by kv engine
   * @param canReadFromFollower Whether check if current kvstore is leader of given partition
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode rangeWithPrefix(GraphSpaceID spaceId,
                                          PartitionID partId,
                                          const std::string& start,
                                          const std::string& prefix,
                                          std::unique_ptr<KVIterator>* iter,
                                          bool canReadFromFollower = false) override;

  /**
   * @brief To forbid to pass rvalue via the 'rangeWithPrefix' parameter.
   */
  nebula::cpp2::ErrorCode rangeWithPrefix(GraphSpaceID spaceId,
                                          PartitionID partId,
                                          std::string&& start,
                                          std::string&& prefix,
                                          std::unique_ptr<KVIterator>* iter,
                                          bool canReadFromFollower = false) override = delete;

  /**
   * @brief Synchronize the kvstore across multiple replica by add a empty log
   *
   * @param spaceId
   * @param partId
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode sync(GraphSpaceID spaceId, PartitionID partId) override;

  /**
   * @brief Write multiple key/values to kvstore asynchronously
   *
   * @param spaceId
   * @param partId
   * @param keyValues Key/values to put
   * @param cb Callback when has a result
   */
  void asyncMultiPut(GraphSpaceID spaceId,
                     PartitionID partId,
                     std::vector<KV>&& keyValues,
                     KVCallback cb) override;

  /**
   * @brief Remove a key from kvstore asynchronously
   *
   * @param spaceId
   * @param partId
   * @param key Key to remove
   * @param cb Callback when has a result
   */
  void asyncRemove(GraphSpaceID spaceId,
                   PartitionID partId,
                   const std::string& key,
                   KVCallback cb) override;

  /**
   * @brief Remove multiple keys from kvstore asynchronously
   *
   * @param spaceId
   * @param partId
   * @param key Keys to remove
   * @param cb Callback when has a result
   */
  void asyncMultiRemove(GraphSpaceID spaceId,
                        PartitionID partId,
                        std::vector<std::string>&& keys,
                        KVCallback cb) override;

  /**
   * @brief Remove keys in range [start, end) asynchronously
   *
   * @param spaceId
   * @param partId
   * @param start Start key
   * @param end End key
   * @param cb Callback when has a result
   */
  void asyncRemoveRange(GraphSpaceID spaceId,
                        PartitionID partId,
                        const std::string& start,
                        const std::string& end,
                        KVCallback cb) override;

  /**
   * @brief Async commit multi operation, difference between asyncMultiPut or asyncMultiRemove
   * is this method allow contains both put and remove together, difference between asyncAtomicOp is
   * that asyncAtomicOp may have CAS
   *
   * @param spaceId
   * @param partId
   * @param batch Encoded write batch
   * @param cb Callback when has a result
   */
  void asyncAppendBatch(GraphSpaceID spaceId,
                        PartitionID partId,
                        std::string&& batch,
                        KVCallback cb) override;

  /**
   * @brief Do some atomic operation on kvstore
   *
   * @param spaceId
   * @param partId
   * @param op Atomic operation
   * @param cb Callback when has a result
   */
  void asyncAtomicOp(GraphSpaceID spaceId,
                     PartitionID partId,
                     MergeableAtomicOp op,
                     KVCallback cb) override;

  /**
   * @brief Get the part object of given spaceId and partId
   *
   * @param spaceId
   * @param partId
   * @return ErrorOr<nebula::cpp2::ErrorCode, std::shared_ptr<Part>> Return the part if succeeded,
   * else return ErrorCode
   */
  ErrorOr<nebula::cpp2::ErrorCode, std::shared_ptr<Part>> part(GraphSpaceID spaceId,
                                                               PartitionID partId) override;

  /**
   * @brief Ingest the sst file under download directory
   *
   * @param spaceId
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode ingest(GraphSpaceID spaceId) override;

  /**
   * @brief Set space related rocksdb option
   *
   * @param spaceId
   * @param configKey Config name
   * @param configValue Config value
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode setOption(GraphSpaceID spaceId,
                                    const std::string& configKey,
                                    const std::string& configValue);

  /**
   * @brief Set space related rocksdb db option
   *
   * @param spaceId
   * @param configKey DB Config name
   * @param configValue Config value
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode setDBOption(GraphSpaceID spaceId,
                                      const std::string& configKey,
                                      const std::string& configValue);

  /**
   * @brief Trigger comapction, only used in rocksdb
   *
   * @param spaceId
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode compact(GraphSpaceID spaceId) override;

  /**
   * @brief Trigger flush, only used in rocksdb
   *
   * @param spaceId
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode flush(GraphSpaceID spaceId) override;

  /**
   * @brief Create a Checkpoint, only used in rocksdb
   *
   * @param spaceId
   * @param name Checkpoint name
   * @return ErrorOr<nebula::cpp2::ErrorCode, std::vector<cpp2::CheckpointInfo>> Return the
   * checkpoint info if succeed, else return ErrorCode
   */
  ErrorOr<nebula::cpp2::ErrorCode, std::vector<cpp2::CheckpointInfo>> createCheckpoint(
      GraphSpaceID spaceId, const std::string& name) override;

  /**
   * @brief Trigger kv engine's backup, mainly for rocksdb PlainTable mounted on tmpfs/ramfs
   *
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode backup();

  /**
   * @brief Drop a Checkpoint, only used in rocksdb
   *
   * @param spaceId
   * @param name Checkpoint name
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode dropCheckpoint(GraphSpaceID spaceId, const std::string& name) override;

  /**
   * @brief Set the write blocking flag, if blocked, only heartbeat can be replicated
   *
   * @param spaceId
   * @param sign True to block. Falst to unblock
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode setWriteBlocking(GraphSpaceID spaceId, bool sign) override;

  /**
   * @brief Whether is leader of given partition or not
   *
   * @param spaceId
   * @param partId
   */
  bool isLeader(GraphSpaceID spaceId, PartitionID partId);

  /**
   * @brief Try to retrieve the space part info of given spaceId
   *
   * @param spaceId
   * @return ErrorOr<nebula::cpp2::ErrorCode, std::shared_ptr<SpacePartInfo>> Return space part info
   * when succeed, return Errorcode when failed
   */
  ErrorOr<nebula::cpp2::ErrorCode, std::shared_ptr<SpacePartInfo>> space(GraphSpaceID spaceId);

  /**
   * @brief Try to retrieve the space listener info of given spaceId
   *
   * @param spaceId
   * @return ErrorOr<nebula::cpp2::ErrorCode, std::shared_ptr<SpaceListenerInfo>> Return space
   * listener info when succeed, return Errorcode when failed
   */
  ErrorOr<nebula::cpp2::ErrorCode, std::shared_ptr<SpaceListenerInfo>> spaceListener(
      GraphSpaceID spaceId);

  /**
   * @brief Add a space, called from part manager
   *
   * @param spaceId
   */
  void addSpace(GraphSpaceID spaceId) override;

  /**
   * @brief Add a partition, called from part manager
   *
   * @param spaceId
   * @param partId
   * @param asLearner Whether start partition as learner
   * @param peers Storage peers, do not contain learner address
   */
  void addPart(GraphSpaceID spaceId,
               PartitionID partId,
               bool asLearner,
               const std::vector<HostAddr>& peers) override;

  /**
   * @brief Remove a space, called from part manager
   *
   * @param spaceId
   * @param isListener Whether the space is listener
   */
  void removeSpace(GraphSpaceID spaceId) override;

  /**
   * @brief clear space data, but not remove the data dirs.
   *
   * @param spaceId space which will be cleared.
   * @return
   */
  nebula::cpp2::ErrorCode clearSpace(GraphSpaceID spaceId) override;

  /**
   * @brief Remove a partition, called from part manager
   *
   * @param spaceId
   * @param partId
   * @param needLock if lock_ has already been locked, we need
   * to set needLock as false, or we set it as true
   */
  void removePart(GraphSpaceID spaceId, PartitionID partId, bool needLock = true) override;

  /**
   * @brief Retrieve the leader distribution
   *
   * @param leaderIds The leader address of all partitions
   * @return int32_t The leader count of all spaces
   */
  int32_t allLeader(
      std::unordered_map<GraphSpaceID, std::vector<meta::cpp2::LeaderInfo>>& leaderIds) override;

  /**
   * @brief Backup the data of a table prefix, for meta backup
   *
   * @param spaceId
   * @param name
   * @param tablePrefix Table prefix
   * @param filter Data filter when iterate the table
   * @return ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>> Return the sst file path if
   * succeed, else return ErrorCode
   */
  ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>> backupTable(
      GraphSpaceID spaceId,
      const std::string& name,
      const std::string& tablePrefix,
      std::function<bool(const folly::StringPiece& key)> filter) override;

  /**
   * @brief Restore from sst files
   *
   * @param spaceId
   * @param files SST file path
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode restoreFromFiles(GraphSpaceID spaceId,
                                           const std::vector<std::string>& files) override;

  /**
   * @brief Add a specified type listener to space. Perform extra initialization of given type
   * listener if necessary. User should call addListenerSpace first then addListenerPart.
   *
   * @param spaceId
   * @param type Listener type
   */
  void addListenerSpace(GraphSpaceID spaceId, meta::cpp2::ListenerType type) override;

  /**
   * @brief Remove a specified type listener from space. Perform extra destruction of given type
   * listener if necessary. User should call removeListenerPart first then removeListenerSpace.
   *
   * @param spaceId
   * @param type Listener type
   */
  void removeListenerSpace(GraphSpaceID spaceId, meta::cpp2::ListenerType type) override;

  /**
   * @brief Add a partition as listener
   *
   * @param spaceId
   * @param partId
   * @param type Listener type
   * @param peers Raft peers of listener
   */
  void addListenerPart(GraphSpaceID spaceId,
                       PartitionID partId,
                       meta::cpp2::ListenerType type,
                       const std::vector<HostAddr>& peers) override;

  /**
   * @brief Remove a listener partition
   *
   * @param spaceId
   * @param partId
   * @param type Listener type
   */
  void removeListenerPart(GraphSpaceID spaceId,
                          PartitionID partId,
                          meta::cpp2::ListenerType type) override;

  /**
   * @brief Check if the partition's listener state has changed, add/remove if necessary
   *
   * @param spaceId
   * @param partId
   * @param remoteListeners The given partition's remote listener list
   */
  void checkRemoteListeners(GraphSpaceID spaceId,
                            PartitionID partId,
                            const std::vector<HostAddr>& remoteListeners) override;

  /**
   * @brief Get all partitions grouped by data path and spaceId
   *
   * @param diskParts Get all space data path and all partition in the path
   */
  void fetchDiskParts(SpaceDiskPartsMap& diskParts) override;

  /**
   * @brief return a WriteBatch object to do batch operation
   *
   * @return std::unique_ptr<WriteBatch>
   */
  std::unique_ptr<WriteBatch> startBatchWrite() override;

  /**
   * @brief Write batch to local storage engine only
   *
   * @param spaceId
   * @param batch
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode batchWriteWithoutReplicator(GraphSpaceID spaceId,
                                                      std::unique_ptr<WriteBatch> batch) override;

  /**
   * @brief Get the kvstore property, only used in rocksdb
   *
   * @param spaceId
   * @param property Property name
   * @return ErrorOr<nebula::cpp2::ErrorCode, std::string> Return the property value in string if
   * succeed, else return ErrorCode
   */
  ErrorOr<nebula::cpp2::ErrorCode, std::string> getProperty(GraphSpaceID spaceId,
                                                            const std::string& property) override;

  /**
   * @brief Register callback when found new partition is added
   *
   * @param funcName Modulename
   * @param func Callback
   * @param existParts All existing partitions
   */
  void registerOnNewPartAdded(const std::string& funcName,
                              std::function<void(std::shared_ptr<Part>&)> func,
                              std::vector<std::pair<GraphSpaceID, PartitionID>>& existParts);

  /**
   * @brief Unregister a module's callback
   *
   * @param funcName modulename
   */
  void unregisterOnNewPartAdded(const std::string& funcName) {
    onNewPartAdded_.erase(funcName);
  }

  /**
   * @brief Register callback to cleanup before a space is removed
   *
   * @param func Callback to cleanup
   */
  void registerBeforeRemoveSpace(std::function<void(GraphSpaceID)> func) {
    beforeRemoveSpace_ = func;
  }

  /**
   * @brief Unregister the callback to cleanup before a space is removed
   *
   */
  void unregisterBeforeRemoveSpace() {
    beforeRemoveSpace_ = nullptr;
  }

 private:
  /**
   * @brief Load partitions by reading system part keys in kv engine
   */
  void loadPartFromDataPath();

  /**
   * @brief Load partitions from meta
   */
  void loadPartFromPartManager();

  /**
   * @brief Load and start listener of local address
   */
  void loadLocalListenerFromPartManager();

  /**
   * @brief Load and add remote listener into current partition's raft group
   */
  void loadRemoteListenerFromPartManager();

  /**
   * @brief Update space specific options
   *
   * @param spaceId
   * @param options Options map
   * @param isDbOption
   */
  void updateSpaceOption(GraphSpaceID spaceId,
                         const std::unordered_map<std::string, std::string>& options,
                         bool isDbOption) override;

  /**
   * @brief Asynchronously start a new KV engine on specified path
   *
   * @param spaceId Space ID
   * @param dataPath
   * @param walPath
   * @return folly::Future<std::pair<GraphSpaceID, std::unique_ptr<KVEngine>>>
   */
  folly::Future<std::pair<GraphSpaceID, std::unique_ptr<KVEngine>>> asyncNewEngine(
              GraphSpaceID spaceId, const std::string& dataPath, const std::string& walPath);


  /**
   * @brief Start a new kv engine on specified path
   *
   * @param spaceId
   * @param dataPath
   * @param walPath
   * @return std::unique_ptr<KVEngine>
   */
  std::unique_ptr<KVEngine> newEngine(GraphSpaceID spaceId,
                                      const std::string& dataPath,
                                      const std::string& walPath);

  /**
   * @brief Start a new part
   *
   * @param spaceId
   * @param partId
   * @param engine Partition's related kv engine
   * @param asLearner Whether start as raft learner
   * @param peers All partition raft peers
   * @return std::shared_ptr<Part>
   */
  std::shared_ptr<Part> newPart(GraphSpaceID spaceId,
                                PartitionID partId,
                                KVEngine* engine,
                                bool asLearner,
                                const std::vector<HostAddr>& raftPeers);

  /**
   * @brief Start a new listener part
   *
   * @param spaceId
   * @param partId
   * @param type Listener type
   * @param peers The raft peer's address
   * @return std::shared_ptr<Listener>
   */
  std::shared_ptr<Listener> newListener(GraphSpaceID spaceId,
                                        PartitionID partId,
                                        meta::cpp2::ListenerType type,
                                        const std::vector<HostAddr>& peers);

  /**
   * @brief Get given partition's kv engine
   *
   * @param spaceId
   * @param partId
   * @return ErrorOr<nebula::cpp2::ErrorCode, KVEngine*> Return kv engine if succeed, return
   * ErrorCode if failed
   */
  ErrorOr<nebula::cpp2::ErrorCode, KVEngine*> engine(GraphSpaceID spaceId, PartitionID partId);

  /**
   * @brief Check if the partition is leader of not
   *
   * @param part
   * @param canReadFromFollower If set to true, will skip the check and return true
   * @return True if we regard as leader
   */
  bool checkLeader(std::shared_ptr<Part> part, bool canReadFromFollower = false) const;

  /**
   * @brief clean useless wal
   */
  void cleanWAL();

  /**
   * @brief Get the vertex id length of given space
   *
   * @param spaceId
   * @return int32_t Vertex id length
   */
  int32_t getSpaceVidLen(GraphSpaceID spaceId);

  /**
   * @brief Remove a space's directory
   */
  void removeSpaceDir(const std::string& dir);

 private:
  // The lock used to protect spaces_
  folly::RWSpinLock lock_;
  std::unordered_map<GraphSpaceID, std::shared_ptr<SpacePartInfo>> spaces_;
  std::unordered_map<GraphSpaceID, std::shared_ptr<SpaceListenerInfo>> spaceListeners_;

  std::shared_ptr<folly::IOThreadPoolExecutor> ioPool_;
  std::shared_ptr<thread::GenericWorker> storeWorker_;
  std::shared_ptr<thread::GenericThreadPool> bgWorkers_;
  HostAddr storeSvcAddr_;
  std::shared_ptr<folly::Executor> workers_;
  HostAddr raftAddr_;
  KVOptions options_;

  std::shared_ptr<raftex::RaftexService> raftService_;
  std::shared_ptr<raftex::SnapshotManager> snapshot_;
  std::shared_ptr<thrift::ThriftClientManager<raftex::cpp2::RaftexServiceAsyncClient>> clientMan_;
  std::shared_ptr<DiskManager> diskMan_;
  folly::ConcurrentHashMap<std::string, std::function<void(std::shared_ptr<Part>&)>>
      onNewPartAdded_;
  std::function<void(GraphSpaceID)> beforeRemoveSpace_{nullptr};
};

}  // namespace kvstore
}  // namespace nebula

#endif  // KVSTORE_NEBULASTORE_H_
