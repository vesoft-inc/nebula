/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef KVSTORE_NEBULASTORE_H_
#define KVSTORE_NEBULASTORE_H_

#include <folly/RWSpinLock.h>
#include <gtest/gtest_prod.h>
#include "common/base/Base.h"
#include "common/interface/gen-cpp2/RaftexServiceAsyncClient.h"
#include "kvstore/KVEngine.h"
#include "kvstore/KVStore.h"
#include "kvstore/Part.h"
#include "kvstore/PartManager.h"
#include "kvstore/raftex/RaftexService.h"
#include "kvstore/Listener.h"
#include "kvstore/ListenerFactory.h"
#include "kvstore/KVEngine.h"
#include "kvstore/raftex/SnapshotManager.h"
#include "utils/Utils.h"

namespace nebula {
namespace kvstore {

using ListenerMap = std::unordered_map<meta::cpp2::ListenerType, std::shared_ptr<Listener>>;

struct SpacePartInfo {
    ~SpacePartInfo() {
        parts_.clear();
        engines_.clear();
        LOG(INFO) << "~SpacePartInfo()";
    }

    std::unordered_map<PartitionID, std::shared_ptr<Part>> parts_;
    std::vector<std::unique_ptr<KVEngine>> engines_;
};

struct SpaceListenerInfo {
    std::unordered_map<PartitionID, ListenerMap> listeners_;
};

class NebulaStore : public KVStore, public Handler {
    FRIEND_TEST(NebulaStoreTest, SimpleTest);
    FRIEND_TEST(NebulaStoreTest, PartsTest);
    FRIEND_TEST(NebulaStoreTest, ThreeCopiesTest);
    FRIEND_TEST(NebulaStoreTest, TransLeaderTest);
    FRIEND_TEST(NebulaStoreTest, CheckpointTest);
    FRIEND_TEST(NebulaStoreTest, ThreeCopiesCheckpointTest);
    friend class ListenerBasicTest;

public:
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
            std::make_shared<thrift::ThriftClientManager<raftex::cpp2::RaftexServiceAsyncClient>>();
    }

    ~NebulaStore();

    // Calculate the raft service address based on the storage service address
    static HostAddr getRaftAddr(const HostAddr& srvcAddr) {
        return Utils::getRaftAddrFromStoreAddr(srvcAddr);
    }

    static HostAddr getStoreAddr(const HostAddr& raftAddr) {
        return Utils::getStoreAddrFromRaftAddr(raftAddr);
    }

    // Pull meta information from the PartManager and initiate
    // the current store instance
    bool init();

    void stop() override;

    uint32_t capability() const override {
        return 0;
    }

    HostAddr address() const {
        return storeSvcAddr_;
    }

    std::shared_ptr<folly::IOThreadPoolExecutor> getIoPool() const {
        return ioPool_;
    }

    std::shared_ptr<thread::GenericThreadPool> getBgWorkers() const {
        return bgWorkers_;
    }

    std::shared_ptr<folly::Executor> getExecutors() const {
        return workers_;
    }

    // Return the current leader
    ErrorOr<ResultCode, HostAddr> partLeader(GraphSpaceID spaceId, PartitionID partId) override;

    PartManager* partManager() const override {
        return options_.partMan_.get();
    }

    bool isListener() const {
        return !options_.listenerPath_.empty();
    }

    ResultCode get(GraphSpaceID spaceId,
                   PartitionID partId,
                   const std::string& key,
                   std::string* value,
                   bool canReadFromFollower = false) override;

    std::pair<ResultCode, std::vector<Status>>
    multiGet(GraphSpaceID spaceId,
             PartitionID partId,
             const std::vector<std::string>& keys,
             std::vector<std::string>* values,
             bool canReadFromFollower = false) override;

    // Get all results in range [start, end)
    ResultCode range(GraphSpaceID spaceId,
                     PartitionID partId,
                     const std::string& start,
                     const std::string& end,
                     std::unique_ptr<KVIterator>* iter,
                     bool canReadFromFollower = false) override;

    // Delete the overloading with a rvalue `start' and `end'
    ResultCode range(GraphSpaceID spaceId,
                     PartitionID partId,
                     std::string&& start,
                     std::string&& end,
                     std::unique_ptr<KVIterator>* iter,
                     bool canReadFromFollower = false) override = delete;

    // Get all results with prefix.
    ResultCode prefix(GraphSpaceID spaceId,
                      PartitionID partId,
                      const std::string& prefix,
                      std::unique_ptr<KVIterator>* iter,
                      bool canReadFromFollower = false) override;

    // Delete the overloading with a rvalue `prefix'
    ResultCode prefix(GraphSpaceID spaceId,
                      PartitionID partId,
                      std::string&& prefix,
                      std::unique_ptr<KVIterator>* iter,
                      bool canReadFromFollower = false) override = delete;

    // Get all results with prefix starting from start
    ResultCode rangeWithPrefix(GraphSpaceID spaceId,
                               PartitionID partId,
                               const std::string& start,
                               const std::string& prefix,
                               std::unique_ptr<KVIterator>* iter,
                               bool canReadFromFollower = false) override;

    // Delete the overloading with a rvalue `prefix'
    ResultCode rangeWithPrefix(GraphSpaceID spaceId,
                               PartitionID partId,
                               std::string&& start,
                               std::string&& prefix,
                               std::unique_ptr<KVIterator>* iter,
                               bool canReadFromFollower = false) override = delete;

    ResultCode sync(GraphSpaceID spaceId, PartitionID partId) override;

    // async batch put.
    void asyncMultiPut(GraphSpaceID spaceId,
                       PartitionID partId,
                       std::vector<KV> keyValues,
                       KVCallback cb) override;

    void asyncRemove(GraphSpaceID spaceId,
                     PartitionID partId,
                     const std::string& key,
                     KVCallback cb) override;

    void asyncMultiRemove(GraphSpaceID spaceId,
                          PartitionID partId,
                          std::vector<std::string> keys,
                          KVCallback cb) override;

    void asyncRemoveRange(GraphSpaceID spaceId,
                          PartitionID partId,
                          const std::string& start,
                          const std::string& end,
                          KVCallback cb) override;

    void asyncAppendBatch(GraphSpaceID spaceId,
                               PartitionID partId,
                               std::string& batch,
                               KVCallback cb) override;

    void asyncAtomicOp(GraphSpaceID spaceId,
                       PartitionID partId,
                       raftex::AtomicOp op,
                       KVCallback cb) override;

    ErrorOr<ResultCode, std::shared_ptr<Part>> part(GraphSpaceID spaceId,
                                                    PartitionID partId) override;

    ResultCode ingest(GraphSpaceID spaceId) override;

    ResultCode setOption(GraphSpaceID spaceId,
                         const std::string& configKey,
                         const std::string& configValue);

    ResultCode setDBOption(GraphSpaceID spaceId,
                           const std::string& configKey,
                           const std::string& configValue);

    ResultCode compact(GraphSpaceID spaceId) override;

    ResultCode flush(GraphSpaceID spaceId) override;

    ErrorOr<ResultCode, std::string> createCheckpoint(GraphSpaceID spaceId,
                                                      const std::string& name) override;

    ResultCode dropCheckpoint(GraphSpaceID spaceId, const std::string& name) override;

    ResultCode setWriteBlocking(GraphSpaceID spaceId, bool sign) override;

    bool isLeader(GraphSpaceID spaceId, PartitionID partId);

    ErrorOr<ResultCode, std::shared_ptr<SpacePartInfo>> space(GraphSpaceID spaceId);

    /**
     * Implement four interfaces in Handler.
     * */
    void addSpace(GraphSpaceID spaceId, bool isListener = false) override;

    void addPart(GraphSpaceID spaceId,
                 PartitionID partId,
                 bool asLearner,
                 const std::vector<HostAddr>& peers = {}) override;

    void removeSpace(GraphSpaceID spaceId, bool isListener) override;

    void removePart(GraphSpaceID spaceId, PartitionID partId) override;

    int32_t allLeader(
        std::unordered_map<GraphSpaceID, std::vector<PartitionID>>& leaderIds) override;

    ErrorOr<ResultCode, std::vector<std::string>> backupTable(
        GraphSpaceID spaceId,
        const std::string& name,
        const std::string& tablePrefix,
        std::function<bool(const folly::StringPiece& key)> filter) override;

    ResultCode restoreFromFiles(GraphSpaceID spaceId,
                                const std::vector<std::string>& files) override;

    void addListener(GraphSpaceID spaceId,
                     PartitionID partId,
                     meta::cpp2::ListenerType type,
                     const std::vector<HostAddr>& peers) override;

    void removeListener(GraphSpaceID spaceId,
                        PartitionID partId,
                        meta::cpp2::ListenerType type) override;

    void checkRemoteListeners(GraphSpaceID spaceId,
                              PartitionID partId,
                              const std::vector<HostAddr>& remoteListeners) override;

    ResultCode multiPutWithoutReplicator(GraphSpaceID spaceId,
                                        std::vector<KV> keyValues) override;

private:
    void loadPartFromDataPath();

    void loadPartFromPartManager();

    void loadLocalListenerFromPartManager();

    void loadRemoteListenerFromPartManager();

    void updateSpaceOption(GraphSpaceID spaceId,
                           const std::unordered_map<std::string, std::string>& options,
                           bool isDbOption) override;

    std::unique_ptr<KVEngine> newEngine(GraphSpaceID spaceId, const std::string& path);

    std::shared_ptr<Part> newPart(GraphSpaceID spaceId,
                                  PartitionID partId,
                                  KVEngine* engine,
                                  bool asLearner,
                                  const std::vector<HostAddr>& defaultPeers);

    std::shared_ptr<Listener> newListener(GraphSpaceID spaceId,
                                          PartitionID partId,
                                          meta::cpp2::ListenerType type,
                                          const std::vector<HostAddr>& peers);

    ErrorOr<ResultCode, KVEngine*> engine(GraphSpaceID spaceId, PartitionID partId);

    bool checkLeader(std::shared_ptr<Part> part, bool canReadFromFollower = false) const;

    void cleanWAL();

private:
    // The lock used to protect spaces_
    folly::RWSpinLock lock_;
    std::unordered_map<GraphSpaceID, std::shared_ptr<SpacePartInfo>> spaces_;
    std::unordered_map<GraphSpaceID, std::shared_ptr<SpaceListenerInfo>> spaceListeners_;

    std::shared_ptr<folly::IOThreadPoolExecutor> ioPool_;
    std::shared_ptr<thread::GenericThreadPool> bgWorkers_;
    HostAddr storeSvcAddr_;
    std::shared_ptr<folly::Executor> workers_;
    HostAddr raftAddr_;
    KVOptions options_;

    std::shared_ptr<raftex::RaftexService> raftService_;
    std::shared_ptr<raftex::SnapshotManager> snapshot_;
    std::shared_ptr<thrift::ThriftClientManager<raftex::cpp2::RaftexServiceAsyncClient>> clientMan_;
};

}   // namespace kvstore
}   // namespace nebula
#endif   // KVSTORE_NEBULASTORE_H_
