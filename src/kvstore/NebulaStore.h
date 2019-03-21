/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef KVSTORE_NEBULASTORE_H_
#define KVSTORE_NEBULASTORE_H_

#include "base/Base.h"
#include <gtest/gtest_prod.h>
#include <folly/RWSpinLock.h>
#include "raftex/RaftexService.h"
#include "kvstore/KVStore.h"
#include "kvstore/PartManager.h"
#include "kvstore/Part.h"
#include "kvstore/KVEngine.h"

namespace nebula {
namespace kvstore {

struct SpacePartInfo {
    std::unordered_map<PartitionID, std::shared_ptr<Part>> parts_;
    std::vector<std::unique_ptr<KVEngine>> engines_;
};


class NebulaStore : public KVStore, public Handler {
    FRIEND_TEST(NebulaStoreTest, SimpleTest);
    FRIEND_TEST(NebulaStoreTest, PartsTest);

public:
    NebulaStore(KVOptions options,
                std::shared_ptr<folly::IOThreadPoolExecutor> ioPool,
                std::shared_ptr<thread::GenericThreadPool> workers,
                HostAddr serviceAddr)
            : ioPool_(ioPool)
            , workers_(workers)
            , storeSvcAddr_(serviceAddr)
            , raftAddr_(serviceAddr.first, serviceAddr.second + 1)
            , options_(std::move(options)) {
        partMan_ = std::move(options_.partMan_);
        init();
    }

    ~NebulaStore() = default;

    /**
     * Pull meta information from the PartManager and initiate
     * the current store instance
     **/
    void init();

    uint32_t capability() const override {
        return 0;
    }

    HostAddr partLeader(GraphSpaceID spaceId, PartitionID partId) override {
        UNUSED(spaceId);
        UNUSED(partId);
        return {0, 0};
    }

    ErrorOr<ResultCode, std::string> get(GraphSpaceID spaceId,
                                         PartitionID partId,
                                         const std::string& key) override;

    ErrorOr<ResultCode, std::vector<std::string>>
    multiGet(GraphSpaceID spaceId,
             PartitionID partId,
             const std::vector<std::string>& keys) override;

    /**
     * Get all results in the range [start, end)
     **/
    ErrorOr<ResultCode, std::unique_ptr<KVIterator>>
    range(GraphSpaceID spaceId,
          PartitionID  partId,
          const std::string& start,
          const std::string& end) override;

    /**
     * Get all results matching the prefix.
     **/
    ErrorOr<ResultCode, std::unique_ptr<KVIterator>>
    prefix(GraphSpaceID spaceId,
           PartitionID  partId,
           const std::string& prefix);

    void asyncMultiPut(GraphSpaceID spaceId,
                       PartitionID  partId,
                       std::vector<KV> keyValues,
                       KVCallback cb) override;

    void asyncRemove(GraphSpaceID spaceId,
                     PartitionID partId,
                     const std::string& key,
                     KVCallback cb) override;

    void asyncMultiRemove(GraphSpaceID spaceId,
                          PartitionID  partId,
                          std::vector<std::string> keys,
                          KVCallback cb) override;

    void asyncRemoveRange(GraphSpaceID spaceId,
                          PartitionID partId,
                          const std::string& start,
                          const std::string& end,
                          KVCallback cb) override;

    ResultCode ingest(GraphSpaceID spaceId,
                      const std::string& extra,
                      const std::vector<std::string>& files);

    ResultCode setOption(GraphSpaceID spaceId,
                         const std::string& configKey,
                         const std::string& configValue);

    ResultCode setDBOption(GraphSpaceID spaceId,
                           const std::string& configKey,
                           const std::string& configValue);

    ResultCode compactAll(GraphSpaceID spaceId);

private:
    using PartEngineMap = std::unordered_map<PartitionID, KVEngine*>;

    /**
     * Implement two interfaces in Handler.
     * */
    void addSpace(GraphSpaceID spaceId) override;

    void addPart(GraphSpaceID spaceId, PartitionID partId) override;

    void removeSpace(GraphSpaceID spaceId) override;

    void removePart(GraphSpaceID spaceId, PartitionID partId) override;

private:
    std::unique_ptr<KVEngine> newEngine(GraphSpaceID spaceId, const std::string& path);

private:
    // The lock used to protect spaces_
    folly::RWSpinLock lock_;
    std::unordered_map<GraphSpaceID, std::unique_ptr<SpacePartInfo>> spaces_;

    std::shared_ptr<folly::IOThreadPoolExecutor> ioPool_;
    std::shared_ptr<thread::GenericThreadPool> workers_;
    HostAddr storeSvcAddr_;
    HostAddr raftAddr_;
    KVOptions options_;

    std::unique_ptr<PartManager> partMan_{nullptr};

    std::shared_ptr<raftex::RaftexService> raftService_;
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_NEBULASTORE_H_

