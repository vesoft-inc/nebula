/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef KVSTORE_NEBULASTORE_H_
#define KVSTORE_NEBULASTORE_H_

#include <gtest/gtest_prod.h>
#include <folly/RWSpinLock.h>
#include "base/Base.h"
#include "kvstore/KVStore.h"
#include "kvstore/PartManager.h"
#include "kvstore/Part.h"
#include "kvstore/KVEngine.h"

namespace nebula {
namespace kvstore {

// <engine pointer, path>
using Engine = std::pair<std::unique_ptr<KVEngine>, std::string>;

using PartEngine = std::unordered_map<PartitionID, Engine*>;

struct GraphSpaceKV {
    std::unordered_map<PartitionID, std::unique_ptr<Part>> parts_;
    std::vector<Engine> engines_;
};

class NebulaStore : public KVStore, public Handler {
    FRIEND_TEST(KVStoreTest, SimpleTest);
    FRIEND_TEST(KVStoreTest, PartsTest);

public:
    explicit NebulaStore(KVOptions options)
            : partMan_(PartManager::instance())
            , options_(std::move(options)) {}

    ~NebulaStore() = default;

    /**
     * Pull meta information from PartManager and init current instance.
     * */
    void init();

    uint32_t capability() const override {
        return 0;
    }

    ResultCode get(GraphSpaceID spaceId,
                   PartitionID  partId,
                   const std::string& key,
                   std::string* value) override;

    /**
     * Get all results in range [start, end)
     * */
    ResultCode range(GraphSpaceID spaceId,
                     PartitionID  partId,
                     const std::string& start,
                     const std::string& end,
                     std::unique_ptr<KVIterator>* iter) override;

    /**
     * Get all results with prefix.
     * */
    ResultCode prefix(GraphSpaceID spaceId,
                      PartitionID  partId,
                      const std::string& prefix,
                      std::unique_ptr<KVIterator>* iter) override;

    /**
     * async batch put.
     * */
    void asyncMultiPut(GraphSpaceID spaceId,
                       PartitionID  partId,
                       std::vector<KV> keyValues,
                       KVCallback cb) override;

    void asyncRemove(GraphSpaceID spaceId,
                     PartitionID partId,
                     const std::string& key,
                     KVCallback cb) override;

    void asyncRemoveRange(GraphSpaceID spaceId,
                          PartitionID partId,
                          const std::string& start,
                          const std::string& end,
                          KVCallback cb) override;

private:
    /**
     * Inplement two interfaces in Handler.
     * */
    void addSpace(GraphSpaceID spaceId) override;

    void addPart(GraphSpaceID spaceId, PartitionID partId) override;

private:
    Engine newEngine(GraphSpaceID spaceId, std::string rootPath);

    std::unique_ptr<Part> newPart(GraphSpaceID spaceId,
                                  PartitionID partId,
                                  const Engine& engine);

private:
    std::unordered_map<GraphSpaceID, std::unique_ptr<GraphSpaceKV>> kvs_;
    // The lock used to protect kvs_
    folly::RWSpinLock lock_;
    PartManager* partMan_ = nullptr;
    KVOptions options_;
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_NEBULASTORE_H_

