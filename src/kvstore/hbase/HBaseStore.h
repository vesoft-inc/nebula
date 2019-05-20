/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef KVSTORE_HBASESTORE_H_
#define KVSTORE_HBASESTORE_H_

#include "base/Base.h"
#include "kvstore/KVStore.h"
#include "kvstore/PartManager.h"
#include "kvstore/KVEngine.h"
#include <gtest/gtest_prod.h>

namespace nebula {
namespace kvstore {

struct GraphSpaceHBase {
    Engine engine_;
    std::set<PartitionID> parts_;
};


class HBaseStore : public KVStore, public Handler {
    FRIEND_TEST(HBaseStoreTest, SimpleTest);

public:
    explicit HBaseStore(KVOptions options)
            : options_(std::move(options)) {
        schemaMan_ = std::move(options_.schemaMan_);
        partMan_ = std::move(options_.partMan_);
    }

    ~HBaseStore() = default;

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

    ResultCode multiGet(GraphSpaceID spaceId,
                        PartitionID partId,
                        const std::vector<std::string>& keys,
                        std::vector<std::string>* values) override;

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

    void asyncMultiRemove(GraphSpaceID spaceId,
                          PartitionID  partId,
                          std::vector<std::string> keys,
                          KVCallback cb) override;

    void asyncRemoveRange(GraphSpaceID spaceId,
                          PartitionID partId,
                          const std::string& start,
                          const std::string& end,
                          KVCallback cb) override;


private:
    /**
     * Implement two interfaces in Handler.
     * */
    void addSpace(GraphSpaceID spaceId) override;

    void removeSpace(GraphSpaceID spaceId) override;

    void addPart(GraphSpaceID spaceId, PartitionID partId) override;

    void removePart(GraphSpaceID spaceId, PartitionID partId) override;

private:
    Engine newEngine(GraphSpaceID spaceId);

private:
    std::unordered_map<GraphSpaceID, std::unique_ptr<GraphSpaceHBase>> kvs_;

    folly::RWSpinLock lock_;

    std::unique_ptr<meta::SchemaManager> schemaMan_{nullptr};

    std::unique_ptr<PartManager> partMan_{nullptr};

    KVOptions options_;
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_HBASESTORE_H_

