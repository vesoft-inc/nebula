/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef STORAGE_KVSTOREIMPL_H_
#define STORAGE_KVSTOREIMPL_H_

#include <gtest/gtest_prod.h>
#include "base/Base.h"
#include "storage/include/KVStore.h"
#include "storage/PartManager.h"
#include "storage/Part.h"
#include "storage/StorageEngine.h"

namespace nebula {
namespace storage {

// <engine pointer, path>
using Engine = std::pair<std::unique_ptr<StorageEngine>, std::string>;

struct GraphSpaceKV {
    std::unordered_map<PartitionID, std::unique_ptr<Part>> parts_;
    std::vector<Engine> engines_;
};

class KVStoreImpl : public KVStore {
    FRIEND_TEST(KVStoreTest, SimpleTest);
public:
    KVStoreImpl(HostAddr local, std::vector<std::string> paths)
        : partMan_(PartManager::instance())
        , local_(local)
        , paths_(std::move(paths)) {}

    ~KVStoreImpl() = default;

    /**
     * Pull meta information from PartManager and init current instance.
     * */
    void init();

    ResultCode get(GraphSpaceID spaceId,
                   PartitionID  partId,
                   const std::string& key,
                   std::string& value) override;

    /**
     * Get all results in range [start, end)
     * */
    ResultCode range(GraphSpaceID spaceId,
                     PartitionID  partId,
                     const std::string& start,
                     const std::string& end,
                     std::unique_ptr<StorageIter>& iter) override;

    /**
     * Get all results with prefix.
     * */
    ResultCode prefix(GraphSpaceID spaceId,
                      PartitionID  partId,
                      const std::string& prefix,
                      std::unique_ptr<StorageIter>& iter) override;

    /**
     * async batch put.
     * */
    ResultCode asyncMultiPut(GraphSpaceID spaceId,
                             PartitionID  partId,
                             std::vector<KV> keyValues,
                             KVCallback cb) override;

private:
    std::vector<Engine> initEngines(GraphSpaceID spaceId);

private:
    std::unordered_map<GraphSpaceID, std::unique_ptr<GraphSpaceKV>> kvs_;
    PartManager* partMan_ = nullptr;
    HostAddr local_;
    std::vector<std::string> paths_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_KVSTOREIMPL_H_

