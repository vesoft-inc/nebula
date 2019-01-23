/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef KVSTORE_PARTMANAGER_H_
#define KVSTORE_PARTMANAGER_H_

#include <gtest/gtest_prod.h>
#include "base/Base.h"

namespace nebula {
namespace kvstore {

using MachineID = uint32_t;

struct PartMeta {
    GraphSpaceID           spaceId_;
    PartitionID            partId_;
    std::vector<MachineID> peers_;
};

using PartsMap  = std::unordered_map<GraphSpaceID, std::unordered_map<PartitionID, PartMeta>>;
/**
 * This class manages all meta information one storage host needed.
 * */
class PartManager {
public:
    /**
     *  Singleton instance will be returned
     * */
    static PartManager* instance();

    virtual ~PartManager() = default;

    /**
     * return PartsMap for machineId
     * */
    virtual PartsMap parts(HostAddr hostAddr) = 0;

    /**
     * return PartMeta for <spaceId, partId>
     * */
    virtual PartMeta partMeta(GraphSpaceID spaceId, PartitionID partId) = 0;

protected:
    PartManager() = default;
    static PartManager* instance_;
};

/**
: * Memory based PartManager, it is used in UTs now.
 * */
class MemPartManager final : public PartManager {
    FRIEND_TEST(KVStoreTest, SimpleTest);
public:
    MemPartManager() = default;

    ~MemPartManager() = default;

    PartsMap parts(HostAddr hostAddr) override;

    PartMeta partMeta(GraphSpaceID spaceId, PartitionID partId) override;

    PartsMap& partsMap() {
        return partsMap_;
    }

private:
    PartsMap partsMap_;
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_PARTMANAGER_H_

