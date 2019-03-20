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
    std::vector<HostAddr>  peers_;
};

using PartsMap  = std::unordered_map<GraphSpaceID, std::unordered_map<PartitionID, PartMeta>>;

class Handler {
public:
    virtual void addSpace(GraphSpaceID spaceId) = 0;
    virtual void addPart(GraphSpaceID spaceId, PartitionID partId) = 0;
};

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

    /**
     * Check current part exist or not.
     * */
    virtual bool partExist(GraphSpaceID spaceId, PartitionID partId) = 0;

    /**
     * Check current space exist or not.
     * */
    virtual bool spaceExist(GraphSpaceID spaceId) = 0;

    /**
     * Register Handler
     * */
    void registerHandler(Handler* handler) {
        handler_ = handler;
    }

protected:
    PartManager() = default;
    static PartManager* instance_;
    Handler* handler_ = nullptr;
};

/**
: * Memory based PartManager, it is used in UTs now.
 * */
class MemPartManager final : public PartManager {
    FRIEND_TEST(KVStoreTest, SimpleTest);
    FRIEND_TEST(KVStoreTest, PartsTest);

public:
    MemPartManager() = default;

    ~MemPartManager() = default;

    PartsMap parts(HostAddr hostAddr) override;

    PartMeta partMeta(GraphSpaceID spaceId, PartitionID partId) override;

    void addPart(GraphSpaceID spaceId, PartitionID partId, std::vector<HostAddr> peers = {}) {
        if (partsMap_.find(spaceId) == partsMap_.end()) {
            if (handler_) {
                handler_->addSpace(spaceId);
            }
        }
        auto& p = partsMap_[spaceId];
        if (p.find(partId) == p.end()) {
            if (handler_) {
                handler_->addPart(spaceId, partId);
            }
        }
        p[partId] = PartMeta();
        auto& pm = p[partId];
        pm.spaceId_ = spaceId;
        pm.partId_  = partId;
        pm.peers_ = std::move(peers);
    }

    bool partExist(GraphSpaceID spaceId, PartitionID partId) override {
        auto it = partsMap_.find(spaceId);
        if (it != partsMap_.end()) {
            auto partIt = it->second.find(partId);
            if (partIt != it->second.end()) {
                return true;
            }
        }
        return false;
    }

    bool spaceExist(GraphSpaceID spaceId) override {
        return partsMap_.find(spaceId) != partsMap_.end();
    }

    void clear() {
        partsMap_.clear();
        handler_ = nullptr;
    }

    PartsMap& partsMap() {
        return partsMap_;
    }

private:
    PartsMap partsMap_;
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_PARTMANAGER_H_

