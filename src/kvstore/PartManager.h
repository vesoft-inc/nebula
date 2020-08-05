/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef KVSTORE_PARTMANAGER_H_
#define KVSTORE_PARTMANAGER_H_

#include <gtest/gtest_prod.h>
#include "base/Base.h"
#include "meta/client/MetaClient.h"

namespace nebula {
namespace kvstore {


class Handler {
public:
    Handler() = default;
    virtual ~Handler() = default;
    virtual void addSpace(GraphSpaceID spaceId) = 0;
    virtual void addPart(GraphSpaceID spaceId, PartitionID partId, bool asLearner) = 0;
    virtual void updateSpaceOption(GraphSpaceID spaceId,
                                   const std::unordered_map<std::string, std::string>& options,
                                   bool isDbOption) = 0;
    virtual void removeSpace(GraphSpaceID spaceId) = 0;
    virtual void removePart(GraphSpaceID spaceId, PartitionID partId) = 0;
    virtual int32_t allLeader(std::unordered_map<GraphSpaceID,
                                                 std::vector<PartitionID>>& leaderIds) = 0;
};


/**
 * This class manages all meta information one storage host needed.
 * */
class PartManager {
public:
    PartManager() = default;

    virtual ~PartManager() = default;

    /**
     * return PartsMap for host
     * */
    virtual PartsMap parts(const HostAddr& host) = 0;

    /**
     * return PartMeta for <spaceId, partId>
     * */
    virtual StatusOr<PartMeta> partMeta(GraphSpaceID spaceId, PartitionID partId) = 0;

    /**
     * Check current part exist or not on host.
     * */
    virtual Status partExist(const HostAddr& host, GraphSpaceID spaceId, PartitionID partId) = 0;

    /**
     * Check current space exist or not.
     * */
    virtual Status spaceExist(const HostAddr& host, GraphSpaceID spaceId) = 0;

    /**
     * Register Handler
     * */
    void registerHandler(Handler* handler) {
        handler_ = handler;
    }

protected:
    Handler* handler_ = nullptr;
};


/**
: * Memory based PartManager, it is used in UTs now.
 * */
class MemPartManager final : public PartManager {
    FRIEND_TEST(NebulaStoreTest, SimpleTest);
    FRIEND_TEST(NebulaStoreTest, PartsTest);
    FRIEND_TEST(NebulaStoreTest, ThreeCopiesTest);
    FRIEND_TEST(NebulaStoreTest, TransLeaderTest);
    FRIEND_TEST(NebulaStoreTest, CheckpointTest);
    FRIEND_TEST(NebulaStoreTest, ThreeCopiesCheckpointTest);
    FRIEND_TEST(NebulaStoreTest, AtomicOpBatchTest);

public:
    MemPartManager() = default;

    ~MemPartManager() override = default;

    PartsMap parts(const HostAddr& host) override;

    StatusOr<PartMeta> partMeta(GraphSpaceID spaceId, PartitionID partId) override;

    void addPart(GraphSpaceID spaceId, PartitionID partId, std::vector<HostAddr> peers = {}) {
        bool noSpace = partsMap_.find(spaceId) == partsMap_.end();
        auto& p = partsMap_[spaceId];
        bool noPart = p.find(partId) == p.end();
        p[partId] = PartMeta();
        auto& pm = p[partId];
        pm.spaceId_ = spaceId;
        pm.partId_  = partId;
        pm.peers_ = std::move(peers);
        if (noSpace && handler_) {
            handler_->addSpace(spaceId);
        }
        if (noPart && handler_) {
            handler_->addPart(spaceId, partId, false);
        }
     }

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

    Status partExist(const HostAddr& host, GraphSpaceID spaceId, PartitionID partId) override;

    Status spaceExist(const HostAddr&, GraphSpaceID spaceId) override {
        if (partsMap_.find(spaceId) != partsMap_.end()) {
            return Status::OK();
        } else {
            return Status::SpaceNotFound();
        }
    }

    PartsMap& partsMap() {
        return partsMap_;
    }

private:
    PartsMap partsMap_;
};


class MetaServerBasedPartManager : public PartManager, public meta::MetaChangedListener {
public:
     explicit MetaServerBasedPartManager(HostAddr host, meta::MetaClient *client = nullptr);

     ~MetaServerBasedPartManager() override;

     PartsMap parts(const HostAddr& host) override;

     StatusOr<PartMeta> partMeta(GraphSpaceID spaceId, PartitionID partId) override;

     Status partExist(const HostAddr& host, GraphSpaceID spaceId, PartitionID partId) override;

     Status spaceExist(const HostAddr& host, GraphSpaceID spaceId) override;

     /**
      * Implement the interfaces in MetaChangedListener
      * */
     void onSpaceAdded(GraphSpaceID spaceId) override;

     void onSpaceRemoved(GraphSpaceID spaceId) override;

     void onSpaceOptionUpdated(GraphSpaceID spaceId,
                               const std::unordered_map<std::string, std::string>& options)
                               override;

     void onPartAdded(const PartMeta& partMeta) override;

     void onPartRemoved(GraphSpaceID spaceId, PartitionID partId) override;

     void onPartUpdated(const PartMeta& partMeta) override;

     void fetchLeaderInfo(std::unordered_map<GraphSpaceID,
                                             std::vector<PartitionID>>& leaderParts) override;

     HostAddr getLocalHost() {
        return localHost_;
     }

     /**
      * for UTs, because the port is chosen by system,
      * we should update port after thrift setup
      * */
     void setLocalHost(HostAddr localHost) {
        localHost_ = std::move(localHost);
     }

private:
     meta::MetaClient *client_{nullptr};
     HostAddr localHost_;
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_PARTMANAGER_H_

