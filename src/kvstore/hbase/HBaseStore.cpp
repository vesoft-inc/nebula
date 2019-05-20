/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "kvstore/hbase/HBaseStore.h"
#include "kvstore/hbase/HBaseEngine.h"
#include <folly/Likely.h>
#include "network/NetworkUtils.h"

/**
 * Check spaceId, partId exists or not.
 * */
#define CHECK_FOR_WRITE(spaceId, partId, cb) \
    folly::RWSpinLock::ReadHolder rh(&lock_); \
    auto spaceIt = kvs_.find(spaceId); \
    if (UNLIKELY(spaceIt == kvs_.end())) { \
        cb(ResultCode::ERR_SPACE_NOT_FOUND, HostAddr(0, 0)); \
        return; \
    } \
    auto& parts = spaceIt->second->parts_; \
    auto partIt = parts.find(partId); \
    if (UNLIKELY(partIt == parts.end())) { \
        cb(ResultCode::ERR_PART_NOT_FOUND, HostAddr(0, 0)); \
        return; \
    }

/**
 * Check spaceId, partId and return related storage engine.
 * */
#define CHECK_AND_RETURN_ENGINE(spaceId, partId) \
    folly::RWSpinLock::ReadHolder rh(&lock_); \
    KVEngine* engine = nullptr; \
    do { \
        auto spaceIt = kvs_.find(spaceId); \
        if (UNLIKELY(spaceIt == kvs_.end())) { \
            return ResultCode::ERR_SPACE_NOT_FOUND; \
        } \
        auto& parts = spaceIt->second->parts_; \
        auto partIt = parts.find(partId); \
        if (UNLIKELY(partIt == parts.end())) { \
            return ResultCode::ERR_PART_NOT_FOUND; \
        } \
        engine = spaceIt->second->engine_.first.get(); \
        CHECK_NOTNULL(engine); \
    } while (false)


namespace nebula {
namespace kvstore {

Engine HBaseStore::newEngine(GraphSpaceID spaceId) {
    std::string ipPort = network::NetworkUtils::intToIPv4(options_.hbaseServer_.first)
                       + ":" + folly::to<std::string>(options_.hbaseServer_.second);
    LOG(INFO) << "Create HBaseEngine for graph space " << spaceId
              << ", and the HBase thrift server is " << ipPort;
    auto engine = std::make_pair(
            std::unique_ptr<KVEngine>(
                new HBaseEngine(spaceId,
                                options_.hbaseServer_,
                                std::move(schemaMan_.get()))),
            ipPort);
    return engine;
}


void HBaseStore::init() {
    CHECK(!!partMan_);
    LOG(INFO) << "Init data from partManager...";
    auto partsMap = partMan_->parts(options_.local_);
    for (auto& entry : partsMap) {
        auto spaceId = entry.first;
        addSpace(spaceId);
        for (auto& partEntry : entry.second) {
            addPart(spaceId, partEntry.first);
        }
    }
    LOG(INFO) << "Register handler...";
    partMan_->registerHandler(this);
}


void HBaseStore::addSpace(GraphSpaceID spaceId) {
    folly::RWSpinLock::WriteHolder wh(&lock_);
    if (this->kvs_.find(spaceId) != this->kvs_.end()) {
        LOG(INFO) << "Space " << spaceId << " has existed!";
        return;
    }
    LOG(INFO) << "Create space " << spaceId;
    this->kvs_[spaceId] = std::make_unique<GraphSpaceHBase>();
    this->kvs_[spaceId]->engine_ = newEngine(spaceId);
    return;
}


void HBaseStore::addPart(GraphSpaceID spaceId, PartitionID partId) {
    folly::RWSpinLock::WriteHolder wh(&lock_);
    auto spaceIt = this->kvs_.find(spaceId);
    CHECK(spaceIt != this->kvs_.end()) << "Space should exist!";
    if (spaceIt->second->parts_.find(partId) != spaceIt->second->parts_.end()) {
        LOG(INFO) << "[" << spaceId << "," << partId << "] has existed!";
        return;
    }
    // Write the information into related engine.
    const auto& engine = spaceIt->second->engine_;
    CHECK_NOTNULL(engine.first.get());
    engine.first->addPart(partId);
    spaceIt->second->parts_.emplace(partId);
    LOG(INFO) << "Space " << spaceId << ", part " << partId << " has been added!";
    return;
}


void HBaseStore::removeSpace(GraphSpaceID spaceId) {
    folly::RWSpinLock::WriteHolder wh(&lock_);
    auto spaceIt = this->kvs_.find(spaceId);
    const auto& engine = spaceIt->second->engine_;
    CHECK_NOTNULL(engine.first.get());
    auto parts = engine.first->allParts();
    for (auto& partId : parts) {
        this->removePart(spaceId, partId);
    }
    CHECK_EQ(0, engine.first->totalPartsNum());
    // Here does *NOT* delete the data, just remove the spaceId from kv_ of Store.
    this->kvs_.erase(spaceIt);
    LOG(INFO) << "Space " << spaceId << " has been removed!";
}


void HBaseStore::removePart(GraphSpaceID spaceId, PartitionID partId) {
    folly::RWSpinLock::WriteHolder wh(&lock_);
    auto spaceIt = this->kvs_.find(spaceId);
    if (spaceIt != this->kvs_.end()) {
        auto partIt = spaceIt->second->parts_.find(partId);
        if (partIt != spaceIt->second->parts_.end()) {
            const auto& engine = spaceIt->second->engine_;
            CHECK_NOTNULL(engine.first.get());
            // The PartitionID is not used in HBase, nothing to do.
            engine.first->removePart(partId);
            spaceIt->second->parts_.erase(partIt);
        }
    }
    LOG(INFO) << "Space " << spaceId << ", part " << partId << " has been removed!";
}


ResultCode HBaseStore::get(GraphSpaceID spaceId, PartitionID partId,
                           const std::string& key,
                           std::string* value) {
    CHECK_AND_RETURN_ENGINE(spaceId, partId);
    return engine->get(key, value);
}


ResultCode HBaseStore::multiGet(GraphSpaceID spaceId, PartitionID partId,
                                const std::vector<std::string>& keys,
                                std::vector<std::string>* values) {
    CHECK_AND_RETURN_ENGINE(spaceId, partId);
    return engine->multiGet(keys, values);
}


ResultCode HBaseStore::range(GraphSpaceID spaceId, PartitionID partId,
                             const std::string& start,
                             const std::string& end,
                             std::unique_ptr<KVIterator>* iter) {
    CHECK_AND_RETURN_ENGINE(spaceId, partId);
    return engine->range(start, end, iter);
}


ResultCode HBaseStore::prefix(GraphSpaceID spaceId, PartitionID partId,
                              const std::string& prefix,
                              std::unique_ptr<KVIterator>* iter) {
    CHECK_AND_RETURN_ENGINE(spaceId, partId);
    return engine->prefix(prefix, iter);
}


void HBaseStore::asyncMultiPut(GraphSpaceID spaceId, PartitionID partId,
                               std::vector<KV> keyValues,
                               KVCallback cb) {
    CHECK_FOR_WRITE(spaceId, partId, cb);
    return cb(spaceIt->second->engine_.first->multiPut(std::move(keyValues)),
              HostAddr(0, 0));
}


void HBaseStore::asyncRemove(GraphSpaceID spaceId,
                             PartitionID partId,
                             const std::string& key,
                             KVCallback cb) {
    CHECK_FOR_WRITE(spaceId, partId, cb);
    return cb(spaceIt->second->engine_.first->remove(key),
              HostAddr(0, 0));
}


void HBaseStore::asyncMultiRemove(GraphSpaceID spaceId,
                                  PartitionID  partId,
                                  std::vector<std::string> keys,
                                  KVCallback cb) {
    CHECK_FOR_WRITE(spaceId, partId, cb);
    return cb(spaceIt->second->engine_.first->multiRemove(std::move(keys)),
              HostAddr(0, 0));
}


void HBaseStore::asyncRemoveRange(GraphSpaceID spaceId,
                                  PartitionID partId,
                                  const std::string& start,
                                  const std::string& end,
                                  KVCallback cb) {
    CHECK_FOR_WRITE(spaceId, partId, cb);
    return cb(spaceIt->second->engine_.first->removeRange(start, end),
              HostAddr(0, 0));
}


}  // namespace kvstore
}  // namespace nebula

