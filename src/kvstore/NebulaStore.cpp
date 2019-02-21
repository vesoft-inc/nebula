/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "kvstore/NebulaStore.h"
#include <folly/Likely.h>
#include <algorithm>
#include <cstdint>
#include "network/NetworkUtils.h"
#include "kvstore/RocksdbEngine.h"

DEFINE_string(engine_type, "rocksdb", "rocksdb, memory...");
DEFINE_string(part_type, "simple", "simple, consensus...");

/**
 * Check spaceId, partId exists or not.
 * */
#define CHECK_FOR_WRITE(spaceId, partId, cb) \
    auto it = kvs_.find(spaceId); \
    if (UNLIKELY(it == kvs_.end())) { \
        cb(ResultCode::ERR_SPACE_NOT_FOUND, HostAddr(0, 0)); \
        return; \
    } \
    auto& parts = it->second->parts_; \
    auto partIt = parts.find(partId); \
    if (UNLIKELY(partIt == parts.end())) { \
        cb(ResultCode::ERR_PART_NOT_FOUND, HostAddr(0, 0)); \
        return; \
    }


/**
 * Check spaceId, partId and return related storage engine.
 * */
#define CHECK_AND_RETURN_ENGINE(spaceId, partId) \
    KVEngine* engine = nullptr; \
    do { \
        auto it = kvs_.find(spaceId); \
        if (UNLIKELY(it == kvs_.end())) { \
            return ResultCode::ERR_SPACE_NOT_FOUND; \
        } \
        auto& parts = it->second->parts_; \
        auto partIt = parts.find(partId); \
        if (UNLIKELY(partIt == parts.end())) { \
            return ResultCode::ERR_PART_NOT_FOUND; \
        } \
        engine = partIt->second->engine(); \
        CHECK_NOTNULL(engine); \
    } while (false)


namespace nebula {
namespace kvstore {

// static
KVStore* KVStore::instance(KVOptions options) {
    auto* instance = new NebulaStore(options);
    static_cast<NebulaStore*>(instance)->init();
    return instance;
}


std::vector<Engine> NebulaStore::initEngines(GraphSpaceID spaceId) {
    decltype(kvs_[spaceId]->engines_) engines;
    for (auto& path : options_.dataPaths_) {
        if (FLAGS_engine_type == "rocksdb") {
            engines.emplace_back(
                new RocksdbEngine(spaceId,
                                  folly::stringPrintf("%s/nebula/%d/data",
                                                      path.c_str(), spaceId),
                                  options_.mergeOp_,
                                  options_.cfFactory_),
                path);
        } else {
            LOG(FATAL) << "Unknown engine type " << FLAGS_engine_type;
        }
    }
    return engines;
}


PartEngine NebulaStore::checkLocalParts(GraphSpaceID spaceId) {
    PartEngine maps;
    for (auto& engine : this->kvs_[spaceId]->engines_) {
        auto parts = engine.first->allParts();
        for (auto partId : parts) {
            if (partMan_->partExist(spaceId, partId)) {
                maps.emplace(partId, &engine);
            } else {
                engine.first->removePart(partId);
            }
        }
    }
    return maps;
}


const Engine& NebulaStore::dispatchPart(GraphSpaceID spaceId,
                                        PartitionID partId,
                                        const PartEngine& maps) {
    auto it = maps.find(partId);
    if (it != maps.end()) {
        return *it->second;
    }
    int32_t minIndex = -1, index = 0;
    int32_t minPartsNum = 0x7FFFFFFF;
    auto& engines = this->kvs_[spaceId]->engines_;
    for (auto& engine : engines) {
        if (engine.first->totalPartsNum() < minPartsNum) {
            minPartsNum = engine.first->totalPartsNum();
            minIndex = index;
        }
        index++;
    }
    CHECK_GE(minIndex, 0) << "engines number:" << engines.size();
    const auto& target = engines[minIndex];
    // Write the information into related engine.
    target.first->addPart(partId);
    return target;
}


void NebulaStore::init() {
    auto partsMap = partMan_->parts(options_.local_);
    LOG(INFO) << "Init all parts, total graph space " << partsMap.size();
    std::for_each(partsMap.begin(), partsMap.end(), [this](auto& idPart) {
        auto spaceId = idPart.first;
        auto& spaceParts = idPart.second;

        this->kvs_[spaceId] = std::make_unique<GraphSpaceKV>();
        this->kvs_[spaceId]->engines_ = initEngines(spaceId);

        auto partEngineMap = checkLocalParts(spaceId);
        // Init kvs[spaceId]->parts
        decltype(this->kvs_[spaceId]->parts_) parts;
        std::for_each(spaceParts.begin(), spaceParts.end(), [&](auto& partItem) {
            auto partId = partItem.first;
            auto& engine = dispatchPart(spaceId, partId, partEngineMap);
            auto& enginePtr = engine.first;
            auto& path = engine.second;
            if (FLAGS_part_type == "simple") {
                parts.emplace(partId, new SimplePart(
                                            spaceId,
                                            partId,
                                            folly::stringPrintf("%s/nebula/%d/wals/%d",
                                                                path.c_str(), spaceId, partId),
                                            enginePtr.get()));
            } else {
                LOG(FATAL) << "Unknown Part type " << FLAGS_part_type;
            }
        });
        this->kvs_[spaceId]->parts_ = std::move(parts);
    });
}


ResultCode NebulaStore::get(GraphSpaceID spaceId, PartitionID partId,
                            const std::string& key,
                            std::string* value) {
    CHECK_AND_RETURN_ENGINE(spaceId, partId);
    return engine->get(key, value);
}


ResultCode NebulaStore::range(GraphSpaceID spaceId, PartitionID partId,
                              const std::string& start,
                              const std::string& end,
                              std::unique_ptr<KVIterator>* iter) {
    CHECK_AND_RETURN_ENGINE(spaceId, partId);
    return engine->range(start, end, iter);
}


ResultCode NebulaStore::prefix(GraphSpaceID spaceId, PartitionID partId,
                               const std::string& prefix,
                               std::unique_ptr<KVIterator>* iter) {
    CHECK_AND_RETURN_ENGINE(spaceId, partId);
    return engine->prefix(prefix, iter);
}


void NebulaStore::asyncMultiPut(GraphSpaceID spaceId, PartitionID partId,
                                std::vector<KV> keyValues,
                                KVCallback cb) {
    CHECK_FOR_WRITE(spaceId, partId, cb);
    return partIt->second->asyncMultiPut(std::move(keyValues), std::move(cb));
}


void NebulaStore::asyncRemove(GraphSpaceID spaceId,
                              PartitionID partId,
                              const std::string& key,
                              KVCallback cb) {
    CHECK_FOR_WRITE(spaceId, partId, cb);
    return partIt->second->asyncRemove(key, std::move(cb));
}


void NebulaStore::asyncRemoveRange(GraphSpaceID spaceId,
                                   PartitionID partId,
                                   const std::string& start,
                                   const std::string& end,
                                   KVCallback cb) {
    CHECK_FOR_WRITE(spaceId, partId, cb);
    return partIt->second->asyncRemoveRange(start, end, std::move(cb));
}

}  // namespace kvstore
}  // namespace nebula

