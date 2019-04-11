/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "kvstore/NebulaStore.h"
#include <folly/Likely.h>
#include <algorithm>
#include <cstdint>
#include "network/NetworkUtils.h"
#include "fs/FileUtils.h"
#include "kvstore/RocksEngine.h"

DEFINE_string(engine_type, "rocksdb", "rocksdb, memory...");
DEFINE_string(part_type, "simple", "simple, consensus...");

/**
 * Check spaceId, partId exists or not.
 * */
#define CHECK_FOR_WRITE(spaceId, partId, cb) \
    folly::RWSpinLock::ReadHolder rh(&lock_); \
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
    folly::RWSpinLock::ReadHolder rh(&lock_); \
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

/**
 * check spaceId is exist and return related partitions
 */
#define CHECK_AND_RETURN_SPACE_ENGINES(spaceId) \
    auto it = kvs_.find(spaceId); \
    if (UNLIKELY(it == kvs_.end())) { \
        return ResultCode::ERR_SPACE_NOT_FOUND; \
    }

namespace nebula {
namespace kvstore {

// static
KVStore* KVStore::instance(KVOptions options) {
    auto* instance = new NebulaStore(std::move(options));
    static_cast<NebulaStore*>(instance)->init();
    return instance;
}


Engine NebulaStore::newEngine(GraphSpaceID spaceId, std::string rootPath) {
    if (FLAGS_engine_type == "rocksdb") {
        auto dataPath = KV_DATA_PATH_FORMAT(rootPath.c_str(), spaceId);
        auto engine = std::make_pair(
                                std::unique_ptr<KVEngine>(
                                    new RocksEngine(spaceId,
                                                    std::move(dataPath),
                                                    options_.mergeOp_,
                                                    options_.cfFactory_)),
                                std::move(rootPath));
        return engine;
    } else {
        LOG(FATAL) << "Unknown Engine type " << FLAGS_engine_type;
    }
}


std::unique_ptr<Part> NebulaStore::newPart(GraphSpaceID spaceId,
                                           PartitionID partId,
                                           const Engine& engine) {
    if (FLAGS_part_type == "simple") {
        return std::unique_ptr<Part>(new SimplePart(
               spaceId,
               partId,
               KV_WAL_PATH_FORMAT(engine.second.c_str(), spaceId, partId),
               engine.first.get()));
    } else {
        LOG(FATAL) << "Unknown Part type " << FLAGS_part_type;
    }
}


void NebulaStore::init() {
    CHECK(!!partMan_);
    LOG(INFO) << "Scan the local path, and init the kvs_";
    {
        folly::RWSpinLock::WriteHolder wh(&lock_);
        for (auto& path : options_.dataPaths_) {
            auto rootPath = folly::stringPrintf("%s/nebula", path.c_str());
            auto dirs = fs::FileUtils::listAllDirsInDir(rootPath.c_str());
            for (auto& dir : dirs) {
                LOG(INFO) << "Scan path " << path << "/" << dir;
                try {
                    auto spaceId = folly::to<GraphSpaceID>(dir);
                    if (!partMan_->spaceExist(options_.local_, spaceId)) {
                        LOG(INFO) << "Space " << spaceId << " not exist any more, remove the data!";
                        auto dataPath = folly::stringPrintf("%s/%s", rootPath.c_str(), dir.c_str());
                        CHECK(fs::FileUtils::remove(dataPath.c_str(), true));
                        continue;
                    }
                    auto engine = newEngine(spaceId, path);
                    auto spaceIt = this->kvs_.find(spaceId);
                    if (spaceIt == this->kvs_.end()) {
                        LOG(INFO) << "Load space " << spaceId << " from disk";
                        this->kvs_.emplace(spaceId, std::make_unique<GraphSpaceKV>());
                    }
                    auto& spaceKV = this->kvs_[spaceId];
                    for (auto& partId : engine.first->allParts()) {
                        if (!partMan_->partExist(options_.local_, spaceId, partId)) {
                            LOG(INFO) << "Part " << partId << " not exist any more, remove it!";
                            engine.first->removePart(partId);
                            continue;
                        }
                        spaceKV->parts_.emplace(partId, newPart(spaceId, partId, engine));
                        LOG(INFO) << "Space " << spaceId
                                  << ", part " << partId << " has been loaded!";
                    }
                    spaceKV->engines_.emplace_back(std::move(engine));
                } catch (std::exception& e) {
                    LOG(FATAL) << "Can't convert " << dir;
                }
            }
        }
    }
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


void NebulaStore::addSpace(GraphSpaceID spaceId) {
    folly::RWSpinLock::WriteHolder wh(&lock_);
    if (this->kvs_.find(spaceId) != this->kvs_.end()) {
        LOG(INFO) << "Space " << spaceId << " has existed!";
        return;
    }
    LOG(INFO) << "Create space " << spaceId;
    this->kvs_[spaceId] = std::make_unique<GraphSpaceKV>();
    for (auto& path : options_.dataPaths_) {
        this->kvs_[spaceId]->engines_.emplace_back(newEngine(spaceId, path));
    }
    return;
}


void NebulaStore::addPart(GraphSpaceID spaceId, PartitionID partId) {
    folly::RWSpinLock::WriteHolder wh(&lock_);
    auto spaceIt = this->kvs_.find(spaceId);
    CHECK(spaceIt != this->kvs_.end()) << "Space should exist!";
    if (spaceIt->second->parts_.find(partId) != spaceIt->second->parts_.end()) {
        LOG(INFO) << "[" << spaceId << "," << partId << "] has existed!";
        return;
    }
    int32_t minIndex = -1, index = 0;
    int32_t minPartsNum = 0x7FFFFFFF;
    auto& engines = spaceIt->second->engines_;
    for (auto& engine : engines) {
        if (engine.first->totalPartsNum() < minPartsNum) {
            minPartsNum = engine.first->totalPartsNum();
            minIndex = index;
        }
        index++;
    }
    CHECK_GE(minIndex, 0) << "engines number:" << engines.size();
    const auto& targetEngine = engines[minIndex];
    // Write the information into related engine.
    targetEngine.first->addPart(partId);
    spaceIt->second->parts_.emplace(partId,
                                    newPart(spaceId, partId, targetEngine));
    LOG(INFO) << "Space " << spaceId << ", part " << partId << " has been added!";
    return;
}


void NebulaStore::removeSpace(GraphSpaceID spaceId) {
    folly::RWSpinLock::WriteHolder wh(&lock_);
    auto spaceIt = this->kvs_.find(spaceId);
    auto& engines = spaceIt->second->engines_;
    for (auto& engine : engines) {
        auto parts = engine.first->allParts();
        for (auto& partId : parts) {
            engine.first->removePart(partId);
        }
        CHECK_EQ(0, engine.first->totalPartsNum());
    }
    this->kvs_.erase(spaceIt);
    // TODO(dangleptr): Should we delete the data?
    LOG(INFO) << "Space " << spaceId << " has been removed!";
}


void NebulaStore::removePart(GraphSpaceID spaceId, PartitionID partId) {
    folly::RWSpinLock::WriteHolder wh(&lock_);
    auto spaceIt = this->kvs_.find(spaceId);
    if (spaceIt != this->kvs_.end()) {
        auto partIt = spaceIt->second->parts_.find(partId);
        if (partIt != spaceIt->second->parts_.end()) {
            auto* e = partIt->second->engine();
            CHECK_NOTNULL(e);
            e->removePart(partId);
        }
    }
    LOG(INFO) << "Space " << spaceId << ", part " << partId << " has been removed!";
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

void NebulaStore::asyncMultiRemove(GraphSpaceID spaceId,
                                   PartitionID  partId,
                                   std::vector<std::string> keys,
                                   KVCallback cb) {
    CHECK_FOR_WRITE(spaceId, partId, cb);
    return partIt->second->asyncMultiRemove(std::move(keys), cb);
}

void NebulaStore::asyncRemoveRange(GraphSpaceID spaceId,
                                   PartitionID partId,
                                   const std::string& start,
                                   const std::string& end,
                                   KVCallback cb) {
    CHECK_FOR_WRITE(spaceId, partId, cb);
    return partIt->second->asyncRemoveRange(start, end, std::move(cb));
}


ResultCode NebulaStore::ingest(GraphSpaceID spaceId,
                               const std::string& extra,
                               const std::vector<std::string>& files) {
    CHECK_AND_RETURN_SPACE_ENGINES(spaceId);
    for (auto& engine : it->second->engines_) {
        auto parts = engine.first->allParts();
        std::vector<std::string> extras;
        for (auto part : parts) {
            for (auto file : files) {
                auto extraPath = folly::stringPrintf("%s/nebula/%d/%d/%s",
                                                     extra.c_str(),
                                                     spaceId,
                                                     part,
                                                     file.c_str());
                LOG(INFO) << "Loading extra path : " << extraPath;
                extras.emplace_back(std::move(extraPath));
            }
        }
        auto code = engine.first->ingest(std::move(extras));
        if (code != ResultCode::SUCCEEDED) {
            return code;
        }
    }
    return ResultCode::SUCCEEDED;
}


ResultCode NebulaStore::setOption(GraphSpaceID spaceId,
                                  const std::string& config_key,
                                  const std::string& config_value) {
    CHECK_AND_RETURN_SPACE_ENGINES(spaceId);
    for (auto& engine : it->second->engines_) {
        auto code = engine.first->setOption(config_key, config_value);
        if (code != ResultCode::SUCCEEDED) {
            return code;
        }
    }
    return ResultCode::SUCCEEDED;
}


ResultCode NebulaStore::setDBOption(GraphSpaceID spaceId,
                                    const std::string& config_key,
                                    const std::string& config_value) {
    CHECK_AND_RETURN_SPACE_ENGINES(spaceId);
    for (auto& engine : it->second->engines_) {
        auto code = engine.first->setDBOption(config_key, config_value);
        if (code != ResultCode::SUCCEEDED) {
            return code;
        }
    }
    return ResultCode::SUCCEEDED;
}

}  // namespace kvstore
}  // namespace nebula

