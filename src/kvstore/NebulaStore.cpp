/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
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
DEFINE_int32(custom_filter_interval_secs, 24 * 3600, "interval to trigger custom compaction");

/**
 * Check spaceId, partId exists or not.
 * */
#define CHECK_FOR_WRITE(spaceId, partId, cb) \
    auto it = spaces_.find(spaceId); \
    if (UNLIKELY(it == spaces_.end())) { \
        cb(ResultCode::ERR_SPACE_NOT_FOUND); \
        return; \
    } \
    auto& parts = it->second->parts_; \
    auto partIt = parts.find(partId); \
    if (UNLIKELY(partIt == parts.end())) { \
        cb(ResultCode::ERR_PART_NOT_FOUND); \
        return; \
    }

/**
 * Check spaceId, partId and return related storage engine.
 * */
#define CHECK_AND_RETURN_ENGINE(spaceId, partId) \
    KVEngine* engine = nullptr; \
    do { \
        auto it = spaces_.find(spaceId); \
        if (UNLIKELY(it == spaces_.end())) { \
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
 * Check spaceId is exist and return related partitions.
 */
#define RETURN_IF_SPACE_NOT_FOUND(spaceId, it) \
    it = spaces_.find(spaceId); \
    do { \
        if (UNLIKELY(it == spaces_.end())) { \
            return ResultCode::ERR_SPACE_NOT_FOUND; \
        } \
    } while (false)

/**
 * Check result and return code when it's unsuccess.
 * */
#define RETURN_ON_FAILURE(code) \
    if (code != ResultCode::SUCCEEDED) { \
        return code; \
    }


namespace nebula {
namespace kvstore {

void NebulaStore::init() {
    setClusterId(clusterMan_->getClusterId());

    CHECK(!!partMan_);
    LOG(INFO) << "Scan the local path, and init the spaces_";
    {
        folly::RWSpinLock::WriteHolder wh(&lock_);
        for (auto& path : options_.dataPaths_) {
            auto rootPath = folly::stringPrintf("%s/nebula", path.c_str());
            auto dirs = fs::FileUtils::listAllDirsInDir(rootPath.c_str());
            for (auto& dir : dirs) {
                LOG(INFO) << "Scan path \"" << path << "/" << dir << "\"";
                try {
                    auto spaceId = folly::to<GraphSpaceID>(dir);
                    if (!partMan_->spaceExist(storeSvcAddr_, spaceId)) {
                        // TODO We might want to have a second thought here.
                        // Removing the data directly feels a little strong
                        LOG(INFO) << "Space " << spaceId
                                  << " does not exist any more, remove the data!";
                        auto dataPath = folly::stringPrintf("%s/%s",
                                                            rootPath.c_str(),
                                                            dir.c_str());
                        CHECK(fs::FileUtils::remove(dataPath.c_str(), true));
                        continue;
                    }
                    auto engine = newEngine(spaceId, path);
                    auto spaceIt = this->spaces_.find(spaceId);
                    if (spaceIt == this->spaces_.end()) {
                        LOG(INFO) << "Load space " << spaceId << " from disk";
                        spaceIt = this->spaces_.emplace(
                            spaceId,
                            std::make_unique<SpacePartInfo>()).first;
                    }
                    for (auto& partId : engine->allParts()) {
                        if (!partMan_->partExist(storeSvcAddr_, spaceId, partId)) {
                            LOG(INFO) << "Part " << partId
                                      << " does not exist any more, remove it!";
                            engine->removePart(partId);
                            continue;
                        }
                    }
                    spaceIt->second->engines_.emplace_back(std::move(engine));
                } catch (std::exception& e) {
                    LOG(FATAL) << "Invalid data directory \"" << dir << "\"";
                }
            }
        }
    }

    LOG(INFO) << "Init data from partManager for " << storeSvcAddr_;
    auto partsMap = partMan_->parts(storeSvcAddr_);
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


std::unique_ptr<KVEngine> NebulaStore::newEngine(GraphSpaceID spaceId,
                                                 const std::string& path) {
    if (FLAGS_engine_type == "rocksdb") {
        if (options_.cfFactory_ != nullptr) {
            options_.cfFactory_->construct(spaceId, FLAGS_custom_filter_interval_secs);
        }
        return std::make_unique<RocksEngine>(spaceId,
                                             path,
                                             options_.mergeOp_,
                                             options_.cfFactory_);
    } else {
        LOG(FATAL) << "Unknown engine type " << FLAGS_engine_type;
        return nullptr;
    }
}


void NebulaStore::addSpace(GraphSpaceID spaceId) {
    folly::RWSpinLock::WriteHolder wh(&lock_);
    if (this->spaces_.find(spaceId) != this->spaces_.end()) {
        LOG(INFO) << "Space " << spaceId << " has existed!";
        return;
    }
    LOG(INFO) << "Create space " << spaceId;
    this->spaces_[spaceId] = std::make_unique<SpacePartInfo>();
    for (auto& path : options_.dataPaths_) {
        this->spaces_[spaceId]->engines_.emplace_back(newEngine(spaceId, path));
    }
    return;
}


void NebulaStore::addPart(GraphSpaceID spaceId, PartitionID partId) {
    folly::RWSpinLock::WriteHolder wh(&lock_);
    auto spaceIt = this->spaces_.find(spaceId);
    CHECK(spaceIt != this->spaces_.end()) << "Space should exist!";
    if (spaceIt->second->parts_.find(partId) != spaceIt->second->parts_.end()) {
        LOG(INFO) << "[" << spaceId << "," << partId << "] has existed!";
        return;
    }

    int32_t minIndex = -1;
    int32_t index = 0;
    int32_t minPartsNum = 0x7FFFFFFF;
    auto& engines = spaceIt->second->engines_;
    for (auto& engine : engines) {
        if (engine->totalPartsNum() < minPartsNum) {
            minPartsNum = engine->totalPartsNum();
            minIndex = index;
        }
        index++;
    }
    CHECK_GE(minIndex, 0) << "engines number:" << engines.size();
    const auto& targetEngine = engines[minIndex];

    // Write the information into related engine.
    targetEngine->addPart(partId);
    spaceIt->second->parts_.emplace(
        partId,
        std::make_shared<Part>(spaceId,
                               partId,
                               raftAddr_,
                               folly::stringPrintf("%s/wal/%d",
                                                   targetEngine->getDataRoot(),
                                                   partId),
                               targetEngine.get(),
                               ioPool_,
                               workers_));
    // TODO: Need to pass in the peers
    spaceIt->second->parts_[partId]->start({});
    LOG(INFO) << "Space " << spaceId << ", part " << partId << " has been added!";
    return;
}


void NebulaStore::removeSpace(GraphSpaceID spaceId) {
    folly::RWSpinLock::WriteHolder wh(&lock_);
    auto spaceIt = this->spaces_.find(spaceId);
    auto& engines = spaceIt->second->engines_;
    for (auto& engine : engines) {
        auto parts = engine->allParts();
        for (auto& partId : parts) {
            engine->removePart(partId);
        }
        CHECK_EQ(0, engine->totalPartsNum());
    }
    this->spaces_.erase(spaceIt);
    // TODO(dangleptr): Should we delete the data?
    LOG(INFO) << "Space " << spaceId << " has been removed!";
}


void NebulaStore::removePart(GraphSpaceID spaceId, PartitionID partId) {
    folly::RWSpinLock::WriteHolder wh(&lock_);
    auto spaceIt = this->spaces_.find(spaceId);
    if (spaceIt != this->spaces_.end()) {
        auto partIt = spaceIt->second->parts_.find(partId);
        if (partIt != spaceIt->second->parts_.end()) {
            auto* e = partIt->second->engine();
            CHECK_NOTNULL(e);
            // Stop the raft
            partIt->second->stop();
            spaceIt->second->parts_.erase(partId);
            e->removePart(partId);
        }
    }
    LOG(INFO) << "Space " << spaceId << ", part " << partId << " has been removed!";
}


ResultCode NebulaStore::get(GraphSpaceID spaceId,
                            PartitionID partId,
                            const std::string& key,
                            std::string* value) {
    folly::RWSpinLock::ReadHolder rh(&lock_);
    CHECK_AND_RETURN_ENGINE(spaceId, partId);
    return engine->get(key, value);
}


ResultCode NebulaStore::multiGet(GraphSpaceID spaceId,
                                 PartitionID partId,
                                 const std::vector<std::string>& keys,
                                 std::vector<std::string>* values) {
    folly::RWSpinLock::ReadHolder rh(&lock_);
    CHECK_AND_RETURN_ENGINE(spaceId, partId);
    return engine->multiGet(keys, values);
}


ResultCode NebulaStore::range(GraphSpaceID spaceId,
                              PartitionID partId,
                              const std::string& start,
                              const std::string& end,
                              std::unique_ptr<KVIterator>* iter) {
    folly::RWSpinLock::ReadHolder rh(&lock_);
    CHECK_AND_RETURN_ENGINE(spaceId, partId);
    return engine->range(start, end, iter);
}


ResultCode NebulaStore::prefix(GraphSpaceID spaceId,
                               PartitionID partId,
                               const std::string& prefix,
                               std::unique_ptr<KVIterator>* iter) {
    folly::RWSpinLock::ReadHolder rh(&lock_);
    CHECK_AND_RETURN_ENGINE(spaceId, partId);
    return engine->prefix(prefix, iter);
}


void NebulaStore::asyncMultiPut(GraphSpaceID spaceId,
                                PartitionID partId,
                                std::vector<KV> keyValues,
                                KVCallback cb) {
    folly::RWSpinLock::ReadHolder rh(&lock_);
    CHECK_FOR_WRITE(spaceId, partId, cb);
    return partIt->second->asyncMultiPut(std::move(keyValues), std::move(cb));
}


void NebulaStore::asyncRemove(GraphSpaceID spaceId,
                              PartitionID partId,
                              const std::string& key,
                              KVCallback cb) {
    folly::RWSpinLock::ReadHolder rh(&lock_);
    CHECK_FOR_WRITE(spaceId, partId, cb);
    return partIt->second->asyncRemove(key, std::move(cb));
}


void NebulaStore::asyncMultiRemove(GraphSpaceID spaceId,
                                   PartitionID  partId,
                                   std::vector<std::string> keys,
                                   KVCallback cb) {
    folly::RWSpinLock::ReadHolder rh(&lock_);
    CHECK_FOR_WRITE(spaceId, partId, cb);
    return partIt->second->asyncMultiRemove(std::move(keys), cb);
}


void NebulaStore::asyncRemoveRange(GraphSpaceID spaceId,
                                   PartitionID partId,
                                   const std::string& start,
                                   const std::string& end,
                                   KVCallback cb) {
    folly::RWSpinLock::ReadHolder rh(&lock_);
    CHECK_FOR_WRITE(spaceId, partId, cb);
    return partIt->second->asyncRemoveRange(start, end, std::move(cb));
}


void NebulaStore::asyncRemovePrefix(GraphSpaceID spaceId,
                                    PartitionID partId,
                                    const std::string& prefix,
                                    KVCallback cb) {
    folly::RWSpinLock::ReadHolder rh(&lock_);
    CHECK_FOR_WRITE(spaceId, partId, cb);
    return partIt->second->asyncRemovePrefix(prefix, std::move(cb));
}


ResultCode NebulaStore::ingest(GraphSpaceID spaceId,
                               const std::string& extra,
                               const std::vector<std::string>& files) {
    decltype(spaces_)::iterator it;
    folly::RWSpinLock::ReadHolder rh(&lock_);
    RETURN_IF_SPACE_NOT_FOUND(spaceId, it);
    for (auto& engine : it->second->engines_) {
        auto parts = engine->allParts();
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
        auto code = engine->ingest(std::move(extras));
        RETURN_ON_FAILURE(code);
    }
    return ResultCode::SUCCEEDED;
}


ResultCode NebulaStore::setOption(GraphSpaceID spaceId,
                                  const std::string& configKey,
                                  const std::string& configValue) {
    decltype(spaces_)::iterator it;
    folly::RWSpinLock::ReadHolder rh(&lock_);
    RETURN_IF_SPACE_NOT_FOUND(spaceId, it);
    for (auto& engine : it->second->engines_) {
        auto code = engine->setOption(configKey, configValue);
        RETURN_ON_FAILURE(code);
    }
    return ResultCode::SUCCEEDED;
}


ResultCode NebulaStore::setDBOption(GraphSpaceID spaceId,
                                    const std::string& configKey,
                                    const std::string& configValue) {
    decltype(spaces_)::iterator it;
    folly::RWSpinLock::ReadHolder rh(&lock_);
    RETURN_IF_SPACE_NOT_FOUND(spaceId, it);
    for (auto& engine : it->second->engines_) {
        auto code = engine->setDBOption(configKey, configValue);
        RETURN_ON_FAILURE(code);
    }
    return ResultCode::SUCCEEDED;
}


ResultCode NebulaStore::compactAll(GraphSpaceID spaceId) {
    decltype(spaces_)::iterator it;
    folly::RWSpinLock::ReadHolder rh(&lock_);
    RETURN_IF_SPACE_NOT_FOUND(spaceId, it);
    for (auto& engine : it->second->engines_) {
        auto code = engine->compactAll();
        RETURN_ON_FAILURE(code);
    }
    return ResultCode::SUCCEEDED;
}

bool NebulaStore::isLeader(GraphSpaceID spaceId, PartitionID partId) {
    folly::RWSpinLock::ReadHolder rh(&lock_);
    auto spaceIt = spaces_.find(spaceId);
    if (spaceIt != this->spaces_.end()) {
        auto partIt = spaceIt->second->parts_.find(partId);
        if (partIt != spaceIt->second->parts_.end()) {
            return partIt->second->isLeader();
        } else {
            return false;
        }
    }
    return false;
}

}  // namespace kvstore
}  // namespace nebula

