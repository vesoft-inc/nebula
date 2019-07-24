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
DEFINE_int32(num_workers, 4, "Number of worker threads");

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

NebulaStore::~NebulaStore() {
    workers_->stop();
    workers_->wait();
    LOG(INFO) << "Stop the raft service...";
    raftService_->stop();
    raftService_->waitUntilStop();
    spaces_.clear();
    LOG(INFO) << "~NebulaStore()";
}

void NebulaStore::init() {
    LOG(INFO) << "Start the raft service...";
    workers_ = std::make_shared<thread::GenericThreadPool>();
    workers_->start(FLAGS_num_workers);
    raftService_ = raftex::RaftexService::createService(ioPool_, raftAddr_.second);
    raftService_->waitUntilReady();
    flusher_ = std::make_unique<wal::BufferFlusher>();
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
                    spaceIt->second->engines_.emplace_back(std::move(engine));
                    auto& enginePtr = spaceIt->second->engines_.back();
                    for (auto& partId : enginePtr->allParts()) {
                        if (!partMan_->partExist(storeSvcAddr_, spaceId, partId)) {
                            LOG(INFO) << "Part " << partId
                                      << " does not exist any more, remove it!";
                            enginePtr->removePart(partId);
                            continue;
                        } else {
                            LOG(INFO) << "Load part " << spaceId << ", " << partId << " from disk";
                            spaceIt->second->parts_.emplace(partId,
                                                            newPart(spaceId,
                                                                    partId,
                                                                    enginePtr.get()));
                        }
                    }
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
        std::vector<PartitionID> partIds;
        for (auto it = entry.second.begin(); it != entry.second.end(); it++) {
            partIds.emplace_back(it->first);
        }
        std::sort(partIds.begin(), partIds.end());
        for (auto& partId : partIds) {
            addPart(spaceId, partId);
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

ErrorOr<ResultCode, HostAddr> NebulaStore::partLeader(GraphSpaceID spaceId, PartitionID partId) {
    folly::RWSpinLock::ReadHolder rh(&lock_);
    auto it = spaces_.find(spaceId);
    if (UNLIKELY(it == spaces_.end())) {
        return ResultCode::ERR_SPACE_NOT_FOUND;
    }
    auto& parts = it->second->parts_;
    auto partIt = parts.find(partId);
    if (UNLIKELY(partIt == parts.end())) {
        return ResultCode::ERR_PART_NOT_FOUND;
    }
    return getStoreAddr(partIt->second->leader());
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
        newPart(spaceId, partId, targetEngine.get()));
    LOG(INFO) << "Space " << spaceId << ", part " << partId << " has been added!";
    return;
}

std::shared_ptr<Part> NebulaStore::newPart(GraphSpaceID spaceId,
                                           PartitionID partId,
                                           KVEngine* engine) {
    auto part = std::make_shared<Part>(spaceId,
                                       partId,
                                       raftAddr_,
                                       folly::stringPrintf("%s/wal/%d",
                                               engine->getDataRoot(),
                                               partId),
                                       engine,
                                       ioPool_,
                                       workers_,
                                       flusher_.get());
    auto partMeta = partMan_->partMeta(spaceId, partId);
    std::vector<HostAddr> peers;
    for (auto& h : partMeta.peers_) {
        if (h != storeSvcAddr_) {
            peers.emplace_back(getRaftAddr(h));
            VLOG(1) << "Add peer " << peers.back();
        }
    }
    raftService_->addPartition(part);
    part->start(std::move(peers));
    return part;
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
            raftService_->removePartition(partIt->second);
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
    auto ret = engine(spaceId, partId);
    if (!ok(ret)) {
        return error(ret);
    }
    auto* e = nebula::value(ret);
    return e->get(key, value);
}


ResultCode NebulaStore::multiGet(GraphSpaceID spaceId,
                                 PartitionID partId,
                                 const std::vector<std::string>& keys,
                                 std::vector<std::string>* values) {
    auto ret = engine(spaceId, partId);
    if (!ok(ret)) {
        return error(ret);
    }
    auto* e = nebula::value(ret);
    return e->multiGet(keys, values);
}


ResultCode NebulaStore::range(GraphSpaceID spaceId,
                              PartitionID partId,
                              const std::string& start,
                              const std::string& end,
                              std::unique_ptr<KVIterator>* iter) {
    auto ret = engine(spaceId, partId);
    if (!ok(ret)) {
        return error(ret);
    }
    auto* e = nebula::value(ret);
    return e->range(start, end, iter);
}


ResultCode NebulaStore::prefix(GraphSpaceID spaceId,
                               PartitionID partId,
                               const std::string& prefix,
                               std::unique_ptr<KVIterator>* iter) {
    auto ret = engine(spaceId, partId);
    if (!ok(ret)) {
        return error(ret);
    }
    auto* e = nebula::value(ret);
    return e->prefix(prefix, iter);
}

void NebulaStore::asyncMultiPut(GraphSpaceID spaceId,
                                PartitionID partId,
                                std::vector<KV> keyValues,
                                KVCallback cb) {
    auto ret = part(spaceId, partId);
    if (!ok(ret)) {
        cb(error(ret));
        return;
    }
    auto part = nebula::value(ret);
    return part->asyncMultiPut(std::move(keyValues), std::move(cb));
}


void NebulaStore::asyncRemove(GraphSpaceID spaceId,
                              PartitionID partId,
                              const std::string& key,
                              KVCallback cb) {
    auto ret = part(spaceId, partId);
    if (!ok(ret)) {
        cb(error(ret));
        return;
    }
    auto part = nebula::value(ret);
    return part->asyncRemove(key, std::move(cb));
}


void NebulaStore::asyncMultiRemove(GraphSpaceID spaceId,
                                   PartitionID  partId,
                                   std::vector<std::string> keys,
                                   KVCallback cb) {
    auto ret = part(spaceId, partId);
    if (!ok(ret)) {
        cb(error(ret));
        return;
    }
    auto part = nebula::value(ret);
    return part->asyncMultiRemove(std::move(keys), std::move(cb));
}


void NebulaStore::asyncRemoveRange(GraphSpaceID spaceId,
                                   PartitionID partId,
                                   const std::string& start,
                                   const std::string& end,
                                   KVCallback cb) {
    auto ret = part(spaceId, partId);
    if (!ok(ret)) {
        cb(error(ret));
        return;
    }
    auto part = nebula::value(ret);
    return part->asyncRemoveRange(start, end, std::move(cb));
}


void NebulaStore::asyncRemovePrefix(GraphSpaceID spaceId,
                                    PartitionID partId,
                                    const std::string& prefix,
                                    KVCallback cb) {
    auto ret = part(spaceId, partId);
    if (!ok(ret)) {
        cb(error(ret));
        return;
    }
    auto part = nebula::value(ret);
    return part->asyncRemovePrefix(prefix, std::move(cb));
}

ErrorOr<ResultCode, std::shared_ptr<Part>> NebulaStore::part(GraphSpaceID spaceId,
                                                             PartitionID partId) {
    folly::RWSpinLock::ReadHolder rh(&lock_);
    auto it = spaces_.find(spaceId);
    if (UNLIKELY(it == spaces_.end())) {
        return ResultCode::ERR_SPACE_NOT_FOUND;
    }
    auto& parts = it->second->parts_;
    auto partIt = parts.find(partId);
    if (UNLIKELY(partIt == parts.end())) {
        return ResultCode::ERR_PART_NOT_FOUND;
    }
    return partIt->second;
}


ResultCode NebulaStore::ingest(GraphSpaceID spaceId,
                               const std::string& extra) {
    decltype(spaces_)::iterator it;
    folly::RWSpinLock::ReadHolder rh(&lock_);
    RETURN_IF_SPACE_NOT_FOUND(spaceId, it);
    for (auto& engine : it->second->engines_) {
        auto parts = engine->allParts();
        std::vector<std::string> extras;
        for (auto part : parts) {
            LOG(INFO) << "Loading Part " << part;
            auto path = folly::stringPrintf("%s/%d", extra.c_str(), part);
            if (!nebula::fs::FileUtils::exist(path)) {
                LOG(ERROR) << path << " not existed";
                return ResultCode::ERR_IO_ERROR;
            }

            auto files = nebula::fs::FileUtils::listAllFilesInDir(path.c_str(), true, "*.sst");
            for (auto file : files) {
                VLOG(3) << "Loading extra path : " << file;
                extras.emplace_back(file);
            }
        }
        if (extras.size() != 0) {
            auto code = engine->ingest(std::move(extras));
            RETURN_ON_FAILURE(code);
        }
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

ErrorOr<ResultCode, KVEngine*> NebulaStore::engine(GraphSpaceID spaceId, PartitionID partId) {
    folly::RWSpinLock::ReadHolder rh(&lock_);
    auto it = spaces_.find(spaceId);
    if (UNLIKELY(it == spaces_.end())) {
        return ResultCode::ERR_SPACE_NOT_FOUND;
    }
    auto& parts = it->second->parts_;
    auto partIt = parts.find(partId);
    if (UNLIKELY(partIt == parts.end())) {
        return ResultCode::ERR_PART_NOT_FOUND;
    }
    return partIt->second->engine();
}

}  // namespace kvstore
}  // namespace nebula

