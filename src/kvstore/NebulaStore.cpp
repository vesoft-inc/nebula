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
#include "kvstore/SnapshotManagerImpl.h"
#include <folly/ScopeGuard.h>

DEFINE_string(engine_type, "rocksdb", "rocksdb, memory...");
DEFINE_int32(custom_filter_interval_secs, 24 * 3600,
             "interval to trigger custom compaction, < 0 means always do default minor compaction");
DEFINE_int32(num_workers, 4, "Number of worker threads");
DEFINE_bool(check_leader, true, "Check leader or not");
DEFINE_int32(clean_wal_interval_secs, 600, "inerval to trigger clean expired wal");

namespace nebula {
namespace kvstore {

NebulaStore::~NebulaStore() {
    LOG(INFO) << "Cut off the relationship with meta client";
    options_.partMan_.reset();
    LOG(INFO) << "Stop the raft service...";
    raftService_->stop();
    LOG(INFO) << "Waiting for the raft service stop...";
    raftService_->waitUntilStop();
    spaces_.clear();
    bgWorkers_->stop();
    bgWorkers_->wait();
    LOG(INFO) << "~NebulaStore()";
}

bool NebulaStore::init() {
    LOG(INFO) << "Start the raft service...";
    bgWorkers_ = std::make_shared<thread::GenericThreadPool>();
    bgWorkers_->start(FLAGS_num_workers, "nebula-bgworkers");
    snapshot_.reset(new SnapshotManagerImpl(this));
    raftService_ = raftex::RaftexService::createService(ioPool_,
                                                        workers_,
                                                        raftAddr_.second);
    if (!raftService_->start()) {
        LOG(ERROR) << "Start the raft service failed";
        return false;
    }

    CHECK(!!options_.partMan_);
    LOG(INFO) << "Scan the local path, and init the spaces_";
    {
        std::unordered_set<std::pair<GraphSpaceID, PartitionID>> spacePartIdSet;
        for (auto& path : options_.dataPaths_) {
            auto rootPath = folly::stringPrintf("%s/nebula", path.c_str());
            auto dirs = fs::FileUtils::listAllDirsInDir(rootPath.c_str());
            for (auto& dir : dirs) {
                LOG(INFO) << "Scan path \"" << path << "/nebula/" << dir << "\"";
                try {
                    GraphSpaceID spaceId;
                    try {
                        spaceId = folly::to<GraphSpaceID>(dir);
                    } catch (const std::exception& ex) {
                        LOG(ERROR) << "Data path invalid: " << ex.what();
                        return false;
                    }

                    if (!options_.partMan_->spaceExist(storeSvcAddr_, spaceId).ok()) {
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

                    KVEngine* enginePtr = nullptr;
                    {
                        folly::RWSpinLock::WriteHolder wh(&lock_);
                        auto engine = newEngine(spaceId, path);
                        auto spaceIt = this->spaces_.find(spaceId);
                        if (spaceIt == this->spaces_.end()) {
                            LOG(INFO) << "Load space " << spaceId << " from disk";
                            spaceIt = this->spaces_.emplace(
                                spaceId,
                                std::make_unique<SpacePartInfo>()).first;
                        }
                        spaceIt->second->engines_.emplace_back(std::move(engine));
                        enginePtr = spaceIt->second->engines_.back().get();
                    }

                    // partIds is the partition in this host waiting to open
                    std::vector<PartitionID> partIds;
                    for (auto& partId : enginePtr->allParts()) {
                        if (!options_.partMan_->partExist(storeSvcAddr_, spaceId, partId).ok()) {
                            LOG(INFO) << "Part " << partId
                                      << " does not exist any more, remove it!";
                            enginePtr->removePart(partId);
                            continue;
                        } else {
                            auto spacePart = std::make_pair(spaceId, partId);
                            if (spacePartIdSet.find(spacePart) != spacePartIdSet.end()) {
                                LOG(INFO) << "Part " << partId
                                          << " has been loaded, skip current one, remove it!";
                                enginePtr->removePart(partId);
                             } else {
                                spacePartIdSet.emplace(spacePart);
                                partIds.emplace_back(partId);
                             }
                        }
                    }
                    if (partIds.empty()) {
                        continue;
                    }

                    std::atomic<size_t> counter(partIds.size());
                    folly::Baton<true, std::atomic> baton;
                    LOG(INFO) << "Need to open " << partIds.size() << " parts of space " << spaceId;
                    for (auto& partId : partIds) {
                        bgWorkers_->addTask([
                                spaceId, partId, enginePtr, &counter, &baton, this] () mutable {
                            auto part = std::make_shared<Part>(spaceId,
                                                               partId,
                                                               raftAddr_,
                                                               folly::stringPrintf("%s/wal/%d",
                                                                       enginePtr->getDataRoot(),
                                                                       partId),
                                                               enginePtr,
                                                               ioPool_,
                                                               bgWorkers_,
                                                               workers_,
                                                               snapshot_);
                            auto status = options_.partMan_->partMeta(spaceId, partId);
                            if (!status.ok()) {
                                LOG(WARNING) << status.status().toString();
                                return;
                            }
                            auto partMeta = status.value();
                            std::vector<HostAddr> peers;
                            for (auto& h : partMeta.peers_) {
                                if (h != storeSvcAddr_) {
                                    peers.emplace_back(getRaftAddr(h));
                                    VLOG(1) << "Add peer " << peers.back();
                                }
                            }
                            raftService_->addPartition(part);
                            part->start(std::move(peers), false);
                            LOG(INFO) << "Load part " << spaceId << ", " << partId << " from disk";

                            {
                                folly::RWSpinLock::WriteHolder holder(&lock_);
                                auto iter = spaces_.find(spaceId);
                                CHECK(iter != spaces_.end());
                                iter->second->parts_.emplace(partId, part);
                            }
                            counter.fetch_sub(1);
                            if (counter.load() == 0) {
                                baton.post();
                            }
                        });
                    }
                    baton.wait();
                    LOG(INFO) << "Load space " << spaceId << " complete";
                } catch (std::exception& e) {
                    LOG(FATAL) << "Invalid data directory \"" << dir << "\"";
                }
            }
        }
    }

    LOG(INFO) << "Init data from partManager for " << storeSvcAddr_;
    auto partsMap = options_.partMan_->parts(storeSvcAddr_);
    for (auto& entry : partsMap) {
        auto spaceId = entry.first;
        addSpace(spaceId);
        std::vector<PartitionID> partIds;
        for (auto it = entry.second.begin(); it != entry.second.end(); it++) {
            partIds.emplace_back(it->first);
        }
        std::sort(partIds.begin(), partIds.end());
        for (auto& partId : partIds) {
            addPart(spaceId, partId, false);
        }
    }

    bgWorkers_->addDelayTask(FLAGS_clean_wal_interval_secs * 1000, &NebulaStore::cleanWAL, this);

    LOG(INFO) << "Register handler...";
    options_.partMan_->registerHandler(this);
    return true;
}

void NebulaStore::stop() {
    for (const auto& space : spaces_) {
        for (const auto& engine : space.second->engines_) {
            engine->stop();
        }
    }
}

std::unique_ptr<KVEngine> NebulaStore::newEngine(GraphSpaceID spaceId,
                                                 const std::string& path) {
    if (FLAGS_engine_type == "rocksdb") {
        std::shared_ptr<KVCompactionFilterFactory> cfFactory = nullptr;
        if (options_.cffBuilder_ != nullptr) {
            cfFactory = options_.cffBuilder_->buildCfFactory(spaceId);
        }
        return std::make_unique<RocksEngine>(spaceId,
                                             path,
                                             options_.mergeOp_,
                                             cfFactory);
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
}


void NebulaStore::addPart(GraphSpaceID spaceId,
                          PartitionID partId,
                          bool asLearner,
                          const std::vector<HostAddr>& peers) {
    folly::RWSpinLock::WriteHolder wh(&lock_);
    auto spaceIt = this->spaces_.find(spaceId);
    CHECK(spaceIt != this->spaces_.end()) << "Space should exist!";
    auto partIt = spaceIt->second->parts_.find(partId);
    if (partIt != spaceIt->second->parts_.end()) {
        LOG(INFO) << "[Space: " << spaceId << ", Part: " << partId << "] has existed!";
        if (!peers.empty()) {
            LOG(INFO) << "[Space: " << spaceId << ", Part: " << partId << "] check peers...";
            partIt->second->checkAndResetPeers(peers);
        }
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
        newPart(spaceId, partId, targetEngine.get(), asLearner, peers));
    LOG(INFO) << "Space " << spaceId << ", part " << partId
              << " has been added, asLearner " << asLearner;
}

std::shared_ptr<Part> NebulaStore::newPart(GraphSpaceID spaceId,
                                           PartitionID partId,
                                           KVEngine* engine,
                                           bool asLearner,
                                           const std::vector<HostAddr>& defaultPeers) {
    auto part = std::make_shared<Part>(spaceId,
                                       partId,
                                       raftAddr_,
                                       folly::stringPrintf("%s/wal/%d",
                                               engine->getDataRoot(),
                                               partId),
                                       engine,
                                       ioPool_,
                                       bgWorkers_,
                                       workers_,
                                       snapshot_);
    std::vector<HostAddr> peers;
    if (defaultPeers.empty()) {
        // pull the information from meta
        auto metaStatus = options_.partMan_->partMeta(spaceId, partId);
        if (!metaStatus.ok()) {
            LOG(ERROR) << "options_.partMan_->partMeta(spaceId, partId); error: "
                       << metaStatus.status().toString()
                       << " spaceId: " << spaceId << ", partId: " << partId;
            return nullptr;
        }

        auto partMeta = metaStatus.value();
        for (auto& h : partMeta.peers_) {
            if (h != storeSvcAddr_) {
                peers.emplace_back(getRaftAddr(h));
                VLOG(1) << "Add peer " << peers.back();
            }
        }
    } else {
        for (auto& h : defaultPeers) {
            if (h != raftAddr_) {
                peers.emplace_back(h);
            }
        }
    }
    raftService_->addPartition(part);
    part->start(std::move(peers), asLearner);
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
            partIt->second->reset();
            spaceIt->second->parts_.erase(partId);
            e->removePart(partId);
        }
    }
    LOG(INFO) << "Space " << spaceId << ", part " << partId << " has been removed!";
}

void NebulaStore::updateSpaceOption(GraphSpaceID spaceId,
                                    const std::unordered_map<std::string, std::string>& options,
                                    bool isDbOption) {
    if (isDbOption) {
        for (const auto& kv : options) {
            setDBOption(spaceId, kv.first, kv.second);
        }
    } else {
        for (const auto& kv : options) {
            setOption(spaceId, kv.first, kv.second);
        }
    }
}

ResultCode NebulaStore::get(GraphSpaceID spaceId,
                            PartitionID partId,
                            const std::string& key,
                            std::string* value) {
    auto ret = part(spaceId, partId);
    if (!ok(ret)) {
        return error(ret);
    }
    auto part = nebula::value(ret);
    if (!checkLeader(part)) {
        return ResultCode::ERR_LEADER_CHANGED;
    }
    return part->engine()->get(key, value);
}


std::pair<ResultCode, std::vector<Status>> NebulaStore::multiGet(
        GraphSpaceID spaceId,
        PartitionID partId,
        const std::vector<std::string>& keys,
        std::vector<std::string>* values) {
    std::vector<Status> status;
    auto ret = part(spaceId, partId);
    if (!ok(ret)) {
        return {error(ret), status};
    }
    auto part = nebula::value(ret);
    if (!checkLeader(part)) {
        return {ResultCode::ERR_LEADER_CHANGED, status};
    }
    status = part->engine()->multiGet(keys, values);
    auto allExist = std::all_of(status.begin(), status.end(),
                                [] (const auto& s) {
                                    return s.ok();
                                });
    if (allExist) {
        return {ResultCode::SUCCEEDED, status};
    } else {
        return {ResultCode::ERR_PARTIAL_RESULT, status};
    }
}


ResultCode NebulaStore::range(GraphSpaceID spaceId,
                              PartitionID partId,
                              const std::string& start,
                              const std::string& end,
                              std::unique_ptr<KVIterator>* iter) {
    auto ret = part(spaceId, partId);
    if (!ok(ret)) {
        return error(ret);
    }
    auto part = nebula::value(ret);
    if (!checkLeader(part)) {
        return ResultCode::ERR_LEADER_CHANGED;
    }
    return part->engine()->range(start, end, iter);
}


ResultCode NebulaStore::prefix(GraphSpaceID spaceId,
                               PartitionID partId,
                               const std::string& prefix,
                               std::unique_ptr<KVIterator>* iter) {
    auto ret = part(spaceId, partId);
    if (!ok(ret)) {
        return error(ret);
    }
    auto part = nebula::value(ret);
    if (!checkLeader(part)) {
        return ResultCode::ERR_LEADER_CHANGED;
    }
    return part->engine()->prefix(prefix, iter);
}


ResultCode NebulaStore::rangeWithPrefix(GraphSpaceID spaceId,
                                        PartitionID  partId,
                                        const std::string& start,
                                        const std::string& prefix,
                                        std::unique_ptr<KVIterator>* iter) {
    auto ret = part(spaceId, partId);
    if (!ok(ret)) {
        return error(ret);
    }
    auto part = nebula::value(ret);
    if (!checkLeader(part)) {
        return ResultCode::ERR_LEADER_CHANGED;
    }
    return part->engine()->rangeWithPrefix(start, prefix, iter);
}


ResultCode NebulaStore::sync(GraphSpaceID spaceId,
                             PartitionID partId) {
    auto partRet = part(spaceId, partId);
    if (!ok(partRet)) {
        return error(partRet);
    }
    auto part = nebula::value(partRet);
    if (!checkLeader(part)) {
        return ResultCode::ERR_LEADER_CHANGED;
    }
    auto ret = ResultCode::SUCCEEDED;
    folly::Baton<true, std::atomic> baton;
    part->sync([&] (kvstore::ResultCode code) {
        ret = code;
        baton.post();
    });
    baton.wait();
    return ret;
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
    part->asyncMultiPut(std::move(keyValues), std::move(cb));
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
    part->asyncRemove(key, std::move(cb));
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
    part->asyncMultiRemove(std::move(keys), std::move(cb));
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
    part->asyncRemoveRange(start, end, std::move(cb));
}

void NebulaStore::asyncAtomicOp(GraphSpaceID spaceId,
                                PartitionID partId,
                                raftex::AtomicOp op,
                                KVCallback cb) {
    auto ret = part(spaceId, partId);
    if (!ok(ret)) {
        cb(error(ret));
        return;
    }
    auto part = nebula::value(ret);
    part->asyncAtomicOp(std::move(op), std::move(cb));
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

ResultCode NebulaStore::ingest(GraphSpaceID spaceId) {
    return ingest(spaceId, "general");
}

ResultCode NebulaStore::ingestTag(GraphSpaceID spaceId, TagID tagId) {
    return ingest(spaceId, folly::stringPrintf("tag/%d", tagId));
}

ResultCode NebulaStore::ingestEdge(GraphSpaceID spaceId, EdgeType edgeType) {
    return ingest(spaceId, folly::stringPrintf("edge/%d", edgeType));
}

ResultCode NebulaStore::ingest(GraphSpaceID spaceId, const std::string& subdir) {
    auto spaceRet = space(spaceId);
    if (!ok(spaceRet)) {
        LOG(INFO) << "Space not found: " << spaceId;
        return error(spaceRet);
    }
    auto space = nebula::value(spaceRet);

    for (auto& engine : space->engines_) {
        auto parts = engine->allParts();
        for (auto part : parts) {
            auto path = folly::stringPrintf(
                "%s/download/%s/%d",
                engine->getDataRoot(), subdir.c_str(), part);
            if (!fs::FileUtils::exist(path)) {
                LOG(INFO) << "Ingest ignore: "
                          << "part[" << part << "], "
                          << "not exist dir[" << path << "]";
                continue;
            }

            auto files = nebula::fs::FileUtils::listAllFilesInDir(path.c_str(), true, "*.sst");
            if (files.empty()) {
                LOG(INFO) << "Ingest ignore: "
                          << "part[" << part << "], "
                          << "empty dir[" << path << "]";
                continue;
            }

            auto code = engine->ingest(files);
            if (code != ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Ingest failed: "
                           << "part[" << part << "], "
                           << "code[" << code << "], "
                           << "dir[" << path << "], "
                           << "files[" << folly::join(",", files) << "]";
                return code;
            } else {
                LOG(INFO) << "Ingest success: "
                          << "part[" << part << "], "
                          << "dir[" << path << "], "
                          << "files[" << folly::join(",", files) << "]";
            }
        }
    }

    return ResultCode::SUCCEEDED;
}


ResultCode NebulaStore::setOption(GraphSpaceID spaceId,
                                  const std::string& configKey,
                                  const std::string& configValue) {
    auto spaceRet = space(spaceId);
    if (!ok(spaceRet)) {
        return error(spaceRet);
    }
    auto space = nebula::value(spaceRet);
    for (auto& engine : space->engines_) {
        auto code = engine->setOption(configKey, configValue);
        if (code != ResultCode::SUCCEEDED) {
            return code;
        }
    }
    return ResultCode::SUCCEEDED;
}


ResultCode NebulaStore::setDBOption(GraphSpaceID spaceId,
                                    const std::string& configKey,
                                    const std::string& configValue) {
    auto spaceRet = space(spaceId);
    if (!ok(spaceRet)) {
        return error(spaceRet);
    }
    auto space = nebula::value(spaceRet);
    for (auto& engine : space->engines_) {
        auto code = engine->setDBOption(configKey, configValue);
        if (code != ResultCode::SUCCEEDED) {
            return code;
        }
    }
    return ResultCode::SUCCEEDED;
}


ResultCode NebulaStore::compact(GraphSpaceID spaceId) {
    auto spaceRet = space(spaceId);
    if (!ok(spaceRet)) {
        return error(spaceRet);
    }
    auto space = nebula::value(spaceRet);

    auto code = ResultCode::SUCCEEDED;
    std::vector<std::thread> threads;
    LOG(INFO) << "Space " << spaceId << " start compaction.";
    for (auto& engine : space->engines_) {
        threads.emplace_back(std::thread([&engine, &code] {
            auto ret = engine->compact();
            if (ret != ResultCode::SUCCEEDED) {
                code = ret;
            }
        }));
    }

    // Wait for all threads to finish
    for (auto& t : threads) {
        t.join();
    }
    LOG(INFO) << "Space " << spaceId << " compaction done.";
    return code;
}

ResultCode NebulaStore::flush(GraphSpaceID spaceId) {
    auto spaceRet = space(spaceId);
    if (!ok(spaceRet)) {
        return error(spaceRet);
    }
    auto space = nebula::value(spaceRet);
    for (auto& engine : space->engines_) {
        auto code = engine->flush();
        if (code != ResultCode::SUCCEEDED) {
            return code;
        }
    }
    return ResultCode::SUCCEEDED;
}

ResultCode NebulaStore::createCheckpoint(GraphSpaceID spaceId, const std::string& name) {
    auto spaceRet = space(spaceId);
    if (!ok(spaceRet)) {
        return error(spaceRet);
    }

    auto space = nebula::value(spaceRet);
    for (auto& engine : space->engines_) {
        auto code = engine->createCheckpoint(name);
        if (code != ResultCode::SUCCEEDED) {
            return code;
        }
        // create wal hard link for all parts
        auto parts = engine->allParts();
        for (auto& part : parts) {
            auto ret = this->part(spaceId, part);
            if (!ok(ret)) {
                LOG(ERROR) << "Part not found. space : " << spaceId << " Part : " << part;
                return error(ret);
            }
            auto walPath = folly::stringPrintf("%s/checkpoints/%s/wal/%d",
                                                      engine->getDataRoot(), name.c_str(), part);
            auto p = nebula::value(ret);
            if (!p->linkCurrentWAL(walPath.data())) {
                return ResultCode::ERR_CHECKPOINT_ERROR;
            }
        }
    }
    return ResultCode::SUCCEEDED;
}

ResultCode NebulaStore::dropCheckpoint(GraphSpaceID spaceId, const std::string& name) {
    auto spaceRet = space(spaceId);
    if (!ok(spaceRet)) {
        return error(spaceRet);
    }
    auto space = nebula::value(spaceRet);
    for (auto& engine : space->engines_) {
        /**
         * Drop checkpoint and wal together 
         **/
        auto checkpointPath = folly::stringPrintf("%s/checkpoints/%s",
                                                  engine->getDataRoot(),
                                                  name.c_str());
        LOG(INFO) << "Drop checkpoint : " << checkpointPath;
        if (!fs::FileUtils::exist(checkpointPath)) {
            continue;
        }
        if (!fs::FileUtils::remove(checkpointPath.data(), true)) {
            LOG(ERROR) << "Drop checkpoint dir failed : " << checkpointPath;
            return ResultCode::ERR_IO_ERROR;
        }
    }
    return ResultCode::SUCCEEDED;
}

ResultCode NebulaStore::setWriteBlocking(GraphSpaceID spaceId, bool sign) {
    auto spaceRet = space(spaceId);
    if (!ok(spaceRet)) {
        LOG(ERROR) << "Get Space " << spaceId << " Failed";
        return error(spaceRet);
    }
    auto space = nebula::value(spaceRet);
    for (auto& engine : space->engines_) {
        auto parts = engine->allParts();
        for (auto& part : parts) {
            auto partRet = this->part(spaceId, part);
            if (!ok(partRet)) {
                LOG(ERROR) << "Part not found. space : " << spaceId << " Part : " << part;
                return error(partRet);
            }
            auto p = nebula::value(partRet);
            auto ret = ResultCode::SUCCEEDED;
            p->setBlocking(sign);
            if (sign) {
                folly::Baton<true, std::atomic> baton;
                p->sync([&ret, &baton] (kvstore::ResultCode code) {
                    if (kvstore::ResultCode::SUCCEEDED != code) {
                        ret = code;
                    }
                    baton.post();
                });
                baton.wait();
            }
            if (ret != ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Part sync failed. space : " << spaceId << " Part : " << part;
                return ret;
            }
        }
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

ErrorOr<ResultCode, std::shared_ptr<SpacePartInfo>> NebulaStore::space(GraphSpaceID spaceId) {
    folly::RWSpinLock::ReadHolder rh(&lock_);
    auto it = spaces_.find(spaceId);
    if (UNLIKELY(it == spaces_.end())) {
        return ResultCode::ERR_SPACE_NOT_FOUND;
    }
    return it->second;
}

int32_t NebulaStore::allLeader(std::unordered_map<GraphSpaceID,
                                                  std::vector<PartitionID>>& leaderIds) {
    folly::RWSpinLock::ReadHolder rh(&lock_);
    int32_t count = 0;
    for (const auto& spaceIt : spaces_) {
        auto spaceId = spaceIt.first;
        for (const auto& partIt : spaceIt.second->parts_) {
            auto partId = partIt.first;
            if (partIt.second->isLeader()) {
                leaderIds[spaceId].emplace_back(partId);
                ++count;
            }
        }
    }
    return count;
}

bool NebulaStore::checkLeader(std::shared_ptr<Part> part) const {
    return !FLAGS_check_leader || (part->isLeader() && part->leaseValid());
}

void NebulaStore::cleanWAL() {
    SCOPE_EXIT {
        bgWorkers_->addDelayTask(FLAGS_clean_wal_interval_secs * 1000,
                                 &NebulaStore::cleanWAL,
                                 this);
    };
    for (const auto& spaceEntry : spaces_) {
        for (const auto& engine : spaceEntry.second->engines_) {
            engine->flush();
        }
        for (const auto& partEntry : spaceEntry.second->parts_) {
            auto& part = partEntry.second;
            if (part->needToCleanWal()) {
                part->wal()->cleanWAL();
            }
        }
    }
}

}  // namespace kvstore
}  // namespace nebula

