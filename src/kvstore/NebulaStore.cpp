/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <algorithm>
#include <folly/Likely.h>
#include <folly/ScopeGuard.h>
#include <thrift/lib/cpp/util/EnumUtils.h>

#include "kvstore/NebulaStore.h"
#include "common/fs/FileUtils.h"
#include "common/network/NetworkUtils.h"
#include "kvstore/RocksEngine.h"
#include "kvstore/SnapshotManagerImpl.h"

DEFINE_string(engine_type, "rocksdb", "rocksdb, memory...");
DEFINE_int32(custom_filter_interval_secs, 24 * 3600,
             "interval to trigger custom compaction, < 0 means always do default minor compaction");
DEFINE_int32(num_workers, 4, "Number of worker threads");
DEFINE_int32(clean_wal_interval_secs, 600, "inerval to trigger clean expired wal");
DEFINE_bool(auto_remove_invalid_space, false, "whether remove data of invalid space when restart");

DECLARE_bool(rocksdb_disable_wal);
DECLARE_int32(rocksdb_backup_interval_secs);
DECLARE_int32(wal_ttl);

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
    spaceListeners_.clear();
    bgWorkers_->stop();
    bgWorkers_->wait();
    storeWorker_->stop();
    storeWorker_->wait();
    LOG(INFO) << "~NebulaStore()";
}

bool NebulaStore::init() {
    LOG(INFO) << "Start the raft service...";
    bgWorkers_ = std::make_shared<thread::GenericThreadPool>();
    bgWorkers_->start(FLAGS_num_workers, "nebula-bgworkers");
    storeWorker_ = std::make_shared<thread::GenericWorker>();
    CHECK(storeWorker_->start());
    snapshot_.reset(new SnapshotManagerImpl(this));
    raftService_ = raftex::RaftexService::createService(ioPool_,
                                                        workers_,
                                                        raftAddr_.port);
    if (!raftService_->start()) {
        LOG(ERROR) << "Start the raft service failed";
        return false;
    }
    diskMan_.reset(new DiskManager(options_.dataPaths_, storeWorker_));
    // todo(doodle): we could support listener and normal storage start at same instance
    if (!isListener()) {
        loadPartFromDataPath();
        loadPartFromPartManager();
        loadRemoteListenerFromPartManager();
    } else {
        loadLocalListenerFromPartManager();
    }

    storeWorker_->addDelayTask(FLAGS_clean_wal_interval_secs * 1000, &NebulaStore::cleanWAL, this);
    storeWorker_->addRepeatTask(
        FLAGS_rocksdb_backup_interval_secs * 1000, &NebulaStore::backup, this);
    LOG(INFO) << "Register handler...";
    options_.partMan_->registerHandler(this);
    return true;
}

void NebulaStore::loadPartFromDataPath() {
    CHECK(!!options_.partMan_);
    LOG(INFO) << "Scan the local path, and init the spaces_";
    std::unordered_set<std::pair<GraphSpaceID, PartitionID>> spacePartIdSet;
    for (auto& path : options_.dataPaths_) {
        auto rootPath = folly::stringPrintf("%s/nebula", path.c_str());
        auto dirs = fs::FileUtils::listAllDirsInDir(rootPath.c_str());
        for (auto& dir : dirs) {
            LOG(INFO) << "Scan path \"" << rootPath << "/" << dir << "\"";
            try {
                GraphSpaceID spaceId;
                try {
                    spaceId = folly::to<GraphSpaceID>(dir);
                } catch (const std::exception& ex) {
                    LOG(ERROR) << "Data path invalid: " << ex.what();
                    continue;
                }

                if (!options_.partMan_->spaceExist(storeSvcAddr_, spaceId).ok()) {
                    if (FLAGS_auto_remove_invalid_space) {
                        auto spaceDir = folly::stringPrintf("%s/%s", rootPath.c_str(), dir.c_str());
                        removeSpaceDir(spaceDir);
                    }
                    continue;
                }

                KVEngine* enginePtr = nullptr;
                {
                    folly::RWSpinLock::WriteHolder wh(&lock_);
                    auto engine = newEngine(spaceId, path, options_.walPath_);
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
                        if (spacePartIdSet.find(spacePart) == spacePartIdSet.end()) {
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
                        auto part = newPart(spaceId, partId, enginePtr, false, {});
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

void NebulaStore::loadPartFromPartManager() {
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
}

void NebulaStore::loadLocalListenerFromPartManager() {
    // Initialize listener on the storeSvcAddr_, and start these listener
    LOG(INFO) << "Init listener from partManager for " << storeSvcAddr_;
    auto listenersMap = options_.partMan_->listeners(storeSvcAddr_);
    for (const auto& spaceEntry : listenersMap) {
        auto spaceId = spaceEntry.first;
        for (const auto& partEntry : spaceEntry.second) {
            auto partId = partEntry.first;
            for (const auto& listener : partEntry.second) {
                addListener(spaceId, partId, std::move(listener.type_), std::move(listener.peers_));
            }
        }
    }
}

void NebulaStore::loadRemoteListenerFromPartManager() {
    folly::RWSpinLock::WriteHolder holder(&lock_);
    // Add remote listener host to raft group
    for (auto& spaceEntry : spaces_) {
        auto spaceId = spaceEntry.first;
        for (const auto& partEntry : spaceEntry.second->parts_) {
            auto partId = partEntry.first;
            auto listeners = options_.partMan_->listenerPeerExist(spaceId, partId);
            if (listeners.ok()) {
                for (const auto& info : listeners.value()) {
                    partEntry.second->addListenerPeer(getRaftAddr(info.first));
                }
            }
        }
    }
}

void NebulaStore::stop() {
    for (const auto& space : spaces_) {
        for (const auto& engine : space.second->engines_) {
            engine->stop();
        }
    }
}

std::unique_ptr<KVEngine> NebulaStore::newEngine(GraphSpaceID spaceId,
                                                 const std::string& dataPath,
                                                 const std::string& walPath) {
    if (FLAGS_engine_type == "rocksdb") {
        std::shared_ptr<KVCompactionFilterFactory> cfFactory = nullptr;
        if (options_.cffBuilder_ != nullptr) {
            cfFactory = options_.cffBuilder_->buildCfFactory(spaceId);
        }
        auto vIdLen = getSpaceVidLen(spaceId);
        return std::make_unique<RocksEngine>(
            spaceId, vIdLen, dataPath, walPath, options_.mergeOp_, cfFactory);
    } else {
        LOG(FATAL) << "Unknown engine type " << FLAGS_engine_type;
        return nullptr;
    }
}

ErrorOr<nebula::cpp2::ErrorCode, HostAddr>
NebulaStore::partLeader(GraphSpaceID spaceId, PartitionID partId) {
    folly::RWSpinLock::ReadHolder rh(&lock_);
    auto it = spaces_.find(spaceId);
    if (UNLIKELY(it == spaces_.end())) {
        return nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    }
    auto& parts = it->second->parts_;
    auto partIt = parts.find(partId);
    if (UNLIKELY(partIt == parts.end())) {
        return nebula::cpp2::ErrorCode::E_PART_NOT_FOUND;
    }
    return getStoreAddr(partIt->second->leader());
}

void NebulaStore::addSpace(GraphSpaceID spaceId, bool isListener) {
    folly::RWSpinLock::WriteHolder wh(&lock_);
    if (!isListener) {
        if (this->spaces_.find(spaceId) != this->spaces_.end()) {
            LOG(INFO) << "Data space " << spaceId << " has existed!";
            return;
        }
        LOG(INFO) << "Create data space " << spaceId;
        this->spaces_[spaceId] = std::make_unique<SpacePartInfo>();
        for (auto& path : options_.dataPaths_) {
            this->spaces_[spaceId]->engines_.emplace_back(
                newEngine(spaceId, path, options_.walPath_));
        }
    } else {
        // listener don't need engine for now
        if (this->spaceListeners_.find(spaceId) != this->spaceListeners_.end()) {
            LOG(INFO) << "Listener space " << spaceId << " has existed!";
            return;
        }
        LOG(INFO) << "Create listener space " << spaceId;
        this->spaceListeners_[spaceId] = std::make_unique<SpaceListenerInfo>();
    }
}

int32_t NebulaStore::getSpaceVidLen(GraphSpaceID spaceId) {
    // todo(doodle): the default value may make prefix bloom filter invalid
    int vIdLen = 8;
    if (options_.schemaMan_) {
        auto stVidLen = options_.schemaMan_->getSpaceVidLen(spaceId);
        if (stVidLen.ok()) {
            vIdLen = stVidLen.value();
        }
    }
    return vIdLen;
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
    auto walPath = folly::stringPrintf("%s/wal/%d", engine->getWalRoot(), partId);
    auto part = std::make_shared<Part>(spaceId,
                                       partId,
                                       raftAddr_,
                                       walPath,
                                       engine,
                                       ioPool_,
                                       bgWorkers_,
                                       workers_,
                                       snapshot_,
                                       clientMan_,
                                       diskMan_);
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
        for (auto& h : partMeta.hosts_) {
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
    diskMan_->addPartToPath(spaceId, partId, engine->getDataRoot());
    return part;
}

void NebulaStore::removeSpace(GraphSpaceID spaceId, bool isListener) {
    folly::RWSpinLock::WriteHolder wh(&lock_);
    if (!isListener) {
        auto spaceIt = this->spaces_.find(spaceId);
        if (spaceIt != this->spaces_.end()) {
            auto& engines = spaceIt->second->engines_;
            for (auto& engine : engines) {
                auto parts = engine->allParts();
                for (auto& partId : parts) {
                    engine->removePart(partId);
                }
                CHECK_EQ(0, engine->totalPartsNum());
            }
            CHECK(spaceIt->second->parts_.empty());
            std::vector<std::string> enginePaths;
            if (FLAGS_auto_remove_invalid_space) {
                for (auto& engine : engines) {
                    enginePaths.emplace_back(engine->getDataRoot());
                }
            }
            this->spaces_.erase(spaceIt);
            if (FLAGS_auto_remove_invalid_space) {
                for (const auto& path : enginePaths) {
                    removeSpaceDir(path);
                }
            }
        }
        LOG(INFO) << "Data space " << spaceId << " has been removed!";
    } else {
        auto spaceIt = this->spaceListeners_.find(spaceId);
        if (spaceIt != this->spaceListeners_.end()) {
            for (const auto& partEntry : spaceIt->second->listeners_) {
                CHECK(partEntry.second.empty());
            }
            this->spaceListeners_.erase(spaceIt);
        }
        LOG(INFO) << "Listener space " << spaceId << " has been removed!";
    }
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
            diskMan_->removePartFromPath(spaceId, partId, e->getDataRoot());
            partIt->second->resetPart();
            spaceIt->second->parts_.erase(partId);
            e->removePart(partId);
        }
    }
    LOG(INFO) << "Space " << spaceId << ", part " << partId << " has been removed!";
}

void NebulaStore::addListener(GraphSpaceID spaceId,
                              PartitionID partId,
                              meta::cpp2::ListenerType type,
                              const std::vector<HostAddr>& peers) {
    folly::RWSpinLock::WriteHolder wh(&lock_);
    auto spaceIt = spaceListeners_.find(spaceId);
    if (spaceIt == spaceListeners_.end()) {
        spaceIt = spaceListeners_.emplace(spaceId, std::make_shared<SpaceListenerInfo>()).first;
    }
    auto partIt = spaceIt->second->listeners_.find(partId);
    if (partIt == spaceIt->second->listeners_.end()) {
        partIt = spaceIt->second->listeners_.emplace(partId, ListenerMap()).first;
    }
    auto listener = partIt->second.find(type);
    if (listener != partIt->second.end()) {
        LOG(INFO) << "Listener of type " << apache::thrift::util::enumNameSafe(type)
                  << " of [Space: " << spaceId << ", Part: " << partId << "] has existed!";
        return;
    }
    partIt->second.emplace(type, newListener(spaceId, partId, std::move(type), peers));
    LOG(INFO) << "Listener of type " << apache::thrift::util::enumNameSafe(type)
              << " of [Space: " << spaceId << ", Part: " << partId << "] is added";
    return;
}

std::shared_ptr<Listener> NebulaStore::newListener(GraphSpaceID spaceId,
                                                   PartitionID partId,
                                                   meta::cpp2::ListenerType type,
                                                   const std::vector<HostAddr>& peers) {
    auto walPath = folly::stringPrintf("%s/%d/%d/wal", options_.listenerPath_.c_str(),
                                       spaceId, partId);
    // snapshot manager and client manager is set to nullptr, listener should never use them
    auto listener = ListenerFactory::createListener(type,
                                                    spaceId,
                                                    partId,
                                                    raftAddr_,
                                                    walPath,
                                                    ioPool_,
                                                    bgWorkers_,
                                                    workers_,
                                                    nullptr,
                                                    nullptr,
                                                    nullptr,
                                                    options_.schemaMan_);
    raftService_->addPartition(listener);
    // add raft group as learner
    std::vector<HostAddr> raftPeers;
    std::transform(peers.begin(), peers.end(), std::back_inserter(raftPeers), [this] (auto&& host) {
        CHECK_NE(host, storeSvcAddr_) << "Should not start part and listener on same host";
        return getRaftAddr(host);
    });
    listener->start(std::move(raftPeers));
    return listener;
}

void NebulaStore::removeListener(GraphSpaceID spaceId,
                                 PartitionID partId,
                                 meta::cpp2::ListenerType type) {
    folly::RWSpinLock::WriteHolder wh(&lock_);
    auto spaceIt = spaceListeners_.find(spaceId);
    if (spaceIt != spaceListeners_.end()) {
        auto partIt = spaceIt->second->listeners_.find(partId);
        if (partIt != spaceIt->second->listeners_.end()) {
            auto listener = partIt->second.find(type);
            if (listener != partIt->second.end()) {
                raftService_->removePartition(listener->second);
                listener->second->resetListener();
                partIt->second.erase(type);
                LOG(INFO) << "Listener of type " << apache::thrift::util::enumNameSafe(type)
                          << " of [Space: " << spaceId << ", Part: " << partId << "] is removed";
                return;
            }
        }
    }
    return;
}

void NebulaStore::checkRemoteListeners(GraphSpaceID spaceId,
                                       PartitionID partId,
                                       const std::vector<HostAddr>& remoteListeners) {
    folly::RWSpinLock::ReadHolder rh(&lock_);
    auto spaceIt = spaces_.find(spaceId);
    if (spaceIt != spaces_.end()) {
        auto partIt = spaceIt->second->parts_.find(partId);
        if (partIt != spaceIt->second->parts_.end()) {
            std::set<HostAddr> raftHosts;
            std::transform(remoteListeners.begin(),
                           remoteListeners.end(),
                           std::inserter(raftHosts, raftHosts.end()),
                           [&](const auto& host) { return getRaftAddr(host); });
            if (raftHosts != partIt->second->listeners()) {
                partIt->second->checkRemoteListeners(raftHosts);
            }
        }
    }
}

void NebulaStore::updateSpaceOption(GraphSpaceID spaceId,
                                    const std::unordered_map<std::string, std::string>& options,
                                    bool isDbOption) {
    storeWorker_->addTask([this, spaceId, opts = options, isDbOption] {
        if (isDbOption) {
            for (const auto& kv : opts) {
                setDBOption(spaceId, kv.first, kv.second);
            }
        } else {
            for (const auto& kv : opts) {
                setOption(spaceId, kv.first, kv.second);
            }
        }
    });
}

void NebulaStore::removeSpaceDir(const std::string& dir) {
    try {
        LOG(INFO) << "Try to remove space directory: " << dir;
        boost::filesystem::remove_all(dir);
    } catch (const boost::filesystem::filesystem_error& e) {
        LOG(ERROR) << "Exception caught while remove directory, please delelte it by manual: "
                   << e.what();
    }
}

nebula::cpp2::ErrorCode
NebulaStore::get(GraphSpaceID spaceId,
                 PartitionID partId,
                 const std::string& key,
                 std::string* value,
                 bool canReadFromFollower) {
    auto ret = part(spaceId, partId);
    if (!ok(ret)) {
        return error(ret);
    }
    auto part = nebula::value(ret);
    if (!checkLeader(part, canReadFromFollower)) {
        return nebula::cpp2::ErrorCode::E_LEADER_CHANGED;
    }
    return part->engine()->get(key, value);
}


std::pair<nebula::cpp2::ErrorCode, std::vector<Status>>
NebulaStore::multiGet(GraphSpaceID spaceId,
                      PartitionID partId,
                      const std::vector<std::string>& keys,
                      std::vector<std::string>* values,
                      bool canReadFromFollower) {
    std::vector<Status> status;
    auto ret = part(spaceId, partId);
    if (!ok(ret)) {
        return {error(ret), status};
    }
    auto part = nebula::value(ret);
    if (!checkLeader(part, canReadFromFollower)) {
        return {nebula::cpp2::ErrorCode::E_LEADER_CHANGED, status};
    }
    status = part->engine()->multiGet(keys, values);
    auto allExist = std::all_of(status.begin(), status.end(),
                                [] (const auto& s) {
                                    return s.ok();
                                });
    if (allExist) {
        return {nebula::cpp2::ErrorCode::SUCCEEDED, status};
    } else {
        return {nebula::cpp2::ErrorCode::E_PARTIAL_RESULT, status};
    }
}


nebula::cpp2::ErrorCode
NebulaStore::range(GraphSpaceID spaceId,
                   PartitionID partId,
                   const std::string& start,
                   const std::string& end,
                   std::unique_ptr<KVIterator>* iter,
                   bool canReadFromFollower) {
    auto ret = part(spaceId, partId);
    if (!ok(ret)) {
        return error(ret);
    }
    auto part = nebula::value(ret);
    if (!checkLeader(part, canReadFromFollower)) {
        return nebula::cpp2::ErrorCode::E_LEADER_CHANGED;
    }
    return part->engine()->range(start, end, iter);
}


nebula::cpp2::ErrorCode
NebulaStore::prefix(GraphSpaceID spaceId,
                    PartitionID partId,
                    const std::string& prefix,
                    std::unique_ptr<KVIterator>* iter,
                    bool canReadFromFollower) {
    auto ret = part(spaceId, partId);
    if (!ok(ret)) {
        return error(ret);
    }
    auto part = nebula::value(ret);
    if (!checkLeader(part, canReadFromFollower)) {
        return nebula::cpp2::ErrorCode::E_LEADER_CHANGED;
    }
    return part->engine()->prefix(prefix, iter);
}


nebula::cpp2::ErrorCode
NebulaStore::rangeWithPrefix(GraphSpaceID spaceId,
                             PartitionID  partId,
                             const std::string& start,
                             const std::string& prefix,
                             std::unique_ptr<KVIterator>* iter,
                             bool canReadFromFollower) {
    auto ret = part(spaceId, partId);
    if (!ok(ret)) {
        return error(ret);
    }
    auto part = nebula::value(ret);
    if (!checkLeader(part, canReadFromFollower)) {
        return nebula::cpp2::ErrorCode::E_LEADER_CHANGED;
    }
    return part->engine()->rangeWithPrefix(start, prefix, iter);
}


nebula::cpp2::ErrorCode
NebulaStore::sync(GraphSpaceID spaceId, PartitionID partId) {
    auto partRet = part(spaceId, partId);
    if (!ok(partRet)) {
        return error(partRet);
    }
    auto part = nebula::value(partRet);
    if (!checkLeader(part)) {
        return nebula::cpp2::ErrorCode::E_LEADER_CHANGED;
    }
    auto ret = nebula::cpp2::ErrorCode::SUCCEEDED;
    folly::Baton<true, std::atomic> baton;
    part->sync([&] (nebula::cpp2::ErrorCode code) {
        ret = code;
        baton.post();
    });
    baton.wait();
    return ret;
}

void NebulaStore::asyncAppendBatch(GraphSpaceID spaceId,
                                   PartitionID partId,
                                   std::string&& batch,
                                   KVCallback cb) {
    auto ret = part(spaceId, partId);
    if (!ok(ret)) {
        cb(error(ret));
        return;
    }
    auto part = nebula::value(ret);
    part->asyncAppendBatch(std::move(batch), std::move(cb));
}

void NebulaStore::asyncMultiPut(GraphSpaceID spaceId,
                                PartitionID partId,
                                std::vector<KV>&& keyValues,
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
                                   std::vector<std::string>&& keys,
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

ErrorOr<nebula::cpp2::ErrorCode, std::shared_ptr<Part>>
NebulaStore::part(GraphSpaceID spaceId, PartitionID partId) {
    folly::RWSpinLock::ReadHolder rh(&lock_);
    auto it = spaces_.find(spaceId);
    if (UNLIKELY(it == spaces_.end())) {
        return nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    }
    auto& parts = it->second->parts_;
    auto partIt = parts.find(partId);
    if (UNLIKELY(partIt == parts.end())) {
        return nebula::cpp2::ErrorCode::E_PART_NOT_FOUND;
    }
    return partIt->second;
}

nebula::cpp2::ErrorCode NebulaStore::ingest(GraphSpaceID spaceId) {
    auto spaceRet = space(spaceId);
    if (!ok(spaceRet)) {
        return error(spaceRet);
    }
    auto space = nebula::value(spaceRet);
    for (auto& engine : space->engines_) {
        auto parts = engine->allParts();
        for (auto part : parts) {
            auto ret = this->engine(spaceId, part);
            if (!ok(ret)) {
                return error(ret);
            }

            auto path = folly::stringPrintf("%s/download/%d", value(ret)->getDataRoot(), part);
            if (!fs::FileUtils::exist(path)) {
                LOG(INFO) << path << " not existed";
                continue;
            }

            auto files = nebula::fs::FileUtils::listAllFilesInDir(path.c_str(), true, "*.sst");
            for (auto file : files) {
                LOG(INFO) << "Ingesting extra file: " << file;
                auto code = engine->ingest(std::vector<std::string>({file}));
                if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                    return code;
                }
            }
        }
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}


nebula::cpp2::ErrorCode
NebulaStore::setOption(GraphSpaceID spaceId,
                       const std::string& configKey,
                       const std::string& configValue) {
    auto spaceRet = space(spaceId);
    if (!ok(spaceRet)) {
        return error(spaceRet);
    }
    auto space = nebula::value(spaceRet);
    for (auto& engine : space->engines_) {
        auto code = engine->setOption(configKey, configValue);
        if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return code;
        }
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}


nebula::cpp2::ErrorCode
NebulaStore::setDBOption(GraphSpaceID spaceId,
                         const std::string& configKey,
                         const std::string& configValue) {
    auto spaceRet = space(spaceId);
    if (!ok(spaceRet)) {
        return error(spaceRet);
    }
    auto space = nebula::value(spaceRet);
    for (auto& engine : space->engines_) {
        auto code = engine->setDBOption(configKey, configValue);
        if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return code;
        }
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}


nebula::cpp2::ErrorCode NebulaStore::compact(GraphSpaceID spaceId) {
    auto spaceRet = space(spaceId);
    if (!ok(spaceRet)) {
        return error(spaceRet);
    }
    auto space = nebula::value(spaceRet);

    auto code = nebula::cpp2::ErrorCode::SUCCEEDED;
    std::vector<std::thread> threads;
    LOG(INFO) << "Space " << spaceId << " start compaction.";
    for (auto& engine : space->engines_) {
        threads.emplace_back(std::thread([&engine, &code] {
            auto ret = engine->compact();
            if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
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

nebula::cpp2::ErrorCode NebulaStore::flush(GraphSpaceID spaceId) {
    auto spaceRet = space(spaceId);
    if (!ok(spaceRet)) {
        return error(spaceRet);
    }
    auto space = nebula::value(spaceRet);
    for (auto& engine : space->engines_) {
        auto code = engine->flush();
        if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return code;
        }
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<cpp2::CheckpointInfo>> NebulaStore::createCheckpoint(
    GraphSpaceID spaceId,
    const std::string& name) {
    auto spaceRet = space(spaceId);
    if (!ok(spaceRet)) {
        return error(spaceRet);
    }

    auto space = nebula::value(spaceRet);
    std::string cpPath;
    std::unordered_map<PartitionID, cpp2::LogInfo> partitionInfo;
    std::vector<cpp2::CheckpointInfo> cpInfo;

    DCHECK(!space->engines_.empty());

    for (auto& engine : space->engines_) {
        auto code = engine->createCheckpoint(name);
        if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return code;
        }
        // Maybe there's a judgment call here.
        cpPath = folly::stringPrintf("%s/checkpoints/%s", engine->getDataRoot(), name.c_str());
        // create wal hard link for all parts
        auto parts = engine->allParts();
        for (auto& part : parts) {
            auto ret = this->part(spaceId, part);
            if (!ok(ret)) {
                LOG(ERROR) << "Part not found. space : " << spaceId << " Part : " << part;
                return error(ret);
            }
            auto walPath = folly::stringPrintf(
                "%s/checkpoints/%s/wal/%d", engine->getWalRoot(), name.c_str(), part);
            auto p = nebula::value(ret);
            if (!p->linkCurrentWAL(walPath.data())) {
                return nebula::cpp2::ErrorCode::E_FAILED_TO_CHECKPOINT;
            }

            if (p->isLeader()) {
                auto logInfo = p->lastLogInfo();
                cpp2::LogInfo info;
                info.set_log_id(logInfo.first);
                info.set_term_id(logInfo.second);
                partitionInfo.emplace(part, std::move(info));
            }
        }
        auto result = nebula::fs::FileUtils::realPath(cpPath.c_str());
        if (!result.ok()) {
            return nebula::cpp2::ErrorCode::E_FAILED_TO_CHECKPOINT;
        }
        nebula::cpp2::PartitionBackupInfo backupInfo;
        nebula::cpp2::CheckpointInfo info;
        backupInfo.set_info(std::move(partitionInfo));
        info.set_path(std::move(result.value()));
        info.set_partition_info(std::move(backupInfo));
        cpInfo.emplace_back(std::move(info));
    }

    return cpInfo;
}

nebula::cpp2::ErrorCode
NebulaStore::dropCheckpoint(GraphSpaceID spaceId, const std::string& name) {
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
            return nebula::cpp2::ErrorCode::E_STORE_FAILURE;
        }
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode NebulaStore::setWriteBlocking(GraphSpaceID spaceId, bool sign) {
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
            p->setBlocking(sign);
        }
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
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

ErrorOr<nebula::cpp2::ErrorCode, KVEngine*>
NebulaStore::engine(GraphSpaceID spaceId, PartitionID partId) {
    folly::RWSpinLock::ReadHolder rh(&lock_);
    auto it = spaces_.find(spaceId);
    if (UNLIKELY(it == spaces_.end())) {
        return nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    }
    auto& parts = it->second->parts_;
    auto partIt = parts.find(partId);
    if (UNLIKELY(partIt == parts.end())) {
        return nebula::cpp2::ErrorCode::E_PART_NOT_FOUND;
    }
    return partIt->second->engine();
}

ErrorOr<nebula::cpp2::ErrorCode, std::shared_ptr<SpacePartInfo>>
NebulaStore::space(GraphSpaceID spaceId) {
    folly::RWSpinLock::ReadHolder rh(&lock_);
    auto it = spaces_.find(spaceId);
    if (UNLIKELY(it == spaces_.end())) {
        return nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    }
    return it->second;
}

ErrorOr<nebula::cpp2::ErrorCode, std::shared_ptr<SpaceListenerInfo>>
NebulaStore::spaceListener(GraphSpaceID spaceId) {
    folly::RWSpinLock::ReadHolder rh(&lock_);
    auto it = spaceListeners_.find(spaceId);
    if (UNLIKELY(it == spaceListeners_.end())) {
        return nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    }
    return it->second;
}

int32_t NebulaStore::allLeader(
    std::unordered_map<GraphSpaceID, std::vector<meta::cpp2::LeaderInfo>>& leaderIds) {
    folly::RWSpinLock::ReadHolder rh(&lock_);
    int32_t count = 0;
    for (const auto& spaceIt : spaces_) {
        auto spaceId = spaceIt.first;
        for (const auto& partIt : spaceIt.second->parts_) {
            auto partId = partIt.first;
            if (partIt.second->isLeader()) {
                meta::cpp2::LeaderInfo partInfo;
                partInfo.set_part_id(partId);
                partInfo.set_term(partIt.second->termId());
                leaderIds[spaceId].emplace_back(std::move(partInfo));
                ++count;
            }
        }
    }
    return count;
}

bool NebulaStore::checkLeader(std::shared_ptr<Part> part, bool canReadFromFollower) const {
    return canReadFromFollower || (part->isLeader() && part->leaseValid());
}

void NebulaStore::cleanWAL() {
    folly::RWSpinLock::ReadHolder rh(&lock_);
    SCOPE_EXIT {
        storeWorker_->addDelayTask(
            FLAGS_clean_wal_interval_secs * 1000, &NebulaStore::cleanWAL, this);
    };
    for (const auto& spaceEntry : spaces_) {
        if (FLAGS_rocksdb_disable_wal) {
            for (const auto& engine : spaceEntry.second->engines_) {
                engine->flush();
            }
        }
        for (const auto& partEntry : spaceEntry.second->parts_) {
            auto& part = partEntry.second;
            if (part->needToCleanWal()) {
                // clean wal by expired time
                part->wal()->cleanWAL();
            }
        }
    }
    for (const auto& spaceEntry : spaceListeners_) {
        for (const auto& partEntry : spaceEntry.second->listeners_) {
            for (const auto& typeEntry : partEntry.second) {
                const auto& listener = typeEntry.second;
                // clean wal by log id
                listener->wal()->cleanWAL(listener->getApplyId());
            }
        }
    }
}

nebula::cpp2::ErrorCode NebulaStore::backup() {
    for (const auto& spaceEntry : spaces_) {
        for (const auto& engine : spaceEntry.second->engines_) {
            auto code = engine->backup();
            if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                return code;
            }
        }
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>>
NebulaStore::backupTable(GraphSpaceID spaceId,
                         const std::string& name,
                         const std::string& tablePrefix,
                         std::function<bool(const folly::StringPiece& key)> filter) {
    auto spaceRet = space(spaceId);
    if (!ok(spaceRet)) {
        return error(spaceRet);
    }

    auto space = nebula::value(spaceRet);
    std::vector<std::string> backupPath;
    for (auto& engine : space->engines_) {
        auto path = engine->backupTable(name, tablePrefix, filter);
        if (!ok(path)) {
            auto result = error(path);
            if (result != nebula::cpp2::ErrorCode::E_BACKUP_EMPTY_TABLE) {
                return result;
            }
            LOG(WARNING) << "Since the table(" << tablePrefix
                         << ") is empty, the backup of the current table is skipped.";
            continue;
        }
        backupPath.emplace_back(value(path));
    }

    if (backupPath.empty()) {
        return nebula::cpp2::ErrorCode::E_BACKUP_EMPTY_TABLE;
    }

    return backupPath;
}

nebula::cpp2::ErrorCode
NebulaStore::restoreFromFiles(GraphSpaceID spaceId, const std::vector<std::string>& files) {
    auto spaceRet = space(spaceId);
    if (!ok(spaceRet)) {
        LOG(ERROR) << "Get Space " << spaceId << " Failed";
        return error(spaceRet);
    }
    auto space = nebula::value(spaceRet);

    DCHECK_EQ(space->engines_.size(), 1);

    for (auto& engine : space->engines_) {
        auto ret = engine->ingest(files, true);
        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return ret;
        }
    }

    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode
NebulaStore::multiPutWithoutReplicator(GraphSpaceID spaceId, std::vector<KV> keyValues) {
    auto spaceRet = space(spaceId);
    if (!ok(spaceRet)) {
        LOG(ERROR) << "Get Space " << spaceId << " Failed";
        return error(spaceRet);
    }
    auto space = nebula::value(spaceRet);

    DCHECK_EQ(space->engines_.size(), 1);

    for (auto& engine : space->engines_) {
        auto ret = engine->multiPut(keyValues);
        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return ret;
        }
    }

    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace kvstore
}  // namespace nebula
