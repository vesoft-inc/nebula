/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "kvstore/NebulaStore.h"

#include <folly/Likely.h>
#include <folly/ScopeGuard.h>
#include <thrift/lib/cpp/util/EnumUtils.h>

#include <algorithm>

#include "common/fs/FileUtils.h"
#include "common/network/NetworkUtils.h"
#include "common/time/WallClock.h"
#include "common/utils/NebulaKeyUtils.h"
#include "kvstore/NebulaSnapshotManager.h"
#include "kvstore/RocksEngine.h"
#include "kvstore/listener/elasticsearch/ESListener.h"

DEFINE_string(engine_type, "rocksdb", "rocksdb, memory...");
DEFINE_int32(custom_filter_interval_secs,
             24 * 3600,
             "interval to trigger custom compaction, < 0 means always do "
             "default minor compaction");
DEFINE_int32(num_workers, 4, "Number of worker threads");
DEFINE_int32(clean_wal_interval_secs, 600, "interval to trigger clean expired wal");
DEFINE_bool(auto_remove_invalid_space, true, "whether remove data of invalid space when restart");

DECLARE_bool(rocksdb_disable_wal);
DECLARE_int32(rocksdb_backup_interval_secs);
DECLARE_int32(wal_ttl);
DEFINE_int32(raft_num_worker_threads, 32, "Raft part number of workers");

namespace nebula {
namespace kvstore {

NebulaStore::~NebulaStore() {
  stop();
  LOG(INFO) << "Cut off the relationship with meta client";
  options_.partMan_.reset();
  bgWorkers_->stop();
  bgWorkers_->wait();
  storeWorker_->stop();
  storeWorker_->wait();
  spaces_.clear();
  spaceListeners_.clear();
  LOG(INFO) << "~NebulaStore()";
}

bool NebulaStore::init() {
  LOG(INFO) << "Start the raft service...";
  bgWorkers_ = std::make_shared<thread::GenericThreadPool>();
  bgWorkers_->start(FLAGS_num_workers, "nebula-bgworkers");
  storeWorker_ = std::make_shared<thread::GenericWorker>();
  CHECK(storeWorker_->start());
  {
    auto pool = apache::thrift::concurrency::PriorityThreadManager::newPriorityThreadManager(
        FLAGS_raft_num_worker_threads);
    pool->setNamePrefix("part-executor");
    pool->start();
    partExecutor_ = std::move(pool);
  }
  snapshot_.reset(new NebulaSnapshotManager(this));
  raftService_ = raftex::RaftexService::createService(ioPool_, workers_, raftAddr_.port);
  if (raftService_ == nullptr) {
    LOG(ERROR) << "Start the raft service failed";
    return false;
  }
  diskMan_.reset(new DiskManager(options_.dataPaths_, storeWorker_));
  // todo(doodle): we could support listener and normal storage start at same
  // instance
  if (!isListener()) {
    // TODO(spw): need to refactor, we could load data from local regardless of partManager,
    // then adjust the data in loadPartFromPartManager.
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
  // avoid duplicate engine created
  std::unordered_set<std::pair<GraphSpaceID, PartitionID>> partSet;
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
          LOG(ERROR) << folly::sformat("Data path {} invalid {}", dir, ex.what());
          continue;
        }

        if (spaceId == 0) {
          // skip the system space, only handle data space here.
          continue;
        }

        auto engine = newEngine(spaceId, path, options_.walPath_);
        std::map<PartitionID, Peers> partRaftPeers;

        // load balancing part info which persisted to local engine.
        for (auto& [partId, raftPeers] : engine->balancePartPeers()) {
          CHECK_NE(raftPeers.size(), 0);

          if (raftPeers.isExpired()) {
            LOG(INFO) << "Space: " << spaceId << ", part:" << partId
                      << " balancing info expired, ignore it.";
            continue;
          }

          auto spacePart = std::make_pair(spaceId, partId);
          if (partSet.find(spacePart) == partSet.end()) {
            partSet.emplace(std::make_pair(spaceId, partId));

            // join the balancing peers with meta peers
            auto metaStatus = options_.partMan_->partMeta(spaceId, partId);
            if (!metaStatus.ok()) {
              LOG(INFO) << "Space: " << spaceId << "; partId: " << partId
                        << " does not exist in part manager when join balancing.";
            } else {
              auto partMeta = metaStatus.value();
              for (auto& h : partMeta.hosts_) {
                auto raftAddr = getRaftAddr(h);
                if (!raftPeers.exist(raftAddr)) {
                  VLOG(1) << "Add raft peer " << raftAddr;
                  raftPeers.addOrUpdate(Peer(raftAddr));
                }
              }
            }

            partRaftPeers.emplace(partId, raftPeers);
          }
        }

        // load normal part ids which persisted to local engine.
        for (auto& partId : engine->allParts()) {
          // first priority: balancing
          bool inBalancing = partRaftPeers.find(partId) != partRaftPeers.end();
          if (inBalancing) {
            continue;
          }

          // second priority: meta
          if (!options_.partMan_->partExist(storeSvcAddr_, spaceId, partId).ok()) {
            LOG(INFO)
                << "Part " << partId
                << " is not in balancing and does not exist in meta any more, will remove it!";
            engine->removePart(partId);
            continue;
          }

          auto spacePart = std::make_pair(spaceId, partId);
          if (partSet.find(spacePart) == partSet.end()) {
            partSet.emplace(spacePart);

            // fill the peers
            auto metaStatus = options_.partMan_->partMeta(spaceId, partId);
            CHECK(metaStatus.ok());
            auto partMeta = metaStatus.value();
            Peers peers;
            for (auto& h : partMeta.hosts_) {
              VLOG(1) << "Add raft peer " << getRaftAddr(h);
              peers.addOrUpdate(Peer(getRaftAddr(h)));
            }
            partRaftPeers.emplace(partId, peers);
          }
        }

        // there is no valid part in this engine, remove it
        if (partRaftPeers.empty()) {
          engine.reset();  // close engine
          if (!options_.partMan_->spaceExist(storeSvcAddr_, spaceId).ok()) {
            if (FLAGS_auto_remove_invalid_space) {
              auto spaceDir = folly::stringPrintf("%s/%s", rootPath.c_str(), dir.c_str());
              removeSpaceDir(spaceDir);
            }
          }
          continue;
        }

        // add to spaces
        KVEngine* enginePtr = nullptr;
        {
          folly::RWSpinLock::WriteHolder wh(&lock_);
          auto spaceIt = this->spaces_.find(spaceId);
          if (spaceIt == this->spaces_.end()) {
            LOG(INFO) << "Load space " << spaceId << " from disk";
            spaceIt = this->spaces_.emplace(spaceId, std::make_unique<SpacePartInfo>()).first;
          }
          spaceIt->second->engines_.emplace_back(std::move(engine));
          enginePtr = spaceIt->second->engines_.back().get();
        }

        std::atomic<size_t> counter(partRaftPeers.size());
        folly::Baton<true, std::atomic> baton;
        LOG(INFO) << "Need to open " << partRaftPeers.size() << " parts of space " << spaceId;
        for (auto& it : partRaftPeers) {
          auto& partId = it.first;
          Peers& raftPeers = it.second;

          bgWorkers_->addTask(
              [spaceId, partId, &raftPeers, enginePtr, &counter, &baton, this]() mutable {
                // create part
                bool isLearner = false;
                std::vector<HostAddr> addrs;  // raft peers
                for (auto& [addr, raftPeer] : raftPeers.getPeers()) {
                  if (addr == raftAddr_) {  // self
                    if (raftPeer.status == Peer::Status::kLearner) {
                      isLearner = true;
                    }
                  } else {  // others
                    if (raftPeer.status == Peer::Status::kNormalPeer ||
                        raftPeer.status == Peer::Status::kPromotedPeer) {
                      addrs.emplace_back(addr);
                    }
                  }
                }
                auto part = newPart(spaceId, partId, enginePtr, isLearner, addrs);
                LOG(INFO) << "Load part " << spaceId << ", " << partId << " from disk";

                // add learner peers
                if (!isLearner) {
                  for (auto& [addr, raftPeer] : raftPeers.getPeers()) {
                    if (addr == raftAddr_) {
                      continue;
                    }

                    if (raftPeer.status == Peer::Status::kLearner) {
                      part->addLearner(addr, true);
                    }
                  }
                }

                // add part to space
                {
                  folly::RWSpinLock::WriteHolder holder(&lock_);
                  auto iter = spaces_.find(spaceId);
                  CHECK(iter != spaces_.end());
                  // Check if part already exists.
                  // Prevent the same part from existing on different dataPaths.
                  auto ret = iter->second->parts_.emplace(partId, part);
                  if (!ret.second) {
                    LOG(FATAL) << "Part already exists, partId " << partId;
                  }
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
    auto& partPeers = entry.second;
    addSpace(spaceId);
    std::vector<PartitionID> partIds;
    for (auto it = partPeers.begin(); it != partPeers.end(); it++) {
      partIds.emplace_back(it->first);
    }
    std::sort(partIds.begin(), partIds.end());
    for (auto& partId : partIds) {
      addPart(spaceId, partId, false, partPeers[partId].hosts_);
    }
  }
}

void NebulaStore::loadLocalListenerFromPartManager() {
  // Initialize listener on the storeSvcAddr_, and start these listener
  LOG(INFO) << "Init listener from partManager for " << storeSvcAddr_;
  auto listenersMap = options_.partMan_->listeners(storeSvcAddr_);
  for (const auto& spaceEntry : listenersMap) {
    auto spaceId = spaceEntry.first;
    for (const auto& typeEntry : spaceEntry.second) {
      auto type = typeEntry.first;
      addListenerSpace(spaceId, type);
      for (const auto& info : typeEntry.second) {
        addListenerPart(spaceId, info.partId_, type, info.peers_);
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
  LOG(INFO) << "Stop the raft service...";
  raftService_->stop();

  LOG(INFO) << "Stop kv engine...";
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

ErrorOr<nebula::cpp2::ErrorCode, HostAddr> NebulaStore::partLeader(GraphSpaceID spaceId,
                                                                   PartitionID partId) {
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

void NebulaStore::addSpace(GraphSpaceID spaceId) {
  folly::RWSpinLock::WriteHolder wh(&lock_);
  // Iterate over all engines to ensure that each dataPath has an engine
  if (this->spaces_.find(spaceId) != this->spaces_.end()) {
    LOG(INFO) << "Data space " << spaceId << " has existed!";
    for (auto& path : options_.dataPaths_) {
      bool engineExist = false;
      auto dataPath = folly::stringPrintf("%s/nebula/%d", path.c_str(), spaceId);
      // Check if given data path contain a kv engine of specified spaceId
      for (auto iter = spaces_[spaceId]->engines_.begin(); iter != spaces_[spaceId]->engines_.end();
           iter++) {
        auto dPath = (*iter)->getDataRoot();
        if (dataPath.compare(dPath) == 0) {
          engineExist = true;
          break;
        }
      }
      if (!engineExist) {
        spaces_[spaceId]->engines_.emplace_back(newEngine(spaceId, path, options_.walPath_));
      }
    }
  } else {
    LOG(INFO) << "Create data space " << spaceId;
    this->spaces_[spaceId] = std::make_unique<SpacePartInfo>();
    for (auto& path : options_.dataPaths_) {
      this->spaces_[spaceId]->engines_.emplace_back(newEngine(spaceId, path, options_.walPath_));
    }
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
  std::vector<HostAddr> raftPeers;
  for (auto& p : peers) {
    raftPeers.push_back(getRaftAddr(p));
  }

  auto spaceIt = this->spaces_.find(spaceId);
  CHECK(spaceIt != this->spaces_.end()) << "Space should exist!";
  auto partIt = spaceIt->second->parts_.find(partId);
  if (partIt != spaceIt->second->parts_.end()) {
    LOG(INFO) << "[Space: " << spaceId << ", Part: " << partId << "] has existed!";
    if (!raftPeers.empty()) {
      partIt->second->checkAndResetPeers(raftPeers);
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

  Peers peersToPersist(raftPeers);
  if (asLearner) {
    peersToPersist.addOrUpdate(Peer(raftAddr_, Peer::Status::kLearner));
  }
  targetEngine->addPart(partId, peersToPersist);

  spaceIt->second->parts_.emplace(
      partId, newPart(spaceId, partId, targetEngine.get(), asLearner, raftPeers));
  LOG(INFO) << "Space " << spaceId << ", part " << partId << " has been added, asLearner "
            << asLearner;
}

std::shared_ptr<Part> NebulaStore::newPart(GraphSpaceID spaceId,
                                           PartitionID partId,
                                           KVEngine* engine,
                                           bool asLearner,
                                           const std::vector<HostAddr>& raftPeers) {
  auto walPath = folly::stringPrintf("%s/wal/%d", engine->getWalRoot(), partId);
  auto part = std::make_shared<Part>(spaceId,
                                     partId,
                                     raftAddr_,
                                     walPath,
                                     engine,
                                     ioPool_,
                                     bgWorkers_,
                                     partExecutor_,
                                     snapshot_,
                                     clientMan_,
                                     diskMan_,
                                     getSpaceVidLen(spaceId));
  std::vector<HostAddr> peersWithoutMe;
  for (auto& p : raftPeers) {
    if (p != raftAddr_) {
      peersWithoutMe.push_back(p);
    }
  }

  raftService_->addPartition(part);
  for (auto& func : onNewPartAdded_) {
    func.second(part);
  }
  part->start(std::move(peersWithoutMe), asLearner);
  diskMan_->addPartToPath(spaceId, partId, engine->getDataRoot());
  return part;
}

void NebulaStore::removeSpace(GraphSpaceID spaceId) {
  folly::RWSpinLock::WriteHolder wh(&lock_);
  if (beforeRemoveSpace_) {
    beforeRemoveSpace_(spaceId);
  }

  auto spaceIt = this->spaces_.find(spaceId);
  if (spaceIt != this->spaces_.end()) {
    for (auto& [partId, part] : spaceIt->second->parts_) {
      // before calling removeSpace, meta client would call removePart to remove all parts in
      // meta cache, which do not contain learners, so we remove them here
      if (part->isLearner()) {
        removePart(spaceId, partId, false);
      }
    }
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
}

nebula::cpp2::ErrorCode NebulaStore::clearSpace(GraphSpaceID spaceId) {
  folly::RWSpinLock::ReadHolder rh(&lock_);
  auto spaceIt = this->spaces_.find(spaceId);
  if (spaceIt != this->spaces_.end()) {
    for (auto& part : spaceIt->second->parts_) {
      auto ret = part.second->cleanupSafely();
      if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "partition clear failed. space: " << spaceId << ", part: " << part.first;
        return ret;
      }
    }
  } else {
    return nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
  }
  LOG(INFO) << "Space " << spaceId << " has been cleared!";
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

void NebulaStore::removePart(GraphSpaceID spaceId, PartitionID partId, bool needLock) {
  folly::RWSpinLock::WriteHolder wh(nullptr);
  if (needLock) {
    wh.reset(&lock_);
  }
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

void NebulaStore::addListenerSpace(GraphSpaceID spaceId, meta::cpp2::ListenerType type) {
  UNUSED(type);
  folly::RWSpinLock::WriteHolder wh(&lock_);
  if (this->spaceListeners_.find(spaceId) != this->spaceListeners_.end()) {
    LOG(INFO) << "Listener space " << spaceId << " has existed!";
  } else {
    LOG(INFO) << "Create listener space " << spaceId;
    this->spaceListeners_[spaceId] = std::make_unique<SpaceListenerInfo>();
  }
  // Perform extra initialization of given type of listener here
}

void NebulaStore::removeListenerSpace(GraphSpaceID spaceId, meta::cpp2::ListenerType type) {
  UNUSED(type);
  folly::RWSpinLock::WriteHolder wh(&lock_);
  auto spaceIt = this->spaceListeners_.find(spaceId);
  if (spaceIt != this->spaceListeners_.end()) {
    // Perform extra destruction of given type of listener here;
  }
  LOG(INFO) << "Listener space " << spaceId << " has been removed!";
}

void NebulaStore::addListenerPart(GraphSpaceID spaceId,
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
  partIt->second.emplace(type, newListener(spaceId, partId, type, peers));
  LOG(INFO) << "Listener of type " << apache::thrift::util::enumNameSafe(type)
            << " of [Space: " << spaceId << ", Part: " << partId << "] is added";
  return;
}

std::shared_ptr<Listener> NebulaStore::newListener(GraphSpaceID spaceId,
                                                   PartitionID partId,
                                                   meta::cpp2::ListenerType type,
                                                   const std::vector<HostAddr>& peers) {
  // Lock has been acquired in addListenerPart.
  // todo(doodle): we don't support start multiple type of listener in same process for now. If we
  // support it later, the wal path may or may not need to be separated depending on how we
  // implement it.
  auto walPath =
      folly::stringPrintf("%s/%d/%d/wal", options_.listenerPath_.c_str(), spaceId, partId);
  std::shared_ptr<Listener> listener;
  if (type == meta::cpp2::ListenerType::ELASTICSEARCH) {
    listener = std::make_shared<ESListener>(spaceId,
                                            partId,
                                            raftAddr_,
                                            walPath,
                                            ioPool_,
                                            bgWorkers_,
                                            partExecutor_,
                                            options_.schemaMan_);
  } else {
    LOG(FATAL) << "Should not reach here";
    return nullptr;
  }
  raftService_->addPartition(listener);
  // add raft group as learner
  std::vector<HostAddr> raftPeers;
  std::transform(peers.begin(), peers.end(), std::back_inserter(raftPeers), [this](auto&& host) {
    CHECK_NE(host, storeSvcAddr_) << "Should not start part and listener on same host";
    return getRaftAddr(host);
  });
  listener->start(std::move(raftPeers));
  return listener;
}

void NebulaStore::removeListenerPart(GraphSpaceID spaceId,
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

void NebulaStore::fetchDiskParts(SpaceDiskPartsMap& diskParts) {
  diskMan_->getDiskParts(diskParts);
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
    LOG(INFO) << "Space directory removed: " << dir;
  } catch (const boost::filesystem::filesystem_error& e) {
    LOG(WARNING) << "Exception caught while remove directory, please delete it by manual: "
                 << e.what();
  }
}

nebula::cpp2::ErrorCode NebulaStore::get(GraphSpaceID spaceId,
                                         PartitionID partId,
                                         const std::string& key,
                                         std::string* value,
                                         bool canReadFromFollower,
                                         const void* snapshot) {
  auto ret = part(spaceId, partId);
  if (!ok(ret)) {
    return error(ret);
  }
  auto part = nebula::value(ret);
  if (!checkLeader(part, canReadFromFollower)) {
    return part->isLeader() ? nebula::cpp2::ErrorCode::E_LEADER_LEASE_FAILED
                            : nebula::cpp2::ErrorCode::E_LEADER_CHANGED;
  }
  return part->engine()->get(key, value, snapshot);
}

const void* NebulaStore::GetSnapshot(GraphSpaceID spaceId, PartitionID partId) {
  auto ret = part(spaceId, partId);
  if (!ok(ret)) {
    return nullptr;
  }
  auto part = nebula::value(ret);
  return part->engine()->GetSnapshot();
}

void NebulaStore::ReleaseSnapshot(GraphSpaceID spaceId, PartitionID partId, const void* snapshot) {
  auto ret = part(spaceId, partId);
  if (!ok(ret)) {
    LOG(INFO) << "Failed to release snapshot for GraphSpaceID " << spaceId << " PartitionID"
              << partId;
    return;
  }
  auto part = nebula::value(ret);
  return part->engine()->ReleaseSnapshot(snapshot);
}

std::pair<nebula::cpp2::ErrorCode, std::vector<Status>> NebulaStore::multiGet(
    GraphSpaceID spaceId,
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
  auto allExist = std::all_of(status.begin(), status.end(), [](const auto& s) { return s.ok(); });
  if (allExist) {
    return {nebula::cpp2::ErrorCode::SUCCEEDED, status};
  } else {
    return {nebula::cpp2::ErrorCode::E_PARTIAL_RESULT, status};
  }
}

nebula::cpp2::ErrorCode NebulaStore::range(GraphSpaceID spaceId,
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

nebula::cpp2::ErrorCode NebulaStore::prefix(GraphSpaceID spaceId,
                                            PartitionID partId,
                                            const std::string& prefix,
                                            std::unique_ptr<KVIterator>* iter,
                                            bool canReadFromFollower,
                                            const void* snapshot) {
  auto ret = part(spaceId, partId);
  if (!ok(ret)) {
    return error(ret);
  }
  auto part = nebula::value(ret);
  if (!checkLeader(part, canReadFromFollower)) {
    return nebula::cpp2::ErrorCode::E_LEADER_CHANGED;
  }
  return part->engine()->prefix(prefix, iter, snapshot);
}

nebula::cpp2::ErrorCode NebulaStore::rangeWithPrefix(GraphSpaceID spaceId,
                                                     PartitionID partId,
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

nebula::cpp2::ErrorCode NebulaStore::sync(GraphSpaceID spaceId, PartitionID partId) {
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
  part->sync([&](nebula::cpp2::ErrorCode code) {
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
                                   PartitionID partId,
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
                                MergeableAtomicOp op,
                                KVCallback cb) {
  auto ret = part(spaceId, partId);
  if (!ok(ret)) {
    cb(error(ret));
    return;
  }
  auto part = nebula::value(ret);
  part->asyncAtomicOp(std::move(op), std::move(cb));
}

ErrorOr<nebula::cpp2::ErrorCode, std::shared_ptr<Part>> NebulaStore::part(GraphSpaceID spaceId,
                                                                          PartitionID partId) {
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

  LOG(INFO) << "Ingesting space " << spaceId;
  auto space = nebula::value(spaceRet);
  std::vector<std::thread> threads;
  nebula::cpp2::ErrorCode code = nebula::cpp2::ErrorCode::SUCCEEDED;
  for (auto& engine : space->engines_) {
    threads.emplace_back(std::thread([&engine, &code, this, spaceId] {
      auto parts = engine->allParts();
      for (auto part : parts) {
        auto ret = this->engine(spaceId, part);
        if (!ok(ret)) {
          code = error(ret);
        } else {
          auto path = folly::stringPrintf("%s/download/%d", value(ret)->getDataRoot(), part);
          if (!fs::FileUtils::exist(path)) {
            LOG(INFO) << path << " not existed";
            continue;
          }

          auto files = nebula::fs::FileUtils::listAllFilesInDir(path.c_str(), true, "*.sst");
          auto result = engine->ingest(std::vector<std::string>(files));
          if (result != nebula::cpp2::ErrorCode::SUCCEEDED) {
            code = result;
          }
        }
      }
    }));
  }

  // Wait for all threads to finish
  for (auto& t : threads) {
    t.join();
  }
  LOG(INFO) << "Space " << spaceId << " ingest done.";
  return code;
}

nebula::cpp2::ErrorCode NebulaStore::setOption(GraphSpaceID spaceId,
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

nebula::cpp2::ErrorCode NebulaStore::setDBOption(GraphSpaceID spaceId,
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
  LOG(INFO) << "Space " << spaceId << " start manual compaction.";
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
    GraphSpaceID spaceId, const std::string& name) {
  /*
   * The default checkpoint directory structure is :
   *   |--FLAGS_data_path
   *   |----nebula
   *   |------space1
   *   |--------data
   *   |--------wal
   *   |--------checkpoints
   *   |----------snapshot1
   *   |------------data
   *   |------------wal
   *   |----------snapshot2
   *   |----------snapshot3
   *
   */
  auto spaceRet = space(spaceId);
  if (!ok(spaceRet)) {
    return error(spaceRet);
  }
  auto space = nebula::value(spaceRet);
  std::vector<cpp2::CheckpointInfo> cpInfoList;

  DCHECK(!space->engines_.empty());
  for (auto& engine : space->engines_) {
    std::string path = folly::sformat("{}/checkpoints/{}", engine->getDataRoot(), name);
    if (!fs::FileUtils::exist(path)) {
      if (!fs::FileUtils::makeDir(path)) {
        LOG(WARNING) << "Make checkpoint dir: " << path << " failed";
        return nebula::cpp2::ErrorCode::E_FAILED_TO_CHECKPOINT;
      }
    }

    // create data checkpoint
    std::string dataPath = folly::sformat("{}/data", path);
    auto code = engine->createCheckpoint(dataPath);
    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return code;
    }

    // create wal checkpoints: make hard link for all parts
    std::unordered_map<PartitionID, cpp2::LogInfo> partsInfo;
    auto parts = engine->allParts();
    for (auto& partId : parts) {
      auto ret = this->part(spaceId, partId);
      if (!ok(ret)) {
        LOG(WARNING) << folly::sformat(
            "space {} part {} not found while creating checkpoint", spaceId, partId);
        return error(ret);
      }

      auto p = nebula::value(ret);
      auto walPath = folly::sformat("{}/wal/{}", path, partId);
      if (!p->linkCurrentWAL(walPath.data())) {
        return nebula::cpp2::ErrorCode::E_FAILED_TO_CHECKPOINT;
      }

      // return last wal info of each part
      if (p->isLeader()) {
        auto logInfo = p->lastLogInfo();
        cpp2::LogInfo info;
        info.log_id_ref() = logInfo.first;
        info.term_id_ref() = logInfo.second;
        partsInfo.emplace(partId, std::move(info));
      }
    }

    auto result = nebula::fs::FileUtils::realPath(path.c_str());
    if (!result.ok()) {
      LOG(WARNING) << "Failed to get path:" << path << "'s real path";
      return nebula::cpp2::ErrorCode::E_FAILED_TO_CHECKPOINT;
    }

    nebula::cpp2::CheckpointInfo cpInfo;
    cpInfo.data_path_ref() = std::move(result.value());
    cpInfo.parts_ref() = std::move(partsInfo);
    cpInfo.space_id_ref() = spaceId;
    cpInfoList.emplace_back(std::move(cpInfo));
  }

  return cpInfoList;
}

nebula::cpp2::ErrorCode NebulaStore::dropCheckpoint(GraphSpaceID spaceId, const std::string& name) {
  auto spaceRet = space(spaceId);
  if (!ok(spaceRet)) {
    return error(spaceRet);
  }
  auto space = nebula::value(spaceRet);
  for (auto& engine : space->engines_) {
    /**
     * Drop checkpoint and wal together
     **/
    auto checkpointPath = folly::sformat("{}/checkpoints/{}", engine->getDataRoot(), name);
    LOG(INFO) << "Drop checkpoint: " << checkpointPath;
    if (!fs::FileUtils::exist(checkpointPath)) {
      continue;
    }

    if (!fs::FileUtils::remove(checkpointPath.data(), true)) {
      LOG(WARNING) << "Drop checkpoint dir failed : " << checkpointPath;
      return nebula::cpp2::ErrorCode::E_STORE_FAILURE;
    }
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode NebulaStore::setWriteBlocking(GraphSpaceID spaceId, bool sign) {
  auto spaceRet = space(spaceId);
  if (!ok(spaceRet)) {
    LOG(WARNING) << "Get Space " << spaceId << " Failed";
    return error(spaceRet);
  }
  auto space = nebula::value(spaceRet);
  for (auto& engine : space->engines_) {
    auto parts = engine->allParts();
    for (auto& part : parts) {
      auto partRet = this->part(spaceId, part);
      if (!ok(partRet)) {
        LOG(WARNING) << "Part not found. space : " << spaceId << " Part : " << part;
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

ErrorOr<nebula::cpp2::ErrorCode, KVEngine*> NebulaStore::engine(GraphSpaceID spaceId,
                                                                PartitionID partId) {
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

ErrorOr<nebula::cpp2::ErrorCode, std::shared_ptr<SpacePartInfo>> NebulaStore::space(
    GraphSpaceID spaceId) {
  folly::RWSpinLock::ReadHolder rh(&lock_);
  auto it = spaces_.find(spaceId);
  if (UNLIKELY(it == spaces_.end())) {
    return nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
  }
  return it->second;
}

ErrorOr<nebula::cpp2::ErrorCode, std::shared_ptr<SpaceListenerInfo>> NebulaStore::spaceListener(
    GraphSpaceID spaceId) {
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
        partInfo.part_id_ref() = partId;
        partInfo.term_ref() = partIt.second->termId();
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
    storeWorker_->addDelayTask(FLAGS_clean_wal_interval_secs * 1000, &NebulaStore::cleanWAL, this);
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
        part->cleanWal();
      }
    }
  }
  for (const auto& spaceEntry : spaceListeners_) {
    for (const auto& partEntry : spaceEntry.second->listeners_) {
      for (const auto& typeEntry : partEntry.second) {
        const auto& listener = typeEntry.second;
        // clean wal by commit log id
        listener->cleanWal();
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

ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>> NebulaStore::backupTable(
    GraphSpaceID spaceId,
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
      LOG(INFO) << "Since the table(" << tablePrefix
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

nebula::cpp2::ErrorCode NebulaStore::restoreFromFiles(GraphSpaceID spaceId,
                                                      const std::vector<std::string>& files) {
  auto spaceRet = space(spaceId);
  if (!ok(spaceRet)) {
    LOG(WARNING) << "Get Space " << spaceId << " Failed";
    return error(spaceRet);
  }
  auto space = nebula::value(spaceRet);

  for (auto& engine : space->engines_) {
    auto ret = engine->ingest(files, true);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return ret;
    }
  }

  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

std::unique_ptr<WriteBatch> NebulaStore::startBatchWrite() {
  return std::make_unique<RocksWriteBatch>();
}

nebula::cpp2::ErrorCode NebulaStore::batchWriteWithoutReplicator(
    GraphSpaceID spaceId, std::unique_ptr<WriteBatch> batch) {
  auto spaceRet = space(spaceId);
  if (!ok(spaceRet)) {
    LOG(WARNING) << "Get Space " << spaceId << " Failed";
    return error(spaceRet);
  }
  auto space = nebula::value(spaceRet);

  for (auto& engine : space->engines_) {
    auto ret = engine->commitBatchWrite(
        std::move(batch), FLAGS_rocksdb_disable_wal, FLAGS_rocksdb_wal_sync, true);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      return ret;
    }
  }

  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

ErrorOr<nebula::cpp2::ErrorCode, std::string> NebulaStore::getProperty(
    GraphSpaceID spaceId, const std::string& property) {
  auto spaceRet = space(spaceId);
  if (!ok(spaceRet)) {
    LOG(WARNING) << "Get Space " << spaceId << " Failed";
    return error(spaceRet);
  }
  auto space = nebula::value(spaceRet);

  folly::dynamic obj = folly::dynamic::object;
  for (size_t i = 0; i < space->engines_.size(); i++) {
    auto val = space->engines_[i]->getProperty(property);
    if (!ok(val)) {
      return error(val);
    }
    auto eng = folly::stringPrintf("Engine %zu", i);
    obj[eng] = std::move(value(val));
  }
  return folly::toJson(obj);
}

void NebulaStore::registerOnNewPartAdded(
    const std::string& funcName,
    std::function<void(std::shared_ptr<Part>&)> func,
    std::vector<std::pair<GraphSpaceID, PartitionID>>& existParts) {
  for (auto& item : spaces_) {
    for (auto& partItem : item.second->parts_) {
      existParts.emplace_back(std::make_pair(item.first, partItem.first));
      func(partItem.second);
    }
  }
  onNewPartAdded_.insert(std::make_pair(funcName, func));
}

}  // namespace kvstore
}  // namespace nebula
