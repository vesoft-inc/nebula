/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/ClusterCheckHandler.h"

#define TASK_HANDLER "ClusterCheckHandler "

DEFINE_int32(meta_cluster_check_interval_secs, 1, "Cluster check interval");

namespace nebula {
namespace meta {

ClusterCheckHandler::ClusterCheckHandler(kvstore::KVStore* kv,
                                         AdminClient* adminClient)
        : kvstore_(kv)
        , adminClient_(adminClient) {
    bgThread_ = std::make_unique<thread::GenericWorker>();
    CHECK(bgThread_->start());
}


ClusterCheckHandler::~ClusterCheckHandler() {
    stop();
    VLOG(3) << TASK_HANDLER << "~ClusterCheckHandler";
}


bool ClusterCheckHandler::getAllSpaces(std::vector<GraphSpaceID>& spaces) {
    // Get all spaces
    folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
    auto prefix = MetaServiceUtils::spacePrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << TASK_HANDLER << "Fetch spaces table error";
        return false;
    }
    while (iter->valid()) {
        auto spaceId = MetaServiceUtils::spaceId(iter->key());
        spaces.push_back(spaceId);
        iter->next();
    }
    return true;
}


void ClusterCheckHandler::stop() {
    if (bgThread_ != nullptr) {
        bgThread_->stop();
        bgThread_->wait();
        bgThread_.reset();
    }
}


bool ClusterCheckHandler::unBlocking() {
    auto ret = kvstore_->setPartBlocking(kDefaultSpaceId, kDefaultPartId, false);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << TASK_HANDLER << "Set unblocking error on meta server";
        return false;
    }

    std::vector<GraphSpaceID> spaces;
    if (!getAllSpaces(spaces)) {
        LOG(ERROR) << TASK_HANDLER << "Get spaces error";
        return false;
    }
    std::unique_ptr<HostLeaderMap> hostLeaderMap = std::make_unique<HostLeaderMap>();
    auto status = adminClient_->getLeaderDist(hostLeaderMap.get()).get();

    if (!status.ok() || hostLeaderMap->empty()) {
        LOG(ERROR) << TASK_HANDLER << "Get parts leader dist error";
        return false;
    }

    std::vector<folly::SemiFuture<Status>> futures;
    for (const auto& space : spaces) {
        std::unordered_map<HostAddr, std::vector<PartitionID>> hostParts;

        for (const auto& host : *hostLeaderMap) {
            hostParts[host.first] = std::move((*hostLeaderMap)[host.first][space]);
        }

        if (hostParts.empty()) {
            LOG(ERROR) << TASK_HANDLER << "Partition collection failed, spaceId is " << space;
            return false;
        }
        for (const auto& hostPart : hostParts) {
            auto host = hostPart.first;
            auto parts = hostPart.second;
            for (auto& part : parts) {
                futures.emplace_back(adminClient_->blockingWrites(space, part, host,
                                     storage::cpp2::EngineSignType::BLOCK_OFF));
            }
        }
    }

    int32_t failed = 0;
    folly::collectAll(futures).then([&](const std::vector<folly::Try<Status>>& tries) {
        for (const auto& t : tries) {
            if (!t.value().ok()) {
                ++failed;
            }
        }
    }).wait();
    if (failed > 0) {
        return false;
    }
    LOG(INFO) << TASK_HANDLER << "Unblocking done";
    return true;
}


void ClusterCheckHandler::addUnblockingTask() {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::writeBlockingLock());
    unblockingRunning_ = true;
    while (!bgThread_->addTask(&ClusterCheckHandler::unBlocking, this).get()) {
        LOG(ERROR) << TASK_HANDLER << "Unblocking failed";
        sleep(FLAGS_meta_cluster_check_interval_secs);
    }
    unblockingRunning_ = false;
}


bool ClusterCheckHandler::dropCheckpoint(const std::string& name) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::snapshotLock());
    auto snapshot = MetaServiceUtils::snapshotKey(name);
    std::string val;
    // Check snapshot is exists
    auto ret = kvstore_->get(kDefaultSpaceId, kDefaultPartId, snapshot, &val);

    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << TASK_HANDLER << "No snapshots found";
        return true;
    }

    auto hosts = MetaServiceUtils::parseSnapshotHosts(val);
    auto peers = network::NetworkUtils::toHosts(hosts);
    if (!peers.ok()) {
        LOG(ERROR) << TASK_HANDLER << "Get snapshot hosts error";
        return false;
    }

    auto activeHosts = ActiveHostsMan::getActiveHosts(kvstore_);
    std::vector<HostAddr> realHosts;
    for (auto& host : peers.value()) {
        if (std::find(activeHosts.begin(), activeHosts.end(), host) != activeHosts.end()) {
            realHosts.emplace_back(host);
        }
    }

    std::vector<GraphSpaceID> spaces;
    if (!getAllSpaces(spaces)) {
        LOG(ERROR) << TASK_HANDLER << "Get spaces error";
        return false;
    }

    for (auto& space : spaces) {
        auto status = adminClient_->dropSnapshot(space, name, realHosts).get();

        if (!status.ok()) {
            return false;
        }
    }

    auto dmRet = kvstore_->dropCheckpoint(kDefaultSpaceId, name);
    if (dmRet != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << TASK_HANDLER << "Drop snapshot error on meta server. snapshot : "
                   << name;
        return false;
    }

    if (!doSyncRemove(snapshot)) {
        LOG(ERROR) << TASK_HANDLER << "Drop snapshot meta data error. snapshot : "
                   << name;
        return false;
    }

    return true;
}


void ClusterCheckHandler::dropCheckpointThreadFunc(const std::string& name) {
    if (!dropCheckpoint(name)) {
        addDropCheckpointTask(name);
    }
}


void ClusterCheckHandler::addDropCheckpointTask(const std::string& name) {
    size_t delayMS = FLAGS_meta_cluster_check_interval_secs * 1000;
    bgThread_->addDelayTask(delayMS, &ClusterCheckHandler::dropCheckpointThreadFunc, this, name);
}


bool ClusterCheckHandler::doSyncRemove(const std::string& key) {
    folly::Baton<true, std::atomic> baton;
    bool ret = false;
    kvstore_->asyncRemove(kDefaultSpaceId,
                          kDefaultPartId,
                          key,
                          [&ret, &baton] (kvstore::ResultCode code) {
                              if (kvstore::ResultCode::SUCCEEDED == code) {
                                    ret = true;
                              } else {
                                  LOG(INFO) << "Put data error on meta server";
                              }
                              baton.post();
                            });
    baton.wait();
    return ret;
}


bool ClusterCheckHandler::isUnblockingRunning() {
    return unblockingRunning_;
}
}  // namespace meta
}  // namespace nebula

