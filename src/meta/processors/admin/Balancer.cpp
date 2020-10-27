/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/admin/Balancer.h"
#include <algorithm>
#include <cstdlib>
#include "kvstore/NebulaStore.h"
#include "meta/common/MetaCommon.h"
#include "meta/processors/Common.h"
#include "meta/ActiveHostsMan.h"
#include "meta/MetaServiceUtils.h"
#include "common/network/NetworkUtils.h"

DEFINE_double(leader_balance_deviation, 0.05, "after leader balance, leader count should in range "
                                              "[avg * (1 - deviation), avg * (1 + deviation)]");

namespace nebula {
namespace meta {

ErrorOr<cpp2::ErrorCode, BalanceID> Balancer::balance(std::unordered_set<HostAddr> hostDel) {
    std::lock_guard<std::mutex> lg(lock_);
    if (!running_) {
        auto retCode = recovery();
        if (retCode != cpp2::ErrorCode::SUCCEEDED) {
            LOG(ERROR) << "Recovery balancer failed!";
            finish();
            return retCode;
        }
        if (plan_ == nullptr) {
            LOG(INFO) << "There is no corrupted plan need to recovery, so create a new one";
            retCode = buildBalancePlan(std::move(hostDel));
            if (retCode != cpp2::ErrorCode::SUCCEEDED) {
                LOG(ERROR) << "Create balance plan failed";
                finish();
                return retCode;
            }
        }
        LOG(INFO) << "Start to invoke balance plan " << plan_->id();
        executor_->add(std::bind(&BalancePlan::invoke, plan_.get()));
        running_ = true;
        return plan_->id();
    }
    CHECK(!!plan_);
    LOG(INFO) << "Balance plan " << plan_->id() << " is still running";
    return plan_->id();
}

StatusOr<BalancePlan> Balancer::show(BalanceID id) const {
    {
        std::lock_guard<std::mutex> lg(lock_);
        if (plan_ != nullptr && plan_->id() == id) {
            return *plan_;
        }
    }
    if (kv_) {
        BalancePlan plan(id, kv_, client_.get());
        auto retCode = plan.recovery(false);
        if (retCode != cpp2::ErrorCode::SUCCEEDED) {
            return Status::Error("Get balance plan failed, id %ld", id);
        }
        return plan;
    }
    return Status::Error("KV is nullptr");
}

StatusOr<BalanceID> Balancer::stop() {
    std::lock_guard<std::mutex> lg(lock_);
    if (!running_) {
        return Status::Error("No running balance plan");
    }
    CHECK(!!plan_);
    plan_->stop();
    LOG(INFO) << "Stop balance plan " << plan_->id();
    return plan_->id();
}

cpp2::ErrorCode Balancer::recovery() {
    CHECK(!plan_) << "plan should be nullptr now";
    if (kv_) {
        auto* store = static_cast<kvstore::NebulaStore*>(kv_);
        if (!store->isLeader(kDefaultSpaceId, kDefaultPartId)) {
            // We need to check whether is leader or not, otherwise we would failed to persist
            // state of BalancePlan and BalanceTask, so we just reject request if not leader.
            return cpp2::ErrorCode::E_LEADER_CHANGED;
        }
        const auto& prefix = BalancePlan::prefix();
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = kv_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "Can't access kvstore, ret = " << static_cast<int32_t>(ret);
            return MetaCommon::to(ret);
        }
        std::vector<int64_t> corruptedPlans;
        while (iter->valid()) {
            auto balanceId = BalancePlan::id(iter->key());
            auto status = BalancePlan::status(iter->val());
            if (status == BalancePlan::Status::IN_PROGRESS
                    || status == BalancePlan::Status::FAILED) {
                corruptedPlans.emplace_back(balanceId);
            }
            iter->next();
        }
        if (corruptedPlans.empty()) {
            LOG(INFO) << "No corrupted plan need to recovery!";
            return cpp2::ErrorCode::SUCCEEDED;
        }
        CHECK_EQ(1, corruptedPlans.size());
        plan_ = std::make_unique<BalancePlan>(corruptedPlans[0], kv_, client_.get());
        plan_->onFinished_ = [this] () {
            auto self = plan_;
            {
                std::lock_guard<std::mutex> lg(lock_);
                if (LastUpdateTimeMan::update(kv_, time::WallClock::fastNowInMilliSec()) !=
                        kvstore::ResultCode::SUCCEEDED) {
                    LOG(INFO) << "Balance plan " << plan_->id() << " update meta failed";
                }
                finish();
            }
        };
        auto recRet = plan_->recovery();
        if (recRet != cpp2::ErrorCode::SUCCEEDED) {
            LOG(ERROR) << "Can't recovery plan " << corruptedPlans[0];
            return recRet;
        }
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

bool Balancer::getAllSpaces(std::vector<std::pair<GraphSpaceID, int32_t>>& spaces,
                            kvstore::ResultCode& retCode) {
    // Get all spaces
    folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
    auto prefix = MetaServiceUtils::spacePrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kv_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        retCode = ret;
        return false;
    }
    while (iter->valid()) {
        auto spaceId = MetaServiceUtils::spaceId(iter->key());
        auto properties = MetaServiceUtils::parseSpace(iter->val());
        spaces.emplace_back(spaceId, properties.replica_factor);
        iter->next();
    }
    return true;
}

cpp2::ErrorCode Balancer::buildBalancePlan(std::unordered_set<HostAddr> hostDel) {
    CHECK(!plan_) << "plan should be nullptr now";
    std::vector<std::pair<GraphSpaceID, int32_t>> spaces;
    kvstore::ResultCode ret = kvstore::ResultCode::SUCCEEDED;
    if (!getAllSpaces(spaces, ret)) {
        return cpp2::ErrorCode::E_STORE_FAILURE;
    }
    plan_ = std::make_unique<BalancePlan>(time::WallClock::fastNowInSec(), kv_, client_.get());
    for (auto spaceInfo : spaces) {
        auto taskRet = genTasks(spaceInfo.first, spaceInfo.second, hostDel);
        if (!ok(taskRet)) {
            return error(taskRet);
        }
        auto tasks = std::move(value(taskRet));
        for (auto& task : tasks) {
            plan_->addTask(std::move(task));
        }
    }
    plan_->onFinished_ = [this] () {
        auto self = plan_;
        {
            std::lock_guard<std::mutex> lg(lock_);
            if (LastUpdateTimeMan::update(kv_, time::WallClock::fastNowInMilliSec()) !=
                    kvstore::ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Balance plan " << plan_->id() << " update meta failed";
            }
            finish();
        }
    };
    if (plan_->tasks_.empty()) {
        return cpp2::ErrorCode::E_BALANCED;
    }
    return plan_->saveInStore();
}

ErrorOr<cpp2::ErrorCode, std::vector<BalanceTask>>
Balancer::genTasks(GraphSpaceID spaceId,
                   int32_t spaceReplica,
                   std::unordered_set<HostAddr> hostDel) {
    CHECK(!!plan_) << "plan should not be nullptr";
    std::unordered_map<HostAddr, std::vector<PartitionID>> hostParts;
    int32_t totalParts = 0;
    // hostParts is current part allocation map
    getHostParts(spaceId, hostParts, totalParts);
    if (totalParts == 0 || hostParts.empty()) {
        LOG(ERROR) << "Invalid space " << spaceId;
        return cpp2::ErrorCode::E_NOT_FOUND;
    }
    std::vector<HostAddr> newlyAdded;
    auto activeHosts = ActiveHostsMan::getActiveHosts(kv_);
    calDiff(hostParts, activeHosts, newlyAdded, hostDel);
    // newHostParts is new part allocation map after balance, it would include newlyAdded
    // and exclude hostDel
    decltype(hostParts) newHostParts(hostParts);
    for (auto& h : newlyAdded) {
        LOG(INFO) << "Found new host " << h;
        newHostParts.emplace(h, std::vector<PartitionID>());
    }
    for (auto& h : hostDel) {
        LOG(INFO) << "Lost host " << h;
        newHostParts.erase(h);
    }
    LOG(INFO) << "Now, try to balance the newHostParts";

    // We have two parts need to balance, the first one is parts on lost hosts and deleted hosts
    // The seconds one is parts on unbalanced host in newHostParts.
    std::vector<BalanceTask> tasks;
    // 1. Iterate through all hosts that would not be included in newHostParts,
    //    move all parts in them to host with minimum part in newHostParts
    for (auto& h : hostDel) {
        auto& lostParts = hostParts[h];
        for (auto& partId : lostParts) {
            // check whether any peers which is alive
            auto alive = checkReplica(hostParts, activeHosts, spaceReplica, partId);
            if (!alive.ok()) {
                LOG(ERROR) << "Error:" << alive;
                return cpp2::ErrorCode::E_NO_VALID_HOST;
            }
            // find a host with minimum parts which doesn't have this part
            auto ret = hostWithMinimalParts(newHostParts, partId);
            if (!ret.ok()) {
                LOG(ERROR) << "Error:" << ret.status();
                return cpp2::ErrorCode::E_NO_VALID_HOST;
            }
            auto& luckyHost = ret.value();
            newHostParts[luckyHost].emplace_back(partId);
            tasks.emplace_back(plan_->id_,
                               spaceId,
                               partId,
                               h,
                               luckyHost,
                               kv_,
                               client_.get());
        }
    }
    if (newHostParts.size() < 2) {
        LOG(INFO) << "Too few hosts, no need for balance!";
        return cpp2::ErrorCode::E_NO_VALID_HOST;
    }
    // 2. Make all hosts in newHostParts balanced
    balanceParts(plan_->id_, spaceId, newHostParts, totalParts, tasks);
    return tasks;
}

void Balancer::balanceParts(BalanceID balanceId,
                            GraphSpaceID spaceId,
                            std::unordered_map<HostAddr, std::vector<PartitionID>>& newHostParts,
                            int32_t totalParts,
                            std::vector<BalanceTask>& tasks) {
    float avgLoad = static_cast<float>(totalParts) / newHostParts.size();
    LOG(INFO) << "The expect avg load is " << avgLoad;
    int32_t minLoad = std::floor(avgLoad);
    int32_t maxLoad = std::ceil(avgLoad);
    auto hosts = sortedHostsByParts(newHostParts);
    CHECK_GT(hosts.size(), 1);
    auto maxPartsHost = hosts.back();
    auto minPartsHost = hosts.front();
    auto lastDelta = maxPartsHost.second - minPartsHost.second + 1;
    while (maxPartsHost.second > maxLoad
            || minPartsHost.second < minLoad
            || maxPartsHost.second - minPartsHost.second < lastDelta) {
        CHECK_GE(maxPartsHost.second, avgLoad);
        CHECK_GE(avgLoad, minPartsHost.second);
        auto& partsFrom = newHostParts[maxPartsHost.first];
        auto& partsTo = newHostParts[minPartsHost.first];
        std::sort(partsFrom.begin(), partsFrom.end());
        std::sort(partsTo.begin(), partsTo.end());
        VLOG(1) << maxPartsHost.first << ":" << partsFrom.size()
                << "->" << minPartsHost.first << ":" << partsTo.size()
                << ", lastDelta=" << lastDelta;
        std::vector<PartitionID> diff;
        std::set_difference(partsFrom.begin(), partsFrom.end(), partsTo.begin(), partsTo.end(),
                            std::inserter(diff, diff.begin()));
        bool noAction = true;
        for (auto& partId : diff) {
            if (partsFrom.size() <= partsTo.size() + 1 ||
                partsFrom.size() <= (size_t)minLoad ||
                partsTo.size() >= (size_t)maxLoad) {
                VLOG(1) << "No need to move any parts from "
                        << maxPartsHost.first << " to " << minPartsHost.first;
                break;
            }
            LOG(INFO) << "[space:" << spaceId << ", part:" << partId << "] "
                      << maxPartsHost.first << "->" << minPartsHost.first;
            auto it = std::find(partsFrom.begin(), partsFrom.end(), partId);
            CHECK(it != partsFrom.end());
            CHECK(std::find(partsTo.begin(), partsTo.end(), partId) == partsTo.end());
            partsFrom.erase(it);
            partsTo.emplace_back(partId);
            tasks.emplace_back(balanceId,
                               spaceId,
                               partId,
                               maxPartsHost.first,
                               minPartsHost.first,
                               kv_,
                               client_.get());
            noAction = false;
        }
        if (noAction) {
            break;
        }
        lastDelta = maxPartsHost.second - minPartsHost.second;
        hosts = sortedHostsByParts(newHostParts);
        maxPartsHost = hosts.back();
        minPartsHost = hosts.front();
    }
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(tasks.begin(), tasks.end(), g);
    LOG(INFO) << "Balance tasks num: " << tasks.size();
    for (auto& task : tasks) {
        LOG(INFO) << task.taskIdStr();
    }
}

void Balancer::getHostParts(GraphSpaceID spaceId,
                            std::unordered_map<HostAddr, std::vector<PartitionID>>& hostParts,
                            int32_t& totalParts) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
    auto prefix = MetaServiceUtils::partPrefix(spaceId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kv_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Access kvstore failed, spaceId " << spaceId;
        return;
    }
    while (iter->valid()) {
        auto key = iter->key();
        PartitionID partId;
        memcpy(&partId, key.data() + prefix.size(), sizeof(PartitionID));
        auto partHosts = MetaServiceUtils::parsePartVal(iter->val());
        for (auto& ph : partHosts) {
            hostParts[ph].emplace_back(partId);
        }
        iter->next();
        totalParts++;
    }

    auto key = MetaServiceUtils::spaceKey(spaceId);
    std::string value;
    auto code = kv_->get(kDefaultSpaceId, kDefaultPartId, key, &value);
    if (code != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Access kvstore failed, spaceId " << spaceId;
        return;
    }
    auto properties = MetaServiceUtils::parseSpace(value);
    CHECK_EQ(totalParts, properties.get_partition_num());
    totalParts *= properties.get_replica_factor();
}

void Balancer::calDiff(const std::unordered_map<HostAddr, std::vector<PartitionID>>& hostParts,
                       const std::vector<HostAddr>& activeHosts,
                       std::vector<HostAddr>& newlyAdded,
                       std::unordered_set<HostAddr>& lost) {
    for (auto it = hostParts.begin(); it != hostParts.end(); it++) {
        VLOG(1) << "Original Host " << it->first << ", parts " << it->second.size();
        if (std::find(activeHosts.begin(), activeHosts.end(), it->first) == activeHosts.end() &&
            std::find(lost.begin(), lost.end(), it->first) == lost.end()) {
            lost.emplace(it->first);
        }
    }
    for (auto& h : activeHosts) {
        VLOG(1) << "Active host " << h;
        if (hostParts.find(h) == hostParts.end()) {
            newlyAdded.emplace_back(h);
        }
    }
}

std::vector<std::pair<HostAddr, int32_t>>
Balancer::sortedHostsByParts(const std::unordered_map<HostAddr,
                                                      std::vector<PartitionID>>& hostParts) {
    std::vector<std::pair<HostAddr, int32_t>> hosts;
    for (auto it = hostParts.begin(); it != hostParts.end(); it++) {
        hosts.emplace_back(it->first, it->second.size());
    }
    std::sort(hosts.begin(), hosts.end(), [](const auto& l, const auto& r) {
        return l.second < r.second;
    });
    return hosts;
}

Status Balancer::checkReplica(
            const std::unordered_map<HostAddr, std::vector<PartitionID>>& hostParts,
            const std::vector<HostAddr>& activeHosts,
            int32_t replica,
            PartitionID partId) {
    // check host hold the part and alive
    auto checkPart = [&] (const auto& entry) {
        auto& host = entry.first;
        auto& peers = entry.second;
        return std::find(peers.begin(), peers.end(), partId) != peers.end() &&
               std::find(activeHosts.begin(), activeHosts.end(), host) != activeHosts.end();
    };
    auto aliveReplica = std::count_if(hostParts.begin(), hostParts.end(), checkPart);
    if (aliveReplica >= replica / 2 + 1) {
        return Status::OK();
    }
    return Status::Error("Not enough alive host hold the part %d", partId);
}

StatusOr<HostAddr> Balancer::hostWithMinimalParts(
                        const std::unordered_map<HostAddr, std::vector<PartitionID>>& hostParts,
                        PartitionID partId) {
    auto hosts = sortedHostsByParts(hostParts);
    for (auto& h : hosts) {
        auto it = hostParts.find(h.first);
        CHECK(it != hostParts.end());
        if (std::find(it->second.begin(), it->second.end(), partId) == it->second.end()) {
            return h.first;
        }
    }
    return Status::Error("No host is suitable for %d", partId);
}

cpp2::ErrorCode Balancer::leaderBalance() {
    if (running_) {
        return cpp2::ErrorCode::E_BALANCER_RUNNING;
    }

    folly::Promise<Status> promise;
    auto future = promise.getFuture();
    std::vector<std::pair<GraphSpaceID, int32_t>> spaces;
    kvstore::ResultCode ret = kvstore::ResultCode::SUCCEEDED;
    if (!getAllSpaces(spaces, ret)) {
        LOG(ERROR) << "Can't access kvstore, ret = " << static_cast<int32_t>(ret);
        return cpp2::ErrorCode::E_STORE_FAILURE;
    }

    bool expected = false;
    if (inLeaderBalance_.compare_exchange_strong(expected, true)) {
        hostLeaderMap_.reset(new HostLeaderMap);
        auto status = client_->getLeaderDist(hostLeaderMap_.get()).get();

        if (!status.ok() || hostLeaderMap_->empty()) {
            inLeaderBalance_ = false;
            return cpp2::ErrorCode::E_RPC_FAILURE;
        }

        std::vector<folly::SemiFuture<Status>> futures;
        for (const auto& spaceInfo : spaces) {
            LeaderBalancePlan plan;
            buildLeaderBalancePlan(hostLeaderMap_.get(), spaceInfo.first, plan);
            simplifyLeaderBalnacePlan(spaceInfo.first, plan);
            for (const auto& task : plan) {
                futures.emplace_back(client_->transLeader(std::get<0>(task), std::get<1>(task),
                                                          std::move(std::get<2>(task)),
                                                          std::move(std::get<3>(task))));
            }
        }

        int32_t failed = 0;
        folly::collectAll(futures).thenTry([&](const auto& result) {
            auto tries = result.value();
            for (const auto& t : tries) {
                if (!t.value().ok()) {
                    ++failed;
                }
            }
        }).wait();
        LOG(ERROR) << failed << " partiton failed to transfer leader";
        inLeaderBalance_ = false;
        return cpp2::ErrorCode::E_BALANCER_FAILURE;
    }
    return cpp2::ErrorCode::E_BALANCER_RUNNING;
}

std::unordered_map<HostAddr, std::vector<PartitionID>>
Balancer::buildLeaderBalancePlan(HostLeaderMap* hostLeaderMap, GraphSpaceID spaceId,
                                 LeaderBalancePlan& plan, bool useDeviation) {
    std::unordered_map<PartitionID, std::vector<HostAddr>> peersMap;
    std::unordered_map<HostAddr, std::vector<PartitionID>> leaderHostParts;
    size_t leaderParts = 0;
    {
        // store peers of all paritions in peerMap
        folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
        auto prefix = MetaServiceUtils::partPrefix(spaceId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = kv_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "Access kvstore failed, spaceId " << spaceId;
            return leaderHostParts;
        }
        while (iter->valid()) {
            auto key = iter->key();
            PartitionID partId;
            memcpy(&partId, key.data() + prefix.size(), sizeof(PartitionID));
            auto peers = MetaServiceUtils::parsePartVal(iter->val());
            peersMap[partId] = std::move(peers);
            ++leaderParts;
            iter->next();
        }
    }

    int32_t totalParts = 0;
    std::unordered_map<HostAddr, std::vector<PartitionID>> allHostParts;
    getHostParts(spaceId, allHostParts, totalParts);

    std::unordered_set<HostAddr> activeHosts;
    for (const auto& host : *hostLeaderMap) {
        // only balance leader between hosts which have valid partition
        if (!allHostParts[host.first].empty()) {
            activeHosts.emplace(host.first);
            leaderHostParts[host.first] = std::move((*hostLeaderMap)[host.first][spaceId]);
        }
    }

    if (activeHosts.empty()) {
        LOG(ERROR) << "No active hosts";
        return leaderHostParts;
    }

    size_t avgLoad = leaderParts / activeHosts.size();
    size_t minLoad = avgLoad;
    size_t maxLoad = avgLoad + 1;
    if (useDeviation) {
        minLoad = std::ceil(static_cast<double> (leaderParts) / activeHosts.size() *
                            (1 - FLAGS_leader_balance_deviation));
        maxLoad = std::floor(static_cast<double> (leaderParts) / activeHosts.size() *
                            (1 + FLAGS_leader_balance_deviation));
    }
    LOG(INFO) << "Build leader balance plan, expected min load: " << minLoad
              << ", max load: " << maxLoad;

    while (true) {
        bool hasUnbalancedHost = false;
        int32_t taskCount = 0;

        for (auto& hostEntry : leaderHostParts) {
            if (minLoad <= hostEntry.second.size() && hostEntry.second.size() <= maxLoad) {
                continue;
            }
            hasUnbalancedHost = true;
            if (hostEntry.second.size() < minLoad) {
                // need to acquire leader from other hosts
                taskCount += acquireLeaders(allHostParts, leaderHostParts, peersMap, activeHosts,
                                            hostEntry.first, minLoad, plan, spaceId);
            } else {
                // need to transfer leader to other hosts
                taskCount += giveupLeaders(leaderHostParts, peersMap, activeHosts,
                                           hostEntry.first, maxLoad, plan, spaceId);
            }
        }

        // If every host is balanced or no more task during this loop, then the plan is done
        if (!hasUnbalancedHost || taskCount == 0) {
            break;
        }
    }
    return leaderHostParts;
}

int32_t Balancer::acquireLeaders(
        std::unordered_map<HostAddr, std::vector<PartitionID>>& allHostParts,
        std::unordered_map<HostAddr, std::vector<PartitionID>>& leaderHostParts,
        std::unordered_map<PartitionID, std::vector<HostAddr>>& peersMap,
        std::unordered_set<HostAddr>& activeHosts,
        HostAddr host,
        size_t minLoad,
        LeaderBalancePlan& plan,
        GraphSpaceID spaceId) {
    // host will loop for the partition which is not leader, and try to acuire the leader
    int32_t taskCount = 0;
    std::vector<PartitionID> diff;
    std::set_difference(allHostParts[host].begin(), allHostParts[host].end(),
                        leaderHostParts[host].begin(), leaderHostParts[host].end(),
                        std::back_inserter(diff));
    auto& hostLeaders = leaderHostParts[host];
    for (const auto& partId : diff) {
        // find the leader of partId
        auto peers = peersMap[partId];
        for (const auto& peer : peers) {
            if (peer == host || !activeHosts.count(peer)) {
                continue;
            }
            // if peer is the leader of partId and can transfer, then transfer it to host
            auto& peerLeaders = leaderHostParts[peer];
            auto it = std::find(peerLeaders.begin(), peerLeaders.end(), partId);
            if (it != peerLeaders.end()) {
                if (minLoad < peerLeaders.size()) {
                    peerLeaders.erase(it);
                    hostLeaders.emplace_back(partId);
                    plan.emplace_back(spaceId, partId, peer, host);
                    LOG(INFO) << "plan trans leader: " << spaceId << " " << partId << " from "
                              << peer.host
                              << peer.port << " to "
                              << host.host
                              << ":" << host.port;
                    ++taskCount;
                    break;
                }
            }
        }
        // if host has enough leader, just return
        if (minLoad <= hostLeaders.size()) {
            break;
        }
    }
    return taskCount;
}

int32_t Balancer::giveupLeaders(
        std::unordered_map<HostAddr, std::vector<PartitionID>>& leaderHostParts,
        std::unordered_map<PartitionID, std::vector<HostAddr>>& peersMap,
        std::unordered_set<HostAddr>& activeHosts,
        HostAddr host,
        size_t maxLoad,
        LeaderBalancePlan& plan,
        GraphSpaceID spaceId) {
    int taskCount = 0;
    auto& hostLeaders = leaderHostParts[host];
    // host will try to transfer the extra leaders to other peers
    for (auto it = hostLeaders.begin(); it != hostLeaders.end(); ) {
        // find the leader of partId
        auto partId = *it;
        auto peers = peersMap[partId];
        bool transfered = false;
        for (const auto& peer : peers) {
            if (host == peer || !activeHosts.count(peer)) {
                continue;
            }
            // If peer can accept this partition leader, than host will transfer to the peer
            auto& peerLeaders = leaderHostParts[peer];
            if (peerLeaders.size() < maxLoad) {
                it = hostLeaders.erase(it);
                peerLeaders.emplace_back(partId);
                plan.emplace_back(spaceId, partId, host, peer);
                LOG(INFO) << "plan trans leader: " << spaceId << " " << partId << " host "
                          << host.host << ":"
                          << host.port << " peer "
                          << peer.host
                          << ":" << peer.port;
                ++taskCount;
                transfered = true;
                break;
            }
        }
        if (!transfered) {
            ++it;
        }
        // if host has enough leader, just return
        if (hostLeaders.size() <= maxLoad) {
            break;
        }
    }
    return taskCount;
}

void Balancer::simplifyLeaderBalnacePlan(GraphSpaceID spaceId, LeaderBalancePlan& plan) {
    // Within a leader balance plan, a partition may be moved several times, but actually
    // we only need to transfer the leadership of a partition from the first host to the
    // last host, and ignore the intermediate ones
    std::unordered_map<PartitionID, LeaderBalancePlan> buckets;
    for (auto& task : plan) {
        buckets[std::get<1>(task)].emplace_back(task);
    }
    plan.clear();
    for (const auto& partEntry : buckets) {
        plan.emplace_back(spaceId, partEntry.first,
                          std::get<2>(partEntry.second.front()),
                          std::get<3>(partEntry.second.back()));
    }
}

}  // namespace meta
}  // namespace nebula
