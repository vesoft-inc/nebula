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
        const auto& prefix = MetaServiceUtils::balancePlanPrefix();
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = kv_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "Can't access kvstore, ret = " << static_cast<int32_t>(ret);
            return MetaCommon::to(ret);
        }
        std::vector<int64_t> corruptedPlans;
        while (iter->valid()) {
            auto status = MetaServiceUtils::parseBalanceStatus(iter->val());
            if (status == BalanceStatus::IN_PROGRESS ||
                status == BalanceStatus::FAILED) {
                auto balanceId = MetaServiceUtils::parseBalanceID(iter->key());
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
                    LOG(ERROR) << "Balance plan " << plan_->id() << " update meta failed";
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

bool Balancer::getAllSpaces(std::vector<std::tuple<GraphSpaceID, int32_t, bool>>& spaces) {
    // Get all spaces
    folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
    auto prefix = MetaServiceUtils::spacePrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kv_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Get all spaces failed";
        return false;
    }

    while (iter->valid()) {
        auto spaceId = MetaServiceUtils::spaceId(iter->key());
        auto properties = MetaServiceUtils::parseSpace(iter->val());
        bool zoned = properties.__isset.group_name ? true : false;
        spaces.emplace_back(spaceId, properties.replica_factor, zoned);
        iter->next();
    }
    return true;
}

cpp2::ErrorCode Balancer::buildBalancePlan(std::unordered_set<HostAddr> hostDel) {
    CHECK(!plan_) << "plan should be nullptr now";
    std::vector<std::tuple<GraphSpaceID, int32_t, bool>> spaces;
    if (!getAllSpaces(spaces)) {
        LOG(ERROR) << "Can't get all spaces";
        return cpp2::ErrorCode::E_STORE_FAILURE;
    }

    plan_ = std::make_unique<BalancePlan>(time::WallClock::fastNowInSec(), kv_, client_.get());
    for (auto spaceInfo : spaces) {
        auto taskRet = genTasks(std::get<0>(spaceInfo),
                                std::get<1>(spaceInfo),
                                std::get<2>(spaceInfo),
                                hostDel);
        if (!ok(taskRet)) {
            LOG(ERROR) << "Generate tasks on space " << std::get<0>(spaceInfo) << " failed";
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
                   bool dependentOnGroup,
                   std::unordered_set<HostAddr> hostDel) {
    CHECK(!!plan_) << "plan should not be nullptr";
    HostParts hostParts;
    int32_t totalParts = 0;
    // hostParts is current part allocation map
    auto result = getHostParts(spaceId, dependentOnGroup, hostParts, totalParts);
    if (!result || totalParts == 0 || hostParts.empty()) {
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
                LOG(ERROR) << "Error: " << alive;
                return cpp2::ErrorCode::E_NO_VALID_HOST;
            }
            // find a host with minimum parts which doesn't have this part
            auto ret = hostWithMinimalParts(newHostParts, partId);
            if (!ret.ok()) {
                LOG(ERROR) << "Error: " << ret.status();
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
                            HostParts& newHostParts,
                            int32_t totalParts,
                            std::vector<BalanceTask>& tasks) {
    auto avgLoad = static_cast<float>(totalParts) / newHostParts.size();
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
            LOG(INFO) << "Here is no action";
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

bool Balancer::getHostParts(GraphSpaceID spaceId,
                            bool dependentOnGroup,
                            HostParts& hostParts,
                            int32_t& totalParts) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
    auto prefix = MetaServiceUtils::partPrefix(spaceId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kv_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Access kvstore failed, spaceId " << spaceId;
        return false;
    }

    std::vector<HostAddr> hosts;
    if (dependentOnGroup) {
        hosts = ActiveHostsMan::getActiveHostsBySpace(kv_, spaceId);
    }

    while (iter->valid()) {
        auto key = iter->key();
        PartitionID partId;
        memcpy(&partId, key.data() + prefix.size(), sizeof(PartitionID));
        auto partHosts = MetaServiceUtils::parsePartVal(iter->val());
        for (auto& ph : partHosts) {
            if (dependentOnGroup &&
                std::find(hosts.begin(), hosts.end(), ph) == hosts.end()) {
                continue;
            }
            hostParts[ph].emplace_back(partId);
        }
        totalParts++;
        iter->next();
    }

    LOG(INFO) << "Host parts size: " << hostParts.size();
    auto key = MetaServiceUtils::spaceKey(spaceId);
    std::string value;
    auto code = kv_->get(kDefaultSpaceId, kDefaultPartId, key, &value);
    if (code != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Access kvstore failed, spaceId " << spaceId;
        return false;
    }

    auto properties = MetaServiceUtils::parseSpace(value);
    CHECK_EQ(totalParts, properties.get_partition_num());
    totalParts *= properties.get_replica_factor();
    return true;
}

void Balancer::calDiff(const HostParts& hostParts,
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
Balancer::sortedHostsByParts(const HostParts& hostParts) {
    std::vector<std::pair<HostAddr, int32_t>> hosts;
    for (auto it = hostParts.begin(); it != hostParts.end(); it++) {
        hosts.emplace_back(it->first, it->second.size());
    }
    std::sort(hosts.begin(), hosts.end(), [](const auto& l, const auto& r) {
        return l.second < r.second;
    });
    return hosts;
}

Status Balancer::checkReplica(const HostParts& hostParts,
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

StatusOr<HostAddr> Balancer::hostWithMinimalParts(const HostParts& hostParts,
                                                  PartitionID partId) {
    auto hosts = sortedHostsByParts(hostParts);
    for (auto& h : hosts) {
        auto it = hostParts.find(h.first);
        if (it == hostParts.end()) {
            LOG(ERROR) << "Host " << h.first << " not found";
            return Status::Error("Host not found");
        }

        if (std::find(it->second.begin(), it->second.end(), partId) == it->second.end()) {
            return h.first;
        }
    }
    return Status::Error("No host is suitable for %d", partId);
}

cpp2::ErrorCode Balancer::leaderBalance() {
    if (running_) {
        LOG(INFO) << "Balance process still running";
        return cpp2::ErrorCode::E_BALANCER_RUNNING;
    }

    folly::Promise<Status> promise;
    auto future = promise.getFuture();

    // Space ID, Replica Factor and Dependent On Group
    std::vector<std::tuple<GraphSpaceID, int32_t, bool>> spaces;
    if (!getAllSpaces(spaces)) {
        LOG(ERROR) << "Can't get spaces";
        return cpp2::ErrorCode::E_STORE_FAILURE;
    }

    bool expected = false;
    if (inLeaderBalance_.compare_exchange_strong(expected, true)) {
        hostLeaderMap_.reset(new HostLeaderMap);
        auto status = client_->getLeaderDist(hostLeaderMap_.get()).get();
        if (!status.ok() || hostLeaderMap_->empty()) {
            LOG(ERROR) << "Get leader distribution failed";
            inLeaderBalance_ = false;
            return cpp2::ErrorCode::E_RPC_FAILURE;
        }

        std::vector<folly::SemiFuture<Status>> futures;
        for (const auto& spaceInfo : spaces) {
            auto spaceId = std::get<0>(spaceInfo);
            auto replicaFactor = std::get<1>(spaceInfo);
            auto dependentOnGroup = std::get<2>(spaceInfo);
            LeaderBalancePlan plan;
            auto balanceResult = buildLeaderBalancePlan(hostLeaderMap_.get(),
                                                        spaceId,
                                                        replicaFactor,
                                                        dependentOnGroup,
                                                        plan);
            if (!balanceResult) {
                LOG(ERROR) << "Building leader balance plan failed "
                           << "Space: " << spaceId;;
                continue;
            }
            simplifyLeaderBalnacePlan(spaceId, plan);
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

        inLeaderBalance_ = false;
        if (failed != 0) {
            LOG(ERROR) << failed << " partiton failed to transfer leader";
        }
        return cpp2::ErrorCode::SUCCEEDED;
    }
    return cpp2::ErrorCode::E_BALANCER_RUNNING;
}

bool Balancer::buildLeaderBalancePlan(HostLeaderMap* hostLeaderMap,
                                      GraphSpaceID spaceId,
                                      int32_t replicaFactor,
                                      bool dependentOnGroup,
                                      LeaderBalancePlan& plan,
                                      bool useDeviation) {
    PartAllocation peersMap;
    HostParts leaderHostParts;
    size_t leaderParts = 0;
    // store peers of all paritions in peerMap
    folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
    auto prefix = MetaServiceUtils::partPrefix(spaceId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kv_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Access kvstore failed, spaceId " << spaceId;
        return false;
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

    int32_t totalParts = 0;
    HostParts allHostParts;
    auto result = getHostParts(spaceId, dependentOnGroup, allHostParts, totalParts);
    if (!result || totalParts == 0 || allHostParts.empty()) {
        LOG(ERROR) << "Invalid space " << spaceId;
        return false;
    }

    std::unordered_set<HostAddr> activeHosts;
    for (const auto& host : *hostLeaderMap) {
        // only balance leader between hosts which have valid partition
        if (!allHostParts[host.first].empty()) {
            activeHosts.emplace(host.first);
            leaderHostParts[host.first] = (*hostLeaderMap)[host.first][spaceId];
        }
    }

    if (activeHosts.empty()) {
        LOG(ERROR) << "No active hosts";
        return false;
    }

    if (dependentOnGroup) {
        for (auto it = allHostParts.begin(); it != allHostParts.end(); it++) {
            auto min = it->second.size() / replicaFactor;
            VLOG(3) << "Host: " << it->first << " Bounds: " << min << " : " << min + 1;
            hostBounds_[it->first] = std::make_pair(min, min + 1);
        }
    } else {
        size_t activeSize = activeHosts.size();
        size_t globalAvg = leaderParts / activeSize;
        size_t globalMin = globalAvg;
        size_t globalMax = globalAvg;
        if (leaderParts % activeSize != 0) {
            globalMax += 1;
        }

        if (useDeviation) {
            globalMin = std::ceil(static_cast<double> (leaderParts) / activeSize *
                                  (1 - FLAGS_leader_balance_deviation));
            globalMax = std::floor(static_cast<double> (leaderParts) / activeSize *
                                   (1 + FLAGS_leader_balance_deviation));
        }
        VLOG(3) << "Build leader balance plan, expected min load: " << globalMin
                << ", max load: " << globalMax << " avg: " << globalAvg;

        for (auto it = allHostParts.begin(); it != allHostParts.end(); it++) {
            hostBounds_[it->first] = std::make_pair(globalMin, globalMax);
        }
    }

    while (true) {
        int32_t taskCount = 0;
        bool hasUnbalancedHost = false;
        for (const auto& hostEntry : leaderHostParts) {
            auto host = hostEntry.first;
            auto& hostMinLoad = hostBounds_[host].first;
            auto& hostMaxLoad = hostBounds_[host].second;
            int32_t partSize = hostEntry.second.size();
            if (hostMinLoad <= partSize && partSize <= hostMaxLoad) {
                VLOG(3) << partSize << " is between min load "
                        << hostMinLoad << " and max load " << hostMaxLoad;
                continue;
            }

            hasUnbalancedHost = true;
            if (partSize < hostMinLoad) {
                // need to acquire leader from other hosts
                LOG(INFO) << "Acquire leaders to host: " << host
                          << " loading: " << partSize
                          << " min loading " << hostMinLoad;
                taskCount += acquireLeaders(allHostParts, leaderHostParts, peersMap,
                                            activeHosts, host, plan, spaceId);
            } else {
                // need to transfer leader to other hosts
                LOG(INFO) << "Giveup leaders from host: " << host
                          << " loading: " << partSize
                          << " max loading " << hostMaxLoad;
                taskCount += giveupLeaders(leaderHostParts, peersMap,
                                           activeHosts, host, plan, spaceId);
            }
        }

        // If every host is balanced or no more task during this loop, then the plan is done
        if (!hasUnbalancedHost || taskCount == 0) {
            LOG(INFO) << "Not need balance";
            break;
        }
    }
    return true;
}

int32_t Balancer::acquireLeaders(HostParts& allHostParts,
                                 HostParts& leaderHostParts,
                                 PartAllocation& peersMap,
                                 std::unordered_set<HostAddr>& activeHosts,
                                 const HostAddr& target,
                                 LeaderBalancePlan& plan,
                                 GraphSpaceID spaceId) {
    // host will loop for the partition which is not leader, and try to acuire the leader
    int32_t taskCount = 0;
    std::vector<PartitionID> diff;
    std::set_difference(allHostParts[target].begin(), allHostParts[target].end(),
                        leaderHostParts[target].begin(), leaderHostParts[target].end(),
                        std::back_inserter(diff));
    auto& targetLeaders = leaderHostParts[target];
    size_t minLoad = hostBounds_[target].first;
    for (const auto& partId : diff) {
        VLOG(3) << "Try acquire leader for part " << partId;
        // find the leader of partId
        auto sources = peersMap[partId];
        for (const auto& source : sources) {
            if (source == target || !activeHosts.count(source)) {
                continue;
            }
            // if peer is the leader of partId and can transfer, then transfer it to host
            auto& sourceLeaders = leaderHostParts[source];
            VLOG(3) << "Check peer: " << source << " min load: " << minLoad
                    << " peerLeaders size: " << sourceLeaders.size();
            auto it = std::find(sourceLeaders.begin(), sourceLeaders.end(), partId);
            if (it != sourceLeaders.end() && minLoad < sourceLeaders.size()) {
                sourceLeaders.erase(it);
                targetLeaders.emplace_back(partId);
                plan.emplace_back(spaceId, partId, source, target);
                LOG(INFO) << "acquire plan trans leader space: " << spaceId
                          << " part: " << partId
                          << " from " << source.host << ":" << source.port
                          << " to " << target.host << ":" << target.port;
                ++taskCount;
                break;
            }
        }

        // if host has enough leader, just return
        if (targetLeaders.size() == minLoad) {
            LOG(INFO) << "Host: " << target  << "'s leader reach " << minLoad;
            break;
        }
    }
    return taskCount;
}

int32_t Balancer::giveupLeaders(HostParts& leaderParts,
                                PartAllocation& peersMap,
                                std::unordered_set<HostAddr>& activeHosts,
                                const HostAddr& source,
                                LeaderBalancePlan& plan,
                                GraphSpaceID spaceId) {
    int32_t taskCount = 0;
    auto& sourceLeaders = leaderParts[source];
    size_t maxLoad = hostBounds_[source].second;
    // host will try to transfer the extra leaders to other peers
    for (auto it = sourceLeaders.begin(); it != sourceLeaders.end(); it++) {
        // find the leader of partId
        auto partId = *it;
        const auto& targets = peersMap[partId];

        // leader should move to the peer with lowest loading
        auto target = std::min_element(targets.begin(), targets.end(),
                                       [&](const auto &l, const auto &r) -> bool {
                                           if (source == l || !activeHosts.count(l)) {
                                               return false;
                                           }
                                           return leaderParts[l].size() < leaderParts[r].size();
                                       });
        // If peer can accept this partition leader, than host will transfer to the peer
        auto& targetLeaders = leaderParts[*target];
        int32_t targetLeaderSize = targetLeaders.size();
        if (targetLeaderSize < hostBounds_[*target].second) {
            it = sourceLeaders.erase(it);
            targetLeaders.emplace_back(partId);
            plan.emplace_back(spaceId, partId, source, *target);
            LOG(INFO) << "giveup plan trans leader space: " << spaceId
                      << " part: " << partId
                      << " from " << source.host << ":" << source.port
                      << " to " << target->host << ":" << target->port;
            ++taskCount;
        }

        // if host has enough leader, just return
        if (sourceLeaders.size() == maxLoad) {
            LOG(INFO) << "Host: " << source  << "'s leader reach " << maxLoad;
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
