//
// Created by fujie on 24-10-21.
//

#ifndef NEBULA_GRAPH_LEADERTRANSFER_H
#define NEBULA_GRAPH_LEADERTRANSFER_H

#include "meta/processors/job/LeaderBalanceJobExecutor.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

#include <algorithm>
#include <cstdlib>
#include <memory>

#include "kvstore/NebulaStore.h"
#include "meta/ActiveHostsMan.h"
#include "meta/MetaServiceUtils.h"
#include "meta/processors/Common.h"
#include "LeaderTransferProcessor.h"


namespace nebula::meta {
nebula::cpp2::ErrorCode LeaderTransferProcessor::buildLeaderTransferPlan(
  const HostAddr &source,
  HostLeaderMap *hostLeaderMap,
  GraphSpaceID spaceId,
  std::vector<std::pair<PartitionID, HostAddr> > &plan) const {
  auto &leaderPartIds = (*hostLeaderMap)[source][spaceId];
  PartAllocation peersMap;
  // store peers of all paritions in peerMap
  {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::lock());
    const auto &prefix = MetaKeyUtils::partPrefix(spaceId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto retCode = kv_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(ERROR) << "Access kvstore failed, spaceId: " << spaceId
                 << ", error: " << apache::thrift::util::enumNameSafe(retCode);
      return retCode;
    }

    while (iter->valid()) {
      PartitionID partId = MetaKeyUtils::parsePartKeyPartId(iter->key());
      auto peers = MetaKeyUtils::parsePartVal(iter->val());
      peersMap[partId] = std::move(peers);
      iter->next();
    }
  }

  auto activeHostsRet = ActiveHostsMan::getActiveHosts(kv_);
  if (!nebula::ok(activeHostsRet)) {
    auto retCode = nebula::error(activeHostsRet);
    LOG(ERROR) << "Get active hosts failed, error: "
               << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }
  auto activeHosts = nebula::value(activeHostsRet);

  std::unordered_map<HostAddr, int32_t> leadersMap;
  // Caculate the leader number of each storage node
  for (auto &spaceLeaders: *hostLeaderMap) {
    auto &host = spaceLeaders.first;
    if (spaceLeaders.second.find(spaceId) == spaceLeaders.second.end()) {
      leadersMap[host] = 0;
    } else {
      leadersMap[host] = spaceLeaders.second[spaceId].size();
    }
  }
  for (auto partId: leaderPartIds) {
    HostAddr candidate;
    int32_t minNum = INT_MAX;
    // Transfer the leader to the partition peer with the least leaders
    for (auto &host: peersMap[partId]) {
      if (host == source) {
        continue;
      }
      auto found = std::find(activeHosts.begin(), activeHosts.end(), host);
      if (found == activeHosts.end()) {
        continue;
      }
      if (leadersMap[host] < minNum) {
        minNum = leadersMap[host];
        candidate = host;
      }
    }
    if (minNum != INT_MAX) {
      plan.emplace_back(partId, candidate);
      ++leadersMap[candidate];
    } else {
      LOG(ERROR) << "Can't find leader candidiate for part " << partId;
      continue;
    }
  }

  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode LeaderTransferProcessor::leaderTransfer(const meta::cpp2::LeaderTransferReq &req) {
  if (running_) {
    LOG(INFO) << "Balance process still running";
    return nebula::cpp2::ErrorCode::E_BALANCER_RUNNING;
  }

  const auto &source = req.get_host();
  auto activeHostRet = ActiveHostsMan::isLived(kv_, source);
  if (!nebula::ok(activeHostRet)) {
    auto code = nebula::error(activeHostRet);
    LOG(ERROR) << "Check host live failed, error: " << apache::thrift::util::enumNameSafe(code);
    return code;
  } else {
    auto isLive = nebula::value(activeHostRet);
    if (!isLive) {
      LOG(ERROR) << "The host is not lived: " << source;
      // no need to transfer leader for inactive host
      return nebula::cpp2::ErrorCode::E_INVALID_HOST;
    }
  }

  auto spaceId = req.get_space_id();
  auto spaceKey = MetaKeyUtils::spaceKey(spaceId);
  std::string val;
  auto retCode = kv_->get(kDefaultSpaceId, kDefaultPartId, spaceKey, &val);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "Get space (" << spaceId << ") failed: "
               << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }
  auto properties = MetaKeyUtils::parseSpace(val);

  bool expected = false;
  // treat leader transfer same as leader balance
  if (inLeaderBalance_.compare_exchange_strong(expected, true)) {
    hostLeaderMap_ = std::make_unique<HostLeaderMap>();
    auto leaderDistRet = client_->getLeaderDist(hostLeaderMap_.get()).get();
    if (!leaderDistRet.ok() || hostLeaderMap_->empty()) {
      LOG(ERROR) << "Get leader distribution failed";
      inLeaderBalance_ = false;
      return nebula::cpp2::ErrorCode::E_RPC_FAILURE;
    }
    if (hostLeaderMap_->find(source) == hostLeaderMap_->end() ||
        (*hostLeaderMap_)[source].empty() ||
        (*hostLeaderMap_)[source].find(spaceId) == (*hostLeaderMap_)[source].end() ||
        (*hostLeaderMap_)[source][spaceId].empty()) {
      LOG(INFO) << "No leader found, no need to transfer leader";
      inLeaderBalance_ = false;
      return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    std::vector<std::pair<PartitionID, HostAddr> > plans;
    retCode = buildLeaderTransferPlan(source,
                                      hostLeaderMap_.get(),
                                      spaceId,
                                      plans);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(ERROR) << "Building leader transfer plan failed. " << "Space: " << spaceId;
      inLeaderBalance_ = false;
      return retCode;
    }
    if (plans.empty()) {
      LOG(INFO) << "No leader transfer plan is generated.";
      inLeaderBalance_ = false;
      return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    int32_t num = 0;
    auto concurrency = req.get_concurrency();
    std::vector<folly::SemiFuture<Status> > futures;
    for (const auto &plan: plans) {
      futures.emplace_back(client_->transLeader(spaceId, // spaceId
                                                plan.first, // partId
                                                source, // source
                                                plan.second)); // destination
      ++num;
      if (concurrency != 0 && num >= concurrency) {
        break;
      }
    }

    int32_t failed = 0;
    folly::collectAll(futures)
      .via(executor_.get())
      .thenTry([&](const auto &result) {
        auto tries = result.value();
        for (const auto &t: tries) {
          if (!t.value().ok()) {
            ++failed;
          }
        }
      }).wait();

    inLeaderBalance_ = false;
    if (failed != 0) {
      LOG(ERROR) << failed << " partiton failed to transfer leader";
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }
  return nebula::cpp2::ErrorCode::E_BALANCER_RUNNING;
}
} // namespace nebula::meta

#endif  // NEBULA_GRAPH_LEADERTRANSFER_H