/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/StorageJobExecutor.h"

#include "common/network/NetworkUtils.h"
#include "common/utils/MetaKeyUtils.h"
#include "common/utils/Utils.h"
#include "interface/gen-cpp2/common_types.h"
#include "meta/ActiveHostsMan.h"
#include "meta/processors/Common.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/job/BalanceJobExecutor.h"
#include "meta/processors/job/CompactJobExecutor.h"
#include "meta/processors/job/FlushJobExecutor.h"
#include "meta/processors/job/RebuildEdgeJobExecutor.h"
#include "meta/processors/job/RebuildFTJobExecutor.h"
#include "meta/processors/job/RebuildTagJobExecutor.h"
#include "meta/processors/job/StatsJobExecutor.h"
#include "meta/processors/job/TaskDescription.h"

DECLARE_int32(heartbeat_interval_secs);
DECLARE_uint32(expired_time_factor);

namespace nebula {
namespace meta {

ErrOrHosts StorageJobExecutor::getTargetHost(GraphSpaceID spaceId) {
  std::unique_ptr<kvstore::KVIterator> iter;
  const auto& partPrefix = MetaKeyUtils::partPrefix(spaceId);
  auto retCode = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, partPrefix, &iter);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Fetch Parts Failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }

  // use vector instead of set because this can convenient for next step
  std::unordered_map<HostAddr, std::vector<PartitionID>> hostAndPart;
  std::vector<std::pair<HostAddr, std::vector<PartitionID>>> hosts;
  while (iter->valid()) {
    auto part = MetaKeyUtils::parsePartKeyPartId(iter->key());
    auto targets = MetaKeyUtils::parsePartVal(iter->val());
    for (auto& target : targets) {
      hostAndPart[target].emplace_back(part);
    }
    iter->next();
  }
  for (auto it = hostAndPart.begin(); it != hostAndPart.end(); it++) {
    hosts.emplace_back(std::pair(it->first, it->second));
  }
  return hosts;
}

ErrOrHosts StorageJobExecutor::getLeaderHost(GraphSpaceID space) {
  const auto& hostPrefix = MetaKeyUtils::leaderPrefix(space);
  std::unique_ptr<kvstore::KVIterator> leaderIter;
  auto retCode = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, hostPrefix, &leaderIter);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Get space " << space
              << "'s part failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }

  std::vector<std::pair<HostAddr, std::vector<PartitionID>>> hosts;
  HostAddr host;
  nebula::cpp2::ErrorCode code;
  for (; leaderIter->valid(); leaderIter->next()) {
    auto spaceAndPart = MetaKeyUtils::parseLeaderKeyV3(leaderIter->key());
    auto partId = spaceAndPart.second;
    std::tie(host, std::ignore, code) = MetaKeyUtils::parseLeaderValV3(leaderIter->val());
    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
      continue;
    }
    auto it =
        std::find_if(hosts.begin(), hosts.end(), [&](auto& item) { return item.first == host; });
    if (it == hosts.end()) {
      hosts.emplace_back(std::make_pair(host, std::vector<PartitionID>{partId}));
    } else {
      it->second.emplace_back(partId);
    }
  }
  // If storage has not report leader distribution to meta and we don't report error here,
  // JobMananger will think of the job consists of 0 task, and the task will not send to any
  // storage. And the job will always be RUNNING.
  if (hosts.empty()) {
    return nebula::cpp2::ErrorCode::E_JOB_HAS_NO_TARGET_STORAGE;
  }
  return hosts;
}

ErrOrHosts StorageJobExecutor::getListenerHost(GraphSpaceID space, cpp2::ListenerType type) {
  const auto& prefix = MetaKeyUtils::listenerPrefix(space, type);
  std::unique_ptr<kvstore::KVIterator> iter;
  auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Get space " << space
              << "'s listener failed, error: " << apache::thrift::util::enumNameSafe(ret);
    return ret;
  }

  auto activeHostsRet =
      ActiveHostsMan::getActiveHosts(kvstore_,
                                     FLAGS_heartbeat_interval_secs * FLAGS_expired_time_factor,
                                     cpp2::HostRole::LISTENER);
  if (!nebula::ok(activeHostsRet)) {
    return nebula::error(activeHostsRet);
  }

  auto activeHosts = std::move(nebula::value(activeHostsRet));
  std::vector<std::pair<HostAddr, std::vector<PartitionID>>> hosts;

  while (iter->valid()) {
    auto host = MetaKeyUtils::deserializeHostAddr(iter->val());
    auto part = MetaKeyUtils::parseListenerPart(iter->key());
    if (std::find(activeHosts.begin(), activeHosts.end(), host) == activeHosts.end()) {
      LOG(INFO) << "Invalid host : " << network::NetworkUtils::toHostsStr({host});
      return nebula::cpp2::ErrorCode::E_INVALID_HOST;
    }
    auto it = std::find_if(
        hosts.begin(), hosts.end(), [&host](auto& item) { return item.first == host; });
    if (it == hosts.end()) {
      hosts.emplace_back(std::make_pair(host, std::vector<PartitionID>{part}));
    } else {
      it->second.emplace_back(part);
    }
    iter->next();
  }
  if (hosts.empty()) {
    return nebula::cpp2::ErrorCode::E_LISTENER_NOT_FOUND;
  }
  return hosts;
}

folly::Future<nebula::cpp2::ErrorCode> StorageJobExecutor::execute() {
  ErrOrHosts addressesRet;
  isRunning_.store(true);

  switch (toHost_) {
    case TargetHosts::LEADER: {
      addressesRet = getLeaderHost(space_);
      break;
    }
    case TargetHosts::LISTENER: {
      addressesRet = getListenerHost(space_, cpp2::ListenerType::ELASTICSEARCH);
      break;
    }
    case TargetHosts::DEFAULT: {
      addressesRet = getTargetHost(space_);
      break;
    }
  }

  if (!nebula::ok(addressesRet)) {
    LOG(INFO) << "Can't get hosts";
    return nebula::error(addressesRet);
  }

  std::vector<PartitionID> parts;
  auto addresses = nebula::value(addressesRet);

  // write all tasks first.
  std::vector<kvstore::KV> data;
  for (auto i = 0U; i != addresses.size(); ++i) {
    TaskDescription task(space_, jobId_, i, addresses[i].first);
    auto taskKey = MetaKeyUtils::taskKey(task.getSpace(), task.getJobId(), task.getTaskId());
    auto taskVal = MetaKeyUtils::taskVal(task.getHost(),
                                         task.getStatus(),
                                         task.getStartTime(),
                                         task.getStopTime(),
                                         task.getErrorCode());
    data.emplace_back(std::move(taskKey), std::move(taskVal));
  }

  folly::Baton<true, std::atomic> baton;
  auto rc = nebula::cpp2::ErrorCode::SUCCEEDED;
  kvstore_->asyncMultiPut(
      kDefaultSpaceId, kDefaultPartId, std::move(data), [&](nebula::cpp2::ErrorCode code) {
        rc = code;
        baton.post();
      });
  baton.wait();
  if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "write to kv store failed, error: " << apache::thrift::util::enumNameSafe(rc);
    return rc;
  }

  std::vector<folly::SemiFuture<Status>> futures;
  for (auto& address : addresses) {
    // Will convert StorageAddr to AdminAddr in AdminClient
    futures.emplace_back(executeInternal(std::move(address.first), std::move(address.second)));
  }

  auto tries = folly::collectAll(std::move(futures)).get();
  for (auto& t : tries) {
    if (t.hasException()) {
      LOG(INFO) << t.exception().what();
      rc = nebula::cpp2::ErrorCode::E_RPC_FAILURE;
      continue;
    }
    if (!t.value().ok()) {
      LOG(INFO) << t.value().toString();
      rc = nebula::cpp2::ErrorCode::E_RPC_FAILURE;
      continue;
    }
  }
  return rc;
}

}  // namespace meta
}  // namespace nebula
