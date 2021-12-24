/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/admin/HBProcessor.h"

#include "common/time/WallClock.h"
#include "meta/ActiveHostsMan.h"
#include "meta/KVBasedClusterIdMan.h"
#include "meta/MetaVersionMan.h"

DEFINE_bool(hosts_whitelist_enabled, true, "Automatically receive the heartbeat report");

namespace nebula {
namespace meta {

HBCounters kHBCounters;

std::atomic<int64_t> HBProcessor::metaVersion_ = -1;

void HBProcessor::onFinished() {
  if (counters_) {
    stats::StatsManager::addValue(counters_->numCalls_);
    stats::StatsManager::addValue(counters_->latency_, this->duration_.elapsedInUSec());
  }

  Base::onFinished();
}

void HBProcessor::process(const cpp2::HBReq& req) {
  HostAddr host((*req.host_ref()).host, (*req.host_ref()).port);
  nebula::cpp2::ErrorCode ret;
  auto role = req.get_role();
  LOG(INFO) << "Receive heartbeat from " << host
            << ", role = " << apache::thrift::util::enumNameSafe(role);

  if (role == cpp2::HostRole::STORAGE) {
    if (!FLAGS_hosts_whitelist_enabled && !ActiveHostsMan::machineRegisted(kvstore_, host)) {
      LOG(ERROR) << "Machine " << host << " is not registed";
      handleErrorCode(nebula::cpp2::ErrorCode::E_MACHINE_NOT_FOUND);
      onFinished();
      return;
    }

    ClusterID peerClusterId = req.get_cluster_id();
    if (peerClusterId == 0) {
      LOG(INFO) << "Set clusterId for new host " << host << "!";
      resp_.set_cluster_id(clusterId_);
    } else if (peerClusterId != clusterId_) {
      LOG(ERROR) << "Reject wrong cluster host " << host << "!";
      handleErrorCode(nebula::cpp2::ErrorCode::E_WRONGCLUSTER);
      onFinished();
      return;
    }

    if (req.disk_parts_ref().has_value()) {
      for (const auto& [spaceId, partDiskMap] : *req.get_disk_parts()) {
        for (const auto& [path, partList] : partDiskMap) {
          auto partListVal = MetaKeyUtils::diskPartsVal(partList);
          std::string key = MetaKeyUtils::diskPartsKey(host, spaceId, path);
          std::vector<kvstore::KV> data;
          data.emplace_back(key, partListVal);
          // doPut() not work, will trigger the asan: use heap memory which is free
          folly::Baton<true, std::atomic> baton;
          kvstore_->asyncMultiPut(kDefaultSpaceId,
                                  kDefaultPartId,
                                  std::move(data),
                                  [this, &baton](nebula::cpp2::ErrorCode code) {
                                    this->handleErrorCode(code);
                                    baton.post();
                                  });
          baton.wait();
        }
      }
    }
  }

  HostInfo info(time::WallClock::fastNowInMilliSec(), role, req.get_git_info_sha());
  if (req.leader_partIds_ref().has_value()) {
    ret = ActiveHostsMan::updateHostInfo(kvstore_, host, info, &*req.leader_partIds_ref());
  } else {
    ret = ActiveHostsMan::updateHostInfo(kvstore_, host, info);
  }
  if (ret == nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
    auto leaderRet = kvstore_->partLeader(kDefaultSpaceId, kDefaultPartId);
    if (nebula::ok(leaderRet)) {
      resp_.set_leader(toThriftHost(nebula::value(leaderRet)));
    }
  }

  auto lastUpdateTimeRet = LastUpdateTimeMan::get(kvstore_);
  if (nebula::ok(lastUpdateTimeRet)) {
    resp_.set_last_update_time_in_ms(nebula::value(lastUpdateTimeRet));
  } else if (nebula::error(lastUpdateTimeRet) == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
    resp_.set_last_update_time_in_ms(0);
  }

  auto version = metaVersion_.load();
  if (version == -1) {
    metaVersion_.store(static_cast<int64_t>(MetaVersionMan::getMetaVersionFromKV(kvstore_)));
  }

  resp_.set_meta_version(metaVersion_.load());

  handleErrorCode(ret);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
