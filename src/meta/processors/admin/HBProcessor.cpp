/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/admin/HBProcessor.h"

#include "common/time/WallClock.h"
#include "meta/ActiveHostsMan.h"
#include "meta/KVBasedClusterIdMan.h"
#include "meta/MetaVersionMan.h"

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

  LOG(INFO) << "Receive heartbeat from " << host
            << ", role = " << apache::thrift::util::enumNameSafe(req.get_role());
  if (req.get_role() == cpp2::HostRole::STORAGE) {
    ClusterID peerCluserId = req.get_cluster_id();
    if (peerCluserId == 0) {
      LOG(INFO) << "Set clusterId for new host " << host << "!";
      resp_.set_cluster_id(clusterId_);
    } else if (peerCluserId != clusterId_) {
      LOG(ERROR) << "Reject wrong cluster host " << host << "!";
      handleErrorCode(nebula::cpp2::ErrorCode::E_WRONGCLUSTER);
      onFinished();
      return;
    }
  }

  HostInfo info(time::WallClock::fastNowInMilliSec(), req.get_role(), req.get_git_info_sha());
  if (req.version_ref().has_value()) {
    info.version_ = *req.version_ref();
  }
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
