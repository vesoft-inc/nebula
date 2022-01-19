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
  auto role = req.get_role();
  LOG(INFO) << "Receive heartbeat from " << host
            << ", role = " << apache::thrift::util::enumNameSafe(role);

  if (role == cpp2::HostRole::STORAGE) {
    if (!ActiveHostsMan::machineRegisted(kvstore_, host)) {
      LOG(INFO) << "Machine " << host << " is not registed";
      handleErrorCode(nebula::cpp2::ErrorCode::E_MACHINE_NOT_FOUND);
      onFinished();
      return;
    }

    // set or check storaged's cluster id
    ClusterID peerClusterId = req.get_cluster_id();
    if (peerClusterId == 0) {
      LOG(INFO) << "Set clusterId for new host " << host << "!";
      resp_.cluster_id_ref() = clusterId_;
    } else if (peerClusterId != clusterId_) {
      LOG(INFO) << "Reject wrong cluster host " << host << "!";
      handleErrorCode(nebula::cpp2::ErrorCode::E_WRONGCLUSTER);
      onFinished();
      return;
    }

    // set disk parts map
    if (req.disk_parts_ref().has_value()) {
      for (const auto& [spaceId, partDiskMap] : *req.get_disk_parts()) {
        for (const auto& [path, partList] : partDiskMap) {
          auto partListVal = MetaKeyUtils::diskPartsVal(partList);
          auto key = MetaKeyUtils::diskPartsKey(host, spaceId, path);
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

  // update host info
  HostInfo info(time::WallClock::fastNowInMilliSec(), role, req.get_git_info_sha());
  if (req.leader_partIds_ref().has_value()) {
    ret = ActiveHostsMan::updateHostInfo(kvstore_, host, info, &*req.leader_partIds_ref());
  } else {
    ret = ActiveHostsMan::updateHostInfo(kvstore_, host, info);
  }
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    handleErrorCode(ret);
    onFinished();
    return;
  }

  // update host dir info
  if (req.get_role() == cpp2::HostRole::STORAGE || req.get_role() == cpp2::HostRole::GRAPH) {
    if (req.dir_ref().has_value()) {
      std::vector<kvstore::KV> data;
      LOG(INFO) << folly::sformat("Update host {} dir info, root path: {}, data path size: {}",
                                  host.toString(),
                                  req.get_dir()->get_root(),
                                  req.get_dir()->get_data().size());
      data.emplace_back(std::make_pair(MetaKeyUtils::hostDirKey(host.host, host.port),
                                       MetaKeyUtils::hostDirVal(*req.get_dir())));
      ret = doSyncPut(data);
    }
  }

  // set update time and meta version
  auto lastUpdateTimeRet = LastUpdateTimeMan::get(kvstore_);
  if (nebula::ok(lastUpdateTimeRet)) {
    resp_.last_update_time_in_ms_ref() = nebula::value(lastUpdateTimeRet);
  } else if (nebula::error(lastUpdateTimeRet) == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
    resp_.last_update_time_in_ms_ref() = 0;
  }

  auto version = metaVersion_.load();
  if (version == -1) {
    metaVersion_.store(static_cast<int64_t>(MetaVersionMan::getMetaVersionFromKV(kvstore_)));
  }

  resp_.meta_version_ref() = metaVersion_.load();

  handleErrorCode(ret);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
