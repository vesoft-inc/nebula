/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/admin/ListClusterInfoProcessor.h"

DECLARE_int32(heartbeat_interval_secs);
DECLARE_int32(agent_heartbeat_interval_secs);
DECLARE_uint32(expired_time_factor);

namespace nebula {
namespace meta {

void ListClusterInfoProcessor::process(const cpp2::ListClusterInfoReq&) {
  auto* store = dynamic_cast<kvstore::NebulaStore*>(kvstore_);
  if (store == nullptr) {
    onFinished();
    return;
  }

  if (!store->isLeader(kDefaultSpaceId, kDefaultPartId)) {
    handleErrorCode(nebula::cpp2::ErrorCode::E_LEADER_CHANGED);
    onFinished();
    return;
  }

  // services(include agent service) group by ip/hostname
  std::unordered_map<std::string, std::vector<cpp2::ServiceInfo>> hostServices;

  // non-meta services, may include inactive services
  folly::SharedMutex::ReadHolder holder(LockUtils::lock());
  const auto& hostPrefix = MetaKeyUtils::hostPrefix();
  auto iterRet = doPrefix(hostPrefix);
  if (!nebula::ok(iterRet)) {
    LOG(INFO) << "get host prefix failed: "
              << apache::thrift::util::enumNameSafe(nebula::error(iterRet));
    handleErrorCode(nebula::cpp2::ErrorCode::E_LIST_CLUSTER_FAILURE);
    onFinished();
    return;
  }
  auto iter = nebula::value(iterRet).get();
  for (; iter->valid(); iter->next()) {
    auto addr = MetaKeyUtils::parseHostKey(iter->key());
    auto info = HostInfo::decode(iter->val());

    if (addr.host == "") {
      LOG(INFO) << folly::sformat("Bad format host:{}, skip it", addr.toString());
      continue;
    }

    if (!isAlive(info)) {
      LOG(INFO) << folly::sformat("{}:{} is not alive, will skip it",
                                  apache::thrift::util::enumNameSafe(info.role_),
                                  addr.toString());
      continue;
    }

    cpp2::ServiceInfo service;
    service.role_ref() = info.role_;
    service.addr_ref() = addr;

    // fill the dir info
    if (info.role_ == meta::cpp2::HostRole::GRAPH || info.role_ == meta::cpp2::HostRole::STORAGE) {
      auto dirKey = MetaKeyUtils::hostDirKey(addr.host, addr.port);
      auto dirRet = doGet(dirKey);
      if (!nebula::ok(dirRet)) {
        LOG(INFO) << folly::sformat("Get host {} dir info for {} failed: {}",
                                    addr.toString(),
                                    apache::thrift::util::enumNameSafe(info.role_),
                                    apache::thrift::util::enumNameSafe(nebula::error(dirRet)));
        handleErrorCode(nebula::error(dirRet));
        onFinished();
        return;
      }
      auto dir = MetaKeyUtils::parseHostDir(std::move(nebula::value(dirRet)));
      service.dir_ref() = std::move(dir);
    }

    if (hostServices.find(addr.host) == hostServices.end()) {
      hostServices[addr.host] = std::vector<cpp2::ServiceInfo>();
    }
    hostServices[addr.host].emplace_back(std::move(service));
  }

  // meta service
  auto partRet = kvstore_->part(kDefaultSpaceId, kDefaultPartId);
  if (!nebula::ok(partRet)) {
    auto code = nebula::error(partRet);
    LOG(INFO) << "get meta part store failed, error: " << apache::thrift::util::enumNameSafe(code);
    handleErrorCode(nebula::cpp2::ErrorCode::E_LIST_CLUSTER_FAILURE);
    onFinished();
    return;
  }
  auto raftPeers = nebula::value(partRet)->peers();
  for (auto& raftAddr : raftPeers) {
    auto metaAddr = Utils::getStoreAddrFromRaftAddr(raftAddr);
    cpp2::ServiceInfo service;
    service.role_ref() = cpp2::HostRole::META;
    service.addr_ref() = metaAddr;

    if (hostServices.find(metaAddr.host) == hostServices.end()) {
      hostServices[metaAddr.host] = std::vector<cpp2::ServiceInfo>();
    }
    hostServices[metaAddr.host].push_back(service);
  }

  // Check there is at least one agent in each host. If there are many ones, we will pick the first.
  std::unordered_map<std::string, std::vector<cpp2::ServiceInfo>> agentServices;
  for (auto& [host, services] : hostServices) {
    int agentCount = 0;
    for (auto it = services.begin(); it != services.end();) {
      if (it->get_role() == cpp2::HostRole::AGENT) {
        agentCount++;

        if (agentCount > 1) {
          LOG(INFO) << folly::sformat(
              "Will erase redundant agent {} from host {}", it->get_addr().toString(), host);
          it = services.erase(it);
          continue;
        }
      }

      it++;
    }

    if (agentCount < 1) {
      LOG(INFO) << folly::sformat("There is no agent in host {}", host);
      handleErrorCode(nebula::cpp2::ErrorCode::E_LIST_CLUSTER_NO_AGENT_FAILURE);
      onFinished();
      return;
    }

    if (services.size() <= 1) {
      LOG(INFO) << "There is no other service than agent in host: " << host;
      continue;
    }
  }

  resp_.host_services_ref() = hostServices;
  resp_.code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
  onFinished();
}

bool ListClusterInfoProcessor::isAlive(const HostInfo& info) {
  int64_t expiredTime =
      FLAGS_heartbeat_interval_secs * FLAGS_expired_time_factor;  // meta/storage/graph
  if (info.role_ == cpp2::HostRole::AGENT) {                      // agent
    expiredTime = FLAGS_agent_heartbeat_interval_secs * FLAGS_expired_time_factor;
  }
  int64_t threshold = expiredTime * 1000;
  auto now = time::WallClock::fastNowInMilliSec();

  return now - info.lastHBTimeInMilliSec_ < threshold;
}

}  // namespace meta
}  // namespace nebula
