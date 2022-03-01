/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/admin/AgentHBProcessor.h"

#include "common/time/WallClock.h"
#include "meta/ActiveHostsMan.h"
#include "meta/KVBasedClusterIdMan.h"
#include "meta/MetaVersionMan.h"

namespace nebula {
namespace meta {

AgentHBCounters kAgentHBCounters;

void AgentHBProcessor::onFinished() {
  if (counters_) {
    stats::StatsManager::addValue(counters_->numCalls_);
    stats::StatsManager::addValue(counters_->latency_, this->duration_.elapsedInUSec());
  }

  Base::onFinished();
}

void AgentHBProcessor::process(const cpp2::AgentHBReq& req) {
  HostAddr agentAddr((*req.host_ref()).host, (*req.host_ref()).port);
  LOG(INFO) << "Receive heartbeat from " << agentAddr << ", role = AGENT";

  std::vector<kvstore::KV> data;
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  nebula::cpp2::ErrorCode ret = nebula::cpp2::ErrorCode::SUCCEEDED;
  do {
    // update agent host info
    HostInfo info(
        time::WallClock::fastNowInMilliSec(), cpp2::HostRole::AGENT, req.get_git_info_sha());
    ret = ActiveHostsMan::updateHostInfo(kvstore_, agentAddr, info, data);
    ret = doSyncPut(std::move(data));
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << folly::sformat("Put agent {} info failed: {}",
                                  agentAddr.toString(),
                                  apache::thrift::util::enumNameSafe(ret));
      break;
    }

    // get services in the agent host
    auto servicesRet = ActiveHostsMan::getServicesInHost(kvstore_, agentAddr.host);
    if (!nebula::ok(servicesRet)) {
      ret = nebula::error(servicesRet);
      LOG(INFO) << folly::sformat("Get active services for {} failed: {}",
                                  agentAddr.host,
                                  apache::thrift::util::enumNameSafe(ret));
      break;
    }

    // get dir info for the services in agent host
    std::unordered_map<HostAddr, nebula::cpp2::DirInfo> serviceDirinfo;
    std::string hostDirHostPrefix = MetaKeyUtils::hostDirHostPrefix(agentAddr.host);
    auto dirIterRet = doPrefix(hostDirHostPrefix);
    if (!nebula::ok(dirIterRet)) {
      ret = nebula::error(dirIterRet);
      LOG(INFO) << folly::sformat("Get host {} dir prefix iterator failed: {}",
                                  agentAddr.host,
                                  apache::thrift::util::enumNameSafe(ret));
      break;
    }
    auto dirIter = std::move(nebula::value(dirIterRet));
    for (; dirIter->valid(); dirIter->next()) {
      HostAddr addr = MetaKeyUtils::parseHostDirKey(dirIter->key());
      nebula::cpp2::DirInfo dir = MetaKeyUtils::parseHostDir(dirIter->val());
      serviceDirinfo[addr] = dir;
    }

    // join the service host info and dir info
    auto services = std::move(nebula::value(servicesRet));
    std::vector<cpp2::ServiceInfo> serviceList;
    for (const auto& [addr, role] : services) {
      if (addr == agentAddr) {
        // skip iteself
        continue;
      }

      if (role == cpp2::HostRole::AGENT) {
        LOG(INFO) << folly::sformat("there is another agent: {} in the host", addr.toString());
        continue;
      }

      auto it = serviceDirinfo.find(addr);
      if (it == serviceDirinfo.end()) {
        LOG(INFO) << folly::sformat("{} dir info not found", addr.toString());
        break;
      }

      cpp2::ServiceInfo serviceInfo;
      serviceInfo.addr_ref() = addr;
      serviceInfo.dir_ref() = it->second;
      serviceInfo.role_ref() = role;
      serviceList.emplace_back(serviceInfo);
    }
    if (serviceList.size() != services.size() - 1) {
      ret = nebula::cpp2::ErrorCode::E_AGENT_HB_FAILUE;
      // missing some services' dir info
      LOG(INFO) << folly::sformat(
          "Missing some services's dir info, excepted service {}, but only got {}",
          services.size() - 1,
          serviceList.size());
      break;
    }

    // add meta service if have, agent should get meta dir info in separate rpc
    // because follower metad can't report it's dir info to the leader metad
    auto partRet = kvstore_->part(kDefaultSpaceId, kDefaultPartId);
    if (!nebula::ok(partRet)) {
      ret = nebula::error(partRet);
      LOG(INFO) << "Get meta part store failed, error: " << apache::thrift::util::enumNameSafe(ret);
      return;
    }
    auto raftPeers = nebula::value(partRet)->peers();
    for (auto& raftAddr : raftPeers) {
      auto metaAddr = Utils::getStoreAddrFromRaftAddr(raftAddr);
      if (metaAddr.host == agentAddr.host) {
        cpp2::ServiceInfo serviceInfo;
        serviceInfo.addr_ref() = metaAddr;
        serviceInfo.role_ref() = cpp2::HostRole::META;
        serviceList.emplace_back(serviceInfo);
      }
    }

    resp_.service_list_ref() = serviceList;
  } while (false);

  handleErrorCode(ret);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
