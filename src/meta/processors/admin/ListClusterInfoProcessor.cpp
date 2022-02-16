/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/admin/ListClusterInfoProcessor.h"

#include <folly/Format.h>                   // for sformat
#include <thrift/lib/cpp/util/EnumUtils.h>  // for enumNameSafe
#include <thrift/lib/cpp2/FieldRef.h>       // for field_ref

#include <algorithm>      // for max
#include <memory>         // for __shared_ptr_access
#include <ostream>        // for operator<<, basic_ost...
#include <string>         // for string, basic_string
#include <type_traits>    // for add_const<>::type
#include <unordered_map>  // for unordered_map, operat...
#include <vector>         // for vector

#include "common/base/Base.h"                 // for UNUSED
#include "common/base/ErrorOr.h"              // for error, ok, value
#include "common/base/Logging.h"              // for LOG, LogMessage, _LOG...
#include "common/datatypes/HostAddr.h"        // for HostAddr
#include "common/utils/MetaKeyUtils.h"        // for MetaKeyUtils, kDefaul...
#include "common/utils/Utils.h"               // for Utils
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode, ErrorCode:...
#include "kvstore/KVIterator.h"               // for KVIterator
#include "kvstore/KVStore.h"                  // for KVStore
#include "kvstore/NebulaStore.h"              // for NebulaStore
#include "kvstore/Part.h"                     // for Part
#include "meta/ActiveHostsMan.h"              // for HostInfo
#include "meta/processors/BaseProcessor.h"    // for BaseProcessor::doGet

namespace nebula {
namespace meta {

void ListClusterInfoProcessor::process(const cpp2::ListClusterInfoReq& req) {
  UNUSED(req);
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
  const auto& hostPrefix = MetaKeyUtils::hostPrefix();
  auto iterRet = doPrefix(hostPrefix);
  if (!nebula::ok(iterRet)) {
    LOG(INFO) << "get host prefix failed:"
              << apache::thrift::util::enumNameSafe(nebula::error(iterRet));
    handleErrorCode(nebula::cpp2::ErrorCode::E_LIST_CLUSTER_FAILURE);
    onFinished();
    return;
  }
  auto iter = nebula::value(iterRet).get();
  for (; iter->valid(); iter->next()) {
    auto addr = MetaKeyUtils::parseHostKey(iter->key());
    auto info = HostInfo::decode(iter->val());

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

  // check: there should be only one agent in each host
  for (const auto& [host, services] : hostServices) {
    int agentCount = 0;
    for (auto& s : services) {
      if (s.get_role() == cpp2::HostRole::AGENT) {
        agentCount++;
      }
    }
    if (agentCount < 1) {
      LOG(INFO) << folly::sformat("There are {} agent count is host {}", agentCount, host);
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
}  // namespace meta
}  // namespace nebula
