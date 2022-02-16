/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/parts/ListPartsProcessor.h"

#include <folly/Range.h>                    // for Range
#include <folly/SharedMutex.h>              // for SharedMutex
#include <gflags/gflags_declare.h>          // for DECLARE_int32
#include <string.h>                         // for memcpy
#include <thrift/lib/cpp/util/EnumUtils.h>  // for enumNameSafe
#include <thrift/lib/cpp2/FieldRef.h>       // for required_field_ref

#include <algorithm>  // for find, max
#include <memory>     // for allocator_traits<>::v...
#include <new>        // for operator new
#include <ostream>    // for operator<<, basic_ost...
#include <string>     // for string, basic_string
#include <tuple>      // for tie, tuple, ignore

#include "common/base/Logging.h"            // for LOG, LogMessage, _LOG...
#include "common/base/Status.h"             // for Status
#include "common/datatypes/HostAddr.h"      // for HostAddr, operator<<
#include "common/utils/MetaKeyUtils.h"      // for MetaKeyUtils, kDefaul...
#include "kvstore/KVIterator.h"             // for KVIterator
#include "kvstore/KVStore.h"                // for KVStore
#include "meta/ActiveHostsMan.h"            // for ActiveHostsMan
#include "meta/processors/BaseProcessor.h"  // for BaseProcessor::doGet
#include "meta/processors/Common.h"         // for LockUtils

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace meta {

void ListPartsProcessor::process(const cpp2::ListPartsReq& req) {
  spaceId_ = req.get_space_id();
  partIds_ = req.get_part_ids();
  std::unordered_map<PartitionID, std::vector<HostAddr>> partHostsMap;

  if (!partIds_.empty()) {
    // Only show the specified parts
    showAllParts_ = false;
    folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
    for (const auto& partId : partIds_) {
      auto partKey = MetaKeyUtils::partKey(spaceId_, partId);
      auto ret = doGet(std::move(partKey));
      if (!nebula::ok(ret)) {
        auto retCode = nebula::error(ret);
        LOG(ERROR) << "Get part failed, error " << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
      }
      auto hosts = std::move(nebula::value(ret));
      partHostsMap[partId] = MetaKeyUtils::parsePartVal(hosts);
    }
  } else {
    // Show all parts
    folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
    auto ret = getAllParts();
    if (!nebula::ok(ret)) {
      handleErrorCode(nebula::error(ret));
      onFinished();
      return;
    }
    partHostsMap = std::move(nebula::value(ret));
  }

  std::vector<cpp2::PartItem> partItems;
  auto activeHostsRet = ActiveHostsMan::getActiveHosts(kvstore_);
  if (!nebula::ok(activeHostsRet)) {
    handleErrorCode(nebula::error(activeHostsRet));
    onFinished();
    return;
  }
  auto activeHosts = std::move(nebula::value(activeHostsRet));

  for (auto& partEntry : partHostsMap) {
    cpp2::PartItem partItem;
    partItem.part_id_ref() = partEntry.first;
    partItem.peers_ref() = std::move(partEntry.second);
    std::vector<HostAddr> losts;
    for (auto& host : partItem.get_peers()) {
      if (std::find(activeHosts.begin(), activeHosts.end(), HostAddr(host.host, host.port)) ==
          activeHosts.end()) {
        losts.emplace_back(host.host, host.port);
      }
    }
    partItem.losts_ref() = std::move(losts);
    partItems.emplace_back(std::move(partItem));
  }
  if (partItems.size() != partHostsMap.size()) {
    LOG(ERROR) << "Maybe lost some partitions!";
  }
  auto retCode = getLeaderDist(partItems);
  if (retCode == nebula::cpp2::ErrorCode::SUCCEEDED) {
    resp_.parts_ref() = std::move(partItems);
  }
  handleErrorCode(retCode);
  onFinished();
}

ErrorOr<nebula::cpp2::ErrorCode, std::unordered_map<PartitionID, std::vector<HostAddr>>>
ListPartsProcessor::getAllParts() {
  std::unordered_map<PartitionID, std::vector<HostAddr>> partHostsMap;

  folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
  const auto& prefix = MetaKeyUtils::partPrefix(spaceId_);
  auto ret = doPrefix(prefix);
  if (!nebula::ok(ret)) {
    auto retCode = nebula::error(ret);
    LOG(ERROR) << "List Parts Failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }

  auto iter = nebula::value(ret).get();
  while (iter->valid()) {
    auto key = iter->key();
    PartitionID partId;
    memcpy(&partId, key.data() + prefix.size(), sizeof(PartitionID));
    std::vector<HostAddr> partHosts = MetaKeyUtils::parsePartVal(iter->val());
    partHostsMap.emplace(partId, std::move(partHosts));
    iter->next();
  }

  return partHostsMap;
}

nebula::cpp2::ErrorCode ListPartsProcessor::getLeaderDist(std::vector<cpp2::PartItem>& partItems) {
  auto activeHostsRet = ActiveHostsMan::getActiveHosts(kvstore_);
  if (!nebula::ok(activeHostsRet)) {
    return nebula::error(activeHostsRet);
  }

  auto activeHosts = std::move(nebula::value(activeHostsRet));

  std::vector<std::string> leaderKeys;
  for (auto& partItem : partItems) {
    auto key = MetaKeyUtils::leaderKey(spaceId_, partItem.get_part_id());
    leaderKeys.emplace_back(std::move(key));
  }

  nebula::cpp2::ErrorCode rc;
  std::vector<Status> statuses;
  std::vector<std::string> values;
  std::tie(rc, statuses) =
      kvstore_->multiGet(kDefaultSpaceId, kDefaultPartId, std::move(leaderKeys), &values);
  if (rc != nebula::cpp2::ErrorCode::SUCCEEDED && rc != nebula::cpp2::ErrorCode::E_PARTIAL_RESULT) {
    return rc;
  }

  HostAddr host;
  nebula::cpp2::ErrorCode code;
  for (auto i = 0U; i != statuses.size(); ++i) {
    if (!statuses[i].ok()) {
      continue;
    }
    std::tie(host, std::ignore, code) = MetaKeyUtils::parseLeaderValV3(values[i]);
    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
      continue;
    }
    if (std::find(activeHosts.begin(), activeHosts.end(), host) == activeHosts.end()) {
      LOG(INFO) << "ignore inactive host: " << host;
      continue;
    }
    partItems[i].leader_ref() = host;
  }

  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula
