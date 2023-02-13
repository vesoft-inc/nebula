/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/admin/SnapShot.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "common/network/NetworkUtils.h"
#include "common/utils/MetaKeyUtils.h"
#include "meta/ActiveHostsMan.h"
#include "meta/processors/Common.h"

namespace nebula {
namespace meta {
ErrorOr<nebula::cpp2::ErrorCode,
        std::unordered_map<GraphSpaceID, std::vector<cpp2::HostBackupInfo>>>
Snapshot::createSnapshot(const std::string& name) {
  auto hostSpacesRet = getHostSpaces();
  if (!nebula::ok(hostSpacesRet)) {
    auto retcode = nebula::error(hostSpacesRet);
    if (retcode != nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
      retcode = nebula::cpp2::ErrorCode::E_STORE_FAILURE;
    }
    return retcode;
  }
  // This structure is used for the subsequent construction of the
  // common.PartitionBackupInfo
  std::unordered_map<GraphSpaceID, std::vector<cpp2::HostBackupInfo>> info;

  auto hostSpaces = nebula::value(hostSpacesRet);
  for (auto const& [host, spaces] : hostSpaces) {
    auto snapshotRet = client_->createSnapshot(spaces, name, host).get();
    if (!snapshotRet.ok()) {
      LOG(INFO) << "create snapshot failed:" << snapshotRet.status().toString();
      return nebula::cpp2::ErrorCode::E_RPC_FAILURE;
    }
    auto backupInfo = snapshotRet.value();

    // split backup info by space id
    std::unordered_map<GraphSpaceID, cpp2::HostBackupInfo> spaceBackup;
    for (auto& ck : backupInfo.get_checkpoints()) {
      auto it = spaceBackup.find(ck.get_space_id());
      if (it == spaceBackup.cend()) {
        cpp2::HostBackupInfo hostBackup;
        hostBackup.host_ref() = host;
        hostBackup.checkpoints_ref().value().push_back(ck);
        spaceBackup[ck.get_space_id()] = std::move(hostBackup);
      } else {
        it->second.checkpoints_ref().value().push_back(ck);
      }
    }

    // insert to global result
    for (auto& [spaceId, binfo] : spaceBackup) {
      auto it = info.find(spaceId);
      if (it == info.cend()) {
        info[spaceId] = {std::move(binfo)};
      } else {
        it->second.emplace_back(binfo);
      }
    }
  }
  return info;
}

nebula::cpp2::ErrorCode Snapshot::dropSnapshot(const std::string& name,
                                               const std::vector<HostAddr>& hosts) {
  auto hostSpacesRet = getHostSpaces();
  if (!nebula::ok(hostSpacesRet)) {
    auto retcode = nebula::error(hostSpacesRet);
    if (retcode != nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
      retcode = nebula::cpp2::ErrorCode::E_STORE_FAILURE;
    }
    return retcode;
  }

  auto hostSpaces = nebula::value(hostSpacesRet);
  for (const auto& [host, spaces] : hostSpaces) {
    if (std::find(hosts.begin(), hosts.end(), host) == hosts.end()) {
      continue;
    }

    auto result = client_->dropSnapshot(spaces, name, host).get();
    if (!result.ok()) {
      LOG(INFO) << folly::sformat("failed to drop checkpoint: {} on host: {}, error: {}",
                                  name,
                                  host.toString(),
                                  result.status().toString());
    }
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode Snapshot::blockingWrites(storage::cpp2::EngineSignType sign) {
  auto hostSpacesRet = getHostSpaces();
  if (!nebula::ok(hostSpacesRet)) {
    auto retcode = nebula::error(hostSpacesRet);
    if (retcode != nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
      retcode = nebula::cpp2::ErrorCode::E_STORE_FAILURE;
    }
    return retcode;
  }

  auto hostSpaces = nebula::value(hostSpacesRet);
  auto ret = nebula::cpp2::ErrorCode::SUCCEEDED;
  for (const auto& [host, spaces] : hostSpaces) {
    if (sign == storage::cpp2::EngineSignType::BLOCK_ON) {
      LOG(INFO) << "will block write host: " << host;
    } else {
      LOG(INFO) << "will unblock write host: " << host;
    }

    auto result = client_->blockingWrites(spaces, sign, host).get();
    if (!result.ok()) {
      LOG(INFO) << "Send blocking sign error on host " << host
                << ", errorcode: " << result.status().toString();
      ret = nebula::cpp2::ErrorCode::E_BLOCK_WRITE_FAILURE;
      if (sign == storage::cpp2::EngineSignType::BLOCK_ON) {
        break;
      }
    } else {
      if (sign == storage::cpp2::EngineSignType::BLOCK_ON) {
        LOG(INFO) << "finished blocking write host: " << host;
      } else {
        LOG(INFO) << "finished unblocking write host: " << host;
      }
    }
  }
  return ret;
}

ErrorOr<nebula::cpp2::ErrorCode, std::map<HostAddr, std::set<GraphSpaceID>>>
Snapshot::getHostSpaces() {
  const auto& prefix = MetaKeyUtils::partPrefix();
  std::unique_ptr<kvstore::KVIterator> iter;
  auto retCode = kv_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Get hosts meta data failed, error: "
              << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }

  std::map<HostAddr, std::set<GraphSpaceID>> hostSpaces;
  for (; iter->valid(); iter->next()) {
    auto partHosts = MetaKeyUtils::parsePartVal(iter->val());
    auto spaceId = MetaKeyUtils::parsePartKeySpaceId(iter->key());

    bool allSpaces = spaces_.empty();
    if (!allSpaces) {
      auto it = spaces_.find(spaceId);
      if (it == spaces_.end()) {
        continue;
      }
    }

    for (auto& host : partHosts) {
      hostSpaces[host].emplace(spaceId);
    }
  }
  return hostSpaces;
}

}  // namespace meta
}  // namespace nebula
