/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <thrift/lib/cpp/util/EnumUtils.h>
#include "meta/processors/admin/SnapShot.h"
#include "meta/common/MetaCommon.h"
#include "meta/processors/Common.h"
#include "meta/ActiveHostsMan.h"
#include "meta/MetaServiceUtils.h"
#include "common/network/NetworkUtils.h"

namespace nebula {
namespace meta {
ErrorOr<nebula::cpp2::ErrorCode, std::unordered_map<GraphSpaceID, std::vector<cpp2::BackupInfo>>>
Snapshot::createSnapshot(const std::string& name) {
    auto retSpacesHostsRet = getSpacesHosts();
    if (!nebula::ok(retSpacesHostsRet)) {
        auto retcode = nebula::error(retSpacesHostsRet);
        if (retcode != nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
            retcode = nebula::cpp2::ErrorCode::E_STORE_FAILURE;
        }
        return retcode;
    }
    // This structure is used for the subsequent construction of the common.PartitionBackupInfo
    std::unordered_map<GraphSpaceID, std::vector<cpp2::BackupInfo>> info;

    auto spacesHosts = nebula::value(retSpacesHostsRet);
    for (const auto& spaceHosts : spacesHosts) {
        for (const auto& host : spaceHosts.second) {
            auto status = client_->createSnapshot(spaceHosts.first, name, host).get();
            if (!status.ok()) {
                return nebula::cpp2::ErrorCode::E_RPC_FAILURE;
            }
            auto backupInfo = status.value();
            auto it = info.find(spaceHosts.first);
            if (it != info.cend()) {
                it->second.emplace_back(backupInfo);
            } else {
                info[spaceHosts.first] = {std::move(backupInfo)};
            }
        }
    }
    return info;
}

nebula::cpp2::ErrorCode
Snapshot::dropSnapshot(const std::string& name,
                       const std::vector<HostAddr>& hosts) {
    auto retSpacesHostsRet = getSpacesHosts();
    if (!nebula::ok(retSpacesHostsRet)) {
        auto retcode = nebula::error(retSpacesHostsRet);
        if (retcode != nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
            retcode = nebula::cpp2::ErrorCode::E_STORE_FAILURE;
        }
        return retcode;
    }

    auto spacesHosts = nebula::value(retSpacesHostsRet);
    for (const auto& spaceHosts : spacesHosts) {
        for (const auto& host : spaceHosts.second) {
            if (std::find(hosts.begin(), hosts.end(), host) != hosts.end()) {
                auto status = client_->dropSnapshot(spaceHosts.first, name, host).get();
                if (!status.ok()) {
                    auto msg = "failed drop checkpoint : \"%s\". on host %s. error %s";
                    auto error = folly::stringPrintf(msg,
                                                     name.c_str(),
                                                     host.toString().c_str(),
                                                     status.toString().c_str());
                    LOG(ERROR) << error;
                }
            }
        }
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode
Snapshot::blockingWrites(storage::cpp2::EngineSignType sign) {
    auto retSpacesHostsRet = getSpacesHosts();
    if (!nebula::ok(retSpacesHostsRet)) {
        auto retcode = nebula::error(retSpacesHostsRet);
        if (retcode != nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
            retcode = nebula::cpp2::ErrorCode::E_STORE_FAILURE;
        }
        return retcode;
    }

    auto spacesHosts = nebula::value(retSpacesHostsRet);
    auto ret = nebula::cpp2::ErrorCode::SUCCEEDED;
    for (const auto& spaceHosts : spacesHosts) {
        for (const auto& host : spaceHosts.second) {
            LOG(INFO) << "will block write host: " << host;
            auto status = client_->blockingWrites(spaceHosts.first, sign, host).get();
            if (!status.ok()) {
                LOG(ERROR) << "Send blocking sign error on host : " << host;
                ret = nebula::cpp2::ErrorCode::E_BLOCK_WRITE_FAILURE;
                if (sign == storage::cpp2::EngineSignType::BLOCK_ON) {
                    break;
                }
            }
        }
    }
    return ret;
}

ErrorOr<nebula::cpp2::ErrorCode, std::map<GraphSpaceID, std::set<HostAddr>>>
Snapshot::getSpacesHosts() {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
    const auto& prefix = MetaServiceUtils::partPrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto retCode = kv_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Get hosts meta data failed, error: "
                   << apache::thrift::util::enumNameSafe(retCode);
        return retCode;
    }

    std::map<GraphSpaceID, std::set<HostAddr>> hostsByspaces;

    for (; iter->valid(); iter->next()) {
        auto partHosts = MetaServiceUtils::parsePartVal(iter->val());
        auto space = MetaServiceUtils::parsePartKeySpaceId(iter->key());
        if (!spaces_.empty()) {
            auto it = spaces_.find(space);
            if (it == spaces_.end()) {
                continue;
            }
        }

        for (auto& ph : partHosts) {
            hostsByspaces[space].emplace(std::move(ph));
        }
    }
    return hostsByspaces;
}

}  // namespace meta
}  // namespace nebula

