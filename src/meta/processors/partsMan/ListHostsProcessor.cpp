/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/partsMan/ListHostsProcessor.h"
#include "meta/ActiveHostsMan.h"
#include "meta/processors/admin/AdminClient.h"
#include "common/version/Version.h"

DECLARE_int32(heartbeat_interval_secs);
DECLARE_uint32(expired_time_factor);
DEFINE_int32(removed_threshold_sec, 24 * 60 * 60,
             "Hosts will be removed in this time if no heartbeat received");

namespace nebula {
namespace meta {

static cpp2::HostRole toHostRole(cpp2::ListHostType type) {
    switch (type) {
        case cpp2::ListHostType::GRAPH:
            return cpp2::HostRole::GRAPH;
        case cpp2::ListHostType::META:
            return cpp2::HostRole::META;
        case cpp2::ListHostType::STORAGE:
            return cpp2::HostRole::STORAGE;
        default:
            return cpp2::HostRole::UNKNOWN;
    }
}

void ListHostsProcessor::process(const cpp2::ListHostsReq& req) {
    nebula::cpp2::ErrorCode retCode;
    {
        folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
        retCode = getSpaceIdNameMap();
        if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
            handleErrorCode(retCode);
            onFinished();
            return;
        }

        meta::cpp2::ListHostType type = req.get_type();
        if (type == cpp2::ListHostType::ALLOC) {
            retCode = fillLeaders();
            if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
                handleErrorCode(retCode);
                onFinished();
                return;
            }

            retCode = fillAllParts();
        } else {
            auto hostRole = toHostRole(type);
            retCode = allHostsWithStatus(hostRole);
        }
    }
    if (retCode == nebula::cpp2::ErrorCode::SUCCEEDED) {
        resp_.set_hosts(std::move(hostItems_));
    }
    handleErrorCode(retCode);
    onFinished();
}

/*
 * now(2020-04-29), assume all metad have same gitInfoSHA
 * this will change if some day
 * meta.thrift support interface like getHostStatus()
 * which return a bunch of host infomation
 * it's not necessary add this interface only for gitInfoSHA
 * */
nebula::cpp2::ErrorCode ListHostsProcessor::allMetaHostsStatus() {
    auto errOrPart = kvstore_->part(kDefaultSpaceId, kDefaultPartId);
    if (!nebula::ok(errOrPart)) {
        auto retCode = nebula::error(errOrPart);
        LOG(ERROR) << "List Hosts Failed, error: " << apache::thrift::util::enumNameSafe(retCode);
        return retCode;
    }
    auto metaPeers = nebula::value(errOrPart)->peers();
    // transform raft port to servre port
    for (auto& metaHost : metaPeers) {
        metaHost = Utils::getStoreAddrFromRaftAddr(metaHost);
    }
    for (auto& host : metaPeers) {
        cpp2::HostItem item;
        item.set_hostAddr(std::move(host));
        item.set_role(cpp2::HostRole::META);
        item.set_git_info_sha(gitInfoSha());
        item.set_status(cpp2::HostStatus::ONLINE);
        item.set_version(simpleVersionString());
        hostItems_.emplace_back(item);
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode ListHostsProcessor::allHostsWithStatus(cpp2::HostRole role) {
    if (role == cpp2::HostRole::META) {
        return allMetaHostsStatus();
    }
    const auto& hostPrefix = MetaServiceUtils::hostPrefix();
    auto ret = doPrefix(hostPrefix);
    if (!nebula::ok(ret)) {
        auto retCode = nebula::error(ret);
        if (retCode != nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
            retCode = nebula::cpp2::ErrorCode::E_NO_HOSTS;
        }
        LOG(ERROR) << "List Hosts Failed, error: "
                   << apache::thrift::util::enumNameSafe(retCode);
        return retCode;
    }

    auto iter = nebula::value(ret).get();
    auto now = time::WallClock::fastNowInMilliSec();
    std::vector<std::string> removeHostsKey;
    while (iter->valid()) {
        HostInfo info = HostInfo::decode(iter->val());
        if (info.role_ != role) {
            iter->next();
            continue;
        }

        cpp2::HostItem item;
        auto host = MetaServiceUtils::parseHostKey(iter->key());
        item.set_hostAddr(std::move(host));

        item.set_role(info.role_);
        item.set_git_info_sha(info.gitInfoSha_);
        if (info.version_.has_value()) {
            item.set_version(info.version_.value());
        }
        if (now - info.lastHBTimeInMilliSec_ < FLAGS_removed_threshold_sec * 1000) {
            // If meta didn't receive heartbeat with 2 periods, regard hosts as offline.
            // Same as ActiveHostsMan::getActiveHosts
            if (now - info.lastHBTimeInMilliSec_ <
                FLAGS_heartbeat_interval_secs * FLAGS_expired_time_factor * 1000) {
                item.set_status(cpp2::HostStatus::ONLINE);
            } else {
                item.set_status(cpp2::HostStatus::OFFLINE);
            }
            hostItems_.emplace_back(item);
        } else {
            removeHostsKey.emplace_back(iter->key());
        }
        iter->next();
    }

    removeExpiredHosts(std::move(removeHostsKey));
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode ListHostsProcessor::fillLeaders() {
    auto retCode = allHostsWithStatus(cpp2::HostRole::STORAGE);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Get all host's status failed";
        return retCode;
    }

    // get hosts which have send heartbeat recently
    auto activeHostsRet = ActiveHostsMan::getActiveHosts(kvstore_);
    if (!nebula::ok(activeHostsRet)) {
        return nebula::error(activeHostsRet);
    }
    auto activeHosts = nebula::value(activeHostsRet);

    const auto& prefix = MetaServiceUtils::leaderPrefix();
    auto iterRet = doPrefix(prefix);
    if (!nebula::ok(iterRet)) {
        retCode = nebula::error(iterRet);
        if (retCode != nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
            retCode = nebula::cpp2::ErrorCode::E_NO_HOSTS;
        }
        LOG(ERROR) << "List leader Hosts Failed, error: "
                   << apache::thrift::util::enumNameSafe(retCode);
        return retCode;
    }

    auto iter = nebula::value(iterRet).get();

    // get hosts which have send heartbeat recently
    HostAddr host;
    TermID term;
    nebula::cpp2::ErrorCode code;
    std::vector<std::string> removeLeadersKey;
    for (; iter->valid(); iter->next()) {
        auto spaceIdAndPartId = MetaServiceUtils::parseLeaderKeyV3(iter->key());
        VLOG(1) << "show hosts: space = " << spaceIdAndPartId.first
                << ", part = " << spaceIdAndPartId.second;
        // If the space in the leader key don't exist, remove leader key
        auto spaceId = spaceIdAndPartId.first;
        auto spaceIter = spaceIdNameMap_.find(spaceId);
        if (spaceIter == spaceIdNameMap_.end()) {
            removeLeadersKey.emplace_back(iter->key());
            continue;
        }

        std::tie(host, term, code) = MetaServiceUtils::parseLeaderValV3(iter->val());
        if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
            continue;
        }
        auto it = std::find(activeHosts.begin(), activeHosts.end(), host);
        if (it == activeHosts.end()) {
            VLOG(1) << "skip inactive host: " << host;
            continue;  // skip inactive host
        }

        auto hostIt = std::find_if(hostItems_.begin(), hostItems_.end(), [&](const auto& hit) {
            return hit.get_hostAddr() == host;
        });

        if (hostIt == hostItems_.end()) {
            VLOG(1) << "skip inactive host";
            continue;
        }

        hostIt->leader_parts_ref()[spaceIter->second].emplace_back(spaceIdAndPartId.second);
    }

    removeInvalidLeaders(std::move(removeLeadersKey));
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode ListHostsProcessor::fillAllParts() {
    std::unique_ptr<kvstore::KVIterator> iter;
    using SpaceNameAndPartitions = std::unordered_map<std::string, std::vector<PartitionID>>;
    std::unordered_map<HostAddr, SpaceNameAndPartitions> allParts;
    for (const auto& spaceId : spaceIds_) {
        // get space name by space id
        const auto& spaceName = spaceIdNameMap_[spaceId];

        const auto& partPrefix = MetaServiceUtils::partPrefix(spaceId);
        auto iterPartRet = doPrefix(partPrefix);
        if (!nebula::ok(iterPartRet)) {
            auto retCode = nebula::error(iterPartRet);
            LOG(ERROR) << "List part failed in list hosts,  error: "
                       << apache::thrift::util::enumNameSafe(retCode);
            return retCode;
        }

        auto partIter = nebula::value(iterPartRet).get();
        std::unordered_map<HostAddr, std::vector<PartitionID>> hostParts;
        while (partIter->valid()) {
            PartitionID partId = MetaServiceUtils::parsePartKeyPartId(partIter->key());
            auto partHosts = MetaServiceUtils::parsePartVal(partIter->val());
            for (auto& host : partHosts) {
                hostParts[host].emplace_back(partId);
            }
            partIter->next();
        }

        for (const auto& hostEntry : hostParts) {
            allParts[hostEntry.first][spaceName] = std::move(hostEntry.second);
        }
    }

    for (const auto& hostEntry : allParts) {
        auto hostAddr = toThriftHost(hostEntry.first);
        auto it = std::find_if(hostItems_.begin(), hostItems_.end(), [&](const auto& item) {
            return item.get_hostAddr() == hostAddr;
        });
        if (it != hostItems_.end()) {
            it->set_all_parts(std::move(hostEntry.second));
        }
    }

    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

// Remove hosts that long time at OFFLINE status
void ListHostsProcessor::removeExpiredHosts(std::vector<std::string>&& removeHostsKey) {
    if (removeHostsKey.empty()) {
        return;
    }
    kvstore_->asyncMultiRemove(kDefaultSpaceId,
                               kDefaultPartId,
                               std::move(removeHostsKey),
                               [] (nebula::cpp2::ErrorCode code) {
            if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                LOG(ERROR) << "Async remove long time offline hosts failed: "
                           << apache::thrift::util::enumNameSafe(code);
            }
        });
}

// Remove invalid leaders
void ListHostsProcessor::removeInvalidLeaders(std::vector<std::string>&& removeLeadersKey) {
    if (removeLeadersKey.empty()) {
        return;
    }
    kvstore_->asyncMultiRemove(kDefaultSpaceId,
                               kDefaultPartId,
                               std::move(removeLeadersKey),
                               [] (nebula::cpp2::ErrorCode code) {
            if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                LOG(ERROR) << "Async remove long time offline hosts failed: "
                           << apache::thrift::util::enumNameSafe(code);
            }
        });
}

nebula::cpp2::ErrorCode ListHostsProcessor::getSpaceIdNameMap() {
    // Get all spaces
    const auto& spacePrefix = MetaServiceUtils::spacePrefix();
    auto iterRet = doPrefix(spacePrefix);
     if (!nebula::ok(iterRet)) {
        auto retCode = nebula::error(iterRet);
        if (retCode != nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
            retCode = nebula::cpp2::ErrorCode::E_NO_HOSTS;
        }
        LOG(ERROR) << "List Hosts Failed, error "
                   << apache::thrift::util::enumNameSafe(retCode);
        return retCode;
    }

    auto iter = nebula::value(iterRet).get();
    while (iter->valid()) {
        auto spaceId = MetaServiceUtils::spaceId(iter->key());
        spaceIds_.emplace_back(spaceId);
        spaceIdNameMap_.emplace(spaceId, MetaServiceUtils::spaceName(iter->val()));
        iter->next();
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

std::unordered_map<std::string, std::vector<PartitionID>>
ListHostsProcessor::getLeaderPartsWithSpaceName(const LeaderParts& leaderParts) {
    std::unordered_map<std::string, std::vector<PartitionID>> result;
    for (const auto& spaceEntry : leaderParts) {
        GraphSpaceID spaceId = spaceEntry.first;
        auto iter = spaceIdNameMap_.find(spaceId);
        if (iter != spaceIdNameMap_.end()) {
            // ignore space not exists
            result.emplace(iter->second, std::move(spaceEntry.second));
        }
    }
    return result;
}

}  // namespace meta
}  // namespace nebula
