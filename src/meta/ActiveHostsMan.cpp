/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/ActiveHostsMan.h"
#include "meta/processors/Common.h"
#include "utils/Utils.h"

DEFINE_int32(expired_threshold_sec, 10 * 60,
             "Hosts will be expired in this time if no heartbeat received");

namespace nebula {
namespace meta {

kvstore::ResultCode ActiveHostsMan::updateHostInfo(kvstore::KVStore* kv,
                                                   const HostAddr& hostAddr,
                                                   const HostInfo& info,
                                                   const LeaderParts* leaderParts) {
    CHECK_NOTNULL(kv);
    std::vector<kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::hostKey(hostAddr.host, hostAddr.port),
                      HostInfo::encodeV2(info));
    if (leaderParts != nullptr) {
        data.emplace_back(MetaServiceUtils::leaderKey(hostAddr.host, hostAddr.port),
                          MetaServiceUtils::leaderVal(*leaderParts));
    }
    folly::SharedMutex::WriteHolder wHolder(LockUtils::spaceLock());
    folly::Baton<true, std::atomic> baton;
    kvstore::ResultCode ret;
    kv->asyncMultiPut(kDefaultSpaceId, kDefaultPartId, std::move(data),
                      [&] (kvstore::ResultCode code) {
        ret = code;
        baton.post();
    });
    baton.wait();
    return ret;
}

std::vector<HostAddr> ActiveHostsMan::getActiveHosts(kvstore::KVStore* kv,
                                                     int32_t expiredTTL,
                                                     cpp2::HostRole role) {
    std::vector<HostAddr> hosts;
    const auto& prefix = MetaServiceUtils::hostPrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        FLOG_ERROR("getActiveHosts failed(%d)", static_cast<int>(ret));
        return hosts;
    }
    int64_t threshold = (expiredTTL == 0 ? FLAGS_expired_threshold_sec : expiredTTL) * 1000;
    auto now = time::WallClock::fastNowInMilliSec();
    while (iter->valid()) {
        auto host = MetaServiceUtils::parseHostKey(iter->key());
        HostInfo info = HostInfo::decode(iter->val());
        if (info.role_ == role) {
            if (now - info.lastHBTimeInMilliSec_ < threshold) {
                hosts.emplace_back(host.host, host.port);
            }
        }
        iter->next();
    }

    return hosts;
}

std::vector<HostAddr> ActiveHostsMan::getActiveHostsInZone(kvstore::KVStore* kv,
                                                           const std::string& zoneName,
                                                           int32_t expiredTTL) {
    std::vector<HostAddr> activeHosts;
    std::string zoneValue;
    auto zoneKey = MetaServiceUtils::zoneKey(zoneName);
    auto ret = kv->get(kDefaultSpaceId, kDefaultPartId, zoneKey, &zoneValue);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Get zone " << zoneName << " failed";
        return activeHosts;
    }

    auto hosts = MetaServiceUtils::parseZoneHosts(std::move(zoneValue));
    auto now = time::WallClock::fastNowInMilliSec();
    int64_t threshold = (expiredTTL == 0 ? FLAGS_expired_threshold_sec : expiredTTL) * 1000;
    for (auto& host : hosts) {
        auto infoRet = getHostInfo(kv, host);
        if (!infoRet.ok()) {
            activeHosts.clear();
            return activeHosts;
        }

        auto info = infoRet.value();
        if (now - info.lastHBTimeInMilliSec_ < threshold) {
            activeHosts.emplace_back(host.host, host.port);
        }
    }
    return activeHosts;
}

std::vector<HostAddr> ActiveHostsMan::getActiveHostsWithGroup(kvstore::KVStore* kv,
                                                              GraphSpaceID spaceId,
                                                              int32_t expiredTTL) {
    std::string spaceValue;
    std::vector<HostAddr> activeHosts;
    auto spaceKey = MetaServiceUtils::spaceKey(spaceId);
    auto ret = kv->get(kDefaultSpaceId, kDefaultPartId, spaceKey, &spaceValue);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Space " << spaceId << " not exist";
        return activeHosts;
    }

    std::string groupValue;
    auto space = MetaServiceUtils::parseSpace(std::move(spaceValue));
    auto groupKey = MetaServiceUtils::groupKey(space.group_name);
    ret = kv->get(kDefaultSpaceId, kDefaultPartId, groupKey, &groupValue);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Get group " << space.group_name << " failed";
        return activeHosts;
    }

    auto zoneNames = MetaServiceUtils::parseZoneNames(std::move(groupValue));
    for (const auto& zoneName : zoneNames) {
        auto hosts = getActiveHostsInZone(kv, zoneName, expiredTTL);
        activeHosts.insert(activeHosts.end(), hosts.begin(), hosts.end());
    }
    return activeHosts;
}

std::vector<HostAddr> ActiveHostsMan::getActiveAdminHosts(kvstore::KVStore* kv,
                                                          int32_t expiredTTL,
                                                          cpp2::HostRole role) {
    auto hosts = getActiveHosts(kv, expiredTTL, role);
    std::vector<HostAddr> adminHosts(hosts.size());
    std::transform(hosts.begin(), hosts.end(), adminHosts.begin(), [](const auto& h) {
        return Utils::getAdminAddrFromStoreAddr(h);
    });
    return adminHosts;
}

bool ActiveHostsMan::isLived(kvstore::KVStore* kv, const HostAddr& host) {
    auto activeHosts = getActiveHosts(kv);
    return std::find(activeHosts.begin(), activeHosts.end(), host) != activeHosts.end();
}

StatusOr<HostInfo> ActiveHostsMan::getHostInfo(kvstore::KVStore* kv, const HostAddr& host) {
    auto hostKey = MetaServiceUtils::hostKey(host.host, host.port);
    std::string hostValue;
    auto ret = kv->get(kDefaultSpaceId, kDefaultPartId, hostKey, &hostValue);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Get host info " << host << " failed";
        return Status::Error("Get host info failed");
    }
    return HostInfo::decode(hostValue);
}

kvstore::ResultCode LastUpdateTimeMan::update(kvstore::KVStore* kv, const int64_t timeInMilliSec) {
    CHECK_NOTNULL(kv);
    std::vector<kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::lastUpdateTimeKey(),
                      MetaServiceUtils::lastUpdateTimeVal(timeInMilliSec));

    folly::SharedMutex::WriteHolder wHolder(LockUtils::lastUpdateTimeLock());
    folly::Baton<true, std::atomic> baton;
    kvstore::ResultCode ret;
    kv->asyncMultiPut(kDefaultSpaceId, kDefaultPartId, std::move(data),
                      [&] (kvstore::ResultCode code) {
        ret = code;
        baton.post();
    });
    baton.wait();
    return kv->sync(kDefaultSpaceId, kDefaultPartId);
}

int64_t LastUpdateTimeMan::get(kvstore::KVStore* kv) {
    CHECK_NOTNULL(kv);
    auto key = MetaServiceUtils::lastUpdateTimeKey();
    std::string val;
    auto ret = kv->get(kDefaultSpaceId, kDefaultPartId, key, &val);
    if (ret == kvstore::ResultCode::SUCCEEDED) {
        return *reinterpret_cast<const int64_t*>(val.data());
    }
    return 0;
}

}  // namespace meta
}  // namespace nebula
