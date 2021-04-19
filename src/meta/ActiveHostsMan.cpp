/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <thrift/lib/cpp/util/EnumUtils.h>
#include "meta/ActiveHostsMan.h"
#include <thrift/lib/cpp/util/EnumUtils.h>
#include "meta/processors/Common.h"
#include "meta/common/MetaCommon.h"
#include "utils/Utils.h"

DECLARE_int32(heartbeat_interval_secs);
DECLARE_uint32(expired_time_factor);

namespace nebula {
namespace meta {

cpp2::ErrorCode ActiveHostsMan::updateHostInfo(kvstore::KVStore* kv,
                                               const HostAddr& hostAddr,
                                               const HostInfo& info,
                                               const AllLeaders* allLeaders) {
    CHECK_NOTNULL(kv);
    std::vector<kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::hostKey(hostAddr.host, hostAddr.port),
                      HostInfo::encodeV2(info));
    std::vector<std::string> leaderKeys;
    std::vector<int64_t> terms;
    if (allLeaders != nullptr) {
        for (auto& spaceLeaders : *allLeaders) {
            auto spaceId = spaceLeaders.first;
            for (auto& partLeader : spaceLeaders.second) {
                auto key = MetaServiceUtils::leaderKey(spaceId, partLeader.get_part_id());
                leaderKeys.emplace_back(std::move(key));
                terms.emplace_back(partLeader.get_term());
            }
        }
        auto keys = leaderKeys;
        std::vector<std::string> vals;
        // let see if this c++17 syntax can pass
        auto [rc, statuses] = kv->multiGet(kDefaultSpaceId, kDefaultPartId, std::move(keys), &vals);
        if (rc != kvstore::ResultCode::SUCCEEDED && rc != kvstore::ResultCode::ERR_PARTIAL_RESULT) {
            auto retCode = MetaCommon::to(rc);
            LOG(INFO) << "error rc = " << apache::thrift::util::enumNameSafe(retCode);
            return retCode;
        }
        TermID term;
        cpp2::ErrorCode code;
        for (auto i = 0U; i != statuses.size(); ++i) {
            if (statuses[i].ok()) {
                std::tie(std::ignore, term, code) = MetaServiceUtils::parseLeaderValV3(vals[i]);
                if (code != cpp2::ErrorCode::SUCCEEDED) {
                    LOG(WARNING) << apache::thrift::util::enumNameSafe(code);
                    continue;
                }
                if (terms[i] <= term) {
                    continue;
                }
            }
            // write directly if not exist, or update if has greater term
            auto val = MetaServiceUtils::leaderValV3(hostAddr, terms[i]);
            data.emplace_back(std::make_pair(leaderKeys[i], std::move(val)));
        }
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
    return MetaCommon::to(ret);
}

ErrorOr<cpp2::ErrorCode, std::vector<HostAddr>>
ActiveHostsMan::getActiveHosts(kvstore::KVStore* kv, int32_t expiredTTL, cpp2::HostRole role) {
    const auto& prefix = MetaServiceUtils::hostPrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        auto retCode = MetaCommon::to(ret);
        LOG(ERROR) << "Failed to getActiveHosts, error "
                   << apache::thrift::util::enumNameSafe(retCode);
        return retCode;
    }

    std::vector<HostAddr> hosts;
    int64_t threshold = (expiredTTL == 0 ?
                         FLAGS_heartbeat_interval_secs * FLAGS_expired_time_factor :
                         expiredTTL) * 1000;
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

ErrorOr<cpp2::ErrorCode, std::vector<HostAddr>>
ActiveHostsMan::getActiveHostsInZone(kvstore::KVStore* kv,
                                     const std::string& zoneName,
                                     int32_t expiredTTL) {
    std::vector<HostAddr> activeHosts;
    std::string zoneValue;
    auto zoneKey = MetaServiceUtils::zoneKey(zoneName);
    auto ret = kv->get(kDefaultSpaceId, kDefaultPartId, zoneKey, &zoneValue);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        auto retCode = MetaCommon::to(ret);
        LOG(ERROR) << "Get zone " << zoneName << " failed, error: "
                   << apache::thrift::util::enumNameSafe(retCode);
        return retCode;
    }

    auto hosts = MetaServiceUtils::parseZoneHosts(std::move(zoneValue));
    auto now = time::WallClock::fastNowInMilliSec();
    int64_t threshold = (expiredTTL == 0 ?
                         FLAGS_heartbeat_interval_secs * FLAGS_expired_time_factor :
                         expiredTTL) * 1000;
    for (auto& host : hosts) {
        auto infoRet = getHostInfo(kv, host);
        if (!nebula::ok(infoRet)) {
            return nebula::error(infoRet);
        }

        auto info = nebula::value(infoRet);
        if (now - info.lastHBTimeInMilliSec_ < threshold) {
            activeHosts.emplace_back(host.host, host.port);
        }
    }
    return activeHosts;
}

ErrorOr<cpp2::ErrorCode, std::vector<HostAddr>>
ActiveHostsMan::getActiveHostsWithGroup(kvstore::KVStore* kv,
                                        GraphSpaceID spaceId,
                                        int32_t expiredTTL) {
    std::string spaceValue;
    std::vector<HostAddr> activeHosts;
    auto spaceKey = MetaServiceUtils::spaceKey(spaceId);
    auto ret = kv->get(kDefaultSpaceId, kDefaultPartId, spaceKey, &spaceValue);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        auto retCode = MetaCommon::to(ret);
        LOG(ERROR) << "Get space failed, error: "
                   << apache::thrift::util::enumNameSafe(retCode);
        return retCode;
    }

    std::string groupValue;
    auto space = MetaServiceUtils::parseSpace(std::move(spaceValue));
    auto groupKey = MetaServiceUtils::groupKey(*space.group_name_ref());
    ret = kv->get(kDefaultSpaceId, kDefaultPartId, groupKey, &groupValue);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        auto retCode = MetaCommon::to(ret);
        LOG(ERROR) << "Get group " << *space.group_name_ref() << " failed, error: "
                   << apache::thrift::util::enumNameSafe(retCode);
        return retCode;
    }

    auto zoneNames = MetaServiceUtils::parseZoneNames(std::move(groupValue));
    for (const auto& zoneName : zoneNames) {
        auto hostsRet = getActiveHostsInZone(kv, zoneName, expiredTTL);
        if (!nebula::ok(hostsRet)) {
            return nebula::error(hostsRet);
        }
        auto hosts = nebula::value(hostsRet);
        activeHosts.insert(activeHosts.end(), hosts.begin(), hosts.end());
    }
    return activeHosts;
}

ErrorOr<cpp2::ErrorCode, std::vector<HostAddr>>
ActiveHostsMan::getActiveAdminHosts(kvstore::KVStore* kv,
                                    int32_t expiredTTL,
                                    cpp2::HostRole role) {
    auto hostsRet = getActiveHosts(kv, expiredTTL, role);
    if (!nebula::ok(hostsRet)) {
        return nebula::error(hostsRet);
    }
    auto hosts = nebula::value(hostsRet);

    std::vector<HostAddr> adminHosts(hosts.size());
    std::transform(hosts.begin(), hosts.end(), adminHosts.begin(), [](const auto& h) {
        return Utils::getAdminAddrFromStoreAddr(h);
    });
    return adminHosts;
}

ErrorOr<cpp2::ErrorCode, bool> ActiveHostsMan::isLived(kvstore::KVStore* kv, const HostAddr& host) {
    auto activeHostsRet = getActiveHosts(kv);
    if (!nebula::ok(activeHostsRet)) {
        return nebula::error(activeHostsRet);
    }
    auto activeHosts = nebula::value(activeHostsRet);

    return std::find(activeHosts.begin(), activeHosts.end(), host) != activeHosts.end();
}

ErrorOr<cpp2::ErrorCode, HostInfo>
ActiveHostsMan::getHostInfo(kvstore::KVStore* kv, const HostAddr& host) {
    auto hostKey = MetaServiceUtils::hostKey(host.host, host.port);
    std::string hostValue;
    auto ret = kv->get(kDefaultSpaceId, kDefaultPartId, hostKey, &hostValue);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        auto retCode = MetaCommon::to(ret);
        LOG(ERROR) << "Get host info " << host << " failed, error: "
                   << apache::thrift::util::enumNameSafe(retCode);
        return retCode;
    }
    return HostInfo::decode(hostValue);
}

cpp2::ErrorCode LastUpdateTimeMan::update(kvstore::KVStore* kv, const int64_t timeInMilliSec) {
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
    ret = kv->sync(kDefaultSpaceId, kDefaultPartId);
    return MetaCommon::to(ret);
}

ErrorOr<cpp2::ErrorCode, int64_t> LastUpdateTimeMan::get(kvstore::KVStore* kv) {
    CHECK_NOTNULL(kv);
    auto key = MetaServiceUtils::lastUpdateTimeKey();
    std::string val;
    auto ret = kv->get(kDefaultSpaceId, kDefaultPartId, key, &val);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        auto retCode = MetaCommon::to(ret);
        LOG(ERROR) << "Get last update time failed, error: "
                   << apache::thrift::util::enumNameSafe(retCode);
        return retCode;
    }
    return *reinterpret_cast<const int64_t*>(val.data());
}

}  // namespace meta
}  // namespace nebula
