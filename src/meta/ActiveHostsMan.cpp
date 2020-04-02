/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/ActiveHostsMan.h"
#include "meta/processors/Common.h"

DEFINE_int32(expired_threshold_sec, 10 * 60,
                     "Hosts will be expired in this time if no heartbeat received");

namespace nebula {
namespace meta {

int ActiveHostsMan::updateHostIPaddress(kvstore::KVStore* kv,
                                        const network::InetAddress& hostAddr,
                                        const std::string& oldIPAddr,
                                        kvstore::ResultCode& ret) {
    auto ipAddress = network::InetAddress::make_inet_address(oldIPAddr.c_str());
    if (ipAddress == hostAddr) {
        return 0;
    }
    std::vector<std::string> removeData;
    folly::Baton<true, std::atomic> baton;
    removeData.emplace_back(MetaServiceUtils::hostKey(ipAddress.toLong(), ipAddress.getPort()));
    removeData.emplace_back(MetaServiceUtils::leaderKey(ipAddress.toLong(), ipAddress.getPort()));
    kv->asyncMultiRemove(
        kDefaultSpaceId, kDefaultPartId, removeData, [&baton, &ret](kvstore::ResultCode code) {
            ret = code;
            baton.post();
        });
    baton.wait();
    if (ret != kvstore::SUCCEEDED && ret != kvstore::ERR_KEY_NOT_FOUND) {
        return -1;
    }

    folly::SharedMutex::WriteHolder wHolder(LockUtils::spaceLock());
    auto prefix = MetaServiceUtils::spacePrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    ret = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        return -1;
    }
    std::vector<kvstore::KV> data;
    while (iter->valid()) {
        auto spaceId = MetaServiceUtils::spaceId(iter->key());
        auto spaceName = MetaServiceUtils::spaceName(iter->val());
        std::string spaceKey = MetaServiceUtils::spaceKey(spaceId);
        std::string propstr;
        auto code = kv->get(kDefaultSpaceId, kDefaultPartId, spaceKey, &propstr);
        if (code != kvstore::SUCCEEDED) {
            LOG(WARNING) << "update Space SpaceName: " << spaceName << " not found";
            continue;
        }
        auto properties = MetaServiceUtils::parseSpace(propstr);
        auto pnum = properties.get_partition_num();
        for (int i = 1; i <= pnum; i++) {
            auto key = MetaServiceUtils::partKey(spaceId, i);
            std::string value;
            code = kv->get(kDefaultSpaceId, kDefaultSpaceId, key, &value);
            if (code != kvstore::SUCCEEDED) {
                continue;
            }
            auto hosts = MetaServiceUtils::parsePartVal(value);
            for (auto it = hosts.begin(); it != hosts.end(); ++it) {
                if (it->ip == ipAddress.toLong()) {
                    it->ip = hostAddr.toLong();
                    it->port = hostAddr.getPort();
                }
            }

            data.emplace_back(MetaServiceUtils::partKey(spaceId, i),
                              MetaServiceUtils::partVal(hosts));
        }
        iter->next();
    }

    if (!data.empty()) {
        baton.reset();
        kv->asyncMultiPut(
            kDefaultSpaceId, kDefaultPartId, std::move(data), [&](kvstore::ResultCode code) {
                ret = code;
                baton.post();
            });
        baton.wait();
        if (ret != kvstore::SUCCEEDED) {
            return -1;
        }
    }

    return 1;
}

kvstore::ResultCode ActiveHostsMan::updateHostInfo(kvstore::KVStore* kv,
                                                   const network::InetAddress& hostAddr,
                                                   const std::string &hostName,
                                                   const HostInfo& info,
                                                   const LeaderParts* leaderParts) {
    CHECK_NOTNULL(kv);
    kvstore::ResultCode ret;
    std::vector<kvstore::KV> data;

    if (!hostName.empty()) {
        std::string oldIPAddr;
        ret = kv->get(kDefaultSpaceId, kDefaultPartId, hostName, &oldIPAddr);

        switch (ret) {
            case kvstore::SUCCEEDED: {
                auto re = updateHostIPaddress(kv, hostAddr, oldIPAddr, ret);
                if (re == -1) {
                    return ret;
                } else if (re == 0) {
                    break;
                }
            }
                // pass
            case kvstore::ERR_KEY_NOT_FOUND:
                data.emplace_back(hostName, hostAddr.encode());
                break;
            default:
                return ret;
        }
    }

    data.emplace_back(MetaServiceUtils::hostKey(hostAddr.toLong(), hostAddr.getPort()),
                      HostInfo::encode(info));
    if (leaderParts != nullptr) {
        data.emplace_back(MetaServiceUtils::leaderKey(hostAddr.toLong(), hostAddr.getPort()),
                          MetaServiceUtils::leaderVal(*leaderParts));
    }
    folly::SharedMutex::WriteHolder wHolder(LockUtils::spaceLock());
    folly::Baton<true, std::atomic> baton;
    kv->asyncMultiPut(kDefaultSpaceId, kDefaultPartId, std::move(data),
                            [&] (kvstore::ResultCode code) {
        ret = code;
        baton.post();
    });
    baton.wait();
    return ret;
}

std::vector<network::InetAddress> ActiveHostsMan::getActiveHosts(kvstore::KVStore* kv,
                                                                 int32_t expiredTTL) {
    std::vector<network::InetAddress> hosts;
    const auto& prefix = MetaServiceUtils::hostPrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        return hosts;
    }
    int64_t threshold = (expiredTTL == 0 ? FLAGS_expired_threshold_sec : expiredTTL) * 1000;
    auto now = time::WallClock::fastNowInMilliSec();
    while (iter->valid()) {
        auto host = MetaServiceUtils::parseHostKey(iter->key());
        HostInfo info = HostInfo::decode(iter->val());
        if (now - info.lastHBTimeInMilliSec_ < threshold) {
            hosts.emplace_back(host.ip, host.port);
        }
        iter->next();
    }
    return hosts;
}

bool ActiveHostsMan::isLived(kvstore::KVStore* kv, const network::InetAddress& host) {
    auto activeHosts = getActiveHosts(kv);
    return std::find(activeHosts.begin(), activeHosts.end(), host) != activeHosts.end();
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
