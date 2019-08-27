/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/ActiveHostsMan.h"
#include "meta/MetaServiceUtils.h"
#include "meta/processors/Common.h"

DEFINE_int32(expired_threshold_sec, 10 * 60,
                     "Hosts will be expired in this time if no heartbeat received");

namespace nebula {
namespace meta {

kvstore::ResultCode ActiveHostsMan::updateHostInfo(kvstore::KVStore* kv,
                                                   const HostAddr& hostAddr,
                                                   const HostInfo& info) {
    CHECK_NOTNULL(kv);
    std::vector<kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::hostKey(hostAddr.first, hostAddr.second),
                      HostInfo::encode(info));
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

std::vector<HostAddr> ActiveHostsMan::getActiveHosts(kvstore::KVStore* kv, int32_t expiredTTL) {
    std::vector<HostAddr> hosts;
    const auto& prefix = MetaServiceUtils::hostPrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        return hosts;
    }
    expiredTTL = (expiredTTL == 0 ? FLAGS_expired_threshold_sec : expiredTTL);
    auto now = time::WallClock::fastNowInSec();
    while (iter->valid()) {
        auto host = MetaServiceUtils::parseHostKey(iter->key());
        HostInfo info = HostInfo::decode(iter->val());
        if (now - info.lastHBTimeInSec_ < expiredTTL) {
            hosts.emplace_back(host.ip, host.port);
        }
        iter->next();
    }
    return hosts;
}

}  // namespace meta
}  // namespace nebula
