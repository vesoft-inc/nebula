/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/ActiveHostsMan.h"
#include "meta/MetaServiceUtils.h"
#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

ActiveHostsMan::ActiveHostsMan(int32_t intervalSeconds, int32_t expiredSeconds,
                               kvstore::KVStore* kv)
    : intervalSeconds_(intervalSeconds)
    , expirationInSeconds_(expiredSeconds) {
    if (kv != nullptr) {
        kvstore_ = dynamic_cast<kvstore::NebulaStore*>(kv);
    }

    CHECK_GT(intervalSeconds, 0)
        << "intervalSeconds " << intervalSeconds << " should > 0!";
    CHECK_GE(expiredSeconds, intervalSeconds)
        << "expiredSeconds " << expiredSeconds
        << " should >= intervalSeconds " << intervalSeconds;
    CHECK(checkThread_.start());
    checkThread_.addTimerTask(intervalSeconds * 1000,
                              intervalSeconds * 1000,
                              &ActiveHostsMan::cleanExpiredHosts,
                              this);
}

bool ActiveHostsMan::updateHostInfo(const HostAddr& hostAddr, const HostInfo& info) {
    std::vector<kvstore::KV> data;
    {
        folly::RWSpinLock::ReadHolder rh(&lock_);
        auto it = hostsMap_.find(hostAddr);
        if (it == hostsMap_.end()) {
            folly::RWSpinLock::UpgradedHolder uh(&lock_);
            hostsMap_.emplace(hostAddr, std::move(info));
            data.emplace_back(MetaServiceUtils::hostKey(hostAddr.first, hostAddr.second),
                              MetaServiceUtils::hostValOnline());
        } else {
            it->second.lastHBTimeInSec_ = info.lastHBTimeInSec_;
        }
    }
    if (kvstore_ != nullptr && !data.empty()) {
        if (kvstore_->isLeader(kDefaultSpaceId, kDefaultPartId)) {
            folly::SharedMutex::WriteHolder wHolder(LockUtils::spaceLock());
            folly::Baton<true, std::atomic> baton;
            kvstore_->asyncMultiPut(kDefaultSpaceId, kDefaultPartId, std::move(data),
                                    [&baton] (kvstore::ResultCode code) {
                UNUSED(code);
                baton.post();
            });
            return baton.try_wait_for(std::chrono::milliseconds(500));
        } else {
            return false;
        }
    }
    return true;
}

std::vector<HostAddr> ActiveHostsMan::getActiveHosts() {
    std::vector<HostAddr> hosts;
    folly::RWSpinLock::ReadHolder rh(&lock_);
    hosts.resize(hostsMap_.size());
    std::transform(hostsMap_.begin(), hostsMap_.end(), hosts.begin(),
                   [](const auto& entry) -> decltype(auto) {
        return entry.first;
    });
    return hosts;
}

void ActiveHostsMan::loadHostMap() {
    if (kvstore_ == nullptr) {
        return;
    }

    const auto& prefix = MetaServiceUtils::hostPrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        return;
    }

    while (iter->valid()) {
        auto host = MetaServiceUtils::parseHostKey(iter->key());
        HostInfo info;
        info.lastHBTimeInSec_ = time::TimeUtils::nowInSeconds();
        if (iter->val() == MetaServiceUtils::hostValOnline()) {
            LOG(INFO) << "load host " << host.ip << ":" << host.port;
            updateHostInfo({host.ip, host.port}, info);
        }
        iter->next();
    }
}

void ActiveHostsMan::cleanExpiredHosts() {
    int64_t now = time::TimeUtils::nowInSeconds();
    std::vector<kvstore::KV> data;
    {
        folly::RWSpinLock::WriteHolder rh(&lock_);
        auto it = hostsMap_.begin();
        while (it != hostsMap_.end()) {
            if ((now - it->second.lastHBTimeInSec_) > expirationInSeconds_) {
                LOG(INFO) << it->first << " expired! last hb time "
                    << it->second.lastHBTimeInSec_;
                data.emplace_back(MetaServiceUtils::hostKey(it->first.first, it->first.second),
                                  MetaServiceUtils::hostValOffline());
                it = hostsMap_.erase(it);
            } else {
                it++;
            }
        }
    }

    if (!data.empty() && kvstore_ != nullptr) {
        folly::SharedMutex::WriteHolder wHolder(LockUtils::spaceLock());
        LOG(INFO) << "set " << data.size() << " expired hosts to offline in meta rocksdb";
        kvstore_->asyncMultiPut(kDefaultSpaceId, kDefaultPartId, std::move(data),
                                [] (kvstore::ResultCode code) {
            CHECK_EQ(code, kvstore::ResultCode::SUCCEEDED);
        });
    }
}

}  // namespace meta
}  // namespace nebula
