/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_ACTIVEHOSTSMAN_H_
#define META_ACTIVEHOSTSMAN_H_

#include "base/Base.h"
#include <gtest/gtest_prod.h>
#include "thread/GenericWorker.h"
#include "time/TimeUtils.h"

namespace nebula {
namespace meta {

struct HostInfo {
    HostInfo() = default;
    explicit HostInfo(int64_t lastHBTimeInSec)
        : lastHBTimeInSec_(lastHBTimeInSec) {}

    bool operator==(const HostInfo& that) const {
        return this->lastHBTimeInSec_ == that.lastHBTimeInSec_;
    }

    bool operator!=(const HostInfo& that) const {
        return !(*this == that);
    }

    int64_t lastHBTimeInSec_ = 0;
};

class ActiveHostsMan final {
    FRIEND_TEST(ActiveHostsManTest, NormalTest);

public:
    ActiveHostsMan(int32_t intervalSeconds, int32_t expiredSeconds)
        : intervalSeconds_(intervalSeconds)
        , expirationInSeconds_(expiredSeconds) {
        CHECK(checkThread_.start());
        checkThread_.addTimerTask(intervalSeconds * 1000,
                                  intervalSeconds * 1000,
                                  &ActiveHostsMan::cleanExpiredHosts,
                                  this);
    }

    ~ActiveHostsMan() {
        checkThread_.stop();
        checkThread_.wait();
    }

    void updateHostInfo(const HostAddr& hostAddr, const HostInfo& info) {
        folly::RWSpinLock::ReadHolder rh(&lock_);
        auto it = hostsMap_.find(hostAddr);
        if (it == hostsMap_.end()) {
            folly::RWSpinLock::UpgradedHolder uh(&lock_);
            hostsMap_.emplace(std::move(hostAddr), std::move(info));
        } else {
            it->second.lastHBTimeInSec_ = info.lastHBTimeInSec_;
        }
    }

    std::vector<HostAddr> getActiveHosts() {
        std::vector<HostAddr> hosts;
        folly::RWSpinLock::ReadHolder rh(&lock_);
        hosts.resize(hostsMap_.size());
        std::transform(hostsMap_.begin(), hostsMap_.end(), hosts.begin(),
                       [](const auto& entry) -> decltype(auto) {
            return entry.first;
        });
        return hosts;
    }

protected:
    void cleanExpiredHosts() {
        int64_t now = time::TimeUtils::nowInSeconds();
        folly::RWSpinLock::WriteHolder rh(&lock_);
        auto it = hostsMap_.begin();
        while (it != hostsMap_.end()) {
            if ((now - it->second.lastHBTimeInSec_) > expirationInSeconds_) {
                LOG(INFO) << it->first << " expired! last hb time "
                          << it->second.lastHBTimeInSec_;
                it = hostsMap_.erase(it);
            } else {
                it++;
            }
        }
    }

private:
    folly::RWSpinLock lock_;
    std::unordered_map<HostAddr, HostInfo> hostsMap_;
    thread::GenericWorker checkThread_;
    int32_t intervalSeconds_ = 0;
    int32_t expirationInSeconds_ = 5 * 60;
};
}  // namespace meta
}  // namespace nebula

#endif  // META_ACTIVEHOSTSMAN_H_
