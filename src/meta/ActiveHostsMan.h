/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_ACTIVEHOSTSMAN_H_
#define META_ACTIVEHOSTSMAN_H_

#include "base/Base.h"
#include <gtest/gtest_prod.h>
#include "thread/GenericWorker.h"
#include "time/TimeUtils.h"
#include "kvstore/NebulaStore.h"

DECLARE_int32(expired_hosts_check_interval_sec);
DECLARE_int32(expired_threshold_sec);

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
    FRIEND_TEST(ProcessorTest, ListHostsTest);

public:
    static ActiveHostsMan* instance(kvstore::KVStore* kv = nullptr) {
        static auto activeHostsMan = std::unique_ptr<ActiveHostsMan>(
                new ActiveHostsMan(FLAGS_expired_hosts_check_interval_sec,
                                   FLAGS_expired_threshold_sec, kv));
        static std::once_flag initFlag;
        std::call_once(initFlag, &ActiveHostsMan::loadHostMap, activeHostsMan.get());
        return activeHostsMan.get();
    }

    ~ActiveHostsMan() {
        checkThread_.stop();
        checkThread_.wait();
    }

    // return true if register host successfully
    bool updateHostInfo(const HostAddr& hostAddr, const HostInfo& info);

    std::vector<HostAddr> getActiveHosts();

    void reset() {
        folly::RWSpinLock::WriteHolder rh(&lock_);
        hostsMap_.clear();
    }

protected:
    void cleanExpiredHosts();

private:
    ActiveHostsMan(int32_t intervalSeconds, int32_t expiredSeconds, kvstore::KVStore* kv = nullptr);

    void loadHostMap();

    void stopClean() {
        checkThread_.stop();
        checkThread_.wait();
    }

    folly::RWSpinLock lock_;
    std::unordered_map<HostAddr, HostInfo> hostsMap_;
    thread::GenericWorker checkThread_;
    int32_t intervalSeconds_ = 0;
    int32_t expirationInSeconds_ = 5 * 60;
    kvstore::NebulaStore* kvstore_ = nullptr;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_ACTIVEHOSTSMAN_H_
