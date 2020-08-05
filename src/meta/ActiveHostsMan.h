/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_ACTIVEHOSTSMAN_H_
#define META_ACTIVEHOSTSMAN_H_

#include "base/Base.h"
#include <gtest/gtest_prod.h>
#include "kvstore/KVStore.h"
#include "meta/MetaServiceUtils.h"

namespace nebula {
namespace meta {

struct HostInfo {
    HostInfo() = default;
    explicit HostInfo(int64_t lastHBTimeInMilliSec)
        : lastHBTimeInMilliSec_(lastHBTimeInMilliSec) {}

    bool operator==(const HostInfo& that) const {
        return this->lastHBTimeInMilliSec_ == that.lastHBTimeInMilliSec_;
    }

    bool operator!=(const HostInfo& that) const {
        return !(*this == that);
    }

    int64_t lastHBTimeInMilliSec_ = 0;

    static std::string encode(const HostInfo& info) {
        std::string encode;
        encode.reserve(sizeof(int64_t));
        encode.append(reinterpret_cast<const char*>(&info.lastHBTimeInMilliSec_), sizeof(int64_t));
        return encode;
    }

    static HostInfo decode(const folly::StringPiece& data) {
        HostInfo info;
        info.lastHBTimeInMilliSec_ = *reinterpret_cast<const int64_t*>(data.data());
        return info;
    }
};

class ActiveHostsMan final {
public:
    ~ActiveHostsMan() = default;

    static kvstore::ResultCode updateHostInfo(kvstore::KVStore* kv,
                                              const HostAddr& hostAddr,
                                              const HostInfo& info,
                                              const LeaderParts* leaderParts = nullptr);

    static std::vector<HostAddr> getActiveHosts(kvstore::KVStore* kv, int32_t expiredTTL = 0);

    static bool isLived(kvstore::KVStore* kv, const HostAddr& host);

protected:
    ActiveHostsMan() = default;
};

class LastUpdateTimeMan final {
public:
    ~LastUpdateTimeMan() = default;

    static kvstore::ResultCode update(kvstore::KVStore* kv, const int64_t timeInMilliSec);

    static int64_t get(kvstore::KVStore* kv);

protected:
    LastUpdateTimeMan() = default;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_ACTIVEHOSTSMAN_H_
