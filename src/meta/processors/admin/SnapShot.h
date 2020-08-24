/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_ADMIN_SNAPSHOT_H_
#define META_ADMIN_SNAPSHOT_H_

#include <gtest/gtest_prod.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include "kvstore/KVStore.h"
#include "network/NetworkUtils.h"
#include "time/WallClock.h"
#include "meta/processors/admin/AdminClient.h"

namespace nebula {
namespace meta {

class Snapshot {
public:
    static Snapshot* instance(kvstore::KVStore* kv, AdminClient* client) {
        static std::unique_ptr<Snapshot> snapshot(new Snapshot(kv, client));
        return snapshot.get();
    }

    ~Snapshot() = default;

    cpp2::ErrorCode createSnapshot(const std::string& name);

    cpp2::ErrorCode dropSnapshot(const std::string& name, const std::vector<HostAddr>& hosts);

    cpp2::ErrorCode blockingWrites(storage::cpp2::EngineSignType sign);

private:
    Snapshot(kvstore::KVStore* kv, AdminClient* client) : kv_(kv), client_(client) {
        executor_.reset(new folly::CPUThreadPoolExecutor(1));
    }

    StatusOr<std::map<GraphSpaceID, std::set<HostAddr>>> getSpacesHosts();

private:
    kvstore::KVStore* kv_{nullptr};
    AdminClient* client_{nullptr};
    std::unique_ptr<folly::Executor> executor_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_ADMIN_SNAPSHOT_H_

