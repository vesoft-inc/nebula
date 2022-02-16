/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_ADMIN_SNAPSHOT_H_
#define META_ADMIN_SNAPSHOT_H_

#include <folly/Executor.h>                         // for Executor
#include <folly/executors/CPUThreadPoolExecutor.h>  // for CPUThreadPoolExec...
#include <gtest/gtest_prod.h>

#include <map>            // for map
#include <memory>         // for unique_ptr
#include <set>            // for set
#include <string>         // for string
#include <unordered_map>  // for unordered_map
#include <unordered_set>  // for unordered_set
#include <utility>        // for move
#include <vector>         // for vector

#include "common/base/ErrorOr.h"  // for ErrorOr
#include "common/network/NetworkUtils.h"
#include "common/thrift/ThriftTypes.h"  // for GraphSpaceID
#include "common/time/WallClock.h"
#include "interface/gen-cpp2/common_types.h"   // for ErrorCode
#include "interface/gen-cpp2/storage_types.h"  // for EngineSignType
#include "kvstore/KVStore.h"
#include "meta/processors/admin/AdminClient.h"

namespace nebula {
namespace meta {
class AdminClient;
namespace cpp2 {
class HostBackupInfo;
}  // namespace cpp2
}  // namespace meta
struct HostAddr;

namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore
struct HostAddr;

namespace meta {
class AdminClient;
namespace cpp2 {
class HostBackupInfo;
}  // namespace cpp2

/**
 * @brief Create and drop snapshots for given spaces in
 *        storage hosts(include followers)
 *        Another feature is providing blocking/unbloking writings.
 *
 */
class Snapshot {
 public:
  static Snapshot* instance(kvstore::KVStore* kv, AdminClient* client) {
    static std::unique_ptr<Snapshot> snapshot(new Snapshot(kv, client));
    return snapshot.get();
  }

  ~Snapshot() = default;

  inline void setSpaces(std::unordered_set<GraphSpaceID> spaces) {
    spaces_ = std::move(spaces);
  }

  ErrorOr<nebula::cpp2::ErrorCode,
          std::unordered_map<GraphSpaceID, std::vector<cpp2::HostBackupInfo>>>
  createSnapshot(const std::string& name);

  /**
   * @brief Drop specified snapshot in given storage hosts
   *
   * @param name snapshot name
   * @param hosts storage hosts
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode dropSnapshot(const std::string& name, const std::vector<HostAddr>& hosts);

  /**
   * @brief Blocking writings before create snapshot, allow writings after create
   *        snapshot failed or completely
   *
   * @param sign BLOCK_ON and BLOCK_OFF(allow)
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode blockingWrites(storage::cpp2::EngineSignType sign);

 private:
  Snapshot(kvstore::KVStore* kv, AdminClient* client) : kv_(kv), client_(client) {
    executor_.reset(new folly::CPUThreadPoolExecutor(1));
  }

  ErrorOr<nebula::cpp2::ErrorCode, std::map<HostAddr, std::set<GraphSpaceID>>> getHostSpaces();

 private:
  kvstore::KVStore* kv_{nullptr};
  AdminClient* client_{nullptr};
  std::unordered_set<GraphSpaceID> spaces_;
  std::unique_ptr<folly::Executor> executor_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_ADMIN_SNAPSHOT_H_
