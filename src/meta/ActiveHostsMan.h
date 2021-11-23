/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_ACTIVEHOSTSMAN_H_
#define META_ACTIVEHOSTSMAN_H_

#include <gtest/gtest_prod.h>

#include "common/base/Base.h"
#include "common/utils/MetaKeyUtils.h"
#include "kvstore/KVStore.h"
#include "meta/MetaCache.h"

namespace nebula {
namespace meta {

class ActiveHostsMan final {
 public:
  ~ActiveHostsMan() = default;

  using AllLeaders = std::unordered_map<GraphSpaceID, std::vector<cpp2::LeaderInfo>>;

  static nebula::cpp2::ErrorCode updateHostInfo(kvstore::KVStore* kv,
                                                const HostAddr& hostAddr,
                                                const HostInfo& info,
                                                const AllLeaders* leaderParts = nullptr);

  static ErrorOr<nebula::cpp2::ErrorCode, std::vector<HostAddr>> getActiveHosts(
      kvstore::KVStore* kv, int32_t expiredTTL = 0, cpp2::HostRole role = cpp2::HostRole::STORAGE);

  static ErrorOr<nebula::cpp2::ErrorCode, std::vector<HostAddr>> getActiveHostsInZone(
      kvstore::KVStore* kv, const std::string& zoneName, int32_t expiredTTL = 0);

  static ErrorOr<nebula::cpp2::ErrorCode, std::vector<HostAddr>> getActiveHostsWithGroup(
      kvstore::KVStore* kv, GraphSpaceID spaceId, int32_t expiredTTL = 0);

  static ErrorOr<nebula::cpp2::ErrorCode, std::vector<HostAddr>> getActiveAdminHosts(
      kvstore::KVStore* kv, int32_t expiredTTL = 0, cpp2::HostRole role = cpp2::HostRole::STORAGE);

  static ErrorOr<nebula::cpp2::ErrorCode, bool> isLived(kvstore::KVStore* kv, const HostAddr& host);

  static ErrorOr<nebula::cpp2::ErrorCode, HostInfo> getHostInfo(kvstore::KVStore* kv,
                                                                const HostAddr& host);

 protected:
  ActiveHostsMan() = default;
};


class LastUpdateTimeMan final {
 public:
  ~LastUpdateTimeMan() = default;

  static nebula::cpp2::ErrorCode update(kvstore::KVStore* kv, const int64_t timeInMilliSec);

  static ErrorOr<nebula::cpp2::ErrorCode, int64_t> get(kvstore::KVStore* kv);

 protected:
  LastUpdateTimeMan() = default;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_ACTIVEHOSTSMAN_H_
