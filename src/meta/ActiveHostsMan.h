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

namespace nebula {
namespace meta {

struct HostInfo {
  HostInfo() = default;
  explicit HostInfo(int64_t lastHBTimeInMilliSec) : lastHBTimeInMilliSec_(lastHBTimeInMilliSec) {}

  HostInfo(int64_t lastHBTimeInMilliSec, cpp2::HostRole role, std::string gitInfoSha)
      : lastHBTimeInMilliSec_(lastHBTimeInMilliSec),
        role_(role),
        gitInfoSha_(std::move(gitInfoSha)) {}

  bool operator==(const HostInfo& that) const {
    return this->lastHBTimeInMilliSec_ == that.lastHBTimeInMilliSec_;
  }

  bool operator!=(const HostInfo& that) const {
    return !(*this == that);
  }

  int64_t lastHBTimeInMilliSec_ = 0;
  cpp2::HostRole role_{cpp2::HostRole::UNKNOWN};
  std::string gitInfoSha_;

  static HostInfo decode(const folly::StringPiece& data) {
    if (data.size() == sizeof(int64_t)) {
      return decodeV1(data);
    }
    return decodeV2(data);
  }

  static HostInfo decodeV1(const folly::StringPiece& data) {
    HostInfo info;
    info.lastHBTimeInMilliSec_ = *reinterpret_cast<const int64_t*>(data.data());
    info.role_ = cpp2::HostRole::STORAGE;
    return info;
  }

  /*
   * int8_t           dataVer
   * int64_t          timestamp
   * sizeof(HostRole) hostRole
   * size_t           lenth of gitInfoSha
   * string           gitInfoSha
   * */
  static std::string encodeV2(const HostInfo& info) {
    std::string encode;
    int8_t dataVer = 2;
    encode.append(reinterpret_cast<const char*>(&dataVer), sizeof(int8_t));
    encode.append(reinterpret_cast<const char*>(&info.lastHBTimeInMilliSec_), sizeof(int64_t));

    encode.append(reinterpret_cast<const char*>(&info.role_), sizeof(cpp2::HostRole));

    size_t len = info.gitInfoSha_.size();
    encode.append(reinterpret_cast<const char*>(&len), sizeof(size_t));

    if (!info.gitInfoSha_.empty()) {
      encode.append(info.gitInfoSha_.data(), len);
    }
    return encode;
  }

  static HostInfo decodeV2(const folly::StringPiece& data) {
    HostInfo info;
    size_t offset = sizeof(int8_t);

    info.lastHBTimeInMilliSec_ = *reinterpret_cast<const int64_t*>(data.data() + offset);
    offset += sizeof(int64_t);

    if (data.size() - offset < sizeof(cpp2::HostRole)) {
      FLOG_FATAL("decode out of range, offset=%zu, actual=%zu", offset, data.size());
    }
    info.role_ = *reinterpret_cast<const cpp2::HostRole*>(data.data() + offset);
    offset += sizeof(cpp2::HostRole);

    if (offset + sizeof(size_t) > data.size()) {
      FLOG_FATAL("decode out of range, offset=%zu, actual=%zu", offset, data.size());
    }
    size_t len = *reinterpret_cast<const size_t*>(data.data() + offset);
    offset += sizeof(size_t);

    if (offset + len > data.size()) {
      FLOG_FATAL("decode out of range, offset=%zu, actual=%zu", offset, data.size());
    }

    info.gitInfoSha_ = std::string(data.data() + offset, len);
    return info;
  }
};

class ActiveHostsMan final {
 public:
  ~ActiveHostsMan() = default;

  using AllLeaders = std::unordered_map<GraphSpaceID, std::vector<cpp2::LeaderInfo>>;
  static nebula::cpp2::ErrorCode updateHostInfo(kvstore::KVStore* kv,
                                                const HostAddr& hostAddr,
                                                const HostInfo& info,
                                                const AllLeaders* leaderParts = nullptr);

  static bool machineRegisted(kvstore::KVStore* kv, const HostAddr& hostAddr);

  static ErrorOr<nebula::cpp2::ErrorCode, std::vector<HostAddr>> getActiveHosts(
      kvstore::KVStore* kv, int32_t expiredTTL = 0, cpp2::HostRole role = cpp2::HostRole::STORAGE);

  static ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::pair<HostAddr, cpp2::HostRole>>>
  getServicesInHost(kvstore::KVStore* kv, std::string hostname);

  static ErrorOr<nebula::cpp2::ErrorCode, std::vector<HostAddr>> getActiveHostsInZone(
      kvstore::KVStore* kv, const std::string& zoneName, int32_t expiredTTL = 0);

  static ErrorOr<nebula::cpp2::ErrorCode, std::vector<HostAddr>> getActiveHostsWithZones(
      kvstore::KVStore* kv, GraphSpaceID spaceId, int32_t expiredTTL = 0);

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
