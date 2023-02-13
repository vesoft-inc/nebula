/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_ACTIVEHOSTSMAN_H_
#define META_ACTIVEHOSTSMAN_H_

#include "common/base/Base.h"
#include "common/utils/MetaKeyUtils.h"
#include "kvstore/KVStore.h"
#include "kvstore/LogEncoder.h"

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

  /**
   * @brief
   * int8_t           dataVer
   * int64_t          timestamp
   * sizeof(HostRole) hostRole
   * size_t           length of gitInfoSha
   * string           gitInfoSha
   *
   * @param info
   * @return
   */
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

  /**
   * @brief Parse a serialized value to HostInfo
   *
   * @param data
   * @return
   */
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

  /**
   * @brief Save host info and leader info into kvStore
   * If partition leader info was updated, it will update LastUpdateTime, indicating the MetaClient
   * update local cache.
   *
   * @param kv Where to save
   * @param hostAddr Which host to save
   * @param info Information of the host
   * @param leaderParts Leader info of all parts, null means don't need to update leader info
   * @return
   */
  static nebula::cpp2::ErrorCode updateHostInfo(kvstore::KVStore* kv,
                                                const HostAddr& hostAddr,
                                                const HostInfo& info,
                                                std::vector<kvstore::KV>& data,
                                                const AllLeaders* leaderParts = nullptr);

  /**
   * @brief Check if the host is registered
   *
   * @param kv From where to get
   * @param hostAddr Which host to register
   * @return
   */
  static bool machineRegistered(kvstore::KVStore* kv, const HostAddr& hostAddr);

  /**
   * @brief Get all registered host
   *
   * @param kv From where to get
   * @param expiredTTL Ignore hosts who do not send heartbeat within longer than expiredTTL
   * @param role Which role of the host to find. maybe GRAPH META STORAGE LISTENER AGENT
   * @return
   */
  static ErrorOr<nebula::cpp2::ErrorCode, std::vector<HostAddr>> getActiveHosts(
      kvstore::KVStore* kv, int32_t expiredTTL = 0, cpp2::HostRole role = cpp2::HostRole::STORAGE);

  /**
   * @brief Get services in the agent host
   *
   * @param kv From where to get
   * @param hostname Hostname or ip
   * @return
   */
  static ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::pair<HostAddr, cpp2::HostRole>>>
  getServicesInHost(kvstore::KVStore* kv, const std::string& hostname);

  /**
   * @brief Get hosts in the zone
   *
   * @param kv From where to get
   * @param zoneName Name of the zone
   * @param expiredTTL Ignore hosts who do not send heartbeat within longer than expiredTTL
   * @return
   */
  static ErrorOr<nebula::cpp2::ErrorCode, std::vector<HostAddr>> getActiveHostsInZone(
      kvstore::KVStore* kv, const std::string& zoneName, int32_t expiredTTL = 0);

  /**
   * @brief Get hosts in the space
   *
   * @param kv From where to get
   * @param spaceId Id of the space
   * @param expiredTTL Ignore hosts who do not send heartbeat within longer than expiredTTL
   * @return
   */
  static ErrorOr<nebula::cpp2::ErrorCode, std::vector<HostAddr>> getActiveHostsWithZones(
      kvstore::KVStore* kv, GraphSpaceID spaceId, int32_t expiredTTL = 0);

  /**
   * @brief Check if a host is alived, by checking if the host send heartbeat within the default
   * time
   *
   * @param kv From where to get the host's information
   * @param host Which host to check
   * @return
   */
  static ErrorOr<nebula::cpp2::ErrorCode, bool> isLived(kvstore::KVStore* kv, const HostAddr& host);

  /**
   * @brief Get hostInfo for a host
   *
   * @param kv From where to get
   * @param host
   * @return
   */
  static ErrorOr<nebula::cpp2::ErrorCode, HostInfo> getHostInfo(kvstore::KVStore* kv,
                                                                const HostAddr& host);

 protected:
  ActiveHostsMan() = default;
};

class LastUpdateTimeMan final {
 public:
  ~LastUpdateTimeMan() = default;

  static void update(std::vector<kvstore::KV>& data, const int64_t timeInMilliSec);

  static void update(kvstore::BatchHolder* batchHolder, const int64_t timeInMilliSec);

 protected:
  LastUpdateTimeMan() = default;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_ACTIVEHOSTSMAN_H_
