/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_METACACHE_H_
#define META_METACACHE_H_

#include "interface/gen-cpp2/meta_stream_types.h"
#include "kvstore/KVStore.h"
#include "meta/MetaVersionMan.h"
#include "meta/PubManager.h"

namespace nebula {
namespace meta {

class HostInfo final {
private:
  cpp2::HostRole role_{cpp2::HostRole::UNKNOWN};
  cpp2::HostStatus status_{cpp2::HostStatus::ALIVE};

  // version of the execuable
  std::string execVer_;
  std::string gitSha_;

  int64_t lastHBTimeInMilliSec_{0};

public:
  /* Constructors */
  HostInfo() = default;
  HostInfo(const HostInfo& info) = default;

  explicit HostInfo(int64_t lastHBTimeInMilliSec) : lastHBTimeInMilliSec_(lastHBTimeInMilliSec) {}

  HostInfo(int64_t lastHBTimeInMilliSec, cpp2::HostRole role, std::string gitSha)
      : role_(role)
      , gitSha_(std::move(gitSha))
      , lastHBTimeInMilliSec_(lastHBTimeInMilliSec) {}

  /* Comparator */
  bool operator==(const HostInfo& rhs) const;

  /* Accessors */
  const cpp2::HostRole& getRole() const;
  HostInfo& setRole(cpp2::HostRole role);
  const cpp2::HostStatus& getStatus() const;
  HostInfo& setStatus(cpp2::HostStatus status);
  const std::string& getExecVer() const;
  HostInfo& setExecVer(const std::string& ver);
  const std::string& getGitSha() const;
  HostInfo& setGitSha(const std::string& sha);
  int64_t getLastHBTimeInMilliSec() const;
  HostInfo& setLastHBTimeInMilliSec(int64_t ms);

  /* Codec */
  static std::string encode(const HostInfo& info);
  static HostInfo decode(const folly::StringPiece& data);

  static HostInfo decodeV1(const folly::StringPiece& data);
  static HostInfo decodeV2(const folly::StringPiece& data);
  static HostInfo decodeV3(const folly::StringPiece& data);
};


/******************************************************************************
 *
 * In-memory cache for all meta information
 *
 * All read from the meta server should now from this cache. This will reduce
 * the load on meta server's storage and speed up the read requests.
 *
 * All write to the meta server will now update the cache right after it
 * succeeds in persisting the information. But we need to make sure the update
 * to both storage and cache is atomic and transactional
 *
 ******************************************************************************/
class MetaCache final {
private:
  // In-memory cache of all meta and status information
  MetaVersion keyVersion_;
  int64_t revision_;

  std::shared_ptr<cpp2::MetaInfo> metaInfo_;
  std::shared_ptr<cpp2::StatusInfo> statusInfo_;
  std::shared_ptr<std::unordered_map<HostAddr, HostInfo>> hostInfo_;

  // Accessory index to speed up lookup
  //   space_name => space_id
  std::unordered_map<std::string, GraphSpaceID> spaceNameIdMap_;
  //   Space_id, index_name => index_id
  std::unordered_map<GraphSpaceID, std::unordered_map<std::string, IndexID>> indexNameIdMap_;

  kvstore::KVStore* store_;

  // Publisher manager
  PubManager pubMan_;

  // Make sure exclusive access
  std::shared_mutex mutex_;

public:
  explicit MetaCache(kvstore::KVStore* store) : store_(store) {
    loadMetaData();
  }

  // Load persisted meta data
  void loadMetaData();

  // Convert graph space name to graph space id
  ErrorOr<nebula::cpp2::ErrorCode, GraphSpaceID> toSpaceId(const std::string& name);
  // Convert index name to index id
  ErrorOr<nebula::cpp2::ErrorCode, IndexID> toIndexId(const std::string& name);

  void addPublisher(::apache::thrift::ServerStreamPublisher<cpp2::MetaData>&& publisher,
                    folly::EventBase* evb,
                    int64_t lastRevision);

private:
  ErrorOr<nebula::cpp2::ErrorCode, GraphSpaceID> toSpaceIdInternal(const std::string& name);

  ErrorOr<nebula::cpp2::ErrorCode, std::unique_ptr<kvstore::KVIterator>> iterByPrefix(
      const std::string& prefix) const;

  void loadGraphSpaces();
  void loadTagSchemas();
  void loadEdgeSchemas();
  void loadTagIndices();
  void loadEdgeIndices();
  void loadFullTextIndices();
  void loadFullTextClients();
  void loadUsers();
  void loadUserRoles();
  void loadPartAlloc();
  void loadConfig();
  void loadListeners();

  void loadLeaderStatus();

  void loadHostInfo();
};

}  // namespace meta
}  // namespace nebula
#endif  // META_METACACHE_H_
