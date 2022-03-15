/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_BASEPROCESSOR_H_
#define META_BASEPROCESSOR_H_

#include <folly/SharedMutex.h>
#include <folly/futures/Future.h>
#include <folly/futures/Promise.h>
#include <thrift/lib/cpp/util/EnumUtils.h>

#include "common/base/Base.h"
#include "common/charset/Charset.h"
#include "common/network/NetworkUtils.h"
#include "common/time/Duration.h"
#include "common/utils/MetaKeyUtils.h"
#include "interface/gen-cpp2/storage_types.h"
#include "kvstore/KVStore.h"
#include "meta/ActiveHostsMan.h"
#include "meta/MetaServiceUtils.h"
#include "meta/processors/Common.h"

namespace nebula {
namespace meta {

using nebula::network::NetworkUtils;
using FieldType = std::pair<std::string, nebula::cpp2::PropertyType>;
using SignType = storage::cpp2::EngineSignType;

#define CHECK_SPACE_ID_AND_RETURN(spaceID)              \
  auto retSpace = spaceExist(spaceID);                  \
  if (retSpace != nebula::cpp2::ErrorCode::SUCCEEDED) { \
    handleErrorCode(retSpace);                          \
    onFinished();                                       \
    return;                                             \
  }

#define CHECK_CODE_AND_BREAK()                      \
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) { \
    break;                                          \
  }

template <typename RESP>
class BaseProcessor {
 public:
  explicit BaseProcessor(kvstore::KVStore* kvstore) : kvstore_(kvstore) {}

  virtual ~BaseProcessor() = default;

  folly::Future<RESP> getFuture() {
    return promise_.getFuture();
  }

 protected:
  /**
   * @brief Destroy current instance when finished.
   *
   */
  virtual void onFinished() {
    promise_.setValue(std::move(resp_));
    delete this;
  }

  /**
   * @brief Set error code and handle leader changed.
   *
   * @param code
   */
  void handleErrorCode(nebula::cpp2::ErrorCode code) {
    resp_.code_ref() = code;
    if (code == nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
      handleLeaderChanged();
    }
  }

  /**
   * @brief Set leader address to reponse.
   *
   */
  void handleLeaderChanged() {
    auto leaderRet = kvstore_->partLeader(kDefaultSpaceId, kDefaultPartId);
    if (ok(leaderRet)) {
      resp_.leader_ref() = toThriftHost(nebula::value(leaderRet));
    } else {
      resp_.code_ref() = nebula::error(leaderRet);
    }
  }

  template <class T>
  std::enable_if_t<std::is_integral<T>::value, cpp2::ID> to(T id, EntryType type) {
    cpp2::ID thriftID;
    switch (type) {
      case EntryType::SPACE:
        thriftID.space_id_ref() = static_cast<GraphSpaceID>(id);
        break;
      case EntryType::TAG:
        thriftID.tag_id_ref() = static_cast<TagID>(id);
        break;
      case EntryType::EDGE:
        thriftID.edge_type_ref() = static_cast<EdgeType>(id);
        break;
      case EntryType::INDEX:
        thriftID.index_id_ref() = static_cast<IndexID>(id);
        break;
      case EntryType::CONFIG:
      case EntryType::ZONE:
        break;
    }
    return thriftID;
  }

  HostAddr toThriftHost(const HostAddr& host) {
    return host;
  }

  /**
   * @brief Put data to kvstore synchronously without side effect.
   *
   * @tparam RESP
   * @param data
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode doSyncPut(std::vector<kvstore::KV> data);

  /**
   * @brief Get the iterator on given meta prefix.
   *
   * @tparam RESP
   * @param key
   * @param canReadFromFollower read from follower's local
   * @return ErrorOr<nebula::cpp2::ErrorCode, std::unique_ptr<kvstore::KVIterator>>
   */
  ErrorOr<nebula::cpp2::ErrorCode, std::unique_ptr<kvstore::KVIterator>> doPrefix(
      const std::string& key, bool canReadFromFollower = false);

  /**
   * @brief Get the given key's value
   *
   * @tparam RESP
   * @param key
   * @return ErrorOr<nebula::cpp2::ErrorCode, std::string>
   */
  ErrorOr<nebula::cpp2::ErrorCode, std::string> doGet(const std::string& key);

  /**
   * @brief Multiple get for given keys.
   *
   * @tparam RESP
   * @param keys
   * @return ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>>
   */
  ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>> doMultiGet(
      const std::vector<std::string>& keys);

  /**
   * @brief Remove given keys from kv store.
   *        Note that it has side effect: it will set the code to the resp_,
   *        and delete the processor instance. So, it could be only used
   *        in the Processor:process() end.
   *
   * @tparam RESP
   * @param key
   */
  void doRemove(const std::string& key);

  /**
   * @brief Range remove.
   *        Note that it has side effect: it will set the code to the resp_,
   *        and delete the processor instance. So, it could be only used
   *        in the Processor:process() end.
   *
   * @tparam RESP
   * @param start
   * @param end
   */
  void doRemoveRange(const std::string& start, const std::string& end);

  /**
   * @brief Batch general operation.
   *        Note that it has side effect: it will set the code to the resp_,
   *        and delete the processor instance. So, it could be only used
   *        in the Processor:process() end.
   *
   * @tparam RESP
   * @param batchOp
   */
  void doBatchOperation(std::string batchOp);

  /**
   * @brief Increment global auto-incremental id and then return it.
   *        Note that it has side effect: it will set the code to the resp_.
   *
   * @tparam RESP
   * @return ErrorOr<nebula::cpp2::ErrorCode, int32_t>
   */
  ErrorOr<nebula::cpp2::ErrorCode, int32_t> autoIncrementId();

  /**
   * @brief Get global incremental id without incrementing it.
   *
   * @tparam RESP
   * @return ErrorOr<nebula::cpp2::ErrorCode, int32_t>
   */
  ErrorOr<nebula::cpp2::ErrorCode, int32_t> getAvailableGlobalId();

  /**
   * @brief Increment space auto-incremental id and then return it.
   *
   * @tparam RESP
   * @param spaceId
   * @return ErrorOr<nebula::cpp2::ErrorCode, int32_t>
   */
  ErrorOr<nebula::cpp2::ErrorCode, int32_t> autoIncrementIdInSpace(GraphSpaceID spaceId);

  /**
   * @brief Check if given space exist or not.
   *
   * @tparam RESP
   * @param spaceId
   * @return SUCCEEDED means exist, E_SPACE_NOT_FOUND means not, others means error.
   */
  nebula::cpp2::ErrorCode spaceExist(GraphSpaceID spaceId);

  /**
   * @brief Check if given user exist or not.
   *
   * @tparam RESP
   * @param account
   * @return SUCCEEDED means exist, E_USER_NOT_FOUND means not, others means error.
   */
  nebula::cpp2::ErrorCode userExist(const std::string& account);

  /**
   * @brief Check if given machine exist.
   *
   * @tparam RESP
   * @param machineKey
   * @return SUCCEEDED means exist, E_NOT_FOUND means not, others means error.
   */
  nebula::cpp2::ErrorCode machineExist(const std::string& machineKey);

  /**
   * @brief Check if given host exist.
   *
   * @tparam RESP
   * @param hostKey
   * @return SUCCEEDED means exist, E_NOT_FOUND means not, others means error.
   */
  nebula::cpp2::ErrorCode hostExist(const std::string& hostKey);

  /**
   * @brief Check the hosts not exist in all zones.
   *
   * @tparam RESP
   * @param hosts
   * @return nebula::cpp1::ErrorCode
   */
  nebula::cpp2::ErrorCode includeByZone(const std::vector<HostAddr>& hosts);

  /**
   * @brief Get space id by space name
   *
   * @tparam RESP
   * @param name
   * @return ErrorOr<nebula::cpp2::ErrorCode, GraphSpaceID>
   */
  ErrorOr<nebula::cpp2::ErrorCode, GraphSpaceID> getSpaceId(const std::string& name);

  /**
   * @brief Get tag id by space id and tag name
   *
   * @tparam RESP
   * @param spaceId
   * @param name tag name
   * @return ErrorOr<nebula::cpp2::ErrorCode, TagID>
   */
  ErrorOr<nebula::cpp2::ErrorCode, TagID> getTagId(GraphSpaceID spaceId, const std::string& name);

  /**
   * @brief Fetch the latest version tag's schema by space id and tag id
   *
   * @tparam RESP
   * @param spaceId
   * @param tagId
   * @return ErrorOr<nebula::cpp2::ErrorCode, cpp2::Schema>
   */
  ErrorOr<nebula::cpp2::ErrorCode, cpp2::Schema> getLatestTagSchema(GraphSpaceID spaceId,
                                                                    const TagID tagId);

  /**
   * @brief Get edge type by space id and edge name
   *
   * @tparam RESP
   * @param spaceId
   * @param name edge name
   * @return ErrorOr<nebula::cpp2::ErrorCode, EdgeType>
   */
  ErrorOr<nebula::cpp2::ErrorCode, EdgeType> getEdgeType(GraphSpaceID spaceId,
                                                         const std::string& name);

  /**
   * @brief Fetch the latest version edge's schema by space id and tag id
   *
   * @tparam RESP
   * @param spaceId
   * @param edgeType
   * @return ErrorOr<nebula::cpp2::ErrorCode, cpp2::Schema>
   */
  ErrorOr<nebula::cpp2::ErrorCode, cpp2::Schema> getLatestEdgeSchema(GraphSpaceID spaceId,
                                                                     const EdgeType edgeType);

  /**
   * @brief Get index id by space id and index name
   *
   * @tparam RESP
   * @param spaceId
   * @param indexName
   * @return ErrorOr<nebula::cpp2::ErrorCode, IndexID>
   */
  ErrorOr<nebula::cpp2::ErrorCode, IndexID> getIndexID(GraphSpaceID spaceId,
                                                       const std::string& indexName);

  /**
   * @brief Check if password identical or not.
   *
   * @tparam RESP
   * @param account
   * @param password
   * @return ErrorOr<nebula::cpp2::ErrorCode, bool>
   */
  ErrorOr<nebula::cpp2::ErrorCode, bool> checkPassword(const std::string& account,
                                                       const std::string& password);

  /**
   * @brief Check if tag/edge contains index when alter it.
   *
   * @tparam RESP
   * @param items
   * @param alterItems
   * @return ErrorCode::E_CONFLICT if contains
   */
  nebula::cpp2::ErrorCode indexCheck(const std::vector<cpp2::IndexItem>& items,
                                     const std::vector<cpp2::AlterSchemaItem>& alterItems);

  /**
   * @brief Check if tag/edge containes full text index when alter it.
   *
   * @tparam RESP
   * @param cols
   * @param alterItems
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode ftIndexCheck(const std::vector<std::string>& cols,
                                       const std::vector<cpp2::AlterSchemaItem>& alterItems);

  /**
   * @brief List all tag/edge index for given space and tag/edge id.
   *
   * @tparam RESP
   * @param spaceId
   * @param tagOrEdge tag id or edge id
   * @return ErrorOr<nebula::cpp2::ErrorCode, std::vector<cpp2::IndexItem>>
   */
  ErrorOr<nebula::cpp2::ErrorCode, std::vector<cpp2::IndexItem>> getIndexes(GraphSpaceID spaceId,
                                                                            int32_t tagOrEdge);

  /**
   * @brief Get all full text indexes for given space and tag/edge id.
   *
   * @tparam RESP
   * @param spaceId
   * @param tagOrEdge
   * @return ErrorOr<nebula::cpp2::ErrorCode, cpp2::FTIndex>
   */
  ErrorOr<nebula::cpp2::ErrorCode, cpp2::FTIndex> getFTIndex(GraphSpaceID spaceId,
                                                             int32_t tagOrEdge);

  /**
   * @brief Check if index on given fields alredy exist.
   *
   * @tparam RESP
   * @param fields
   * @param item
   * @return true
   * @return false
   */
  bool checkIndexExist(const std::vector<cpp2::IndexFieldDef>& fields, const cpp2::IndexItem& item);

  /**
   * @brief Get zone id by zone name
   *
   * @tparam RESP
   * @param zoneName
   * @return ErrorOr<nebula::cpp2::ErrorCode, ZoneID>
   */
  ErrorOr<nebula::cpp2::ErrorCode, ZoneID> getZoneId(const std::string& zoneName);

  /**
   * @brief Check if given space exist given type's listener.
   *
   * @tparam RESP
   * @param space
   * @param type
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode listenerExist(GraphSpaceID space, cpp2::ListenerType type);

  /**
   * @brief Used in BR, after restore meta data, it will replace partition info from
   *        current ip to backup ip.
   *
   * @param ipv4From
   * @param ipv4To
   * @param direct A direct value of true means that data will not be written to follow via
   *               the raft protocol, but will be written directly to local disk
   * @return ErrorOr<nebula::cpp2::ErrorCode, bool>
   */
  ErrorOr<nebula::cpp2::ErrorCode, bool> replaceHostInPartition(const HostAddr& ipv4From,
                                                                const HostAddr& ipv4To,
                                                                bool direct = false);

  /**
   * @brief Used in BR, after restore meta data, it will replace Zone info from
   *        current ip to backup ip.
   *
   * @param ipv4From
   * @param ipv4To
   * @param direct A direct value of true means that data will not be written to follow via
   *               the raft protocol, but will be written directly to local disk
   * @return ErrorOr<nebula::cpp2::ErrorCode, bool>
   */
  ErrorOr<nebula::cpp2::ErrorCode, bool> replaceHostInZone(const HostAddr& ipv4From,
                                                           const HostAddr& ipv4To,
                                                           bool direct = false);

  /**
   * @brief Get all parts' distribution information of a space.
   *
   * @param spaceId
   * @return ErrorOr<nebula::cpp2::ErrorCode, std::unordered_map<PartitionID,
   *         std::vector<HostAddr>>> map for part id -> peer hosts.
   */
  ErrorOr<nebula::cpp2::ErrorCode, std::unordered_map<PartitionID, std::vector<HostAddr>>>
  getAllParts(GraphSpaceID spaceId);

 protected:
  kvstore::KVStore* kvstore_ = nullptr;
  RESP resp_;
  folly::Promise<RESP> promise_;
  time::Duration duration_;

  static const int32_t maxIndexLimit = 16;
};

}  // namespace meta
}  // namespace nebula

#include "meta/processors/BaseProcessor-inl.h"

#endif  // META_BASEPROCESSOR_H_
