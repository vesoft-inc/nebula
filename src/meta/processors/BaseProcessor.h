/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_BASEPROCESSOR_H_
#define META_BASEPROCESSOR_H_

#include "common/base/Base.h"
#include "common/charset/Charset.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "common/time/Duration.h"
#include "common/network/NetworkUtils.h"
#include <folly/futures/Promise.h>
#include <folly/futures/Future.h>
#include <folly/SharedMutex.h>
#include <thrift/lib/cpp/util/EnumUtils.h>
#include "kvstore/KVStore.h"
#include "meta/MetaServiceUtils.h"
#include "meta/common/MetaCommon.h"
#include "meta/processors/Common.h"
#include "meta/ActiveHostsMan.h"

namespace nebula {
namespace meta {

using nebula::network::NetworkUtils;
using FieldType = std::pair<std::string, cpp2::PropertyType>;
using SignType = storage::cpp2::EngineSignType;

#define CHECK_SPACE_ID_AND_RETURN(spaceID) \
    auto retSpace = spaceExist(spaceID); \
    if (retSpace != nebula::cpp2::ErrorCode::SUCCEEDED) { \
        handleErrorCode(retSpace); \
        onFinished(); \
        return; \
    }

/**
 * Check segment is consist of numbers and letters and should not empty.
 * */
#define CHECK_SEGMENT(segment) \
    if (!MetaCommon::checkSegment(segment)) { \
        handleErrorCode(nebula::cpp2::ErrorCode::E_STORE_SEGMENT_ILLEGAL); \
        onFinished(); \
        return; \
    }

template<typename RESP>
class BaseProcessor {
public:
    explicit BaseProcessor(kvstore::KVStore* kvstore)
            : kvstore_(kvstore) {}

    virtual ~BaseProcessor() = default;

    folly::Future<RESP> getFuture() {
        return promise_.getFuture();
    }

protected:
    /**
     * Destroy current instance when finished.
     * */
    virtual void onFinished() {
        promise_.setValue(std::move(resp_));
        delete this;
    }

    void handleErrorCode(nebula::cpp2::ErrorCode code,
                         GraphSpaceID spaceId = kDefaultSpaceId,
                         PartitionID partId = kDefaultPartId) {
        resp_.set_code(code);
        if (code == nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
            handleLeaderChanged(spaceId, partId);
        }
    }

    void handleLeaderChanged(GraphSpaceID spaceId, PartitionID partId) {
        auto leaderRet = kvstore_->partLeader(spaceId, partId);
        if (ok(leaderRet)) {
            resp_.set_leader(toThriftHost(nebula::value(leaderRet)));
        } else {
            resp_.set_code(nebula::error(leaderRet));
        }
    }

    template<class T>
    std::enable_if_t<std::is_integral<T>::value, cpp2::ID>
    to(T id, EntryType type) {
        cpp2::ID thriftID;
        switch (type) {
        case EntryType::SPACE:
            thriftID.set_space_id(static_cast<GraphSpaceID>(id));
            break;
        case EntryType::TAG:
            thriftID.set_tag_id(static_cast<TagID>(id));
            break;
        case EntryType::EDGE:
            thriftID.set_edge_type(static_cast<EdgeType>(id));
            break;
        case EntryType::INDEX:
            thriftID.set_index_id(static_cast<IndexID>(id));
            break;
        case EntryType::CONFIG:
        case EntryType::GROUP:
        case EntryType::ZONE:
            break;
        }
        return thriftID;
    }

    HostAddr toThriftHost(const HostAddr& host) {
        return host;
    }

    /**
     * General put function.
     * */
    void doPut(std::vector<kvstore::KV> data);

    ErrorOr<nebula::cpp2::ErrorCode, std::unique_ptr<kvstore::KVIterator>>
    doPrefix(const std::string& key);

    /**
     * General get function.
     * */
    ErrorOr<nebula::cpp2::ErrorCode, std::string>
    doGet(const std::string& key);

    /**
     * General multi get function.
     * */
    ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>>
    doMultiGet(const std::vector<std::string>& keys);

    /**
     * General remove function.
     * */
    void doRemove(const std::string& key);

    /**
     * Remove keys from start to end, doesn't contain end.
     * */
    void doRemoveRange(const std::string& start, const std::string& end);

    /**
     * Scan keys from start to end, doesn't contain end.
     * */
    ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>>
    doScan(const std::string& start, const std::string& end);
     /**
     * General multi remove function.
     **/
     void doMultiRemove(std::vector<std::string> keys);

    /**
     * Get all hosts
     * */
    ErrorOr<nebula::cpp2::ErrorCode, std::vector<HostAddr>> allHosts();

    /**
     * Get one auto-increment Id.
     * */
    ErrorOr<nebula::cpp2::ErrorCode, int32_t> autoIncrementId();

    /**
     * Check spaceId exist or not.
     * */
    nebula::cpp2::ErrorCode spaceExist(GraphSpaceID spaceId);

    /**
     * Check user exist or not.
     **/
    nebula::cpp2::ErrorCode userExist(const std::string& account);

    /**
     * Check host has been registered or not.
     * */
    nebula::cpp2::ErrorCode hostExist(const std::string& hostKey);

    /**
     * Return the spaceId for name.
     * */
    ErrorOr<nebula::cpp2::ErrorCode, GraphSpaceID> getSpaceId(const std::string& name);

    /**
     * Return the tagId for name.
     */
    ErrorOr<nebula::cpp2::ErrorCode, TagID>
    getTagId(GraphSpaceID spaceId, const std::string& name);

    /**
     * Fetch the latest version tag's schema.
     */
    ErrorOr<nebula::cpp2::ErrorCode, cpp2::Schema>
    getLatestTagSchema(GraphSpaceID spaceId, const TagID tagId);

    /**
     * Return the edgeType for name.
     */
    ErrorOr<nebula::cpp2::ErrorCode, EdgeType>
    getEdgeType(GraphSpaceID spaceId, const std::string& name);

    /**
     * Fetch the latest version edge's schema.
     */
    ErrorOr<nebula::cpp2::ErrorCode, cpp2::Schema>
    getLatestEdgeSchema(GraphSpaceID spaceId, const EdgeType edgeType);

    ErrorOr<nebula::cpp2::ErrorCode, IndexID>
    getIndexID(GraphSpaceID spaceId, const std::string& indexName);

    ErrorOr<nebula::cpp2::ErrorCode, bool>
    checkPassword(const std::string& account, const std::string& password);

    nebula::cpp2::ErrorCode doSyncPut(std::vector<kvstore::KV> data);

    void doSyncPutAndUpdate(std::vector<kvstore::KV> data);

    void doSyncMultiRemoveAndUpdate(std::vector<std::string> keys);

    /**
     * Check the edge or tag contains indexes when alter it.
     **/
    nebula::cpp2::ErrorCode
    indexCheck(const std::vector<cpp2::IndexItem>& items,
               const std::vector<cpp2::AlterSchemaItem>& alterItems);

    nebula::cpp2::ErrorCode
    ftIndexCheck(const std::vector<std::string>& cols,
                 const std::vector<cpp2::AlterSchemaItem>& alterItems);

    ErrorOr<nebula::cpp2::ErrorCode, std::vector<cpp2::IndexItem>>
    getIndexes(GraphSpaceID spaceId, int32_t tagOrEdge);

    ErrorOr<nebula::cpp2::ErrorCode, cpp2::FTIndex>
    getFTIndex(GraphSpaceID spaceId, int32_t tagOrEdge);

    bool checkIndexExist(const std::vector<cpp2::IndexFieldDef>& fields,
                         const cpp2::IndexItem& item);

    ErrorOr<nebula::cpp2::ErrorCode, GroupID> getGroupId(const std::string& groupName);

    ErrorOr<nebula::cpp2::ErrorCode, ZoneID> getZoneId(const std::string& zoneName);

    nebula::cpp2::ErrorCode
    listenerExist(GraphSpaceID space, cpp2::ListenerType type);

    // A direct value of true means that data will not be written to follow via the raft protocol,
    // but will be written directly to local disk
    ErrorOr<nebula::cpp2::ErrorCode, bool>
    replaceHostInPartition(const HostAddr& ipv4From,
                           const HostAddr& ipv4To,
                           bool direct = false);

    ErrorOr<nebula::cpp2::ErrorCode, bool>
    replaceHostInZone(const HostAddr& ipv4From,
                      const HostAddr& ipv4To,
                      bool direct = false);

protected:
    kvstore::KVStore*         kvstore_ = nullptr;
    RESP                      resp_;
    folly::Promise<RESP>      promise_;
    time::Duration            duration_;

    static const int32_t      maxIndexLimit = 16;
};

}  // namespace meta
}  // namespace nebula

#include "meta/processors/BaseProcessor.inl"

#endif  // META_BASEPROCESSOR_H_

