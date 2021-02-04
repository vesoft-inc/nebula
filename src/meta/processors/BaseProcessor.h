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
#include "common/base/StatusOr.h"
#include "common/time/Duration.h"
#include "common/network/NetworkUtils.h"
#include <folly/futures/Promise.h>
#include <folly/futures/Future.h>
#include <folly/SharedMutex.h>
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
    if (spaceExist(spaceID) == Status::SpaceNotFound()) { \
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND); \
        onFinished(); \
        return; \
    }

/**
 * Check segment is consist of numbers and letters and should not empty.
 * */
#define CHECK_SEGMENT(segment) \
    if (!MetaCommon::checkSegment(segment)) { \
        handleErrorCode(cpp2::ErrorCode::E_STORE_SEGMENT_ILLEGAL); \
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

    void handleErrorCode(cpp2::ErrorCode code, GraphSpaceID spaceId = kDefaultSpaceId,
                         PartitionID partId = kDefaultPartId) {
        resp_.set_code(code);
        if (code == cpp2::ErrorCode::E_LEADER_CHANGED) {
            handleLeaderChanged(spaceId, partId);
        }
    }

    void handleLeaderChanged(GraphSpaceID spaceId, PartitionID partId) {
        auto leaderRet = kvstore_->partLeader(spaceId, partId);
        if (ok(leaderRet)) {
            resp_.set_leader(toThriftHost(nebula::value(leaderRet)));
        } else {
            resp_.set_code(MetaCommon::to(nebula::error(leaderRet)));
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

    StatusOr<std::unique_ptr<kvstore::KVIterator>> doPrefix(const std::string& key);

    /**
     * General get function.
     * */
    StatusOr<std::string> doGet(const std::string& key);

    /**
     * General multi get function.
     * */
    StatusOr<std::vector<std::string>> doMultiGet(const std::vector<std::string>& keys);

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
     StatusOr<std::vector<std::string>> doScan(const std::string& start,
                                               const std::string& end);
     /**
     * General multi remove function.
     **/
     void doMultiRemove(std::vector<std::string> keys);

    /**
     * Get all hosts
     * */
    StatusOr<std::vector<HostAddr>> allHosts();

    /**
     * Get one auto-increment Id.
     * */
    ErrorOr<cpp2::ErrorCode, int32_t> autoIncrementId();

    /**
     * Check spaceId exist or not.
     * */
    Status spaceExist(GraphSpaceID spaceId);

    /**
     * Check user exist or not.
     **/
    Status userExist(const std::string& account);

    /**
     * Check host has been registered or not.
     * */
    Status hostExist(const std::string& hostKey);

    /**
     * Return the spaceId for name.
     * */
    StatusOr<GraphSpaceID> getSpaceId(const std::string& name);

    /**
     * Return the tagId for name.
     */
    StatusOr<TagID> getTagId(GraphSpaceID spaceId, const std::string& name);

    /**
     * Fetch the latest version tag's schema.
     */
    StatusOr<cpp2::Schema>
    getLatestTagSchema(GraphSpaceID spaceId, const TagID tagId);

    /**
     * Check if tag or edge has ttl
     */
    bool tagOrEdgeHasTTL(const cpp2::Schema& latestSchema);

    /**
     * Return the edgeType for name.
     */
    StatusOr<EdgeType> getEdgeType(GraphSpaceID spaceId, const std::string& name);

    /**
     * Fetch the latest version edge's schema.
     */
    StatusOr<cpp2::Schema>
    getLatestEdgeSchema(GraphSpaceID spaceId, const EdgeType edgeType);

    StatusOr<IndexID> getIndexID(GraphSpaceID spaceId, const std::string& indexName);

    bool checkPassword(const std::string& account, const std::string& password);

    kvstore::ResultCode doSyncPut(std::vector<kvstore::KV> data);

    void doSyncPutAndUpdate(std::vector<kvstore::KV> data);

    void doSyncMultiRemoveAndUpdate(std::vector<std::string> keys);

    /**
     * Check the edge or tag contains indexes when alter it.
     **/
    cpp2::ErrorCode indexCheck(const std::vector<cpp2::IndexItem>& items,
                               const std::vector<cpp2::AlterSchemaItem>& alterItems);

    StatusOr<std::vector<cpp2::IndexItem>>
    getIndexes(GraphSpaceID spaceId, int32_t tagOrEdge);

    bool checkIndexExist(const std::vector<cpp2::IndexFieldDef>& fields,
                         const cpp2::IndexItem& item);

    StatusOr<GroupID> getGroupId(const std::string& groupName);

    StatusOr<ZoneID> getZoneId(const std::string& zoneName);

    Status listenerExist(GraphSpaceID space, cpp2::ListenerType type);

protected:
    kvstore::KVStore* kvstore_ = nullptr;
    RESP resp_;
    folly::Promise<RESP> promise_;
    time::Duration duration_;
};

}  // namespace meta
}  // namespace nebula

#include "meta/processors/BaseProcessor.inl"

#endif  // META_BASEPROCESSOR_H_

