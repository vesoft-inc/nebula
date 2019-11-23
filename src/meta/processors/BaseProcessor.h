/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_BASEPROCESSOR_H_
#define META_BASEPROCESSOR_H_

#include "base/Base.h"
#include <folly/futures/Promise.h>
#include <folly/futures/Future.h>
#include <folly/SharedMutex.h>
#include "base/StatusOr.h"
#include "time/Duration.h"
#include "kvstore/KVStore.h"
#include "meta/MetaServiceUtils.h"
#include "meta/common/MetaCommon.h"
#include "network/NetworkUtils.h"
#include "meta/processors/Common.h"
#include "stats/Stats.h"

namespace nebula {
namespace meta {

using nebula::network::NetworkUtils;

#define CHECK_SPACE_ID_AND_RETURN(spaceID) \
    if (spaceExist(spaceID) == Status::SpaceNotFound()) { \
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND); \
        onFinished(); \
        return; \
    }

#define CHECK_USER_ID_AND_RETURN(userID) \
    if (userExist(userID) == Status::UserNotFound()) { \
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND); \
        onFinished(); \
        return; \
    }

/**
 * Check segment is consist of numbers and letters and should not empty.
 * */
#define CHECK_SEGMENT(segment) \
    if (!MetaCommon::checkSegment(segment)) { \
        resp_.set_code(cpp2::ErrorCode::E_STORE_SEGMENT_ILLEGAL); \
        onFinished(); \
        return; \
    }

template<typename RESP>
class BaseProcessor {
public:
    explicit BaseProcessor(kvstore::KVStore* kvstore, stats::Stats* stats = nullptr)
            : kvstore_(kvstore), stats_(stats) {}

    virtual ~BaseProcessor() = default;

    folly::Future<RESP> getFuture() {
        return promise_.getFuture();
    }

protected:
    /**
     * Destroy current instance when finished.
     * */
    void onFinished() {
        stats::Stats::addStatsValue(stats_,
                                    resp_.get_code() == cpp2::ErrorCode::SUCCEEDED,
                                    this->duration_.elapsedInUSec());
        promise_.setValue(std::move(resp_));
        delete this;
    }

    cpp2::ErrorCode to(kvstore::ResultCode code) {
        switch (code) {
        case kvstore::ResultCode::SUCCEEDED:
            return cpp2::ErrorCode::SUCCEEDED;
        case kvstore::ResultCode::ERR_KEY_NOT_FOUND:
            return cpp2::ErrorCode::E_NOT_FOUND;
        case kvstore::ResultCode::ERR_LEADER_CHANGED:
            return cpp2::ErrorCode::E_LEADER_CHANGED;
        default:
            return cpp2::ErrorCode::E_UNKNOWN;
        }
    }

    cpp2::ErrorCode to(const Status& status) {
        switch (status.code()) {
        case Status::kOk:
            return cpp2::ErrorCode::SUCCEEDED;
        case Status::kSpaceNotFound:
        case Status::kHostNotFound:
        case Status::kTagNotFound:
        case Status::kUserNotFound:
            return cpp2::ErrorCode::E_NOT_FOUND;
        default:
            return cpp2::ErrorCode::E_UNKNOWN;
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
        case EntryType::USER:
            thriftID.set_user_id(static_cast<UserID>(id));
        case EntryType::CONFIG:
            break;
        }
        return thriftID;
    }

    nebula::cpp2::HostAddr toThriftHost(const HostAddr& host) {
        nebula::cpp2::HostAddr tHost;
        tHost.set_ip(host.first);
        tHost.set_port(host.second);
        return tHost;
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
    void doRemoveRange(const std::string& start,
                       const std::string& end);

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
    StatusOr<std::vector<nebula::cpp2::HostAddr>> allHosts();

    /**
     * Get one auto-increment Id.
     * */
    ErrorOr<cpp2::ErrorCode, int32_t> autoIncrementId();

    /**
     * Check spaceId exist or not.
     * */
    Status spaceExist(GraphSpaceID spaceId);

    /**
     * Check userId exist or not.
     **/
    Status userExist(UserID userId);

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
     * Return the edgeType for name.
     */
    StatusOr<EdgeType> getEdgeType(GraphSpaceID spaceId, const std::string& name);

    StatusOr<UserID> getUserId(const std::string& account);

    bool checkPassword(UserID userId, const std::string& password);

    StatusOr<std::string> getUserAccount(UserID userId);

protected:
    kvstore::KVStore* kvstore_ = nullptr;
    RESP resp_;
    folly::Promise<RESP> promise_;
    stats::Stats* stats_ = nullptr;
    time::Duration duration_;
};

}  // namespace meta
}  // namespace nebula

#include "meta/processors/BaseProcessor.inl"

#endif  // META_BASEPROCESSOR_H_

