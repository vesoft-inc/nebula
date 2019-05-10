/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_BASEPROCESSOR_H_
#define META_BASEPROCESSOR_H_

#include "base/Base.h"
#include <folly/futures/Promise.h>
#include <folly/futures/Future.h>
#include <folly/SharedMutex.h>
#include "interface/gen-cpp2/meta_types.h"
#include "base/StatusOr.h"
#include "time/Duration.h"
#include "kvstore/KVStore.h"
#include "meta/MetaServiceUtils.h"
#include "meta/common/MetaCommon.h"
#include "network/NetworkUtils.h"

namespace nebula {
namespace meta {

using nebula::network::NetworkUtils;

class LockUtils {
public:
    LockUtils() = delete;
#define GENERATE_LOCK(Entry) \
    static folly::SharedMutex& Entry##Lock() { \
        static folly::SharedMutex l; \
        return l; \
    }

GENERATE_LOCK(space);
GENERATE_LOCK(id);
GENERATE_LOCK(tag);
GENERATE_LOCK(edge);

#undef GENERATE_LOCK
};

/**
 * Check segemnt is consist of numbers and letters and should not empty.
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
    void onFinished() {
        promise_.setValue(std::move(resp_));
        delete this;
    }

    cpp2::ErrorCode to(kvstore::ResultCode code) {
        switch (code) {
        case kvstore::ResultCode::SUCCEEDED:
            return cpp2::ErrorCode::SUCCEEDED;
        case kvstore::ResultCode::ERR_KEY_NOT_FOUND:
            return cpp2::ErrorCode::E_NOT_FOUND;
        default:
            return cpp2::ErrorCode::E_UNKNOWN;
        }
    }

    cpp2::ErrorCode to(Status status) {
        switch (status.code()) {
        case Status::kOk:
            return cpp2::ErrorCode::SUCCEEDED;
        case Status::kSpaceNotFound:
        case Status::kHostNotFound:
        case Status::kTagNotFound:
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
        }
        return thriftID;
    }

    /**
     * General put function.
     * */
    void doPut(std::vector<kvstore::KV> data);

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
    int32_t autoIncrementId();

    /**
     * Check spaceId exist or not.
     * */
    Status spaceExist(GraphSpaceID spaceId);

    /**
     * Check multi host_name exists or not.
     * */
    Status hostsExist(const std::vector<std::string>& name);

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

protected:
    kvstore::KVStore* kvstore_ = nullptr;
    RESP resp_;
    folly::Promise<RESP> promise_;
    const PartitionID kDefaultPartId_ = 0;
    const GraphSpaceID kDefaultSpaceId_ = 0;
};

}  // namespace meta
}  // namespace nebula

#include "meta/processors/BaseProcessor.inl"

#endif  // META_BASEPROCESSOR_H_

