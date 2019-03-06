/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_BASEPROCESSOR_H_
#define META_BASEPROCESSOR_H_

#include "base/Base.h"
#include <mutex>
#include <folly/futures/Promise.h>
#include <folly/futures/Future.h>
#include <folly/RWSpinLock.h>
#include "interface/gen-cpp2/meta_types.h"
#include "base/StatusOr.h"
#include "time/Duration.h"
#include "kvstore/KVStore.h"
#include "meta/MetaUtils.h"

namespace nebula {
namespace meta {

class LockUtils {
public:
    LockUtils() = delete;
#define GENERATE_LOCK(E) \
    static folly::RWSpinLock& E##Lock() { \
        static folly::RWSpinLock l; \
        return l; \
    }

GENERATE_LOCK(space);
GENERATE_LOCK(id);
GENERATE_LOCK(tag);

#undef GENERATE_LOCK
};

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

    template<class T>
    typename std::enable_if<std::is_integral<T>::value, cpp2::ID>::type
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
    bool spaceExist(GraphSpaceID spaceId);

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

