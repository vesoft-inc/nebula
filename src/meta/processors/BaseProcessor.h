/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_BASEPROCESSOR_H_
#define META_BASEPROCESSOR_H_

#include "base/Base.h"
#include "base/StatusOr.h"
#include <folly/RWSpinLock.h>
#include <folly/futures/Promise.h>
#include <folly/futures/Future.h>
#include "kvstore/KVStore.h"
#include "meta/MetaUtils.h"
#include "interface/gen-cpp2/meta_types.h"
#include "time/Duration.h"

namespace nebula {
namespace meta {

enum IDType {
    SPACE,
    TAG,
    EDGE,
};

template<typename RESP>
class BaseProcessor {
public:
    BaseProcessor(kvstore::KVStore* kvstore, folly::RWSpinLock* lock)
            : kvstore_(kvstore)
            , lock_(lock) {}

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
        default:
            return cpp2::ErrorCode::SUCCEEDED;
        }
    }

    template<class T>
    typename std::enable_if<std::is_integral<T>::value, cpp2::ID>::type
    to(T id, IDType type) {
        cpp2::ID thriftID;
        switch (type) {
        case IDType::SPACE:
            thriftID.set_space_id(static_cast<GraphSpaceID>(id));
            break;
        case IDType::TAG:
            thriftID.set_tag_id(static_cast<TagID>(id));
            break;
        case IDType::EDGE:
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
     * Check space_name exists or not, if existed, return the id.
     * */
    StatusOr<GraphSpaceID> spaceExist(const std::string& name);

protected:
    kvstore::KVStore* kvstore_ = nullptr;
    folly::RWSpinLock* lock_ = nullptr;
    RESP resp_;
    folly::Promise<RESP> promise_;
    const PartitionID kDefaultPartId_ = 0;
    const GraphSpaceID kDefaultSpaceId_ = 0;
};

}  // namespace meta
}  // namespace nebula

#include "meta/processors/BaseProcessor.inl"

#endif  // META_BASEPROCESSOR_H_

