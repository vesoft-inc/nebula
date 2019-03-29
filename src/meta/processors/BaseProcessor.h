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
#include "interface/gen-cpp2/meta_types.h"
#include "base/StatusOr.h"
#include "time/Duration.h"
#include "kvstore/KVStore.h"
#include "meta/MetaUtils.h"

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
     * General get function.
     * When an error occurs, return Get Failed
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

    /**
     * Check key if start with some prefix used by meta server.
     * */
    bool checkRetainedPrefix(const std::string& key);

protected:
    kvstore::KVStore* kvstore_ = nullptr;
    std::unique_ptr<std::lock_guard<std::mutex>> guard_;
    RESP resp_;
    folly::Promise<RESP> promise_;
    const PartitionID kDefaultPartId_ = 0;
    const GraphSpaceID kDefaultSpaceId_ = 0;
    static std::mutex lock_;
};

template<typename RESP>
std::mutex BaseProcessor<RESP>::lock_;

}  // namespace meta
}  // namespace nebula

#include "meta/processors/BaseProcessor.inl"

#endif  // META_BASEPROCESSOR_H_

