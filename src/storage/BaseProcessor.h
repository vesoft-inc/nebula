/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef STORAGE_BASEPROCESSOR_H_
#define STORAGE_BASEPROCESSOR_H_

#include "base/Base.h"
#include <folly/futures/Promise.h>
#include <folly/futures/Future.h>
#include "kvstore/include/KVStore.h"

namespace nebula {
namespace storage {

template<typename REQ, typename RESP>
class BaseProcessor {
public:
    BaseProcessor(kvstore::KVStore* kvstore)
        : kvstore_(kvstore) {}

    virtual ~BaseProcessor() = default;

    folly::Future<RESP> getFuture() {
        return promise_.getFuture();
    }

    virtual void process(const REQ& req) = 0;

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
        case kvstore::ResultCode::SUCCESSED:
            return cpp2::ErrorCode::SUCCEEDED;
        case kvstore::ResultCode::ERR_LEADER_CHANAGED:
            return cpp2::ErrorCode::E_LEADER_CHANGED;
        case kvstore::ResultCode::ERR_SPACE_NOT_FOUND:
            return cpp2::ErrorCode::E_SPACE_NOT_FOUND;
        case kvstore::ResultCode::ERR_PART_NOT_FOUND:
            return cpp2::ErrorCode::E_PART_NOT_FOUND;
        default:
            return cpp2::ErrorCode::E_UNKNOWN;
        }
    }

protected:
    kvstore::KVStore* kvstore_ = nullptr;
    RESP resp_;
    folly::Promise<RESP> promise_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_BASEPROCESSOR_H_
