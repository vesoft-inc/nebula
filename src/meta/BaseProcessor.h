/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_BASEPROCESSOR_H_
#define META_BASEPROCESSOR_H_

#include "base/Base.h"
#include <folly/RWSpinLock.h>
#include <folly/futures/Promise.h>
#include <folly/futures/Future.h>
#include "kvstore/include/KVStore.h"
#include "meta/MetaUtils.h"
#include "interface/gen-cpp2/meta_types.h"
#include "time/Duration.h"

namespace nebula {
namespace meta {

static const GraphSpaceID kDefaultSpaceId = 0L;
static const PartitionID  kDefaultPartId  = 0L;

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
        case kvstore::ResultCode::SUCCESSED:
            return cpp2::ErrorCode::SUCCEEDED;
        case kvstore::ResultCode::ERR_LEADER_CHANAGED:
            return cpp2::ErrorCode::E_LEADER_CHANGED;
        case kvstore::ResultCode::ERR_KEY_NOT_FOUND:
            return cpp2::ErrorCode::E_PARENT_NOT_FOUND;
        default:
            return cpp2::ErrorCode::E_UNKNOWN;
        }
    }

protected:
    kvstore::KVStore* kvstore_ = nullptr;
    folly::RWSpinLock* lock_ = nullptr;
    RESP resp_;
    folly::Promise<RESP> promise_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_BASEPROCESSOR_H_
