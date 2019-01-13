/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

template<typename RESP>
cpp2::ErrorCode BaseProcessor<RESP>::to(kvstore::ResultCode code) {
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

template<typename RESP>
void BaseProcessor<RESP>::doPut(GraphSpaceID spaceId,
                                PartitionID partId,
                                std::vector<kvstore::KV> data) {
    this->kvstore_->asyncMultiPut(spaceId, partId, std::move(data),
                                  [partId, this](kvstore::ResultCode code, HostAddr addr) {
        cpp2::ResultCode thriftResult;
        thriftResult.code = to(code);
        thriftResult.part_id = partId;
        if (code == kvstore::ResultCode::ERR_LEADER_CHANAGED) {
            thriftResult.get_leader()->ip = addr.first;
            thriftResult.get_leader()->port = addr.second;
        }
        bool finished = false;
        {
            std::lock_guard<folly::SpinLock> lg(this->lock_);
            this->codes_.emplace_back(std::move(thriftResult));
            this->callingNum_--;
            if (this->callingNum_ == 0) {
                this->resp_.set_codes(std::move(this->codes_));
                this->resp_.set_latency_in_ms(this->duration_.elapsedInMSec());
                finished = true;
            }
        }
        if (finished) {
            this->onFinished();
        }
    });
}


}  // namespace storage
}  // namespace nebula
