/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

template<typename RESP>
cpp2::ErrorCode BaseProcessor<RESP>::to(kvstore::ResultCode code) {
    switch (code) {
    case kvstore::ResultCode::SUCCEEDED:
        return cpp2::ErrorCode::SUCCEEDED;
    case kvstore::ResultCode::ERR_LEADER_CHANGED:
        return cpp2::ErrorCode::E_LEADER_CHANGED;
    case kvstore::ResultCode::ERR_SPACE_NOT_FOUND:
        return cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    case kvstore::ResultCode::ERR_PART_NOT_FOUND:
        return cpp2::ErrorCode::E_PART_NOT_FOUND;
    case kvstore::ResultCode::ERR_CONSENSUS_ERROR:
        return cpp2::ErrorCode::E_CONSENSUS_ERROR;
    default:
        return cpp2::ErrorCode::E_UNKNOWN;
    }
}

template <typename RESP>
void BaseProcessor<RESP>::handleAsync(GraphSpaceID spaceId,
                                      PartitionID partId,
                                      kvstore::ResultCode code) {
    VLOG(3) << "partId:" << partId << ", code:" << static_cast<int32_t>(code);

    bool finished = false;
    {
        std::lock_guard<std::mutex> lg(this->lock_);
        if (code != kvstore::ResultCode::SUCCEEDED) {
            if (code == kvstore::ResultCode::ERR_LEADER_CHANGED) {
                handleLeaderChanged(spaceId, partId);
            } else {
                pushResultCode(to(code), partId);
            }
        }
        this->callingNum_--;
        if (this->callingNum_ == 0) {
            finished = true;
        }
    }
    if (finished) {
        this->onFinished();
    }
}

template <typename RESP>
void BaseProcessor<RESP>::doPut(GraphSpaceID spaceId,
                                PartitionID partId,
                                std::vector<kvstore::KV> data) {
    this->kvstore_->asyncMultiPut(
        spaceId, partId, std::move(data), [spaceId, partId, this](kvstore::ResultCode code) {
            handleAsync(spaceId, partId, code);
        });
}

template <typename RESP>
void BaseProcessor<RESP>::doRemove(GraphSpaceID spaceId,
                                   PartitionID partId,
                                   std::vector<std::string> keys) {
    this->kvstore_->asyncMultiRemove(
        spaceId, partId, std::move(keys), [spaceId, partId, this](kvstore::ResultCode code) {
            handleAsync(spaceId, partId, code);
        });
}

template <typename RESP>
void BaseProcessor<RESP>::doRemoveRange(GraphSpaceID spaceId,
                                        PartitionID partId,
                                        std::string start,
                                        std::string end) {
    this->kvstore_->asyncRemoveRange(
        spaceId, partId, start, end, [spaceId, partId, this](kvstore::ResultCode code) {
            handleAsync(spaceId, partId, code);
        });
}

}  // namespace storage
}  // namespace nebula
