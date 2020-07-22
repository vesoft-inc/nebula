/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

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
    case kvstore::ResultCode::ERR_CHECKPOINT_ERROR:
        return cpp2::ErrorCode::E_FAILED_TO_CHECKPOINT;
    case kvstore::ResultCode::ERR_WRITE_BLOCK_ERROR:
        return cpp2::ErrorCode::E_CHECKPOINT_BLOCKED;
    case kvstore::ResultCode::ERR_PARTIAL_RESULT:
        return cpp2::ErrorCode::E_PARTIAL_RESULT;
    case kvstore::ResultCode::ERR_INVALID_FIELD_VALUE:
        return cpp2::ErrorCode::E_INVALID_FIELD_VALUE;
    case kvstore::ResultCode::ERR_RESULT_FILTERED:
        return cpp2::ErrorCode::E_FILTER_OUT;
    case kvstore::ResultCode::ERR_EDGE_NOT_FOUND:
        return cpp2::ErrorCode::E_EDGE_NOT_FOUND;
    case kvstore::ResultCode::ERR_TAG_NOT_FOUND:
        return cpp2::ErrorCode::E_TAG_NOT_FOUND;
    case kvstore::ResultCode::ERR_INVALID_DATA:
        return cpp2::ErrorCode::E_INVALID_DATA;
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
        handleErrorCode(code, spaceId, partId);
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
meta::cpp2::ColumnDef BaseProcessor<RESP>::columnDef(std::string name,
                                                     meta::cpp2::PropertyType type) {
    nebula::meta::cpp2::ColumnDef column;
    column.set_name(std::move(name));
    column.set_type(type);
    return column;
}

template <typename RESP>
void BaseProcessor<RESP>::pushResultCode(cpp2::ErrorCode code, PartitionID partId) {
    if (code != cpp2::ErrorCode::SUCCEEDED) {
        cpp2::PartitionResult thriftRet;
        thriftRet.set_code(code);
        thriftRet.set_part_id(partId);
        codes_.emplace_back(std::move(thriftRet));
    }
}

template <typename RESP>
void BaseProcessor<RESP>::pushResultCode(cpp2::ErrorCode code,
                                         PartitionID partId,
                                         HostAddr leader) {
    if (code != cpp2::ErrorCode::SUCCEEDED) {
        cpp2::PartitionResult thriftRet;
        thriftRet.set_code(code);
        thriftRet.set_part_id(partId);
        thriftRet.set_leader(leader);
        codes_.emplace_back(std::move(thriftRet));
    }
}

template <typename RESP>
void BaseProcessor<RESP>::handleErrorCode(kvstore::ResultCode code,
                                          GraphSpaceID spaceId,
                                          PartitionID partId) {
    if (code != kvstore::ResultCode::SUCCEEDED) {
        if (code == kvstore::ResultCode::ERR_LEADER_CHANGED) {
            handleLeaderChanged(spaceId, partId);
        } else {
            pushResultCode(to(code), partId);
        }
    }
}

template <typename RESP>
void BaseProcessor<RESP>::handleLeaderChanged(GraphSpaceID spaceId,
                                              PartitionID partId) {
    auto addrRet = env_->kvstore_->partLeader(spaceId, partId);
    if (ok(addrRet)) {
        auto leader = value(std::move(addrRet));
        this->pushResultCode(cpp2::ErrorCode::E_LEADER_CHANGED, partId, leader);
    } else {
        LOG(ERROR) << "Fail to get part leader, spaceId: " << spaceId
                   << ", partId: " << partId << ", ResultCode: " << error(addrRet);
        this->pushResultCode(to(error(addrRet)), partId);
    }
}

template <typename RESP>
void BaseProcessor<RESP>::doPut(GraphSpaceID spaceId,
                                PartitionID partId,
                                std::vector<kvstore::KV> data) {
    this->env_->kvstore_->asyncMultiPut(
        spaceId, partId, std::move(data), [spaceId, partId, this](kvstore::ResultCode code) {
            handleAsync(spaceId, partId, code);
    });
}

template <typename RESP>
kvstore::ResultCode BaseProcessor<RESP>::doSyncPut(GraphSpaceID spaceId,
                                                   PartitionID partId,
                                                   std::vector<kvstore::KV> data) {
    folly::Baton<true, std::atomic> baton;
    auto ret = kvstore::ResultCode::SUCCEEDED;
    env_->kvstore_->asyncMultiPut(spaceId,
                            partId,
                            std::move(data),
                            [&ret, &baton] (kvstore::ResultCode code) {
        if (kvstore::ResultCode::SUCCEEDED != code) {
            ret = code;
        }
        baton.post();
    });
    baton.wait();
    return ret;
}

template <typename RESP>
void BaseProcessor<RESP>::doRemove(GraphSpaceID spaceId,
                                   PartitionID partId,
                                   std::vector<std::string> keys) {
    this->env_->kvstore_->asyncMultiRemove(
        spaceId, partId, std::move(keys), [spaceId, partId, this](kvstore::ResultCode code) {
        handleAsync(spaceId, partId, code);
    });
}

template <typename RESP>
StatusOr<std::string>
BaseProcessor<RESP>::encodeRowVal(const meta::NebulaSchemaProvider* schema,
                                  const std::vector<std::string>& propNames,
                                  const std::vector<Value>& props) {
    RowWriterV2 rowWrite(schema);
    // If req.prop_names is not empty, use the property name in req.prop_names
    // Otherwise, use property name in schema
    if (!propNames.empty()) {
        for (size_t i = 0; i < propNames.size(); i++) {
            auto wRet = rowWrite.setValue(propNames[i], props[i]);
            if (wRet != WriteResult::SUCCEEDED) {
                return Status::Error("Add field faild");
            }
        }
    } else {
        for (size_t i = 0; i < props.size(); i++) {
            auto wRet = rowWrite.setValue(i, props[i]);
            if (wRet != WriteResult::SUCCEEDED) {
                return Status::Error("Add field faild");
            }
        }
    }

    auto wRet = rowWrite.finish();
    if (wRet != WriteResult::SUCCEEDED) {
        return Status::Error("Add field faild");
    }

    return std::move(rowWrite).moveEncodedStr();
}

}  // namespace storage
}  // namespace nebula
