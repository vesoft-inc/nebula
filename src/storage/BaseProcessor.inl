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
    case kvstore::ResultCode::ERR_CHECKPOINT_ERROR:
        return cpp2::ErrorCode::E_FAILED_TO_CHECKPOINT;
    case kvstore::ResultCode::ERR_WRITE_BLOCK_ERROR:
        return cpp2::ErrorCode::E_CHECKPOINT_BLOCKED;
    case kvstore::ResultCode::ERR_PARTIAL_RESULT:
        return cpp2::ErrorCode::E_PARTIAL_RESULT;
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
void BaseProcessor<RESP>::doPut(GraphSpaceID spaceId,
                                PartitionID partId,
                                std::vector<kvstore::KV> data) {
    this->kvstore_->asyncMultiPut(
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
    kvstore_->asyncMultiPut(spaceId,
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
    this->kvstore_->asyncMultiRemove(
        spaceId, partId, std::move(keys), [spaceId, partId, this](kvstore::ResultCode code) {
            handleAsync(spaceId, partId, code);
        });
}

template<typename RESP>
kvstore::ResultCode BaseProcessor<RESP>::doRange(GraphSpaceID spaceId,
                                                 PartitionID partId,
                                                 const std::string& start,
                                                 const std::string& end,
                                                 std::unique_ptr<kvstore::KVIterator>* iter) {
    return kvstore_->range(spaceId, partId, start, end, iter);
}

template<typename RESP>
kvstore::ResultCode BaseProcessor<RESP>::doPrefix(GraphSpaceID spaceId,
                                                  PartitionID partId,
                                                  const std::string& prefix,
                                                  std::unique_ptr<kvstore::KVIterator>* iter) {
    return kvstore_->prefix(spaceId, partId, prefix, iter);
}

template<typename RESP>
kvstore::ResultCode BaseProcessor<RESP>::doRangeWithPrefix(
        GraphSpaceID spaceId, PartitionID partId, const std::string& start,
        const std::string& prefix, std::unique_ptr<kvstore::KVIterator>* iter) {
    return kvstore_->rangeWithPrefix(spaceId, partId, start, prefix, iter);
}

template <typename RESP>
StatusOr<IndexValues>
BaseProcessor<RESP>::collectIndexValues(RowReader* reader,
                                        const std::vector<nebula::cpp2::ColumnDef>& cols) {
    IndexValues values;
    if (reader == nullptr) {
        return Status::Error("Invalid row reader");
    }
    for (auto& col : cols) {
        auto res = RowReader::getPropByName(reader, col.get_name());
        if (ok(res)) {
            auto val = NebulaKeyUtils::encodeVariant(value(std::move(res)));
            values.emplace_back(col.get_type().get_type(), std::move(val));
        } else {
            VLOG(1) << "Bad value for prop: " << col.get_name();
            // TODO: Should return NULL
            auto defaultVal = RowReader::getDefaultProp(col.get_type().get_type());
            if (!defaultVal.ok()) {
                // Should never reach here.
                return Status::Error("Skip bad type of prop : %s", col.get_name().c_str());
            } else {
                auto val = NebulaKeyUtils::encodeVariant(std::move(defaultVal).value());
                values.emplace_back(col.get_type().get_type(), std::move(val));
            }
        }
    }
    return values;
}

template <typename RESP>
void BaseProcessor<RESP>::collectProps(RowReader* reader,
                                       const std::vector<PropContext>& props,
                                       Collector* collector) {
    for (auto& prop : props) {
        if (reader != nullptr) {
            const auto& name = prop.prop_.get_name();
            auto res = RowReader::getPropByName(reader, name);
            VariantType v;
            if (!ok(res)) {
                VLOG(1) << "Bad value for prop: " << name;
                // TODO: Should return NULL
                auto defaultVal = RowReader::getDefaultProp(prop.type_.type);
                if (!defaultVal.ok()) {
                    // Should never reach here.
                    LOG(FATAL) << "Get default value failed for " << name;
                    continue;
                } else {
                    v = std::move(defaultVal).value();
                }
            } else {
                v = value(std::move(res));
            }
            if (prop.returned_) {
                switch (v.which()) {
                    case VAR_INT64:
                        collector->collectInt64(boost::get<int64_t>(v), prop);
                        break;
                    case VAR_DOUBLE:
                        collector->collectDouble(boost::get<double>(v), prop);
                        break;
                    case VAR_BOOL:
                        collector->collectBool(boost::get<bool>(v), prop);
                        break;
                    case VAR_STR:
                        collector->collectString(boost::get<std::string>(v), prop);
                        break;
                    default:
                        LOG(FATAL) << "Unknown VariantType: " << v.which();
                }
            }
        }
    }
}

}  // namespace storage
}  // namespace nebula
