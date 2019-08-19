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
    default:
        return cpp2::ErrorCode::E_UNKNOWN;
    }
}


template<typename RESP>
void BaseProcessor<RESP>::doPut(GraphSpaceID spaceId,
                                PartitionID partId,
                                std::vector<kvstore::KV> data) {
    this->kvstore_->asyncMultiPut(spaceId,
                                  partId,
                                  std::move(data),
                                  [spaceId, partId, this](kvstore::ResultCode code) {
        VLOG(3) << "partId:" << partId << ", code:" << static_cast<int32_t>(code);

        cpp2::ResultCode thriftResult;
        thriftResult.set_code(to(code));
        thriftResult.set_part_id(partId);
        if (code == kvstore::ResultCode::ERR_LEADER_CHANGED) {
            nebula::cpp2::HostAddr leader;
            auto addrRet = kvstore_->partLeader(spaceId, partId);
            CHECK(ok(addrRet));
            auto addr = value(std::move(addrRet));
            leader.set_ip(addr.first);
            leader.set_port(addr.second);
            thriftResult.set_leader(std::move(leader));
        }
        bool finished = false;
        {
            std::lock_guard<std::mutex> lg(this->lock_);
            if (thriftResult.code != cpp2::ErrorCode::SUCCEEDED) {
                this->codes_.emplace_back(std::move(thriftResult));
            }
            this->callingNum_--;
            if (this->callingNum_ == 0) {
                result_.set_failed_codes(std::move(this->codes_));
                finished = true;
            }
        }
        if (finished) {
            this->onFinished();
        }
    });
}

template<typename RESP>
void BaseProcessor<RESP>::doRemove(GraphSpaceID spaceId,
                                   PartitionID partId,
                                   std::vector<std::string> keys) {
    this->kvstore_->asyncMultiRemove(spaceId,
                                     partId,
                                     std::move(keys),
                                     [spaceId, partId, this](kvstore::ResultCode code) {
        VLOG(3) << "partId:" << partId << ", code:" << static_cast<int32_t>(code);

        cpp2::ResultCode thriftResult;
        thriftResult.set_code(to(code));
        thriftResult.set_part_id(partId);
        if (code == kvstore::ResultCode::ERR_LEADER_CHANGED) {
            nebula::cpp2::HostAddr leader;
            auto addrRet = kvstore_->partLeader(spaceId, partId);
            CHECK(ok(addrRet));
            auto addr = value(std::move(addrRet));
            leader.set_ip(addr.first);
            leader.set_port(addr.second);
            thriftResult.set_leader(std::move(leader));
        }
        bool finished = false;
        {
            std::lock_guard<std::mutex> lg(this->lock_);
            if (thriftResult.code != cpp2::ErrorCode::SUCCEEDED) {
                this->codes_.emplace_back(std::move(thriftResult));
            }
            this->callingNum_--;
            if (this->callingNum_ == 0) {
                result_.set_failed_codes(std::move(this->codes_));
                finished = true;
            }
        }
        if (finished) {
            this->onFinished();
        }
    });
}

template<typename RESP>
void BaseProcessor<RESP>::doRemoveRange(GraphSpaceID spaceId,
                                        PartitionID partId,
                                        std::string start,
                                        std::string end) {
    this->kvstore_->asyncRemoveRange(spaceId, partId, start, end,
                                     [spaceId, partId, this](kvstore::ResultCode code) {
        VLOG(3) << "partId:" << partId << ", code:" << static_cast<int32_t>(code);

        cpp2::ResultCode thriftResult;
        thriftResult.set_code(to(code));
        thriftResult.set_part_id(partId);
        if (code == kvstore::ResultCode::ERR_LEADER_CHANGED) {
            nebula::cpp2::HostAddr leader;
            auto addrRet = kvstore_->partLeader(spaceId, partId);
            CHECK(ok(addrRet));
            auto addr = value(std::move(addrRet));
            leader.set_ip(addr.first);
            leader.set_port(addr.second);
            thriftResult.set_leader(std::move(leader));
        }
        bool finished = false;
        {
            std::lock_guard<std::mutex> lg(this->lock_);
            if (thriftResult.code != cpp2::ErrorCode::SUCCEEDED) {
                this->codes_.emplace_back(thriftResult);
            }
            this->callingNum_--;
            if (this->callingNum_ == 0) {
                result_.set_failed_codes(std::move(this->codes_));
                finished = true;
            }
        }
        if (finished) {
            this->onFinished();
        }
    });
}


template<typename RESP>
StatusOr<std::pair<std::string, std::string>> BaseProcessor<RESP>::assembleIndex(
        GraphSpaceID spaceId, PartitionID partId, nebula::cpp2::IndexStatus status,
        nebula::cpp2::IndexType type, const cpp2::IndexItem* indexItem, int32_t id,
        std::string key, std::string val) {
    std::pair<std::string, std::string> data;
    auto indexId = indexItem->get_index_id();
    switch (status) {
        case nebula::cpp2::IndexStatus::NORMAL :
        {
            switch (type) {
                case nebula::cpp2::IndexType::EDGE :
                {
                    auto version = NebulaKeyUtils::parseEdgeVersion(key);
                    auto& props = indexItem->get_props();
                    auto cols = props.find(id);
                    if (cols == props.end()) {
                        return Status::Error("index type error");
                    }
                    auto ret = assembleEdgeIndex(spaceId, cols->second, id, val);
                    if (!ret.ok()) {
                        return Status::Error("index type error");
                    }
                    auto indexKey = NebulaKeyUtils::edgeIndexkey(partId, indexId, id, version,
                                                                 ret.value());
                    return std::pair<std::string, std::string>(std::move(indexKey), key);
                }
                case nebula::cpp2::IndexType::TAG :
                {
                    auto version = NebulaKeyUtils::parseTagVersion(key);
                    auto& props = indexItem->get_props();
                    auto cols = props.find(id);
                    if (cols == props.end()) {
                        return Status::Error("index type error");
                    }
                    auto ret = assembleTagIndex(spaceId, cols->second, id, val);
                    if (!ret.ok()) {
                        return Status::Error("index type error");
                    }
                    auto vid = NebulaKeyUtils::parseVertexId(key);
                    auto indexKey = NebulaKeyUtils::tagIndexkey(partId, indexId, vid, version,
                                                                ret.value());
                    return std::pair<std::string, std::string>(std::move(indexKey), key);
                }
                case nebula::cpp2::IndexType::UNKNOWN :
                default:
                    return Status::Error("index type error");
            }
        }
        case nebula::cpp2::IndexStatus::BUILDING :
        {   auto version = time::WallClock::fastNowInMicroSec();
            switch (type) {
                case nebula::cpp2::IndexType::EDGE :
                {
                    std::pair<std::string, std::string> raw(key, val);
                    auto preKey = NebulaKeyUtils::preIndexKey(partId, indexId, version);
                    auto preVal = NebulaKeyUtils::preIndexValue(nebula::cpp2::IndexType::EDGE,
                                                                nebula::cpp2::IndexOpType::INSERT,
                                                                std::move(raw), {});
                    return std::pair<std::string, std::string>(std::move(preKey),
                                                               std::move(preVal));
                }
                case nebula::cpp2::IndexType::TAG :
                {
                    std::pair<std::string, std::string> raw(key, val);
                    auto preKey = NebulaKeyUtils::preIndexKey(partId, indexId, version);
                    auto preVal = NebulaKeyUtils::preIndexValue(nebula::cpp2::IndexType::TAG,
                                                                nebula::cpp2::IndexOpType::INSERT,
                                                                std::move(raw), {});
                    return std::pair<std::string, std::string>(std::move(preKey),
                                                               std::move(preVal));
                }
                case nebula::cpp2::IndexType::UNKNOWN :
                default:
                    return Status::Error("index type error");
            }
        }
        case nebula::cpp2::IndexStatus::INVALID :
        case nebula::cpp2::IndexStatus::CONSTRUCTING :
        default:
            return Status::Error("index status error");
    }
}

template<typename RESP>
StatusOr<std::string> BaseProcessor<RESP>::assembleEdgeIndex(GraphSpaceID spaceId,
        const std::vector<std::string> cols, EdgeType type, std::string val) {
    auto reader = RowReader::getEdgePropReader(this->schemaMan_, val, spaceId, type);
    auto ret = collectColsVal(reader.get(), cols);
    if (ret.ok()) {
        return ret.value();
    }
    return Status::Error("");
}

template<typename RESP>
StatusOr<std::string> BaseProcessor<RESP>::assembleTagIndex(GraphSpaceID spaceId,
        const std::vector<std::string> cols, TagID id, std::string val) {
    auto reader = RowReader::getTagPropReader(this->schemaMan_, val, spaceId, id);
    auto ret = collectColsVal(reader.get(), cols);
    if (ret.ok()) {
        return ret.value();
    }
    return Status::Error("");
}

template<typename RESP>
StatusOr<std::string> BaseProcessor<RESP>::collectColsVal(RowReader* reader,
                                                          std::vector<std::string> cols) {
    // TODO : is better to use VariantType?

    std::string val;
    val.reserve(256);
    for (auto& col : cols) {
        auto res = RowReader::getPropByName(reader, col);
        if (!ok(res)) {
            return Status::Error("Get Prop failing by col : %s", col);
        }
        auto&& v = value(std::move(res));

        switch (v.which()) {
            case VAR_INT64:
                val.append(folly::to<std::string>(boost::get<int64_t>(v)));
                break;
            case VAR_DOUBLE:
                val.append(folly::to<std::string>(boost::get<double>(v)));
                break;
            case VAR_BOOL:
                val.append(folly::to<std::string>(boost::get<bool>(v)));
                break;
            case VAR_STR:
                val.append(boost::get<std::string>(v));
                break;
            default:
                return Status::Error("Schema prop type error");
        }  // switch
    }
    return val;
}

}  // namespace storage
}  // namespace nebula
