/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "storage/IndexBaseProcessor.h"


DECLARE_int32(bulk_number_per_index_creation);

#define INDEX_CREATE_DONE                                              \
do {                                                                   \
    bool finished = false;                                             \
    {                                                                  \
        std::lock_guard<std::mutex> lg(this->lock_);                   \
        if (thriftResult.code != cpp2::ErrorCode::SUCCEEDED) {         \
        this->codes_.emplace_back(thriftResult);                       \
        }                                                              \
        this->callingNum_--;                                           \
        if (this->callingNum_ == 0) {                                  \
            this->result_.set_failed_codes(std::move(this->codes_));   \
            finished = true;                                           \
        }                                                              \
    }                                                                  \
    if (finished) {                                                    \
        this->kvstore_->deleteSnapshot(spaceId_);                      \
        this->onFinished();                                            \
    }                                                                  \
    return;                                                            \
} while (false)                                                        \


namespace nebula {
namespace storage {

template<typename RESP>
void IndexBaseProcessor<RESP>::doIndexCreate(PartitionID partId) {
    std::string key, val;
    StatusOr<std::string> indexKey;
    std::vector<kvstore::KV> data;

    int32_t batchPutNum = 0;
    cpp2::ResultCode thriftResult;
    thriftResult.set_code(cpp2::ErrorCode::SUCCEEDED);
    thriftResult.set_part_id(partId);

    auto prefix = NebulaKeyUtils::partPrefix(partId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = this->kvstore_->prefixSnapshot(spaceId_, partId, prefix, &iter);

    if (ret != kvstore::ResultCode::SUCCEEDED || !iter) {
        thriftResult.set_code(this->to(ret));
        INDEX_CREATE_DONE;
    }

    while (iter->valid()) {
        key = iter->key().str();
        val = iter->val().str();
        iter->next();
        indexKey = (indexType_ == nebula::cpp2::IndexType::EDGE) ?
                   assembleEdgeIndexKey(spaceId_, partId, key, std::move(val)) :
                   assembleVertexIndexKey(spaceId_, partId, key, std::move(val));

        if (!indexKey.ok()) {
            continue;
        }
        data.emplace_back(std::move(indexKey.value()), std::move(key));
        batchPutNum++;
        if (batchPutNum == FLAGS_bulk_number_per_index_creation) {
            auto code = doBatchPut(spaceId_, partId, data);
            if (code == cpp2::ErrorCode::E_LEADER_CHANGED) {
                nebula::cpp2::HostAddr leader;
                auto addrRet = this->kvstore_->partLeader(spaceId_, partId);
                CHECK(ok(addrRet));
                auto addr = value(std::move(addrRet));
                leader.set_ip(addr.first);
                leader.set_port(addr.second);
                thriftResult.set_leader(leader);
            } else if (code != cpp2::ErrorCode::SUCCEEDED) {
                thriftResult.set_code(code);
                INDEX_CREATE_DONE;
            }
            data.clear();
            batchPutNum = 0;
        }
    }

    if (!data.empty()) {
        auto code = doBatchPut(spaceId_, partId, data);
        if (code == cpp2::ErrorCode::E_LEADER_CHANGED) {
            nebula::cpp2::HostAddr leader;
            auto addrRet = this->kvstore_->partLeader(spaceId_, partId);
            CHECK(ok(addrRet));
            auto addr = value(std::move(addrRet));
            leader.set_ip(addr.first);
            leader.set_port(addr.second);
            thriftResult.set_leader(leader);
        } else if (code != cpp2::ErrorCode::SUCCEEDED) {
            thriftResult.set_code(code);
            INDEX_CREATE_DONE;
        }
        data.clear();
    }

    // TODO : send index status 'CONSTRUCTING' via meta client,
    //        should block all actions of insert|delete| update.
    //        until index create done of each parts.

    // Assemble new data changes during index creation

    prefix = NebulaKeyUtils::preIndexPrefix(partId, indexId_);
    std::unique_ptr<kvstore::KVIterator> endIter;
    auto endRet = this->kvstore_->prefixSnapshot(spaceId_, partId, prefix, &endIter);

    if (endRet != kvstore::ResultCode::SUCCEEDED || !iter) {
        thriftResult.set_code(this->to(ret));
        INDEX_CREATE_DONE;
    }

    while (endIter->valid()) {
        val = endIter->val().str();
        auto code = pastIndexCreate(partId, val);
        if (code == cpp2::ErrorCode::E_LEADER_CHANGED) {
            nebula::cpp2::HostAddr leader;
            auto addrRet = this->kvstore_->partLeader(spaceId_, partId);
            CHECK(ok(addrRet));
            auto addr = value(std::move(addrRet));
            leader.set_ip(addr.first);
            leader.set_port(addr.second);
            thriftResult.set_leader(leader);
        } else if (code != cpp2::ErrorCode::SUCCEEDED) {
            thriftResult.set_code(code);
            INDEX_CREATE_DONE;
        }
        endIter->next();
    }

    INDEX_CREATE_DONE;
}

template<typename RESP>
cpp2::ErrorCode IndexBaseProcessor<RESP>::pastIndexCreate(PartitionID partId,
                                                          const folly::StringPiece &raw) {
    auto type = *reinterpret_cast<const nebula::cpp2::IndexType*>(raw.begin());
    auto op = *reinterpret_cast<const nebula::cpp2::IndexOpType*>
            (raw.begin() + sizeof(nebula::cpp2::IndexType));

    auto keyLen = *reinterpret_cast<const int32_t*>(raw.begin() + sizeof(nebula::cpp2::IndexType) +
                                                    sizeof(nebula::cpp2::IndexOpType));

    auto valLen = *reinterpret_cast<const int32_t*>(raw.begin() + sizeof(nebula::cpp2::IndexType) +
                                                    sizeof(nebula::cpp2::IndexOpType) +
                                                    sizeof(int32_t) + keyLen);

    auto key = raw.subpiece(sizeof(nebula::cpp2::IndexType) + sizeof(nebula::cpp2::IndexOpType) +
                            sizeof(int32_t), keyLen);

    auto val = raw.subpiece(sizeof(nebula::cpp2::IndexType) + sizeof(nebula::cpp2::IndexOpType) +
                            sizeof(int32_t) * 2 + keyLen, valLen);

    switch (op) {
        case nebula::cpp2::IndexOpType::INSERT :
        {
            std::pair<std::string, std::string> data(key, val);
            return pastDoInsert(partId, type, std::move(data));
        }
        case nebula::cpp2::IndexOpType::DELETE :
        {
            std::pair<std::string, std::string> data(key, val);
            return pastDoDelete(partId, type, std::move(data));
        }
        case nebula::cpp2::IndexOpType::UPDATE :
        {
            std::pair<std::string, std::string> oldData(key, val);
            std::pair<std::string, std::string> newData(key, val);
            return pastDoUpdate(partId, type, std::move(oldData), std::move(newData));
        }
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

template<typename RESP>
StatusOr<std::vector<kvstore::KV>> IndexBaseProcessor<RESP>::parseIndexKey(PartitionID partId,
        nebula::cpp2::IndexType type, const std::pair<std::string, std::string> data) {
    auto key = data.first;
    auto val = data.second;
    StatusOr<std::string> indexKey;
    switch (type) {
        case nebula::cpp2::IndexType::EDGE :
        {
            indexKey = assembleEdgeIndexKey(spaceId_, partId ,
                                            data.first, data.second);
            break;
        }
        case nebula::cpp2::IndexType::TAG :
        {
            indexKey = assembleVertexIndexKey(spaceId_, partId,
                                              data.first, data.second);
            break;
        }
        case nebula::cpp2::IndexType::UNKNOWN : {
            return Status::Error("Unknown index type");
        }
    }

    if (!indexKey.ok()) {
        return Status::Error("Assemble index key error");
    }

    return  std::vector<kvstore::KV>({{indexKey.value(), key}});
}

template<typename RESP>
cpp2::ErrorCode IndexBaseProcessor<RESP>::pastDoInsert(PartitionID partId,
                                                       nebula::cpp2::IndexType type,
        const std::pair<std::string, std::string> data) {
    auto ret = parseIndexKey(partId, type, data);
    if (!ret.ok()) {
        LOG(ERROR) << "Parse index key error";
        return cpp2::ErrorCode::E_UNKNOWN;
    }
    return doBatchPut(spaceId_, partId, std::move(ret.value()));
}

template<typename RESP>
cpp2::ErrorCode IndexBaseProcessor<RESP>::pastDoDelete(PartitionID partId,
                                                       nebula::cpp2::IndexType type,
        const std::pair<std::string, std::string> data) {
    auto ret = parseIndexKey(partId, type, data);
    if (!ret.ok()) {
        LOG(ERROR) << "Parse index key error";
        return cpp2::ErrorCode::E_UNKNOWN;
    }
    const auto& kv = ret.value().front();
    return doDelete(spaceId_, partId, std::move(kv.first));
}

template<typename RESP>
cpp2::ErrorCode IndexBaseProcessor<RESP>::pastDoUpdate(PartitionID partId,
                                                       nebula::cpp2::IndexType type,
        const std::pair<std::string, std::string> oldData,
        const std::pair<std::string, std::string> newData) {
    auto oldDataRet = parseIndexKey(partId, type, oldData);

    if (!oldDataRet.ok()) {
        LOG(ERROR) << "Parse index key error";
        return cpp2::ErrorCode::E_UNKNOWN;
    }

    auto newDataRet = parseIndexKey(partId, type, newData);

    if (!newDataRet.ok()) {
        LOG(ERROR) << "Parse index key error";
        return cpp2::ErrorCode::E_UNKNOWN;
    }

    // TODO Is better to use writeBatch?

    const auto& kv = oldDataRet.value().front();

    auto deleteRet = doDelete(spaceId_, spaceId_, kv.first);

    if (deleteRet != cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Delete old index data error";
        return deleteRet;
    }

    return doBatchPut(spaceId_, partId, newDataRet.value());
}

template<typename RESP>
cpp2::ErrorCode IndexBaseProcessor<RESP>::doBatchPut(GraphSpaceID spaceId, PartitionID partId,
                                          std::vector<kvstore::KV> data) {
    cpp2::ErrorCode ret = cpp2::ErrorCode::SUCCEEDED;
    folly::Baton<true, std::atomic> baton;
    this->kvstore_->asyncMultiPut(spaceId, partId, std::move(data),
                            [&] (kvstore::ResultCode code) {
                                if (code != kvstore::ResultCode::SUCCEEDED) {
                                    ret = this->to(code);
                                }
                                baton.post();
                            });
    baton.wait();
    return ret;
}

template<typename RESP>
cpp2::ErrorCode IndexBaseProcessor<RESP>::doDelete(GraphSpaceID spaceId, PartitionID partId,
                                                   const std::string &key) {
    cpp2::ErrorCode ret = cpp2::ErrorCode::SUCCEEDED;
    folly::Baton<true, std::atomic> baton;
    this->kvstore_->asyncRemove(spaceId, partId, key,
                            [&] (kvstore::ResultCode code) {
                                if (code != kvstore::ResultCode::SUCCEEDED) {
                                    ret = this->to(code);
                                }
                                baton.post();
                            });
    baton.wait();
    return ret;
}

template<typename RESP>
void IndexBaseProcessor<RESP>::doBatchDelete(GraphSpaceID spaceId,
                                             PartitionID partId,
                                             std::string prefix) {
    this->kvstore_->asyncRemovePrefix(spaceId,
                                      partId,
                                      std::move(prefix),
                                      [spaceId, partId, this](kvstore::ResultCode code) {
        VLOG(3) << "partId:" << partId << ", code:" << static_cast<int32_t>(code);
        cpp2::ResultCode thriftResult;
        thriftResult.set_code(this->to(code));
        thriftResult.set_part_id(partId);
        if (code == kvstore::ResultCode::ERR_LEADER_CHANGED) {
            nebula::cpp2::HostAddr leader;
            auto addrRet = this->kvstore_->partLeader(spaceId, partId);
            CHECK(ok(addrRet));
            auto addr = value(std::move(addrRet));
            leader.set_ip(addr.first);
            leader.set_port(addr.second);
            thriftResult.set_leader(leader);
        }
        bool finished = false;
        {
            std::lock_guard<std::mutex> lg(this->lock_);
            if (thriftResult.code != cpp2::ErrorCode::SUCCEEDED) {
                this->codes_.emplace_back(thriftResult);
            }
            this->callingNum_--;
            if (this->callingNum_ == 0) {
                this->result_.set_failed_codes(std::move(this->codes_));
                finished = true;
            }
        }
        if (finished) {
            this->onFinished();
        }
    });
}

template<typename RESP>
StatusOr<std::string> IndexBaseProcessor<RESP>::assembleEdgeIndexKey(GraphSpaceID spaceId,
        PartitionID partId, std::string key, std::string val) {
    if (folly::to<int32_t>(key.size()) != NebulaKeyUtils::getEdgeLen()) {
        return Status::Error("Skip this row");
    }
    auto edgeType = NebulaKeyUtils::parseEdgeType(key);
    auto reader = RowReader::getEdgePropReader(this->schemaMan_, val, spaceId, edgeType);
    for (const auto &prop : props_) {
        auto ret = this->schemaMan_->getNewestEdgeSchemaVer(spaceId, edgeType);
        if (!ret.ok()) {
            return Status::Error("Space %d edge %d invalid", spaceId, edgeType);
        }
        if (edgeType == prop.first && ret.value() == reader->schemaVer()) {
            auto propVal = this->collectColsVal(reader.get(), prop.second);
            if (!propVal.ok()) {
                return Status::Error("Get edge Prop failing");
            }
            auto ver = NebulaKeyUtils::parseEdgeVersion(key);
            auto raw = NebulaKeyUtils::edgeIndexkey(partId, indexId_,
                                                    edgeType, ver, propVal.value());
            return raw;
        }
    }
    return Status::Error("Not is newly version by edge");
}

template<typename RESP>
StatusOr<std::string> IndexBaseProcessor<RESP>::assembleVertexIndexKey(GraphSpaceID spaceId,
        PartitionID partId, std::string key, std::string val) {
    if (folly::to<int32_t>(key.size()) != NebulaKeyUtils::getVertexLen()) {
        return Status::Error("Skip this row");
    }

    auto tagId = NebulaKeyUtils::parseTagId(key);
    auto reader = RowReader::getTagPropReader(this->schemaMan_, val, spaceId, tagId);
    for (const auto &prop : props_) {
        auto ret = this->schemaMan_->getNewestTagSchemaVer(spaceId, tagId);
        if (!ret.ok()) {
            return Status::Error("Space %d tag %d invalid", spaceId, tagId);
        }
        if (tagId == prop.first && ret.value() == reader->schemaVer()) {
            auto propVal = this->collectColsVal(reader.get(), prop.second);
            if (!propVal.ok()) {
                return Status::Error("Get tag Prop failing");
            }
            auto ver = NebulaKeyUtils::parseTagVersion(key);
            auto vId = NebulaKeyUtils::parseVertexId(key);
            auto raw = NebulaKeyUtils::tagIndexkey(partId, indexId_, vId, ver, propVal.value());
            return raw;
        }
    }
    return Status::Error("Not is newly version by vertex");
}

}  // namespace storage
}  // namespace nebula
