/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/MetaServiceUtils.h"
#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

template<typename RESP>
void BaseProcessor<RESP>::doPut(std::vector<kvstore::KV> data) {
    folly::Baton<true, std::atomic> baton;
    kvstore_->asyncMultiPut(kDefaultSpaceId,
                            kDefaultPartId,
                            std::move(data),
                            [this, &baton] (kvstore::ResultCode code) {
        this->handleErrorCode(MetaCommon::to(code));
        baton.post();
    });
    baton.wait();
    this->onFinished();
}


template<typename RESP>
StatusOr<std::unique_ptr<kvstore::KVIterator>>
BaseProcessor<RESP>::doPrefix(const std::string& key) {
    std::unique_ptr<kvstore::KVIterator> iter;
    auto code = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, key, &iter);
    if (code != kvstore::ResultCode::SUCCEEDED) {
        return Status::Error("Prefix Failed");
    }
    return iter;
}


template<typename RESP>
StatusOr<std::string> BaseProcessor<RESP>::doGet(const std::string& key) {
    std::string value;
    auto code = kvstore_->get(kDefaultSpaceId, kDefaultPartId, key, &value);
    switch (code) {
        case kvstore::ResultCode::SUCCEEDED:
            return value;
        case kvstore::ResultCode::ERR_KEY_NOT_FOUND:
            return Status::Error("Key Not Found");
        default:
            return Status::Error("Get Failed");
    }
}


template<typename RESP>
StatusOr<std::vector<std::string>>
BaseProcessor<RESP>::doMultiGet(const std::vector<std::string>& keys) {
    std::vector<std::string> values;
    auto ret = kvstore_->multiGet(kDefaultSpaceId, kDefaultPartId, keys, &values);
    if (ret.first != kvstore::ResultCode::SUCCEEDED) {
        return Status::Error("MultiGet Failed");
    }
    return values;
}


template<typename RESP>
void BaseProcessor<RESP>::doRemove(const std::string& key) {
    folly::Baton<true, std::atomic> baton;
    kvstore_->asyncRemove(kDefaultSpaceId,
                          kDefaultPartId,
                          key,
                          [this, &baton] (kvstore::ResultCode code) {
        this->handleErrorCode(MetaCommon::to(code));
        baton.post();
    });
    baton.wait();
    this->onFinished();
}


template<typename RESP>
void BaseProcessor<RESP>::doMultiRemove(std::vector<std::string> keys) {
    folly::Baton<true, std::atomic> baton;
    kvstore_->asyncMultiRemove(kDefaultSpaceId,
                               kDefaultPartId,
                               std::move(keys),
                               [this, &baton] (kvstore::ResultCode code) {
        this->handleErrorCode(MetaCommon::to(code));
        baton.post();
    });
    baton.wait();
    this->onFinished();
}

template<typename RESP>
kvstore::ResultCode BaseProcessor<RESP>::multiRemove(std::vector<std::string> keys) {
    folly::Baton<true, std::atomic> baton;
    kvstore::ResultCode retCode;
    kvstore_->asyncMultiRemove(kDefaultSpaceId,
                               kDefaultPartId,
                               std::move(keys),
                               [this, &baton, &retCode] (kvstore::ResultCode code) {
        this->handleErrorCode(MetaCommon::to(code));
        retCode = code;
        baton.post();
    });
    baton.wait();
    return retCode;
}

template<typename RESP>
void BaseProcessor<RESP>::doRemoveRange(const std::string& start,
                                        const std::string& end) {
    folly::Baton<true, std::atomic> baton;
    kvstore_->asyncRemoveRange(kDefaultSpaceId,
                               kDefaultPartId,
                               start,
                               end,
                               [this, &baton] (kvstore::ResultCode code) {
        this->handleErrorCode(MetaCommon::to(code));
        baton.post();
    });
    baton.wait();
    this->onFinished();
}


template<typename RESP>
StatusOr<std::vector<std::string>> BaseProcessor<RESP>::doScan(const std::string& start,
                                                               const std::string& end) {
    std::unique_ptr<kvstore::KVIterator> iter;
    auto code = kvstore_->range(kDefaultSpaceId, kDefaultPartId, start, end, &iter);
    if (code != kvstore::ResultCode::SUCCEEDED) {
        return Status::Error("Scan Failed");
    }

    std::vector<std::string> values;
    while (iter->valid()) {
        values.emplace_back(iter->val());
        iter->next();
    }
    return values;
}


template<typename RESP>
StatusOr<std::vector<nebula::cpp2::HostAddr>> BaseProcessor<RESP>::allHosts() {
    std::vector<nebula::cpp2::HostAddr> hosts;
    const auto& prefix = MetaServiceUtils::hostPrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        return Status::Error("Can't find any hosts");
    }

    while (iter->valid()) {
        nebula::cpp2::HostAddr h;
        auto hostAddrPiece = iter->key().subpiece(prefix.size());
        memcpy(&h, hostAddrPiece.data(), hostAddrPiece.size());
        hosts.emplace_back(std::move(h));
        iter->next();
    }
    return hosts;
}


template<typename RESP>
ErrorOr<cpp2::ErrorCode, int32_t> BaseProcessor<RESP>::autoIncrementId() {
    folly::SharedMutex::WriteHolder holder(LockUtils::idLock());
    static const std::string kIdKey = "__id__";
    int32_t id;
    std::string val;
    auto ret = kvstore_->get(kDefaultSpaceId, kDefaultPartId, kIdKey, &val);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        if (ret != kvstore::ResultCode::ERR_KEY_NOT_FOUND) {
            return MetaCommon::to(ret);
        }
        id = 1;
    } else {
        id = *reinterpret_cast<const int32_t*>(val.c_str()) + 1;
    }

    std::vector<kvstore::KV> data;
    data.emplace_back(kIdKey,
                      std::string(reinterpret_cast<const char*>(&id), sizeof(id)));
    folly::Baton<true, std::atomic> baton;
    kvstore_->asyncMultiPut(kDefaultSpaceId,
                            kDefaultPartId,
                            std::move(data),
                            [&] (kvstore::ResultCode code) {
        ret = code;
        baton.post();
    });
    baton.wait();
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        return MetaCommon::to(ret);
    } else {
        return id;
    }
}


template<typename RESP>
Status BaseProcessor<RESP>::spaceExist(GraphSpaceID spaceId) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
    auto spaceKey = MetaServiceUtils::spaceKey(spaceId);
    auto ret = doGet(std::move(spaceKey));
    if (ret.ok()) {
        return Status::OK();
    }
    return Status::SpaceNotFound();
}


template<typename RESP>
Status BaseProcessor<RESP>::userExist(const std::string& account) {
    auto userKey = MetaServiceUtils::userKey(account);
    auto ret = doGet(userKey);
    if (ret.ok()) {
        return Status::OK();
    }
    return Status::UserNotFound();
}

template<typename RESP>
Status BaseProcessor<RESP>::hostExist(const std::string& hostKey) {
    auto ret = doGet(hostKey);
    if (ret.ok()) {
        return Status::OK();
    }
    return Status::HostNotFound();
}


template<typename RESP>
StatusOr<GraphSpaceID> BaseProcessor<RESP>::getSpaceId(const std::string& name) {
    auto indexKey = MetaServiceUtils::indexSpaceKey(name);
    auto ret = doGet(indexKey);
    if (ret.ok()) {
        return *reinterpret_cast<const GraphSpaceID*>(ret.value().c_str());
    }
    return Status::SpaceNotFound(folly::stringPrintf("Space %s not found", name.c_str()));
}


template<typename RESP>
StatusOr<TagID> BaseProcessor<RESP>::getTagId(GraphSpaceID spaceId, const std::string& name) {
    auto indexKey = MetaServiceUtils::indexTagKey(spaceId, name);
    std::string val;
    auto ret = doGet(indexKey);
    if (ret.ok()) {
        return *reinterpret_cast<const TagID*>(ret.value().c_str());
    }
    return Status::TagNotFound(folly::stringPrintf("Tag %s not found", name.c_str()));
}

template<typename RESP>
StatusOr<EdgeType> BaseProcessor<RESP>::getEdgeType(GraphSpaceID spaceId,
                                                    const std::string& name) {
    auto indexKey = MetaServiceUtils::indexEdgeKey(spaceId, name);
    auto ret = doGet(indexKey);
    if (ret.ok()) {
        return *reinterpret_cast<const EdgeType*>(ret.value().c_str());
    }
    return Status::EdgeNotFound(folly::stringPrintf("Edge %s not found", name.c_str()));
}


template <typename RESP>
std::unordered_map<std::string, nebula::cpp2::ValueType>
BaseProcessor<RESP>::getLatestTagFields(const nebula::cpp2::Schema& latestTagSchema) {
    std::unordered_map<std::string, nebula::cpp2::ValueType> propertyNames;
    for (auto &column : latestTagSchema.get_columns()) {
        propertyNames.emplace(std::move(column.get_name()),
                              std::move(column.get_type()));
    }
    return propertyNames;
}


template <typename RESP>
StatusOr<nebula::cpp2::Schema>
BaseProcessor<RESP>::getLatestTagSchema(GraphSpaceID spaceId, const TagID tagId) {
    auto key = MetaServiceUtils::schemaTagPrefix(spaceId, tagId);
    auto ret = doPrefix(key);
    if (!ret.ok()) {
        LOG(ERROR) << "Tag Prefix " << key << " not found";
        return Status::Error(folly::stringPrintf("Tag Prefix %s not found", key.c_str()));
    }

    auto iter = ret.value().get();
    return MetaServiceUtils::parseSchema(iter->val());
}


template <typename RESP>
std::unordered_map<std::string, nebula::cpp2::ValueType>
BaseProcessor<RESP>::getLatestEdgeFields(const nebula::cpp2::Schema& latestEdgeSchema) {
    std::unordered_map<std::string, nebula::cpp2::ValueType> propertyNames;
    for (auto &column : latestEdgeSchema.get_columns()) {
        propertyNames.emplace(std::move(column.get_name()),
                              std::move(column.get_type()));
    }
    return propertyNames;
}


template <typename RESP>
StatusOr<nebula::cpp2::Schema>
BaseProcessor<RESP>::getLatestEdgeSchema(GraphSpaceID spaceId, const EdgeType edgeType) {
    auto key = MetaServiceUtils::schemaEdgePrefix(spaceId, edgeType);
    auto ret = doPrefix(key);
    if (!ret.ok()) {
        LOG(ERROR) << "Edge Prefix " << key << " not found";
        return Status::Error(folly::stringPrintf("Edge Prefix %s not found", key.c_str()));
    }

    auto iter = ret.value().get();
    return MetaServiceUtils::parseSchema(iter->val());
}


template <typename RESP>
bool BaseProcessor<RESP>::tagOrEdgeHasTTL(const nebula::cpp2::Schema& latestSchema) {
    auto schemaProp = latestSchema.get_schema_prop();
    if (schemaProp.get_ttl_col() && !schemaProp.get_ttl_col()->empty()) {
         return true;
    }
    return false;
}


template<typename RESP>
StatusOr<IndexID>
BaseProcessor<RESP>::getIndexID(GraphSpaceID spaceId, const std::string& indexName) {
    auto indexKey = MetaServiceUtils::indexIndexKey(spaceId, indexName);
    auto ret = doGet(indexKey);
    if (ret.ok()) {
        return *reinterpret_cast<const IndexID*>(ret.value().c_str());
    }
    return Status::IndexNotFound(folly::stringPrintf("Index %s not found", indexName.c_str()));
}

template<typename RESP>
bool BaseProcessor<RESP>::checkPassword(const std::string& account, const std::string& password) {
    auto userKey = MetaServiceUtils::userKey(account);
    auto ret = doGet(userKey);
    return MetaServiceUtils::parseUserPwd(ret.value()) == password;
}

template<typename RESP>
kvstore::ResultCode BaseProcessor<RESP>::doSyncPut(std::vector<kvstore::KV> data) {
    folly::Baton<true, std::atomic> baton;
    auto ret = kvstore::ResultCode::SUCCEEDED;
    kvstore_->asyncMultiPut(kDefaultSpaceId,
                            kDefaultPartId,
                            std::move(data),
                            [&ret, &baton] (kvstore::ResultCode code) {
                                if (kvstore::ResultCode::SUCCEEDED != code) {
                                    ret = code;
                                    LOG(INFO) << "Put data error on meta server";
                                }
                                baton.post();
                            });
    baton.wait();
    return ret;
}

template<typename RESP>
void BaseProcessor<RESP>::doSyncPutAndUpdate(std::vector<kvstore::KV> data) {
    folly::Baton<true, std::atomic> baton;
    auto ret = kvstore::ResultCode::SUCCEEDED;
    kvstore_->asyncMultiPut(kDefaultSpaceId,
                            kDefaultPartId,
                            std::move(data),
                            [&ret, &baton] (kvstore::ResultCode code) {
        if (kvstore::ResultCode::SUCCEEDED != code) {
            ret = code;
            LOG(INFO) << "Put data error on meta server";
        }
        baton.post();
    });
    baton.wait();
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        this->handleErrorCode(MetaCommon::to(ret));
        this->onFinished();
        return;
    }
    ret = LastUpdateTimeMan::update(kvstore_, time::WallClock::fastNowInMilliSec());
    this->handleErrorCode(MetaCommon::to(ret));
    this->onFinished();
}

template<typename RESP>
void BaseProcessor<RESP>::doSyncMultiRemoveAndUpdate(std::vector<std::string> keys) {
    folly::Baton<true, std::atomic> baton;
    auto ret = kvstore::ResultCode::SUCCEEDED;
    kvstore_->asyncMultiRemove(kDefaultSpaceId,
                               kDefaultPartId,
                               std::move(keys),
                               [&ret, &baton] (kvstore::ResultCode code) {
        if (kvstore::ResultCode::SUCCEEDED != code) {
            ret = code;
            LOG(INFO) << "Remove data error on meta server";
        }
        baton.post();
    });
    baton.wait();
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        this->handleErrorCode(MetaCommon::to(ret));
        this->onFinished();
        return;
    }
    ret = LastUpdateTimeMan::update(kvstore_, time::WallClock::fastNowInMilliSec());
    this->handleErrorCode(MetaCommon::to(ret));
    this->onFinished();
}

template<typename RESP>
StatusOr<std::vector<nebula::cpp2::IndexItem>>
BaseProcessor<RESP>::getIndexes(GraphSpaceID spaceId,
                                int32_t tagOrEdge) {
    std::vector<nebula::cpp2::IndexItem> items;
    auto indexPrefix = MetaServiceUtils::indexPrefix(spaceId);
    auto iterRet = doPrefix(indexPrefix);
    if (!iterRet.ok()) {
        return iterRet.status();
    }
    auto indexIter = iterRet.value().get();
    while (indexIter->valid()) {
        auto item = MetaServiceUtils::parseIndex(indexIter->val());
        if (item.get_schema_id().getType() == nebula::cpp2::SchemaID::Type::tag_id &&
            item.get_schema_id().get_tag_id() == tagOrEdge) {
            items.emplace_back(std::move(item));
        } else if (item.get_schema_id().getType() == nebula::cpp2::SchemaID::Type::edge_type &&
                   item.get_schema_id().get_edge_type() == tagOrEdge) {
            items.emplace_back(std::move(item));
        }
        indexIter->next();
    }
    return items;
}

template<typename RESP>
cpp2::ErrorCode
BaseProcessor<RESP>::indexCheck(const std::vector<nebula::cpp2::IndexItem>& items,
                                const std::vector<cpp2::AlterSchemaItem>& alterItems) {
    for (const auto& index : items) {
        for (const auto& tagItem : alterItems) {
            if (tagItem.op == nebula::meta::cpp2::AlterSchemaOp::CHANGE ||
                tagItem.op == nebula::meta::cpp2::AlterSchemaOp::DROP) {
                const auto& tagCols = tagItem.get_schema().get_columns();
                const auto& indexCols = index.get_fields();
                for (const auto& tCol : tagCols) {
                    auto it = std::find_if(indexCols.begin(), indexCols.end(),
                                           [&] (const auto& iCol) {
                                               return tCol.name == iCol.name;
                                           });
                    if (it != indexCols.end()) {
                        LOG(ERROR) << "Index conflict, index :" << index.get_index_name()
                                   << ", column : " << tCol.name;
                        return cpp2::ErrorCode::E_CONFLICT;
                    }
                }
            }
        }
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula
