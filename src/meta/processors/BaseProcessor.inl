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
        this->resp_.set_code(to(code));
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
    return std::move(iter);
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
    auto code = kvstore_->multiGet(kDefaultSpaceId, kDefaultPartId, keys, &values);
    if (code != kvstore::ResultCode::SUCCEEDED) {
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
        this->resp_.set_code(to(code));
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
        this->resp_.set_code(to(code));
        baton.post();
    });
    baton.wait();
    this->onFinished();
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
        this->resp_.set_code(to(code));
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
            return to(ret);
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
        return to(ret);
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
Status BaseProcessor<RESP>::userExist(UserID spaceId) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::userLock());
    auto userKey = MetaServiceUtils::userKey(spaceId);
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

template<typename RESP>
StatusOr<std::unordered_map<std::string, nebula::cpp2::ValueType>>
BaseProcessor<RESP>::getLatestTagFields(GraphSpaceID spaceId,
                                        const std::string& name) {
    auto result = getTagId(spaceId, name);
    if (!result.ok()) {
        LOG(ERROR) << "Tag " << name << " not found";
        return Status::Error(folly::stringPrintf("Tag %s not found", name.c_str()));
    }

    auto key = MetaServiceUtils::schemaTagPrefix(spaceId, result.value());
    auto ret = doPrefix(key);
    if (!ret.ok()) {
        LOG(ERROR) << "Tag Prefix " << key << " not found";
        return Status::Error(folly::stringPrintf("Tag Prefix %s not found", key.c_str()));
    }

    auto iter = ret.value().get();
    auto latestSchema = MetaServiceUtils::parseSchema(iter->val());
    std::unordered_map<std::string, nebula::cpp2::ValueType> propertyNames;
    for (auto &column : latestSchema.get_columns()) {
        propertyNames.emplace(std::move(column.get_name()),
                              std::move(column.get_type()));
    }
    return propertyNames;
}

template<typename RESP>
StatusOr<std::unordered_map<std::string, nebula::cpp2::ValueType>>
BaseProcessor<RESP>::getLatestEdgeFields(GraphSpaceID spaceId,
                                         const std::string& name) {
    auto result = getEdgeType(spaceId, name);
    if (!result.ok()) {
        LOG(ERROR) << "Edge " << name << " not found";
        return Status::Error(folly::stringPrintf("Edge %s not found", name.c_str()));
    }

    auto edgeType = to(result.value(), EntryType::EDGE);
    auto key = MetaServiceUtils::schemaEdgePrefix(spaceId, result.value());
    auto ret = doPrefix(key);
    if (!ret.ok()) {
        LOG(ERROR) << "Edge Prefix " << key << " not found";
        return Status::Error(folly::stringPrintf("Edge Prefix %s not found", key.c_str()));
    }

    auto iter = ret.value().get();
    auto latestSchema = MetaServiceUtils::parseSchema(iter->val());
    std::unordered_map<std::string, nebula::cpp2::ValueType> propertyNames;
    for (auto &column : latestSchema.get_columns()) {
        propertyNames.emplace(std::move(column.get_name()),
                              std::move(column.get_type()));
    }
    return propertyNames;
}

template<typename RESP>
StatusOr<TagIndexID>
BaseProcessor<RESP>::getTagIndexID(GraphSpaceID spaceId, const std::string& indexName) {
    auto indexKey = MetaServiceUtils::indexTagIndexKey(spaceId, indexName);
    auto ret = doGet(indexKey);
    if (ret.ok()) {
        return *reinterpret_cast<const TagIndexID*>(ret.value().c_str());
    }
    return Status::TagIndexNotFound(folly::stringPrintf("Tag Index %s not found",
                                                        indexName.c_str()));
}

template<typename RESP>
StatusOr<EdgeIndexID>
BaseProcessor<RESP>::getEdgeIndexID(GraphSpaceID spaceId, const std::string& indexName) {
    auto indexKey = MetaServiceUtils::indexEdgeIndexKey(spaceId, indexName);
    auto ret = doGet(indexKey);
    if (ret.ok()) {
        return *reinterpret_cast<const EdgeIndexID*>(ret.value().c_str());
    }
    return Status::EdgeIndexNotFound(folly::stringPrintf("Edge Index %s not found",
                                                         indexName.c_str()));
}

template<typename RESP>
StatusOr<UserID>
BaseProcessor<RESP>::getUserId(const std::string& account) {
    auto indexKey = MetaServiceUtils::indexUserKey(account);
    auto ret = doGet(indexKey);
    if (ret.ok()) {
        return *reinterpret_cast<const UserID*>(ret.value().c_str());
    }
    return Status::UserNotFound(folly::stringPrintf("User %s not found", account.c_str()));
}

template<typename RESP>
bool BaseProcessor<RESP>::checkPassword(UserID userId, const std::string& password) {
    auto userKey = MetaServiceUtils::userKey(userId);
    auto ret = doGet(userKey);
    if (ret.ok()) {
        return  ret.value().compare(sizeof(int32_t), password.size(), password) == 0;
    }
    return false;
}

template<typename RESP>
StatusOr<std::string>
BaseProcessor<RESP>::getUserAccount(UserID userId) {
    auto key = MetaServiceUtils::userKey(userId);
    auto ret = doGet(key);
    if (!ret.ok()) {
        return Status::UserNotFound(folly::stringPrintf("User not found by id %d", userId));
    }

    return MetaServiceUtils::parseUserItem(ret.value()).get_account();
}

template<typename RESP>
bool BaseProcessor<RESP>::doSyncPut(std::vector<kvstore::KV> data) {
    folly::Baton<true, std::atomic> baton;
    bool ret = false;
    kvstore_->asyncMultiPut(kDefaultSpaceId,
                            kDefaultPartId,
                            std::move(data),
                            [&ret, &baton] (kvstore::ResultCode code) {
                                if (kvstore::ResultCode::SUCCEEDED == code) {
                                    ret = true;
                                } else {
                                    LOG(INFO) << "Put data error on meta server";
                                }
                                baton.post();
                            });
    baton.wait();
    return ret;
}

}  // namespace meta
}  // namespace nebula
