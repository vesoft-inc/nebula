/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/schemaMan/CopySchemaFromSpaceProcessor.h"

namespace nebula {
namespace meta {

void CopySchemaFromSpaceProcessor::process(const cpp2::CopySchemaFromSpaceReq& req) {
    {
        folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
        auto fSpaceRet = getSpaceId(req.get_from_space());
        if (!fSpaceRet.ok()) {
            resp_.set_code(to(fSpaceRet.status()));
            onFinished();
            return;
        }

        auto cSpaceRet = getSpaceId(req.get_current_space());
        if (!cSpaceRet.ok()) {
            resp_.set_code(to(cSpaceRet.status()));
            onFinished();
            return;
        }

        fromSpaceId_ = fSpaceRet.value();
        currentSpaceId_ = cSpaceRet.value();
    }
    std::vector<kvstore::KV> data;
    folly::SharedMutex::WriteHolder tagWHolder(LockUtils::tagLock());
    folly::SharedMutex::WriteHolder edgeWHolder(LockUtils::edgeLock());
    if (!copySchemas(data)) {
        onFinished();
        return;
    }
    if (req.get_need_index()) {
        folly::SharedMutex::WriteHolder tagIndexWHolder(LockUtils::tagIndexLock());
        folly::SharedMutex::WriteHolder edgeIndexWHolder(LockUtils::edgeIndexLock());
        if (!copyIndexes(data)) {
            onFinished();
            return;
        }
        resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
        doSyncPutAndUpdate(std::move(data));
    } else {
        resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
        doSyncPutAndUpdate(std::move(data));
    }
}

bool CopySchemaFromSpaceProcessor::isExist(const std::string &name) {
    // Check edgeName if exist in current space's tags
    auto existRet = getEdgeType(currentSpaceId_, name);
    if (existRet.ok()) {
        return true;
    }

    // Check tagName if exist in current space's edges
    existRet = getTagId(currentSpaceId_, name);
    if (existRet.ok()) {
        return true;
    }
    return false;
}


bool CopySchemaFromSpaceProcessor::copySchemas(std::vector<kvstore::KV> &data) {
    // copy tags
    std::unique_ptr<kvstore::KVIterator> iter;
    auto tagPrefix = MetaServiceUtils::schemaTagsPrefix(fromSpaceId_);
    auto tagRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, tagPrefix, &iter);
    if (tagRet != kvstore::ResultCode::SUCCEEDED) {
        resp_.set_code(to(tagRet));
        return false;
    }

    while (iter->valid()) {
        auto val = iter->val();
        auto nameLen = *reinterpret_cast<const int32_t *>(val.data());
        auto tagName = val.subpiece(sizeof(int32_t), nameLen).str();
        auto schema = MetaServiceUtils::parseSchema(val);

        // if current space exists the same name of tag or edge, return error
        if (isExist(tagName)) {
            resp_.set_code(cpp2::ErrorCode::E_EXISTED);
            return false;
        }

        auto ret = autoIncrementId();
        if (!nebula::ok(ret)) {
            LOG(ERROR) << "Copy tag: " << tagName << " failed, get tag id failed";
            resp_.set_code(nebula::error(ret));
            return false;
        }
        auto tagId = nebula::value(ret);
        data.emplace_back(MetaServiceUtils::indexTagKey(currentSpaceId_, tagName),
                          std::string(reinterpret_cast<const char*>(&tagId), sizeof(TagID)));
        VLOG(2) << "Copy tag " << tagName << ", tagId " << tagId;
        data.emplace_back(MetaServiceUtils::schemaTagKey(currentSpaceId_, tagId, 0),
                          MetaServiceUtils::schemaTagVal(tagName, schema));
        iter->next();
    }

    // copy edges
    auto edgePrefix = MetaServiceUtils::schemaEdgesPrefix(fromSpaceId_);
    auto edgeRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, edgePrefix, &iter);
    resp_.set_code(to(edgeRet));
    if (edgeRet != kvstore::ResultCode::SUCCEEDED) {
        return false;
    }

    while (iter->valid()) {
        auto val = iter->val();
        auto nameLen = *reinterpret_cast<const int32_t *>(val.data());
        auto edgeName = val.subpiece(sizeof(int32_t), nameLen).str();
        auto schema = MetaServiceUtils::parseSchema(val);

        if (isExist(edgeName)) {
            resp_.set_code(cpp2::ErrorCode::E_EXISTED);
            return false;
        }

        auto ret = autoIncrementId();
        if (!nebula::ok(ret)) {
            LOG(ERROR) << "Copy edge: " << edgeName << " failed, get edge type failed";
            resp_.set_code(nebula::error(ret));
            return false;
        }
        auto edgeType = nebula::value(ret);
        data.emplace_back(MetaServiceUtils::indexEdgeKey(currentSpaceId_, edgeName),
                          std::string(reinterpret_cast<const char*>(&edgeType),
                                      sizeof(EdgeType)));
        VLOG(2) << "Copy edge " << edgeName << ", edgeType " << edgeType;
        data.emplace_back(MetaServiceUtils::schemaEdgeKey(currentSpaceId_, edgeType, 0),
                          MetaServiceUtils::schemaEdgeVal(edgeName, schema));
        iter->next();
    }
    return true;
}

bool CopySchemaFromSpaceProcessor::copyIndexes(std::vector<kvstore::KV> &data) {
    auto prefix = MetaServiceUtils::indexPrefix(fromSpaceId_);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        resp_.set_code(to(ret));
        LOG(ERROR) << "List Tag Index Failed: SpaceID " << fromSpaceId_;
        return false;
    }

    while (iter->valid()) {
        auto val = iter->val();
        auto indexItem = MetaServiceUtils::parseIndex(val);
        auto indexName = indexItem.get_index_name();
        // if current space exists the same name of tag or edge, return error
        if (isExist(indexName)) {
            resp_.set_code(cpp2::ErrorCode::E_EXISTED);
            return false;
        }
        auto idRet = autoIncrementId();
        if (!nebula::ok(idRet)) {
            LOG(ERROR) << "Copy tag index: " << indexName << " failed, get tag index id failed";
            resp_.set_code(nebula::error(idRet));
            return false;
        }

        auto indexId = nebula::value(idRet);
        data.emplace_back(MetaServiceUtils::indexIndexKey(currentSpaceId_, indexName),
                          std::string(reinterpret_cast<const char*>(&indexId),
                                      sizeof(IndexID)));
        VLOG(2) << "Copy index " << indexName << ", index id " << indexId;
        data.emplace_back(MetaServiceUtils::indexKey(currentSpaceId_, indexId),
                          MetaServiceUtils::indexVal(indexItem));
        iter->next();
    }
    return true;
}
}  // namespace meta
}  // namespace nebula

