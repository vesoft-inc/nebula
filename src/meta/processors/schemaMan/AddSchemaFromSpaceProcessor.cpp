/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/schemaMan/AddSchemaFromSpaceProcessor.h"

namespace nebula {
namespace meta {

void AddSchemaFromSpaceProcessor::process(const cpp2::AddSchemaFromSpaceReq& req) {
    GraphSpaceID currentSpaceId, fromSpaceId = -1;
    {
        folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
        auto cSpaceRet = getSpaceId(req.get_current_space());
        if (!cSpaceRet.ok()) {
            resp_.set_code(to(cSpaceRet.status()));
            onFinished();
            return;
        }

        auto fSpaceRet = getSpaceId(req.get_from_space());
        if (!fSpaceRet.ok()) {
            resp_.set_code(to(fSpaceRet.status()));
            onFinished();
            return;
        }
        currentSpaceId = std::move(cSpaceRet).value();
        fromSpaceId = std::move(fSpaceRet).value();
    }
    std::vector<kvstore::KV> data;
    std::unique_ptr<kvstore::KVIterator> iter;
    folly::SharedMutex::WriteHolder tagWHolder(LockUtils::tagLock());
    folly::SharedMutex::WriteHolder edgeWHolder(LockUtils::edgeLock());
    // add tags
    auto tagPrefix = MetaServiceUtils::schemaTagsPrefix(fromSpaceId);
    auto tagRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, tagPrefix, &iter);
    if (tagRet != kvstore::ResultCode::SUCCEEDED) {
        resp_.set_code(to(tagRet));
        onFinished();
        return;
    }

    while (iter->valid()) {
        auto val = iter->val();
        auto nameLen = *reinterpret_cast<const int32_t *>(val.data());
        auto tagName = val.subpiece(sizeof(int32_t), nameLen).str();
        auto schema = MetaServiceUtils::parseSchema(val);

        // if current space exists the same name of tag or edge, return error
        if (isExist(currentSpaceId, tagName)) {
            resp_.set_code(cpp2::ErrorCode::E_EXISTED);
            onFinished();
            return;
        }

        auto ret = autoIncrementId();
        if (!nebula::ok(ret)) {
            LOG(ERROR) << "Add tag: " << tagName << " failed, get tag type failed";
            resp_.set_code(nebula::error(ret));
            onFinished();
            return;
        }
        auto tagId = nebula::value(ret);
        data.emplace_back(MetaServiceUtils::indexTagKey(currentSpaceId, tagName),
                            std::string(reinterpret_cast<const char*>(&tagId),
                                        sizeof(TagID)));
        VLOG(2) << "Add tag " << tagName << ", tagId " << tagId;
        data.emplace_back(MetaServiceUtils::schemaTagKey(currentSpaceId, tagId, 0),
                            MetaServiceUtils::schemaTagVal(tagName, schema));
        iter->next();
    }

    // add edges
    auto edgePrefix = MetaServiceUtils::schemaEdgesPrefix(fromSpaceId);
    auto edgeRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, edgePrefix, &iter);
    resp_.set_code(to(edgeRet));
    if (edgeRet != kvstore::ResultCode::SUCCEEDED) {
        onFinished();
        return;
    }

    while (iter->valid()) {
        auto val = iter->val();
        auto nameLen = *reinterpret_cast<const int32_t *>(val.data());
        auto edgeName = val.subpiece(sizeof(int32_t), nameLen).str();
        auto schema = MetaServiceUtils::parseSchema(val);

        if (isExist(currentSpaceId, edgeName)) {
            resp_.set_code(cpp2::ErrorCode::E_EXISTED);
            onFinished();
            return;
        }

        auto ret = autoIncrementId();
        if (!nebula::ok(ret)) {
            LOG(ERROR) << "Add edge: " << edgeName << " failed, get edge type failed";
            resp_.set_code(nebula::error(ret));
            onFinished();
            return;
        }
        auto edgeType = nebula::value(ret);
        data.emplace_back(MetaServiceUtils::indexEdgeKey(currentSpaceId, edgeName),
                            std::string(reinterpret_cast<const char*>(&edgeType),
                                        sizeof(EdgeType)));
        VLOG(2) << "Add edge " << edgeName << ", edgeType " << edgeType;
        data.emplace_back(MetaServiceUtils::schemaEdgeKey(currentSpaceId, edgeType, 0),
                            MetaServiceUtils::schemaEdgeVal(edgeName, schema));
        iter->next();
    }

    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    doSyncPutAndUpdate(std::move(data));
}

bool AddSchemaFromSpaceProcessor::isExist(const GraphSpaceID spaceId,
                                          const std::string &name) {
    // Check edgeName if exist in current space's tags
    auto existRet = getEdgeType(spaceId, name);
    if (existRet.ok()) {
        return true;
    }

    // Check tagName if exist in current space's edges
    existRet = getTagId(spaceId, name);
    if (existRet.ok()) {
        return true;
    }
    return false;
}
}  // namespace meta
}  // namespace nebula

