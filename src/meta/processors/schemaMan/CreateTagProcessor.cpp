/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/schemaMan/CreateTagProcessor.h"
#include "meta/processors/schemaMan/SchemaUtil.h"

namespace nebula {
namespace meta {

void CreateTagProcessor::process(const cpp2::CreateTagReq& req) {
    GraphSpaceID spaceId = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(spaceId);
    auto tagName = req.get_tag_name();
    {
        // if there is an edge of the same name
        // TODO: there exists race condition, we should address it in the future
        folly::SharedMutex::ReadHolder rHolder(LockUtils::edgeLock());
        auto conflictRet = getEdgeType(spaceId, tagName);
        if (nebula::ok(conflictRet)) {
            LOG(ERROR) << "Failed to create tag `" << tagName
                       << "': some edge with the same name already exists.";
            resp_.set_id(to(nebula::value(conflictRet), EntryType::TAG));
            handleErrorCode(nebula::cpp2::ErrorCode::E_CONFLICT);
            onFinished();
            return;
        } else {
            auto retCode = nebula::error(conflictRet);
            if (retCode != nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND) {
                LOG(ERROR) << "Failed to create tag " << tagName << " error "
                           << apache::thrift::util::enumNameSafe(retCode);
                handleErrorCode(retCode);
                onFinished();
                return;
            }
        }
    }

    auto columns = req.get_schema().get_columns();
    if (!SchemaUtil::checkType(columns)) {
        handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
        onFinished();
        return;
    }

    cpp2::Schema schema;
    schema.set_columns(std::move(columns));
    schema.set_schema_prop(req.get_schema().get_schema_prop());

    folly::SharedMutex::WriteHolder wHolder(LockUtils::tagLock());
    auto ret = getTagId(spaceId, tagName);
    if (nebula::ok(ret)) {
        if (req.get_if_not_exists()) {
            handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
        } else {
            LOG(ERROR) << "Create Tag Failed :" << tagName << " has existed";
            handleErrorCode(nebula::cpp2::ErrorCode::E_EXISTED);
        }
        resp_.set_id(to(nebula::value(ret), EntryType::TAG));
        onFinished();
        return;
    } else {
        auto retCode = nebula::error(ret);
        if (retCode != nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND) {
            LOG(ERROR) << "Failed to create tag " << tagName << " error "
                       << apache::thrift::util::enumNameSafe(retCode);
            handleErrorCode(retCode);
            onFinished();
            return;
        }
    }

    auto tagRet = autoIncrementId();
    if (!nebula::ok(tagRet)) {
        LOG(ERROR) << "Create tag failed : Get tag id failed.";
        handleErrorCode(nebula::error(tagRet));
        onFinished();
        return;
    }

    auto tagId = nebula::value(tagRet);
    std::vector<kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::indexTagKey(spaceId, tagName),
                      std::string(reinterpret_cast<const char*>(&tagId), sizeof(TagID)));
    data.emplace_back(MetaServiceUtils::schemaTagKey(spaceId, tagId, 0),
                      MetaServiceUtils::schemaVal(tagName, schema));

    LOG(INFO) << "Create Tag " << tagName << ", TagID " << tagId;

    resp_.set_id(to(tagId, EntryType::TAG));
    doSyncPutAndUpdate(std::move(data));
}

}  // namespace meta
}  // namespace nebula
