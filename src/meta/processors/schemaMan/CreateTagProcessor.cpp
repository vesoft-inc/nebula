/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/schemaMan/CreateTagProcessor.h"
#include "meta/processors/schemaMan/Utils.h"

namespace nebula {
namespace meta {

void CreateTagProcessor::process(const cpp2::CreateTagReq& req) {
    auto checkRet = checkDefaultValueType(req.get_schema().get_columns());
    if (checkRet != cpp2::ErrorCode::SUCCEEDED) {
        handleErrorCode(checkRet);
        onFinished();
        return;
    }
    CHECK_SPACE_ID_AND_RETURN(req.get_space_id());
    auto tagName = req.get_tag_name();
    {
        // if there is an edge of the same name
        // TODO: there exists race condition, we should address it in the future
        folly::SharedMutex::ReadHolder rHolder(LockUtils::edgeLock());
        auto conflictRet = getEdgeType(req.get_space_id(), tagName);
        if (conflictRet.ok()) {
            LOG(ERROR) << "Failed to create tag `" << tagName
                       << "': some edge with the same name already exists.";
            resp_.set_id(to(conflictRet.value(), EntryType::TAG));
            handleErrorCode(cpp2::ErrorCode::E_CONFLICT);
            onFinished();
            return;
        }
    }

    folly::SharedMutex::WriteHolder wHolder(LockUtils::tagLock());
    auto ret = getTagId(req.get_space_id(), tagName);
    if (ret.ok()) {
        if (req.get_if_not_exists()) {
            handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
        } else {
            LOG(ERROR) << "Create Tag Failed :" << tagName << " has existed";
            handleErrorCode(cpp2::ErrorCode::E_EXISTED);
        }
        resp_.set_id(to(ret.value(), EntryType::TAG));
        onFinished();
        return;
    }

    auto tagRet = autoIncrementId();
    if (!nebula::ok(tagRet)) {
        LOG(ERROR) << "Create tag failed : Get tag id failed";
        handleErrorCode(nebula::error(tagRet));
        onFinished();
        return;
    }
    auto tagId = nebula::value(tagRet);
    std::vector<kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::indexTagKey(req.get_space_id(), tagName),
                      std::string(reinterpret_cast<const char*>(&tagId), sizeof(TagID)));
    data.emplace_back(MetaServiceUtils::schemaTagKey(req.get_space_id(), tagId, 0),
                      MetaServiceUtils::schemaVal(req.get_tag_name(), req.get_schema()));

    LOG(INFO) << "Create Tag " << tagName << ", TagID " << tagId;
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_id(to(tagId, EntryType::TAG));
    doSyncPutAndUpdate(std::move(data));
}

}  // namespace meta
}  // namespace nebula
