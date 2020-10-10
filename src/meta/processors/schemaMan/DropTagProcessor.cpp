/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/schemaMan/DropTagProcessor.h"

namespace nebula {
namespace meta {

void DropTagProcessor::process(const cpp2::DropTagReq& req) {
    CHECK_SPACE_ID_AND_RETURN(req.get_space_id());
    GraphSpaceID spaceId = req.get_space_id();
    folly::SharedMutex::WriteHolder wHolder(LockUtils::tagLock());
    TagID tagId;
    auto indexKey = MetaServiceUtils::indexTagKey(spaceId, req.get_tag_name());
    auto iRet = doGet(indexKey);
    if (iRet.ok()) {
        tagId = *reinterpret_cast<const TagID *>(iRet.value().data());
        resp_.set_id(to(tagId, EntryType::TAG));
    } else {
        handleErrorCode(req.get_if_exists() ? cpp2::ErrorCode::SUCCEEDED
                                           : cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    auto indexes = getIndexes(spaceId, tagId);
    if (!indexes.ok()) {
        handleErrorCode(MetaCommon::to(indexes.status()));
        onFinished();
        return;
    }
    if (!indexes.value().empty()) {
        LOG(ERROR) << "Drop tag error, index conflict";
        handleErrorCode(cpp2::ErrorCode::E_CONFLICT);
        onFinished();
        return;
    }

    auto ret = getTagKeys(req.get_space_id(), tagId);
    if (!ret.ok()) {
        LOG(ERROR) << "Drop Tag Failed : " << req.get_tag_name() << " not found";
        handleErrorCode(MetaCommon::to(ret.status()));
        onFinished();
        return;
    }
    auto keys = std::move(ret).value();
    keys.emplace_back(indexKey);
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    LOG(INFO) << "Drop Tag " << req.get_tag_name();
    doSyncMultiRemoveAndUpdate(std::move(keys));
}

StatusOr<std::vector<std::string>> DropTagProcessor::getTagKeys(GraphSpaceID id, TagID tagId) {
    std::vector<std::string> keys;
    auto key = MetaServiceUtils::schemaTagPrefix(id, tagId);
    auto iterRet = doPrefix(key);
    if (!iterRet.ok()) {
        return Status::Error("Tag get error by id : %d !", tagId);
    }

    auto iter = iterRet.value().get();
    while (iter->valid()) {
        keys.emplace_back(iter->key());
        iter->next();
    }
    return keys;
}

}  // namespace meta
}  // namespace nebula


