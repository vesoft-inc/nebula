/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/schemaMan/DropTagProcessor.h"

namespace nebula {
namespace meta {

void DropTagProcessor::process(const cpp2::DropTagReq& req) {
    GraphSpaceID spaceId = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(spaceId);

    folly::SharedMutex::ReadHolder rHolder(LockUtils::snapshotLock());
    folly::SharedMutex::WriteHolder wHolder(LockUtils::tagLock());
    auto tagName = req.get_tag_name();

    TagID tagId;
    auto indexKey = MetaServiceUtils::indexTagKey(spaceId, tagName);
    auto iRet = doGet(indexKey);
    if (nebula::ok(iRet)) {
        tagId = *reinterpret_cast<const TagID *>(nebula::value(iRet).c_str());
        resp_.set_id(to(tagId, EntryType::TAG));
    } else {
        auto retCode = nebula::error(iRet);
        if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
            if (req.get_if_exists()) {
                retCode = nebula::cpp2::ErrorCode::SUCCEEDED;
            } else {
                LOG(ERROR) << "Drop tag failed :" << tagName << " not found.";
                retCode = nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND;
            }
        } else {
            LOG(ERROR) << "Get Tag failed, tag name " << tagName
                       << " error: " << apache::thrift::util::enumNameSafe(retCode);
        }
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    auto indexes = getIndexes(spaceId, tagId);
    if (!nebula::ok(indexes)) {
        handleErrorCode(nebula::error(indexes));
        onFinished();
        return;
    }
    if (!nebula::value(indexes).empty()) {
        LOG(ERROR) << "Drop tag error, index conflict, please delete index first.";
        handleErrorCode(nebula::cpp2::ErrorCode::E_CONFLICT);
        onFinished();
        return;
    }

    auto ftIdxRet = getFTIndex(spaceId, tagId);
    if (nebula::ok(ftIdxRet)) {
        LOG(ERROR) << "Drop tag error, fulltext index conflict, "
                   << "please delete fulltext index first.";
        handleErrorCode(nebula::cpp2::ErrorCode::E_CONFLICT);
        onFinished();
        return;
    }

    if (nebula::error(ftIdxRet) != nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND) {
        handleErrorCode(nebula::error(ftIdxRet));
        onFinished();
        return;
    }

    auto ret = getTagKeys(spaceId, tagId);
    if (!nebula::ok(ret)) {
        handleErrorCode(nebula::error(ret));
        onFinished();
        return;
    }

    auto keys = nebula::value(ret);
    keys.emplace_back(indexKey);
    LOG(INFO) << "Drop Tag " << tagName;
    doSyncMultiRemoveAndUpdate(std::move(keys));
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>>
DropTagProcessor::getTagKeys(GraphSpaceID id, TagID tagId) {
    std::vector<std::string> keys;
    auto key = MetaServiceUtils::schemaTagPrefix(id, tagId);
    auto iterRet = doPrefix(key);
    if (!nebula::ok(iterRet)) {
        LOG(ERROR) << "Tag schema prefix failed, tag id " << tagId;
        return nebula::error(iterRet);
    }

    auto iter = nebula::value(iterRet).get();
    while (iter->valid()) {
        keys.emplace_back(iter->key());
        iter->next();
    }
    return keys;
}

}  // namespace meta
}  // namespace nebula


