/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/schemaMan/GetTagProcessor.h"

namespace nebula {
namespace meta {

void GetTagProcessor::process(const cpp2::GetTagReq& req) {
    GraphSpaceID spaceId = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(spaceId);
    auto tagName = req.get_tag_name();
    auto ver = req.get_version();

    folly::SharedMutex::ReadHolder rHolder(LockUtils::tagLock());
    auto tagIdRet = getTagId(spaceId, tagName);
    if (!nebula::ok(tagIdRet)) {
        LOG(ERROR) << "Get tag " << tagName << " failed.";
        handleErrorCode(nebula::error(tagIdRet));
        onFinished();
        return;
    }
    auto tagId = nebula::value(tagIdRet);

    std::string schemaValue;
    // Get the lastest version
    if (ver < 0) {
        auto tagPrefix = MetaServiceUtils::schemaTagPrefix(spaceId, tagId);
        auto ret = doPrefix(tagPrefix);
        if (!nebula::ok(ret)) {
            LOG(ERROR) << "Get Tag SpaceID: " << spaceId << ", tagName: " << tagName
                       << ", latest version failed.";
            handleErrorCode(nebula::error(ret));
            onFinished();
            return;
        }
        auto iter = nebula::value(ret).get();
        if (!iter->valid()) {
            LOG(ERROR) << "Get Tag SpaceID: " << spaceId << ", tagName: "
                       << tagName << ", latest version " << " not found.";
            handleErrorCode(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND);
            onFinished();
            return;
        }
        schemaValue = iter->val().str();
    } else {
        auto tagKey = MetaServiceUtils::schemaTagKey(spaceId, tagId, ver);
        auto ret = doGet(tagKey);
        if (!nebula::ok(ret)) {
            LOG(ERROR) << "Get Tag SpaceID: " << spaceId << ", tagName: " << tagName
                       << ", version " << ver << " failed.";
            handleErrorCode(nebula::error(ret));
            onFinished();
            return;
        }
        schemaValue = nebula::value(ret);
    }

    VLOG(3) << "Get Tag SpaceID: " << spaceId << ", tagName: " << tagName
            << ", version " << ver;

    handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
    resp_.set_schema(MetaServiceUtils::parseSchema(schemaValue));
    onFinished();
}

}  // namespace meta
}  // namespace nebula

