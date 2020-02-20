/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/schemaMan/GetTagProcessor.h"

namespace nebula {
namespace meta {

void GetTagProcessor::process(const cpp2::GetTagReq& req) {
    CHECK_SPACE_ID_AND_RETURN(req.get_space_id());
    folly::SharedMutex::ReadHolder rHolder(LockUtils::tagLock());
    auto tagIdRet = getTagId(req.get_space_id(), req.get_tag_name());
    if (!tagIdRet.ok()) {
        handleErrorCode(MetaCommon::to(tagIdRet.status()));
        onFinished();
        return;
    }
    auto tagId = tagIdRet.value();

    std::string schemaValue;
    // Get the lastest version
    if (req.get_version() < 0) {
        auto tagPrefix = MetaServiceUtils::schemaTagPrefix(req.get_space_id(), tagId);
        auto ret = doPrefix(tagPrefix);
        if (!ret.ok()) {
            LOG(ERROR) << "Get Tag SpaceID: " << req.get_space_id()
                       << ", tagName: " << req.get_tag_name()
                       << ", version " << req.get_version() << " not found";
            handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
            onFinished();
            return;
        }
        schemaValue = ret.value()->val().str();
    } else {
        auto tagKey = MetaServiceUtils::schemaTagKey(req.get_space_id(),
                                                     tagId,
                                                     req.get_version());
        auto ret = doGet(tagKey);
        if (!ret.ok()) {
            LOG(ERROR) << "Get Tag SpaceID: " << req.get_space_id()
                       << ", tagName: " << req.get_tag_name()
                       << ", version " << req.get_version() << " not found";
            handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
            onFinished();
            return;
        }
        schemaValue = ret.value();
    }

    VLOG(3) << "Get Tag SpaceID: " << req.get_space_id()
            << ", tagName: " << req.get_tag_name()
            << ", version " << req.get_version();

    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_schema(MetaServiceUtils::parseSchema(schemaValue));
    onFinished();
}
}  // namespace meta
}  // namespace nebula

