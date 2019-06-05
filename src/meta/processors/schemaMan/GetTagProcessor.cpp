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
    std::string tagKey = MetaServiceUtils::schemaTagKey(req.get_space_id(),
                                                        req.get_tag_id(),
                                                        req.get_version());
    auto ret = doGet(std::move(tagKey));
    if (!ret.ok()) {
        LOG(ERROR) << "Get Tag SpaceID: " << req.get_space_id() << ", tagID: " << req.get_tag_id()
                   << ", version " << req.get_version() << " not found";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    VLOG(3) << "Get Tag SpaceID: " << req.get_space_id() << ", tagID: " << req.get_tag_id()
            << ", version " << req.get_version();
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_schema(MetaServiceUtils::parseSchema(ret.value()));
    onFinished();
}
}  // namespace meta
}  // namespace nebula

