/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/GetTagProcessor.h"

namespace nebula {
namespace meta {

void GetTagProcessor::process(const cpp2::GetTagReq& req) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::tagLock());
    std::string tagKey = MetaServiceUtils::schemaTagKey(req.get_space_id(),
                                                        req.get_tag_id(),
                                                        req.get_version());
    auto ret = doGet(std::move(tagKey));
    if (!ret.ok()) {
        LOG(INFO) << "Get Tag SpaceID: " << req.get_space_id() << ", tagID: " << req.get_tag_id()
                  << ", version " << req.get_version() << " Not Found";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    LOG(INFO) << "Get Tag SpaceID: " << req.get_space_id() << ", tagID: " << req.get_tag_id()
              << ", version " << req.get_version();
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_schema(MetaServiceUtils::parseSchema(ret.value()));
    onFinished();
}
}  // namespace meta
}  // namespace nebula

