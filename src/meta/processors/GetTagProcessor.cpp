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
    std::string val;
    std::string tagKey = MetaServiceUtils::schemaTagKey(req.get_space_id(),
                                                        req.get_tag_id(),
                                                        req.get_version());
    auto ret = kvstore_->get(kDefaultSpaceId_, kDefaultPartId_, std::move(tagKey), &val);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    resp_.set_schema(MetaServiceUtils::parseSchema(val));
    onFinished();
}
}  // namespace meta
}  // namespace nebula

