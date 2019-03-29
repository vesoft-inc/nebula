/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/GetTagProcessor.h"

namespace nebula {
namespace meta {

void GetTagProcessor::process(const cpp2::GetTagReq& req) {
    auto& tagLock = LockUtils::tagLock();
    if (!tagLock.try_lock_shared()) {
        onFinished();
        return;
    }
    std::string val;
    std::string tagKey = MetaUtils::schemaTagKey(req.get_space_id(),
                                                 req.get_tag_id(),
                                                 req.get_version());
    auto ret = kvstore_->get(kDefaultSpaceId_, kDefaultPartId_, std::move(tagKey), &val);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        tagLock.unlock_shared();
        onFinished();
        return;
    }
    tagLock.unlock_shared();
    resp_.set_schema(MetaUtils::parseSchema(val));
    onFinished();
}
}  // namespace meta
}  // namespace nebula

