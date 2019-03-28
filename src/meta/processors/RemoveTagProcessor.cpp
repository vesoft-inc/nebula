/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/RemoveTagProcessor.h"

namespace nebula {
namespace meta {

void RemoveTagProcessor::process(const cpp2::RemoveTagReq& req) {
    TagID tagID = -1;
    std::string tagVal;
    if (!spaceExist(req.get_space_id())) {
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    folly::RWSpinLock::WriteHolder wHolder(LockUtils::tagLock());
    auto indexKey = MetaUtils::indexKey(EntryType::TAG, req.get_tag_name());
    auto ret = kvstore_->get(kDefaultSpaceId_, kDefaultPartId_, indexKey, &tagVal);
    if (ret == kvstore::ResultCode::SUCCEEDED) {
        tagID = *reinterpret_cast<const TagID *>(tagVal.data());
    } else {
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    std::string start = MetaUtils::schemaTagKey(req.get_space_id(), tagID, 0x0000000);
    std::string end = MetaUtils::schemaTagKey(req.get_space_id(), tagID, 0x7FFFFFFF);
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_id(to(tagID, IDType::TAG));
    doRemoveMixed(indexKey, start, end);
}

}  // namespace meta
}  // namespace nebula

