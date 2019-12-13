/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/indexMan/DropTagIndexProcessor.h"

namespace nebula {
namespace meta {

void DropTagIndexProcessor::process(const cpp2::DropTagIndexReq& req) {
    auto spaceID = req.get_space_id();
    auto indexName = req.get_index_name();
    CHECK_SPACE_ID_AND_RETURN(spaceID);
    folly::SharedMutex::WriteHolder wHolder(LockUtils::tagIndexLock());

    auto tagIndexID = getTagIndexID(spaceID, indexName);
    if (!tagIndexID.ok()) {
        LOG(ERROR) << "Tag Index not exist Space: " << spaceID << " Index name: " << indexName;
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    std::vector<std::string> keys;
    keys.emplace_back(MetaServiceUtils::indexTagIndexKey(spaceID, indexName));
    keys.emplace_back(MetaServiceUtils::tagIndexKey(spaceID, tagIndexID.value()));

    LOG(INFO) << "Drop Tag Index " << indexName;
    resp_.set_id(to(tagIndexID.value(), EntryType::TAG_INDEX));
    doMultiRemove(keys);
}

}  // namespace meta
}  // namespace nebula

