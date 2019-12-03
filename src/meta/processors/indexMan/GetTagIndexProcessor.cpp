/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/indexMan/GetTagIndexProcessor.h"

namespace nebula {
namespace meta {

void GetTagIndexProcessor::process(const cpp2::GetTagIndexReq& req) {
    auto spaceID = req.get_space_id();
    auto indexName = req.get_index_name();
    CHECK_SPACE_ID_AND_RETURN(spaceID);
    folly::SharedMutex::ReadHolder rHolder(LockUtils::tagIndexLock());

    auto tagIndexIDResult = getTagIndexID(spaceID, indexName);
    if (!tagIndexIDResult.ok()) {
        LOG(ERROR) << "Get Tag Index SpaceID: " << spaceID
                   << " Index Name: " << indexName << " not found";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    LOG(INFO) << "Get Tag Index SpaceID: " << spaceID << " Index Name: " << indexName;
    auto tagKey = MetaServiceUtils::tagIndexKey(spaceID, tagIndexIDResult.value());
    auto tagResult = doGet(tagKey);
    if (!tagResult.ok()) {
        LOG(ERROR) << "Get Tag Index Failed: SpaceID " << spaceID
                   << " Index Name: " << indexName << " status: " << tagResult.status();
        onFinished();
        return;
    }

    cpp2::TagIndexItem item;
    item.set_index_id(tagIndexIDResult.value());
    item.set_fields(MetaServiceUtils::parseTagIndex(tagResult.value()));
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_item(std::move(item));
    onFinished();
}

}  // namespace meta
}  // namespace nebula
