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

    auto tagIndexID = getIndexID(spaceID, indexName);
    if (!tagIndexID.ok()) {
        LOG(ERROR) << "Tag Index not exists in Space: " << spaceID << " Index name: " << indexName;
        if (req.get_if_exists()) {
            handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
        } else {
            handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        }
        onFinished();
        return;
    }

    std::vector<std::string> keys;

    // delete rebuild index status info
    auto indexStatusKey = MetaServiceUtils::rebuildIndexStatus(spaceID, 'T', indexName);
    std::string status;
    auto ret = kvstore_->get(kDefaultSpaceId, kDefaultPartId, indexStatusKey, &status);
    if (ret == kvstore::ResultCode::SUCCEEDED) {
        if (status == "RUNNING") {
            LOG(ERROR) << "Tag Index status is RUNNING, drop index is not allowed. Index : "
                       << indexName;
            handleErrorCode(cpp2::ErrorCode::E_CONFLICT);
            onFinished();
            return;
        } else {
            keys.emplace_back(std::move(indexStatusKey));
        }
    }
    keys.emplace_back(MetaServiceUtils::indexIndexKey(spaceID, indexName));
    keys.emplace_back(MetaServiceUtils::indexKey(spaceID, tagIndexID.value()));

    LOG(INFO) << "Drop Tag Index " << indexName;
    resp_.set_id(to(tagIndexID.value(), EntryType::INDEX));
    doSyncMultiRemoveAndUpdate(std::move(keys));
}

}  // namespace meta
}  // namespace nebula

