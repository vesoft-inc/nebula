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
    const auto& indexName = req.get_index_name();
    CHECK_SPACE_ID_AND_RETURN(spaceID);
    folly::SharedMutex::WriteHolder wHolder(LockUtils::tagIndexLock());

    auto tagIndexIDRet = getIndexID(spaceID, indexName);
    if (!nebula::ok(tagIndexIDRet)) {
        auto retCode = nebula::error(tagIndexIDRet);
        if (retCode == nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND) {
            if (req.get_if_exists()) {
                retCode = nebula::cpp2::ErrorCode::SUCCEEDED;
            } else {
                LOG(ERROR) << "Drop Tag Index Failed, index name " << indexName
                           << " not exists in Space: "<< spaceID;
            }
        } else {
            LOG(ERROR) << "Drop Tag Index Failed, index name " << indexName
                       << " error: " << apache::thrift::util::enumNameSafe(retCode);
        }
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    auto tagIndexID = nebula::value(tagIndexIDRet);
    std::vector<std::string> keys;
    keys.emplace_back(MetaServiceUtils::indexIndexKey(spaceID, indexName));
    keys.emplace_back(MetaServiceUtils::indexKey(spaceID, tagIndexID));

    LOG(INFO) << "Drop Tag Index " << indexName;
    resp_.set_id(to(tagIndexID, EntryType::INDEX));
    doSyncMultiRemoveAndUpdate(std::move(keys));
}

}  // namespace meta
}  // namespace nebula

