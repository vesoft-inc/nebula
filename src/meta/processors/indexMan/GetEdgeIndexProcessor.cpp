/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/indexMan/GetEdgeIndexProcessor.h"

namespace nebula {
namespace meta {

void GetEdgeIndexProcessor::process(const cpp2::GetEdgeIndexReq& req) {
    auto spaceID = req.get_space_id();
    auto indexName = req.get_index_name();
    CHECK_SPACE_ID_AND_RETURN(spaceID);
    folly::SharedMutex::ReadHolder rHolder(LockUtils::edgeIndexLock());
    auto edgeIndexIDResult = getEdgeIndexID(spaceID, indexName);
    if (!edgeIndexIDResult.ok()) {
        LOG(ERROR) << "Get Edge Index SpaceID: " << spaceID
                   << " Index Name: " << indexName << " not found";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    LOG(INFO) << "Get Edge Index SpaceID: " << spaceID << " Index Name: " << indexName;
    auto edgeKey = MetaServiceUtils::edgeIndexKey(spaceID, edgeIndexIDResult.value());
    auto edgeResult = doGet(edgeKey);
    if (!edgeResult.ok()) {
        LOG(ERROR) << "Get Edge Index Failed: SpaceID " << spaceID << " Index Name: " << indexName;
        onFinished();
        return;
    }

    cpp2::EdgeIndexItem item;
    item.set_index_id(edgeIndexIDResult.value());
    item.set_properties(MetaServiceUtils::parseEdgeIndex(edgeResult.value()));
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_item(std::move(item));
    onFinished();
}

}  // namespace meta
}  // namespace nebula
