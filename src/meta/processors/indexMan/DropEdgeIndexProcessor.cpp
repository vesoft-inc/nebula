/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/indexMan/DropEdgeIndexProcessor.h"

namespace nebula {
namespace meta {

void DropEdgeIndexProcessor::process(const cpp2::DropEdgeIndexReq& req) {
    auto spaceID = req.get_space_id();
    auto indexName = req.get_index_name();
    CHECK_SPACE_ID_AND_RETURN(spaceID);
    folly::SharedMutex::WriteHolder wHolder(LockUtils::edgeIndexLock());

    auto edgeIndexID = getEdgeIndexID(spaceID, indexName);
    if (!edgeIndexID.ok()) {
        LOG(ERROR) << "Edge Index not exist Space: " << spaceID << " Index name: " << indexName;
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    std::vector<std::string> keys;
    keys.emplace_back(MetaServiceUtils::indexEdgeIndexKey(spaceID, indexName));
    keys.emplace_back(MetaServiceUtils::edgeIndexKey(spaceID, edgeIndexID.value()));

    LOG(INFO) << "Drop Edge Index " << indexName;
    resp_.set_id(to(edgeIndexID.value(), EntryType::EDGE_INDEX));
    doMultiRemove(keys);
}

}  // namespace meta
}  // namespace nebula

