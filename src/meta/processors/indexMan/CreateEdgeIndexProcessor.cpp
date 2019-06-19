/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/indexMan/CreateEdgeIndexProcessor.h"

namespace nebula {
namespace meta {

void CreateEdgeIndexProcessor::process(const cpp2::CreateEdgeIndexReq& req) {
    auto spaceID = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(spaceID);
    folly::SharedMutex::WriteHolder wHolder(LockUtils::edgeIndexLock());
    auto properties = req.get_properties();
    auto indexName = properties.get_index_name();
    auto ret = getEdgeIndexID(spaceID, indexName);
    if (ret.ok()) {
        LOG(ERROR) << "Create Edge Index Failed: " << indexName << " already existed";
        resp_.set_id(to(ret.value(), EntryType::EDGE_INDEX));
        resp_.set_code(cpp2::ErrorCode::E_EXISTED);
        onFinished();
        return;
    }

    for (auto const &element : properties.get_edge_fields()) {
        ret = getEdgeType(spaceID, element.first);
        if (!ret.ok()) {
            LOG(ERROR) << "Create Edge Index Failed: " << element.first << " not exist";
            resp_.set_id(to(ret.value(), EntryType::EDGE_INDEX));
            resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
            onFinished();
            return;
        }
    }

    std::vector<kvstore::KV> data;
    EdgeIndexID edgeIndex = autoIncrementId();
    data.emplace_back(MetaServiceUtils::indexEdgeIndexKey(spaceID, indexName),
                      std::string(reinterpret_cast<const char*>(&edgeIndex), sizeof(EdgeIndexID)));
    data.emplace_back(MetaServiceUtils::edgeIndexKey(spaceID, edgeIndex),
                      MetaServiceUtils::edgeIndexVal(properties));
    LOG(INFO) << "Create Edge Index " << indexName << ", edgeIndex " << edgeIndex;
    resp_.set_id(to(edgeIndex, EntryType::EDGE_INDEX));
    doPut(std::move(data));
}

}  // namespace meta
}  // namespace nebula

