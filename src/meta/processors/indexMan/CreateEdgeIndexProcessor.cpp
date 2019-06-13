/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/indexMan/CreateEdgeIndexProcessor.h"

namespace nebula {
namespace meta {

void CreateEdgeIndexProcessor::process(const cpp2::CreateEdgeIndexReq& req) {
    CHECK_SPACE_ID_AND_RETURN(req.get_space_id());
    folly::SharedMutex::WriteHolder wHolder(LockUtils::edgeIndexLock());
    auto properties = req.get_properties();
    auto edgeName = properties.get_edge_name();
    auto ret = getEdgeIndexID(req.get_space_id(), edgeName);
    if (ret.ok()) {
        LOG(ERROR) << "Create Edge Index Failed "
                   << edgeName << " already existed";
        resp_.set_id(to(ret.value(), EntryType::EDGE_INDEX));
        resp_.set_code(cpp2::ErrorCode::E_EXISTED);
        onFinished();
        return;
    }

    ret = getEdgeType(req.get_space_id(), properties.get_edge_name());
    if (!ret.ok()) {
        LOG(ERROR) << "Create Edge Index Failed "
                   << properties.get_edge_name() << " not exist";
        resp_.set_id(to(ret.value(), EntryType::EDGE));
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    std::vector<kvstore::KV> data;
    EdgeIndexID edgeIndex = autoIncrementId();
    data.emplace_back(MetaServiceUtils::indexEdgeIndexKey(req.get_space_id(), edgeName),
                      std::string(reinterpret_cast<const char*>(&edgeIndex), sizeof(EdgeIndexID)));
    data.emplace_back(MetaServiceUtils::edgeIndexKey(req.get_space_id(), edgeIndex),
                      MetaServiceUtils::edgeIndexVal(properties));
    LOG(INFO) << "Create Edge Index " << edgeName << ", edgeIndex " << edgeIndex;
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_id(to(edgeIndex, EntryType::EDGE_INDEX));
    doPut(std::move(data));
}

}  // namespace meta
}  // namespace nebula

