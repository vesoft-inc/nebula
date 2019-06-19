/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/schemaMan/CreateEdgeProcessor.h"
#include "time/TimeUtils.h"

namespace nebula {
namespace meta {

void CreateEdgeProcessor::process(const cpp2::CreateEdgeReq& req) {
    auto spaceId = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(spaceId);
    folly::SharedMutex::WriteHolder wHolder(LockUtils::edgeLock());
    auto ret = getEdgeType(spaceId, req.get_edge_name());
    if (ret.ok()) {
        resp_.set_id(to(ret.value(), EntryType::EDGE));
        resp_.set_code(cpp2::ErrorCode::E_EXISTED);
        onFinished();
        return;
    }
    std::vector<kvstore::KV> data;
    EdgeType edgeType = autoIncrementId();
    data.emplace_back(MetaServiceUtils::indexEdgeKey(spaceId, req.get_edge_name()),
                      std::string(reinterpret_cast<const char*>(&edgeType), sizeof(edgeType)));
    data.emplace_back(MetaServiceUtils::schemaEdgeKey(spaceId, edgeType, 0),
                      MetaServiceUtils::schemaEdgeVal(req.get_edge_name(), req.get_schema()));
    data.emplace_back(MetaServiceUtils::schemaLatestEdgeKey(spaceId, edgeType),
                      MetaServiceUtils::schemaEdgeVal(req.get_edge_name(), req.get_schema()));

    LOG(INFO) << "Create Edge " << req.get_edge_name() << ", edgeType " << edgeType;
    resp_.set_id(to(edgeType, EntryType::EDGE));
    doPut(std::move(data));
}

}  // namespace meta
}  // namespace nebula

