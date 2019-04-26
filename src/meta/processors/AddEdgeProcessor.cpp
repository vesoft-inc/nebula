/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/AddEdgeProcessor.h"
#include "time/TimeUtils.h"

namespace nebula {
namespace meta {

void AddEdgeProcessor::process(const cpp2::AddEdgeReq& req) {
    CHECK_SPACE_ID_AND_RETURN(req.get_space_id());
    folly::SharedMutex::WriteHolder wHolder(LockUtils::edgeLock());
    auto ret = getEdgeType(req.get_space_id(), req.get_edge_name());
    if (ret.ok()) {
        LOG(ERROR) << "Add Edge Failed : " << req.get_edge_name() << " have existed";
        resp_.set_id(to(ret.value(), EntryType::EDGE));
        resp_.set_code(cpp2::ErrorCode::E_EXISTED);
        onFinished();
        return;
    }

    auto version = time::TimeUtils::nowInMSeconds();
    EdgeType edgeType = autoIncrementId();
    std::vector<kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::edgeIndexKey(req.get_space_id(), req.get_edge_name()),
                      std::string(reinterpret_cast<const char*>(&edgeType), sizeof(edgeType)));
    LOG(INFO) << "Add Edge " << req.get_edge_name() << ", edgeType " << edgeType
              << ", version " << version;
    data.emplace_back(MetaServiceUtils::schemaEdgeKey(req.get_space_id(), edgeType, version),
                      MetaServiceUtils::schemaEdgeVal(req.get_schema()));
    resp_.set_id(to(edgeType, EntryType::EDGE));
    doPut(std::move(data));
}

}  // namespace meta
}  // namespace nebula
