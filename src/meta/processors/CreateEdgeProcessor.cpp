/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/CreateEdgeProcessor.h"
#include "time/TimeUtils.h"

namespace nebula {
namespace meta {

void CreateEdgeProcessor::process(const cpp2::CreateEdgeReq& req) {
    if (spaceExist(req.get_space_id()) == Status::SpaceNotFound()) {
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    folly::SharedMutex::WriteHolder wHolder(LockUtils::edgeLock());
    auto ret = getEdgeType(req.get_edge_name());
    if (ret.ok()) {
        resp_.set_id(to(ret.value(), EntryType::EDGE));
        resp_.set_code(cpp2::ErrorCode::E_EXISTED);
        onFinished();
        return;
    }
    std::vector<kvstore::KV> data;
    EdgeType edgeType = autoIncrementId();
    data.emplace_back(MetaServiceUtils::indexKey(EntryType::EDGE, req.get_edge_name()),
                      std::string(reinterpret_cast<const char*>(&edgeType), sizeof(edgeType)));
    data.emplace_back(MetaServiceUtils::schemaEdgeKey(req.get_space_id(), edgeType, 0),
                      MetaServiceUtils::schemaEdgeVal(req.get_edge_name(), req.get_schema()));

    LOG(INFO) << "Create Edge  " << req.get_edge_name() << ", edgeType " << edgeType;
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_id(to(edgeType, EntryType::EDGE));
    doPut(std::move(data));
}

}  // namespace meta
}  // namespace nebula

