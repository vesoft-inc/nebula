/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/AddEdgeProcessor.h"
#include "time/TimeUtils.h"

namespace nebula {
namespace meta {

void AddEdgeProcessor::process(const cpp2::WriteEdgeReq& req) {
    if (spaceExist(req.get_space_id()) == Status::SpaceNotFound()) {
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    folly::SharedMutex::WriteHolder wHolder(LockUtils::edgeLock());
    auto ret = getEdge(req.get_edge_name());
    std::vector<kvstore::KV> data;
    if (ret.ok()) {
        resp_.set_id(to(ret.value(), EntryType::EDGE));
        resp_.set_code(cpp2::ErrorCode::E_EXISTED);
        onFinished();
        return;
    }
    auto version = std::numeric_limits<int64_t>::max() - time::TimeUtils::nowInMSeconds();
    EdgeType edgeType = autoIncrementId();
    data.emplace_back(MetaServiceUtils::indexKey(EntryType::EDGE, req.get_edge_name()),
                      std::string(reinterpret_cast<const char*>(&edgeType), sizeof(edgeType)));
    data.emplace_back(MetaServiceUtils::schemaEdgeKey(req.get_space_id(), edgeType, version),
                      MetaServiceUtils::schemaEdgeVal(req.get_edge_name(), req.get_schema()));
    VLOG(3) << "Add edge schema type = " << edgeType << ", name = " << req.get_edge_name();
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_id(to(edgeType, EntryType::EDGE));
    doPut(std::move(data));
}

StatusOr<EdgeType> AddEdgeProcessor::getEdge(const std::string& edgeName) {
    auto indexKey = MetaServiceUtils::indexKey(EntryType::EDGE, edgeName);
    std::string val;
    auto ret = kvstore_->get(kDefaultSpaceId_, kDefaultPartId_, indexKey, &val);
    if (ret == kvstore::ResultCode::SUCCEEDED) {
        return *reinterpret_cast<const EdgeType*>(val.c_str());
    }
    return Status::Error("No Edge!");
}

}  // namespace meta
}  // namespace nebula

