/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/schemaMan/CreateEdgeProcessor.h"

namespace nebula {
namespace meta {

void CreateEdgeProcessor::process(const cpp2::CreateEdgeReq& req) {
    CHECK_SPACE_ID_AND_RETURN(req.get_space_id());
    {
        // if there is an tag of the same name
        // TODO: there exists race condition, we should address it in the future
        folly::SharedMutex::ReadHolder rHolder(LockUtils::tagLock());
        auto conflictRet = getTagId(req.get_space_id(), req.get_edge_name());
        if (conflictRet.ok()) {
            LOG(ERROR) << "Failed to create edge `" << req.get_edge_name()
                       << "': some tag with the same name already exists.";
            resp_.set_id(to(conflictRet.value(), EntryType::EDGE));
            resp_.set_code(cpp2::ErrorCode::E_CONFLICT);
            onFinished();
            return;
        }
    }

    folly::SharedMutex::WriteHolder wHolder(LockUtils::edgeLock());
    auto ret = getEdgeType(req.get_space_id(), req.get_edge_name());
    if (ret.ok()) {
        resp_.set_id(to(ret.value(), EntryType::EDGE));
        resp_.set_code(cpp2::ErrorCode::E_EXISTED);
        onFinished();
        return;
    }

    std::vector<kvstore::KV> data;
    EdgeType edgeType = autoIncrementId();
    data.emplace_back(MetaServiceUtils::indexEdgeKey(req.get_space_id(), req.get_edge_name()),
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

