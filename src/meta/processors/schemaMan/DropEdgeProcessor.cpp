/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/schemaMan/DropEdgeProcessor.h"

namespace nebula {
namespace meta {

void DropEdgeProcessor::process(const cpp2::DropEdgeReq& req) {
    CHECK_SPACE_ID_AND_RETURN(req.get_space_id());
    folly::SharedMutex::WriteHolder wHolder(LockUtils::edgeLock());
    auto ret = getEdgeKeys(req.get_space_id(), req.get_edge_name());
    if (!ret.ok()) {
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    LOG(INFO) << "Drop Edge " << req.get_edge_name();
    doMultiRemove(std::move(ret.value()));
}

StatusOr<std::vector<std::string>> DropEdgeProcessor::getEdgeKeys(GraphSpaceID id,
                                                                  const std::string& edgeName) {
    auto indexKey = MetaServiceUtils::indexEdgeKey(id, edgeName);
    std::vector<std::string> keys;
    EdgeType edgeType;
    auto ret = doGet(indexKey);
    if (ret.ok()) {
        edgeType = *reinterpret_cast<const EdgeType *>(ret.value().data());
        resp_.set_id(to(edgeType, EntryType::EDGE));
        keys.emplace_back(indexKey);
    } else {
        return Status::Error("No Edge!");
    }

    auto key = MetaServiceUtils::schemaEdgePrefix(id, edgeType);
    auto iterRet = doPrefix(key);
    if (!iterRet.ok()) {
        return Status::Error("Edge get error by id : %d !", edgeType);
    }

    auto iter = iterRet.value().get();
    while (iter->valid()) {
        keys.emplace_back(iter->key());
        iter->next();
    }
    return keys;
}

}  // namespace meta
}  // namespace nebula


