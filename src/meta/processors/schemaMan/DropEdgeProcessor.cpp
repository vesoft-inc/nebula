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
    GraphSpaceID spaceId = req.get_space_id();
    folly::SharedMutex::WriteHolder wHolder(LockUtils::edgeLock());
    EdgeType edgeType;
    auto indexKey = MetaServiceUtils::indexEdgeKey(spaceId, req.get_edge_name());
    auto iRet = doGet(indexKey);
    if (iRet.ok()) {
        edgeType = *reinterpret_cast<const EdgeType *>(iRet.value().data());
        resp_.set_id(to(edgeType, EntryType::EDGE));
    } else {
        handleErrorCode(req.get_if_exists() == true ? cpp2::ErrorCode::SUCCEEDED
                                                   : cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    auto indexes = getIndexes(spaceId, edgeType);
    if (!indexes.ok()) {
        handleErrorCode(MetaCommon::to(indexes.status()));
        onFinished();
        return;
    }
    if (!indexes.value().empty()) {
        LOG(ERROR) << "Drop edge error, index conflict";
        handleErrorCode(cpp2::ErrorCode::E_CONFLICT);
        onFinished();
        return;
    }

    auto ret = getEdgeKeys(req.get_space_id(), edgeType);
    if (!ret.ok()) {
        handleErrorCode(MetaCommon::to(ret.status()));
        onFinished();
        return;
    }
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    auto keys = std::move(ret).value();
    keys.emplace_back(std::move(indexKey));
    LOG(INFO) << "Drop Edge " << req.get_edge_name();
    doSyncMultiRemoveAndUpdate(std::move(keys));
}

StatusOr<std::vector<std::string>> DropEdgeProcessor::getEdgeKeys(GraphSpaceID id,
                                                                  EdgeType edgeType) {
    std::vector<std::string> keys;
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


