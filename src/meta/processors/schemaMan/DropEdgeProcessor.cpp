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
    std::string edgeVal;
    EdgeType edgeType;
    auto ret = kvstore_->get(kDefaultSpaceId, kDefaultPartId, indexKey, &edgeVal);
    if (ret == kvstore::ResultCode::SUCCEEDED) {
        edgeType = *reinterpret_cast<const EdgeType *>(edgeVal.data());
        resp_.set_id(to(edgeType, EntryType::EDGE));
        keys.emplace_back(indexKey);
    } else {
        return Status::Error("No Edge!");
    }

    std::unique_ptr<kvstore::KVIterator> iter;
    auto key = MetaServiceUtils::schemaEdgePrefix(id, edgeType);
    ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, key, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        return Status::Error("Edge get error by id : %d !", edgeType);
    }

    while (iter->valid()) {
        keys.emplace_back(iter->key());
        iter->next();
    }
    return keys;
}

}  // namespace meta
}  // namespace nebula


