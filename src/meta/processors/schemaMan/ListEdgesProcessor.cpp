/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/schemaMan/ListEdgesProcessor.h"

namespace nebula {
namespace meta {

void ListEdgesProcessor::process(const cpp2::ListEdgesReq& req) {
    CHECK_SPACE_ID_AND_RETURN(req.get_space_id());
    folly::SharedMutex::ReadHolder rHolder(LockUtils::edgeLock());
    auto spaceId = req.get_space_id();
    auto prefix = MetaServiceUtils::schemaEdgesPrefix(spaceId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    resp_.set_code(to(ret));
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        onFinished();
        return;
    }
    decltype(resp_.edges) edges;
    while (iter->valid()) {
        auto key = iter->key();
        auto val = iter->val();
        auto edgeType = *reinterpret_cast<const EdgeType *>(key.data() + prefix.size());
        auto vers = MetaServiceUtils::parseEdgeVersion(key);
        auto nameLen = *reinterpret_cast<const int32_t *>(val.data());
        auto edgeName = val.subpiece(sizeof(int32_t), nameLen).str();
        auto schema = MetaServiceUtils::parseSchema(val);
        edges.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                           edgeType, edgeName, vers, schema);
        iter->next();
    }
    resp_.set_edges(std::move(edges));
    onFinished();
}
}  // namespace meta
}  // namespace nebula

