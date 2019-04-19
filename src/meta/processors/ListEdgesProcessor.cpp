/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/ListEdgesProcessor.h"

namespace nebula {
namespace meta {

void ListEdgesProcessor::process(const cpp2::ListEdgesReq& req) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::edgeLock());
    auto spaceId = req.get_space_id();
    auto prefix = MetaServiceUtils::schemaEdgesPrefix(spaceId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId_, kDefaultPartId_, prefix, &iter);
    resp_.set_code(to(ret));
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        onFinished();
        return;
    }
    decltype(resp_.edges) edges;
    while (iter->valid()) {
        cpp2::EdgeItem tag;
        auto key = iter->key();
        auto val = iter->val();
        auto edgeType = *reinterpret_cast<const EdgeType *>(key.data() + prefix.size());
        auto vers = *reinterpret_cast<const int64_t *>(key.data() +
                                                       prefix.size() + sizeof(EdgeType));
        auto nameLen = *reinterpret_cast<const int32_t *>(val.data());
        auto edgeName = val.subpiece(sizeof(int32_t), nameLen).str();
        auto schema = MetaServiceUtils::parseSchema(val);
        cpp2::EdgeItem edgeItem(apache::thrift::FragileConstructor::FRAGILE,
                                edgeType, edgeName, vers, schema);
        edges.emplace_back(std::move(edgeItem));
        iter->next();
    }
    resp_.set_edges(std::move(edges));
    onFinished();
}


}  // namespace meta
}  // namespace nebula

