/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/ListEdgesProcessor.h"

namespace nebula {
namespace meta {

void ListEdgesProcessor::process(const cpp2::ListEdgesReq& req) {
    CHECK_SPACE_ID_AND_RETURN(req.get_space_id());
    folly::SharedMutex::ReadHolder rHolder(LockUtils::edgeLock());
    auto spaceId = req.get_space_id();
    auto prefix = MetaServiceUtils::schemaEdgesPrefix(spaceId);
    auto ret = doPrefix(prefix);
    if (!ret.ok()) {
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

     auto iter = ret.value().get();
    decltype(resp_.edges) edges;
    while (iter->valid()) {
/**
	auto key = iter->key();
        auto val = iter->val();
	auto edgeType = *reinterpret_cast<const EdgeType *>(key.data() + prefix.size());
	auto version = *reinterpret_cast<const int64_t *>(key.data() + prefix.size() +
			                                  sizeof(EdgeType));
	auto name = getEdgeName(spaceId, edgeType);
	auto schema = MetaServiceUtils::parseSchema(val);
	edges.emplace_back()
**/
    }
    resp_.set_edges(std::move(edges));
    onFinished();
}

}  // namespace meta
}  // namespace nebula
