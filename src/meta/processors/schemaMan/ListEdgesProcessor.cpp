/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/schemaMan/ListEdgesProcessor.h"

namespace nebula {
namespace meta {

void ListEdgesProcessor::process(const cpp2::ListEdgesReq& req) {
    GraphSpaceID spaceId = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(spaceId);

    folly::SharedMutex::ReadHolder rHolder(LockUtils::edgeLock());
    auto prefix = MetaServiceUtils::schemaEdgesPrefix(spaceId);
    auto ret = doPrefix(prefix);
    if (!nebula::ok(ret)) {
        LOG(ERROR) << "List Edges failed, SpaceID: " << spaceId;
        handleErrorCode(nebula::error(ret));
        onFinished();
        return;
    }

    auto iter = nebula::value(ret).get();
    std::vector<nebula::meta::cpp2::EdgeItem> edges;
    while (iter->valid()) {
        auto key = iter->key();
        auto val = iter->val();
        auto edgeType = *reinterpret_cast<const EdgeType *>(key.data() + prefix.size());
        auto version = MetaServiceUtils::parseEdgeVersion(key);
        auto nameLen = *reinterpret_cast<const int32_t *>(val.data());
        auto edgeName = val.subpiece(sizeof(int32_t), nameLen).str();
        auto schema = MetaServiceUtils::parseSchema(val);
        cpp2::EdgeItem edge;
        edge.set_edge_type(edgeType);
        edge.set_edge_name(std::move(edgeName));
        edge.set_version(version);
        edge.set_schema(std::move(schema));
        edges.emplace_back(std::move(edge));
        iter->next();
    }
    resp_.set_edges(std::move(edges));
    handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}

}  // namespace meta
}  // namespace nebula

