/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/NebulaKeyUtils.h"
#include "storage/query/QueryEdgeKeysProcessor.h"


namespace nebula {
namespace storage {

void QueryEdgeKeysProcessor::process(const cpp2::EdgeKeyRequest& req) {
    auto spaceId = req.get_space_id();
    auto vId = req.get_vid();
    auto partId = req.get_part_id();
    CHECK_NOTNULL(kvstore_);

    std::vector<cpp2::EdgeKey> edges;
    auto prefix = NebulaKeyUtils::edgePrefix(partId, vId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = this->kvstore_->prefix(spaceId, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        VLOG(3) << "Error! ret = " << static_cast<int32_t>(ret) << ", spaceId = " << spaceId
                << ", partId =  " << partId << ", vertexId = " << vId;
        if (ret == kvstore::ResultCode::ERR_LEADER_CHANGED) {
            this->handleLeaderChanged(spaceId, partId);
        } else {
            this->pushResultCode(this->to(ret), partId);
        }
        this->onFinished();
        return;
    }
    while (iter->valid()) {
        auto key = iter->key();
        if (NebulaKeyUtils::isEdge(key)) {
            auto src = NebulaKeyUtils::getSrcId(key);
            auto dst = NebulaKeyUtils::getDstId(key);
            auto edgeType = NebulaKeyUtils::getEdgeType(key);
            auto rank = NebulaKeyUtils::getRank(key);
            cpp2::EdgeKey edge;
            edge.set_src(src);
            edge.set_edge_type(edgeType);
            edge.set_ranking(rank);
            edge.set_dst(dst);
            edges.emplace_back(std::move(edge));
        }
        iter->next();
    }
    resp_.set_edge_keys(edges);
    this->onFinished();
}

}  // namespace storage
}  // namespace nebula
