/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/NebulaKeyUtils.h"
#include "storage/query/QueryEdgeKeysProcessor.h"

namespace nebula {
namespace storage {

void QueryEdgeKeysProcessor::process(const cpp2::EdgeKeysRequest& req) {
    CHECK_NOTNULL(kvstore_);
    auto spaceId = req.get_space_id();
    const auto& partVertices = req.get_parts();

    std::unordered_map<VertexID, std::vector<cpp2::EdgeKey>> edge_keys;
    std::for_each(partVertices.begin(), partVertices.end(), [&](auto& pv) {
        auto part = pv.first;
        const auto& vertices = pv.second;
        std::for_each(vertices.begin(), vertices.end(), [&](auto& v) {
            auto prefix = NebulaKeyUtils::edgePrefix(part, v);
            std::unique_ptr<kvstore::KVIterator> iter;
            auto ret = this->kvstore_->prefix(spaceId, part, prefix, &iter);
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                VLOG(3) << "Error! ret = " << static_cast<int32_t>(ret) << ", spaceId = " << spaceId
                        << ", partId =  " << part << ", vertexId = " << v;
                this->pushResultCode(this->to(ret), part);
                this->onFinished();
                return;
            }

            std::vector<cpp2::EdgeKey> edges;
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
            edge_keys.emplace(v, std::move(edges));
        });
    });

    resp_.set_edge_keys(std::move(edge_keys));
    this->onFinished();
}

}  // namespace storage
}  // namespace nebula
