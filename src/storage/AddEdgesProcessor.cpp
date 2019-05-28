/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "storage/AddEdgesProcessor.h"
#include "base/NebulaKeyUtils.h"
#include <algorithm>
#include <limits>
#include "time/TimeUtils.h"

namespace nebula {
namespace storage {

void AddEdgesProcessor::process(const cpp2::AddEdgesRequest& req) {
    auto spaceId = req.get_space_id();
    auto version = std::numeric_limits<int64_t>::max() - time::TimeUtils::nowInUSeconds();
    callingNum_ = req.parts.size();
    CHECK_NOTNULL(kvstore_);
    std::for_each(req.parts.begin(), req.parts.end(), [&](auto& partEdges){
        auto partId = partEdges.first;
        std::vector<kvstore::KV> data;
        std::for_each(partEdges.second.begin(), partEdges.second.end(), [&](auto& edge){
            auto key = NebulaKeyUtils::edgeKey(partId, edge.key.src, edge.key.edge_type,
                                               edge.key.ranking, edge.key.dst, version);
            auto values = assembleValues(edge.get_props_value());
            data.emplace_back(std::move(key), std::move(values));
        });
        doPut(spaceId, partId, std::move(data));
    });
}

}  // namespace storage
}  // namespace nebula
