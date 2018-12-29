/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#include "storage/AddEdgesProcessor.h"
#include <algorithm>
#include "time/TimeUtils.h"
#include "storage/KeyUtils.h"

namespace nebula {
namespace storage {

void AddEdgesProcessor::process(const cpp2::AddEdgesRequest& req) {
    auto spaceId = req.get_space_id();
    auto now = time::TimeUtils::nowInMSeconds();
    callingNum_ = req.parts.size();
    CHECK_NOTNULL(kvstore_);
    std::for_each(req.parts.begin(), req.parts.end(), [&](auto& partEdges){
        auto partId = partEdges.first;
        std::vector<kvstore::KV> data;
        std::for_each(partEdges.second.begin(), partEdges.second.end(), [&](auto& edge){
            auto key = KeyUtils::edgeKey(partId, edge.key.src, edge.key.edge_type,
                                         edge.key.ranking, edge.key.dst, now);
            data.emplace_back(std::move(key), std::move(edge.get_props()));
        });
        doPut(spaceId, partId, std::move(data));
    });
}

}  // namespace storage
}  // namespace nebula
