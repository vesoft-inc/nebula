/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "storage/mutate/DeleteEdgesProcessor.h"
#include <algorithm>
#include <limits>
#include "base/NebulaKeyUtils.h"

namespace nebula {
namespace storage {

void DeleteEdgesProcessor::process(const cpp2::DeleteEdgesRequest& req) {
    auto spaceId = req.get_space_id();
    std::for_each(req.parts.begin(), req.parts.end(), [&](auto &partEdges) {
        callingNum_ += partEdges.second.size();
    });
    CHECK_NOTNULL(kvstore_);

    std::for_each(req.parts.begin(), req.parts.end(), [&](auto &partEdges) {
        auto partId = partEdges.first;
        std::for_each(partEdges.second.begin(), partEdges.second.end(), [&](auto &edgeKey) {
            auto start = NebulaKeyUtils::edgeKey(partId,
                                                 edgeKey.src,
                                                 edgeKey.edge_type,
                                                 edgeKey.ranking,
                                                 edgeKey.dst,
                                                 0);
            auto end = NebulaKeyUtils::edgeKey(partId,
                                               edgeKey.src,
                                               edgeKey.edge_type,
                                               edgeKey.ranking,
                                               edgeKey.dst,
                                               std::numeric_limits<int64_t>::max());
            doRemoveRange(spaceId, partId, start, end);
        });
    });
}

}  // namespace storage
}  // namespace nebula

