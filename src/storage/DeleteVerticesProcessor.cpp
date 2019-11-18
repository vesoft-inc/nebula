/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/DeleteVerticesProcessor.h"
#include "base/NebulaKeyUtils.h"

namespace nebula {
namespace storage {

void DeleteVerticesProcessor::process(const cpp2::DeleteVerticesRequest& req) {
    auto spaceId = req.get_space_id();
    const auto& partVertices = req.get_parts();
    std::for_each(partVertices.begin(), partVertices.end(), [&](auto& pv) {
        callingNum_ += pv.second.size();
    });

    std::for_each(partVertices.begin(), partVertices.end(), [&](auto& pv) {
        auto part = pv.first;
        const auto& vertices = pv.second;
        std::for_each(vertices.begin(), vertices.end(), [&](auto& v){
            auto prefix = NebulaKeyUtils::vertexPrefix(part, v);
            doRemovePrefix(spaceId, part, std::move(prefix));
        });
    });
}

}  // namespace storage
}  // namespace nebula
