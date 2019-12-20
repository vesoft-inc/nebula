/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/mutate/AddVerticesProcessor.h"
#include "base/NebulaKeyUtils.h"
#include <algorithm>
#include <limits>
#include "time/WallClock.h"

DECLARE_bool(enable_vertex_cache);

namespace nebula {
namespace storage {

void AddVerticesProcessor::process(const cpp2::AddVerticesRequest& req) {
    auto version =
        std::numeric_limits<int64_t>::max() - time::WallClock::fastNowInMicroSec();
    // Switch version to big-endian, make sure the key is in ordered.
    version = folly::Endian::big(version);

    const auto& partVertices = req.get_parts();
    auto spaceId = req.get_space_id();
    callingNum_ = partVertices.size();
    CHECK_NOTNULL(kvstore_);
    std::for_each(partVertices.begin(), partVertices.end(), [&](auto& pv) {
        auto partId = pv.first;
        const auto& vertices = pv.second;
        std::vector<kvstore::KV> data;
        std::for_each(vertices.begin(), vertices.end(), [&](auto& v){
            const auto& tags = v.get_tags();
            std::for_each(tags.begin(), tags.end(), [&](auto& tag) {
                VLOG(3) << "PartitionID: " << partId << ", VertexID: " << v.get_id()
                        << ", TagID: " << tag.get_tag_id() << ", TagVersion: " << version;
                auto key = NebulaKeyUtils::vertexKey(partId, v.get_id(),
                                                     tag.get_tag_id(), version);
                data.emplace_back(std::move(key), std::move(tag.get_props()));
                if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
                    vertexCache_->evict(std::make_pair(v.get_id(), tag.get_tag_id()), partId);
                    VLOG(3) << "Evict cache for vId " << v.get_id()
                            << ", tagId " << tag.get_tag_id();
                }
            });
        });
        doPut(spaceId, partId, std::move(data));
    });
}

}  // namespace storage
}  // namespace nebula
