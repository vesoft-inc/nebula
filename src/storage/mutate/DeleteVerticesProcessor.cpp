/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/mutate/DeleteVerticesProcessor.h"
#include "base/NebulaKeyUtils.h"

DECLARE_bool(enable_vertex_cache);

namespace nebula {
namespace storage {

void DeleteVerticesProcessor::process(const cpp2::DeleteVerticesRequest& req) {
    auto space = req.get_space_id();
    const auto& partVertices = req.get_parts();
    std::for_each(partVertices.begin(), partVertices.end(), [&](auto& pv) {
        callingNum_ += pv.second.size();
    });

    std::for_each(partVertices.begin(), partVertices.end(), [&](auto& pv) {
        auto part = pv.first;
        const auto& vertices = pv.second;
        std::for_each(vertices.begin(), vertices.end(), [&](auto& v){
            auto prefix = NebulaKeyUtils::vertexPrefix(part, v);

            // Evict vertices from cache
            if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
                std::unique_ptr<kvstore::KVIterator> iter;
                auto ret = this->kvstore_->prefix(space, part, prefix, &iter);
                if (ret != kvstore::ResultCode::SUCCEEDED) {
                    VLOG(3) << "Error! ret = " << static_cast<int32_t>(ret) << ", space " << space;
                    this->onFinished();
                    return;
                }

                while (iter->valid()) {
                    auto key = iter->key();
                    if (NebulaKeyUtils::isVertex(key)) {
                        auto tag = NebulaKeyUtils::getTagId(key);
                        VLOG(3) << "Evict vertex cache for VID " << v << ", TagID " << tag;
                        vertexCache_->evict(std::make_pair(v, tag), part);
                    }
                    iter->next();
                }
            }
            doRemovePrefix(space, part, std::move(prefix));
        });
    });
}

}  // namespace storage
}  // namespace nebula
