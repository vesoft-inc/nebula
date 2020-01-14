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

std::string DeleteVerticesProcessor::deleteVertex(GraphSpaceID spaceId,
                                                  PartitionID partId,
                                                  VertexID vId) {
    std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();
    auto prefix = NebulaKeyUtils::vertexPrefix(partId, vId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = this->kvstore_->prefix(spaceId, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        VLOG(3) << "Error! ret = " << static_cast<int32_t>(ret)
                << ", spaceId " << spaceId;
        return "";
    }
    TagID latestVVId = -1;
    while (iter->valid()) {
        auto key = iter->key();
        auto tagId = NebulaKeyUtils::getTagId(key);
        if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
            if (NebulaKeyUtils::isVertex(key)) {
                VLOG(3) << "Evict vertex cache for vId " << vId << ", tagId " << tagId;
                vertexCache_->evict(std::make_pair(vId, tagId), partId);
            }
        }
        /**
         * example ,the prefix result as below :
         *     V1_tag1_version3
         *     V1_tag1_version2
         *     V1_tag1_version1
         *     V1_tag2_version3
         *     V1_tag2_version2
         *     V1_tag2_version1
         *     V1_tag3_version3
         *     V1_tag3_version2
         *     V1_tag3_version1
         * Because index depends on latest version of tag.
         * So only V1_tag1_version3, V1_tag2_version3 and V1_tag3_version3 are needed,
         * Using newlyVertexId to identify if it is the latest version
         */
        if (latestVVId != tagId) {
            std::unique_ptr<RowReader> reader;
            for (auto& index : indexes_) {
                auto indexId = index.get_index_id();
                if (index.get_tagOrEdge() == tagId) {
                    if (reader == nullptr) {
                        reader = RowReader::getTagPropReader(this->schemaMan_,
                                                             iter->val(),
                                                             spaceId,
                                                             tagId);
                    }
                    const auto& cols = index.get_cols();
                    auto values = collectIndexValues(reader.get(), cols);
                    auto indexKey = NebulaKeyUtils::vertexIndexKey(partId,
                                                                   indexId,
                                                                   vId,
                                                                   values);
                    batchHolder->remove(std::move(indexKey));
                }
            }
            latestVVId = tagId;
        }
        batchHolder->remove(key.str());
        iter->next();
    }
    return encodeBatchValue(batchHolder->getBatch());
}

}  // namespace storage
}  // namespace nebula
