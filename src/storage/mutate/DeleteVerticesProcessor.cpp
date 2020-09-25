/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/mutate/DeleteVerticesProcessor.h"
#include "utils/NebulaKeyUtils.h"

DECLARE_bool(enable_vertex_cache);

namespace nebula {
namespace storage {

void DeleteVerticesProcessor::process(const cpp2::DeleteVerticesRequest& req) {
    auto spaceId = req.get_space_id();
    const auto& partVertices = req.get_parts();
    auto iRet = indexMan_->getTagIndexes(spaceId);
    if (iRet.ok()) {
        indexes_ = std::move(iRet).value();
    }

    if (indexes_.empty()) {
        std::for_each(partVertices.begin(), partVertices.end(), [this](auto& pv) {
            this->callingNum_ += pv.second.size();
        });

        std::vector<std::string> keys;
        keys.reserve(32);
        for (auto pv = partVertices.begin(); pv != partVertices.end(); pv++) {
            auto part = pv->first;
            const auto& vertices = pv->second;
            for (auto v = vertices.begin(); v != vertices.end(); v++) {
                auto prefix = NebulaKeyUtils::vertexPrefix(part, *v);
                std::unique_ptr<kvstore::KVIterator> iter;
                auto ret = this->kvstore_->prefix(spaceId, part, prefix, &iter);
                if (ret != kvstore::ResultCode::SUCCEEDED) {
                    VLOG(3) << "Error! ret = " << static_cast<int32_t>(ret)
                            << ", spaceID " << spaceId;
                    this->handleErrorCode(ret, spaceId, part);
                    this->onFinished();
                    return;
                }
                keys.clear();
                while (iter->valid()) {
                    auto key = iter->key();
                    if (NebulaKeyUtils::isVertex(key)) {
                        auto tag = NebulaKeyUtils::getTagId(key);
                        // Evict vertices from cache
                        if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
                            VLOG(3) << "Evict vertex cache for VID " << *v << ", TagID " << tag;
                            vertexCache_->evict(std::make_pair(*v, tag));
                        }
                        keys.emplace_back(key.str());
                    }
                    iter->next();
                }
                doRemove(spaceId, part, keys);
            }
        }
    } else {
        callingNum_ = req.parts.size();
        std::for_each(req.parts.begin(), req.parts.end(), [spaceId, this](auto &pv) {
            auto partId = pv.first;
            auto atomic = [spaceId,
                           partId,
                           v = std::move(pv.second),
                           this]() -> folly::Optional<std::string> {
                return deleteVertices(spaceId, partId, v);
            };

            auto callback = [spaceId, partId, this](kvstore::ResultCode code) {
                VLOG(3) << "partId:" << partId << ", code:" << static_cast<int32_t>(code);
                handleAsync(spaceId, partId, code);
            };
            this->kvstore_->asyncAtomicOp(spaceId, partId, atomic, callback);
        });
    }
}

folly::Optional<std::string>
DeleteVerticesProcessor::deleteVertices(GraphSpaceID spaceId,
                                        PartitionID partId,
                                        const std::vector<VertexID>& vertices) {
    std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();
    for (auto& vertex : vertices) {
        auto prefix = NebulaKeyUtils::vertexPrefix(partId, vertex);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = this->kvstore_->prefix(spaceId, partId, prefix, &iter);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            VLOG(3) << "Error! ret = " << static_cast<int32_t>(ret)
                    << ", spaceId " << spaceId;
            return folly::none;
        }
        TagID latestVVId = -1;
        while (iter->valid()) {
            auto key = iter->key();
            if (!NebulaKeyUtils::isVertex(key)) {
                iter->next();
                continue;
            }
            auto tagId = NebulaKeyUtils::getTagId(key);
            if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
                VLOG(3) << "Evict vertex cache for vertex ID " << vertex << ", tagId " << tagId;
                vertexCache_->evict(std::make_pair(vertex, tagId));
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
                RowReader reader = RowReader::getEmptyRowReader();
                for (auto& index : indexes_) {
                    auto indexId = index->get_index_id();
                    if (index->get_schema_id().get_tag_id() == tagId) {
                        if (reader == nullptr) {
                            reader = RowReader::getTagPropReader(this->schemaMan_,
                                                                 iter->val(),
                                                                 spaceId,
                                                                 tagId);
                            if (reader == nullptr) {
                                LOG(WARNING) << "Bad format row";
                                return folly::none;
                            }
                        }
                        const auto& cols = index->get_fields();
                        auto values = collectIndexValues(reader.get(), cols);
                        if (!values.ok()) {
                            continue;
                        }
                        auto indexKey = NebulaKeyUtils::vertexIndexKey(partId,
                                                                       indexId,
                                                                       vertex,
                                                                       values.value());
                        batchHolder->remove(std::move(indexKey));
                    }
                }
                latestVVId = tagId;
            }
            batchHolder->remove(key.str());
            iter->next();
        }
    }
    return encodeBatchValue(batchHolder->getBatch());
}

}  // namespace storage
}  // namespace nebula
