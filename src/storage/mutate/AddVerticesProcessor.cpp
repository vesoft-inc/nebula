/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/mutate/AddVerticesProcessor.h"
#include "utils/NebulaKeyUtils.h"
#include <algorithm>
#include <limits>

DECLARE_bool(enable_vertex_cache);

namespace nebula {
namespace storage {

void AddVerticesProcessor::process(const cpp2::AddVerticesRequest& req) {
    auto version =
        std::numeric_limits<int64_t>::max() - time::WallClock::fastNowInMicroSec();
    // Switch version to big-endian, make sure the key is in ordered.
    version = folly::Endian::big(version);

    const auto& partVertices = req.get_parts();
    spaceId_ = req.get_space_id();
    callingNum_ = partVertices.size();
    auto iRet = indexMan_->getTagIndexes(spaceId_);
    if (iRet.ok()) {
        indexes_ = std::move(iRet).value();
    }

    CHECK_NOTNULL(kvstore_);
    if (indexes_.empty()) {
        std::for_each(partVertices.begin(), partVertices.end(), [&](auto& pv) {
            auto partId = pv.first;
            const auto& vertices = pv.second;
            std::vector<kvstore::KV> data;
            std::for_each(vertices.begin(), vertices.end(), [&](auto& v) {
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
            doPut(spaceId_, partId, std::move(data));
        });
    } else {
        std::for_each(partVertices.begin(), partVertices.end(), [&](auto &pv) {
            auto partId = pv.first;
            auto atomic = [version, partId, vertices = std::move(pv.second), this]()
                          -> folly::Optional<std::string> {
                return addVertices(version, partId, vertices);
            };
            auto callback = [partId, this](kvstore::ResultCode code) {
                handleAsync(spaceId_, partId, code);
            };
            this->kvstore_->asyncAtomicOp(spaceId_, partId, atomic, callback);
        });
    }
}

std::string AddVerticesProcessor::addVertices(int64_t version, PartitionID partId,
                                              const std::vector<cpp2::Vertex>& vertices) {
    std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();
    /*
     * Define the map newIndexes to avoid inserting duplicate vertex.
     * This map means :
     * map<vertex_unique_key, prop_value> ,
     * -- vertex_unique_key is only used as the unique key , for example:
     * insert below vertices in the same request:
     *     kv(part1_vid1_tag1 , v1)
     *     kv(part1_vid1_tag1 , v2)
     *     kv(part1_vid1_tag1 , v3)
     *     kv(part1_vid1_tag1 , v4)
     *
     * Ultimately, kv(part1_vid1_tag1 , v4) . It's just what I need.
     */
    std::map<std::string, std::string> newVertices;
    std::for_each(vertices.begin(), vertices.end(), [&](auto& v) {
        auto vId = v.get_id();
        const auto& tags = v.get_tags();
        std::for_each(tags.begin(), tags.end(), [&](auto& tag) {
            auto tagId = tag.get_tag_id();
            auto prop = tag.get_props();
            VLOG(3) << "PartitionID: " << partId << ", VertexID: " << vId
                    << ", TagID: " << tagId << ", TagVersion: " << version;
            auto key = NebulaKeyUtils::vertexKey(partId, vId, tagId, version);
            newVertices[key] = std::move(prop);
            if (FLAGS_enable_vertex_cache && this->vertexCache_ != nullptr) {
                this->vertexCache_->evict(std::make_pair(vId, tagId), partId);
                VLOG(3) << "Evict cache for vId " << vId << ", tagId " << tagId;
            }
        });
    });

    for (auto& v : newVertices) {
        StatusOr<std::string> valResult;
        std::unique_ptr<RowReader> nReader;
        auto tagId = NebulaKeyUtils::getTagId(v.first);
        auto vId = NebulaKeyUtils::getVertexId(v.first);
        for (auto& index : indexes_) {
            if (tagId == index->get_schema_id().get_tag_id()) {
                /*
                 * step 1 , Delete old version index if exists.
                 */
                if (!valResult.ok()) {
                    auto prefix = NebulaKeyUtils::vertexPrefix(partId, vId, tagId);
                    valResult = findObsoleteValue(spaceId_, partId, prefix);
                }

                if (valResult.ok()) {
                    auto reader = RowReader::getTagPropReader(schemaMan_,
                                                              valResult.value(),
                                                              spaceId_,
                                                              tagId);
                    if (reader == nullptr) {
                        LOG(WARNING) << "Bad format row";
                        return "";
                    }

                    auto originalIndexKey = vertexIndexKey(partId, vId, reader.get(), index);
                    if (!originalIndexKey.empty()) {
                        if (env_->getPartID() != partId &&
                            env_->getIndexID() != index->get_index_id()) {
                            batchHolder->remove(std::move(originalIndexKey));
                        } else {
                            auto modifyOpKey = modifyOperationKey(partId, originalIndexKey);
                            batchHolder->remove(std::move(modifyOpKey));
                        }
                    }
                }
                /*
                 * step 2 , Insert new vertex index
                 */
                if (nReader == nullptr) {
                    nReader = RowReader::getTagPropReader(schemaMan_,
                                                          v.second,
                                                          spaceId_,
                                                          tagId);
                    if (nReader == nullptr) {
                        LOG(WARNING) << "Bad format row";
                        return "";
                    }
                }

                auto currentIndexKey = vertexIndexKey(partId, vId, nReader.get(), index);
                if (env_->getPartID() != partId &&
                    env_->getIndexID() != index->get_index_id()) {
                    batchHolder->put(std::move(currentIndexKey), "");
                } else {
                    auto modifyOpKey = modifyOperationKey(partId, currentIndexKey);
                    batchHolder->put(std::move(modifyOpKey), "");
                }
            }
        }
        /*
         * step 3 , Insert new vertex data
         */
        auto key = v.first;
        auto prop = v.second;
        batchHolder->put(std::move(key), std::move(prop));
    }
    return encodeBatchValue(batchHolder->getBatch());
}

}  // namespace storage
}  // namespace nebula
