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
    spaceId_ = req.get_space_id();
    callingNum_ = partVertices.size();
    auto iRet = schemaMan_->getTagIndexes(spaceId_);
    if (iRet.ok()) {
        for (auto& index : iRet.value()) {
            indexes_.emplace_back(index);
        }
    }

    CHECK_NOTNULL(kvstore_);
    if (indexes_.empty()) {
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
            doPut(spaceId_, partId, std::move(data));
        });
    } else {
        std::for_each(partVertices.begin(), partVertices.end(), [&](auto &pv) {
            auto partId = pv.first;
            const auto &vertices = pv.second;
            auto atomic = [&]() -> std::string {
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
     * map<vertex_unique_key, pair<vertex_prop, IndexItem>> ,
     * -- vertex_unique_key is only used as the unique key , for example:
     * insert below vertices in the same request:
     *     kv(part1_vid1_tag1 , v1)
     *     kv(part1_vid1_tag1 , v2)
     *     kv(part1_vid1_tag1 , v3)
     *     kv(part1_vid1_tag1 , v4)
     *
     * Ultimately, kv(part1_vid1_tag1 , v4) . It's just what I need.
     */
    std::map<std::string, std::pair<std::string, nebula::cpp2::IndexItem>> newIndexes;
    std::for_each(vertices.begin(), vertices.end(), [&](auto& v) {
        auto vId = v.get_id();
            const auto& tags = v.get_tags();
            std::for_each(tags.begin(), tags.end(), [&](auto& tag) {
                auto tagId = tag.get_tag_id();
                auto prop = tag.get_props();
                VLOG(3) << "PartitionID: " << partId << ", VertexID: " << vId
                        << ", TagID: " << tagId << ", TagVersion: " << version;
                auto key = NebulaKeyUtils::vertexKey(partId, vId, tagId, version);
                batchHolder->put(std::move(key), std::move(prop));
                if (FLAGS_enable_vertex_cache && this->vertexCache_ != nullptr) {
                    this->vertexCache_->evict(std::make_pair(vId, tagId), partId);
                    VLOG(3) << "Evict cache for vId " << vId << ", tagId " << tagId;
                }
                for (auto& index : indexes_) {
                    if (index.get_tagOrEdge() == tagId) {
                        auto vkVal = NebulaKeyUtils::vertexKey(partId, vId, tagId, version);
                        newIndexes[vkVal] = std::make_pair(tag.get_props(), index);
                    }
                }
            });
        });

    for (auto& index : newIndexes) {
        /*
         * step 1 , Delete old version index if exists.
         */
        auto oi = obsoleteIndex(partId,
                                NebulaKeyUtils::getVertexId(index.first),
                                index.second.second);
        if (!oi.c_str()) {
            batchHolder->remove(std::move(oi));
        }
        /*
         * step 2 , Insert new vertex data
         */
        auto key = index.first;
        auto prop = index.second.first;
        batchHolder->put(std::move(key), std::move(prop));
        /*
         * step 3 , Insert new vertex index
         */
        auto ni = newIndex(partId,
                           NebulaKeyUtils::getVertexId(index.first),
                           index.second);
        batchHolder->put(std::move(ni), "");
    }
    return encodeBatchValue(batchHolder->getBatch());
}

std::string AddVerticesProcessor::obsoleteIndex(PartitionID partId,
                                                VertexID vId,
                                                const nebula::cpp2::IndexItem& index) {
    auto prefix = NebulaKeyUtils::vertexPrefix(partId, vId, index.get_tagOrEdge());
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(this->spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Error! ret = " << static_cast<int32_t>(ret)
                   << ", spaceId " << this->spaceId_;
        return "";
    }
    if (iter && iter->valid()) {
        auto reader = RowReader::getTagPropReader(this->schemaMan_,
                                                  iter->val(),
                                                  spaceId_,
                                                  index.get_tagOrEdge());
        const auto& cols = index.get_cols();
        auto values = collectIndexValues(reader.get(), cols);
        auto indexKey = NebulaKeyUtils::vertexIndexKey(partId,
                                                       index.get_index_id(),
                                                       vId,
                                                       values);
        return indexKey;
    }
    return "";
}

std::string AddVerticesProcessor::newIndex(PartitionID partId,
                                           VertexID vId,
                                           const std::pair<std::string,
                                                           nebula::cpp2::IndexItem>& index) {
    auto reader = RowReader::getTagPropReader(this->schemaMan_,
                                              index.first,
                                              spaceId_,
                                              index.second.get_tagOrEdge());
    auto values = collectIndexValues(reader.get(), index.second.get_cols());
    return NebulaKeyUtils::vertexIndexKey(partId,
                                          index.second.get_index_id(),
                                          vId, values);
}

}  // namespace storage
}  // namespace nebula
