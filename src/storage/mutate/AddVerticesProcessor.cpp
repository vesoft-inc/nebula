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
                          -> std::string {
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
    std::for_each(vertices.begin(), vertices.end(), [&](auto& v) {
        auto vId = v.get_id();
        const auto& tags = v.get_tags();
        std::for_each(tags.begin(), tags.end(), [&](auto& tag) {
            auto tagId = tag.get_tag_id();
            auto prop = tag.get_props();
            VLOG(3) << "PartitionID: " << partId << ", VertexID: " << vId
                    << ", TagID: " << tagId << ", TagVersion: " << version;
            auto key = NebulaKeyUtils::vertexKey(partId, vId, tagId, version);
            if (FLAGS_enable_vertex_cache && this->vertexCache_ != nullptr) {
                this->vertexCache_->evict(std::make_pair(vId, tagId), partId);
                VLOG(3) << "Evict cache for vId " << vId << ", tagId " << tagId;
            }

            std::string val;
            std::unique_ptr<RowReader> nReader;
            for (auto& index : indexes_) {
                if (tagId != index->get_schema_id().get_tag_id()) {
                    continue;
                }

                if (val.empty()) {
                    val = findObsoleteIndex(partId, vId, tagId);
                }
                if (!val.empty()) {
                    auto reader = RowReader::getTagPropReader(schemaMan_,
                                                              val,
                                                              spaceId_,
                                                              tagId);
                    auto oi = indexKey(partId, vId, reader.get(), index);
                    if (!oi.empty()) {
                        batchHolder->remove(std::move(oi));
                    }
                }
                if (nReader == nullptr) {
                    nReader = RowReader::getTagPropReader(schemaMan_,
                                                          prop,
                                                          spaceId_,
                                                          tagId);
                }
                auto ni = indexKey(partId, vId, nReader.get(), index);
                batchHolder->put(std::move(ni), "");
            }

            batchHolder->put(std::move(key), std::move(prop));
        });
    });
    return encodeBatchValue(batchHolder->getBatch());
}

std::string AddVerticesProcessor::findObsoleteIndex(PartitionID partId,
                                                    VertexID vId,
                                                    TagID tagId) {
    auto prefix = NebulaKeyUtils::vertexPrefix(partId, vId, tagId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(this->spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Error! ret = " << static_cast<int32_t>(ret)
                   << ", spaceId " << this->spaceId_;
        return "";
    }
    if (iter && iter->valid()) {
        return iter->val().str();
    }
    return "";
}

std::string AddVerticesProcessor::indexKey(PartitionID partId,
                                           VertexID vId,
                                           RowReader* reader,
                                           std::shared_ptr<nebula::cpp2::IndexItem> index) {
    auto values = collectIndexValues(reader, index->get_fields());
    return NebulaKeyUtils::vertexIndexKey(partId,
                                          index->get_index_id(),
                                          vId, values);
}

}  // namespace storage
}  // namespace nebula
