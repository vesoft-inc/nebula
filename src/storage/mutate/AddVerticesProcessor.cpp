/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/mutate/AddVerticesProcessor.h"
#include "utils/NebulaKeyUtils.h"
#include <algorithm>
#include <limits>
#include "time/WallClock.h"

DECLARE_bool(enable_vertex_cache);

namespace nebula {
namespace storage {

void AddVerticesProcessor::process(const cpp2::AddVerticesRequest& req) {
    auto version = FLAGS_enable_multi_versions ?
        std::numeric_limits<int64_t>::max() - time::WallClock::fastNowInMicroSec() : 0L;
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
    std::unordered_set<std::pair<VertexID, TagID>> uniqueIDs;
    uniqueIDs.reserve(128);

    std::for_each(partVertices.begin(), partVertices.end(), [&](auto& pv) {
        std::vector<kvstore::KV> data;
        data.reserve(128);
        std::vector<std::tuple<VertexID, TagID, std::string>> cacheData;
        if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
            cacheData.reserve(128);
        }
        auto partId = pv.first;
        const auto& vertices = pv.second;

        uniqueIDs.clear();
        std::for_each(vertices.rbegin(), vertices.rend(), [&](auto& v) {
            const auto& tags = v.get_tags();
            std::for_each(tags.begin(), tags.end(), [&](auto& tag) {
                auto uniqueKey = std::make_pair(v.get_id(), tag.get_tag_id());
                if (uniqueIDs.find(uniqueKey) != uniqueIDs.end()) {
                    return;
                }

                VLOG(3) << "PartitionID: " << partId << ", VertexID: " << v.get_id()
                        << ", TagID: " << tag.get_tag_id() << ", TagVersion: " << version;
                auto key = NebulaKeyUtils::vertexKey(partId, v.get_id(),
                                                     tag.get_tag_id(), version);
                if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
                    cacheData.emplace_back(v.get_id(), tag.get_tag_id(), tag.get_props());
                }
                data.emplace_back(std::move(key), std::move(tag.get_props()));
                uniqueIDs.emplace(uniqueKey);
            });
        });

        auto callback = [partId,
                         this,
                         cacheData = std::move(cacheData)] (kvstore::ResultCode code) mutable {
            if (FLAGS_enable_vertex_cache
                && vertexCache_ != nullptr
                && code == kvstore::ResultCode::SUCCEEDED) {
                for (auto&& tup : cacheData) {
                    vertexCache_->insert(std::make_pair(std::get<0>(tup),
                                                        std::get<1>(tup)),
                                         std::move(std::get<2>(tup)));
                }
            }
            handleAsync(spaceId_, partId, code);
        };
        if (indexes_.empty()) {
            this->kvstore_->asyncMultiPut(spaceId_,
                                          partId,
                                          std::move(data),
                                          std::move(callback));
        } else {
            auto atomicOp = [partId, data = std::move(data), this]() mutable
                                            -> folly::Optional<std::string> {
                return addVerticesWithIndex(partId, std::move(data));
            };

            this->kvstore_->asyncAtomicOp(spaceId_,
                                          partId,
                                          std::move(atomicOp),
                                          std::move(callback));
        }
    });
}

std::string AddVerticesProcessor::addVerticesWithIndex(PartitionID partId,
                                                       std::vector<kvstore::KV>&& data) {
    std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();
    for (auto& v : data) {
        std::string val;
        RowReader nReader = RowReader::getEmptyRowReader();
        auto tagId = NebulaKeyUtils::getTagId(v.first);
        auto vId = NebulaKeyUtils::getVertexId(v.first);
        for (auto& index : indexes_) {
            if (tagId == index->get_schema_id().get_tag_id()) {
                /*
                 * step 1 , Delete old version index if exists.
                 */
                if (val.empty()) {
                    val = findObsoleteIndex(partId, vId, tagId);
                }
                if (!val.empty()) {
                    auto reader = RowReader::getTagPropReader(this->schemaMan_,
                                                              val,
                                                              spaceId_,
                                                              tagId);
                    if (reader == nullptr) {
                        LOG(WARNING) << "Bad format row";
                        return "";
                    }
                    auto oi = indexKey(partId, vId, reader.get(), index);
                    if (!oi.empty()) {
                        batchHolder->remove(std::move(oi));
                    }
                }
                /*
                 * step 2 , Insert new vertex index
                 */
                if (nReader == nullptr) {
                    nReader = RowReader::getTagPropReader(this->schemaMan_,
                                                          v.second,
                                                          spaceId_,
                                                          tagId);
                    if (nReader == nullptr) {
                        LOG(WARNING) << "Bad format row";
                        return "";
                    }
                }
                auto ni = indexKey(partId, vId, nReader.get(), index);
                if (!ni.empty()) {
                    batchHolder->put(std::move(ni), "");
                }
            }
        }
        /*
         * step 3 , Insert new vertex data
         */
        batchHolder->put(std::move(v.first), std::move(v.second));
    }
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
    if (!values.ok()) {
        return "";
    }
    return NebulaKeyUtils::vertexIndexKey(partId,
                                          index->get_index_id(),
                                          vId, values.value());
}

}  // namespace storage
}  // namespace nebula
