/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/mutate/AddVerticesProcessor.h"
#include "utils/NebulaKeyUtils.h"
#include "utils/IndexKeyUtils.h"
#include <algorithm>
#include "common/time/WallClock.h"
#include "codec/RowWriterV2.h"
#include "storage/StorageFlags.h"

DECLARE_bool(enable_vertex_cache);

namespace nebula {
namespace storage {

void AddVerticesProcessor::process(const cpp2::AddVerticesRequest& req) {
    auto version =
        std::numeric_limits<int64_t>::max() - time::WallClock::fastNowInMicroSec();
    // Switch version to big-endian, make sure the key is in ordered.
    version = folly::Endian::big(version);

    spaceId_ = req.get_space_id();
    const auto& partVertices = req.get_parts();
    const auto& propNamesMap = req.get_prop_names();

    CHECK_NOTNULL(env_->schemaMan_);
    auto ret = env_->schemaMan_->getSpaceVidLen(spaceId_);
    if (!ret.ok()) {
        LOG(ERROR) << ret.status();
        for (auto& part : partVertices) {
            pushResultCode(cpp2::ErrorCode::E_INVALID_SPACEVIDLEN, part.first);
        }
        onFinished();
        return;
    }
    spaceVidLen_ = ret.value();
    callingNum_ = partVertices.size();

    CHECK_NOTNULL(env_->indexMan_);
    auto iRet = env_->indexMan_->getTagIndexes(spaceId_);
    if (iRet.ok()) {
        indexes_ = std::move(iRet).value();
    }

    CHECK_NOTNULL(env_->kvstore_);
    for (auto& part : partVertices) {
        auto partId = part.first;
        const auto& vertices = part.second;

        std::vector<kvstore::KV> data;
        data.reserve(32);
        for (auto& vertex : vertices) {
            auto vid = vertex.get_id();
            const auto& newTags = vertex.get_tags();

            if (!NebulaKeyUtils::isValidVidLen(spaceVidLen_, vid)) {
                LOG(ERROR) << "Space " << spaceId_ << ", vertex length invalid, "
                            << " space vid len: " << spaceVidLen_ << ",  vid is " << vid;
                pushResultCode(cpp2::ErrorCode::E_INVALID_VID, partId);
                onFinished();
                return;
            }

            for (auto& newTag : newTags) {
                auto tagId = newTag.get_tag_id();
                VLOG(3) << "PartitionID: " << partId << ", VertexID: " << vid
                        << ", TagID: " << tagId << ", TagVersion: " << version;

                auto key = NebulaKeyUtils::vertexKey(spaceVidLen_, partId, vid,
                                                     tagId, version);
                auto schema = env_->schemaMan_->getTagSchema(spaceId_, tagId);
                if (!schema) {
                    LOG(ERROR) << "Space " << spaceId_ << ", Tag " << tagId << " invalid";
                    pushResultCode(cpp2::ErrorCode::E_TAG_NOT_FOUND, partId);
                    onFinished();
                    return;
                }

                auto props = newTag.get_props();
                auto iter = propNamesMap.find(tagId);
                std::vector<std::string> propNames;
                if (iter != propNamesMap.end()) {
                    propNames = iter->second;
                }

                auto retEnc = encodeRowVal(schema.get(), propNames, props);
                if (!retEnc.ok()) {
                    LOG(ERROR) << retEnc.status();
                    pushResultCode(cpp2::ErrorCode::E_DATA_TYPE_MISMATCH, partId);
                    onFinished();
                    return;
                }
                data.emplace_back(std::move(key), std::move(retEnc.value()));

                if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
                    vertexCache_->evict(std::make_pair(vid, tagId), partId);
                    VLOG(3) << "Evict cache for vId " << vid
                            << ", tagId " << tagId;
                }
            }
        }
        if (indexes_.empty()) {
            doPut(spaceId_, partId, std::move(data));
        } else {
            auto atomic = [partId, vertices = std::move(data), this]()
                          -> folly::Optional<std::string> {
                return addVertices(partId, vertices);
            };
            auto callback = [partId, this](kvstore::ResultCode code) {
                handleAsync(spaceId_, partId, code);
            };
            this->env_->kvstore_->asyncAtomicOp(spaceId_, partId, atomic, callback);
        }
    }
}

folly::Optional<std::string>
AddVerticesProcessor::addVertices(PartitionID partId,
                                  const std::vector<kvstore::KV>& vertices) {
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
    std::for_each(vertices.begin(), vertices.end(),
                 [&newVertices](const std::map<std::string, std::string>::value_type& v)
                 { newVertices[v.first] = v.second; });

    for (auto& v : newVertices) {
        std::string val;
        std::unique_ptr<RowReader> nReader;
        auto tagId = NebulaKeyUtils::getTagId(spaceVidLen_, v.first);
        auto vId = NebulaKeyUtils::getVertexId(spaceVidLen_, v.first);
        for (auto& index : indexes_) {
            if (tagId == index->get_schema_id().get_tag_id()) {
                /*
                 * step 1 , Delete old version index if exists.
                 */
                if (val.empty()) {
                    auto obsIdx = findObsoleteIndex(partId, vId.str(), tagId);
                    if (obsIdx == folly::none) {
                        return folly::none;
                    }
                    val = std::move(obsIdx).value();
                }
                if (!val.empty()) {
                    auto reader = RowReader::getTagPropReader(this->env_->schemaMan_,
                                                              spaceId_,
                                                              tagId,
                                                              val);
                    if (reader == nullptr) {
                        LOG(ERROR) << "Bad format row";
                        return folly::none;
                    }
                    auto oi = indexKey(partId, vId.str(), reader.get(), index);
                    if (!oi.empty()) {
                        batchHolder->remove(std::move(oi));
                    }
                }
                /*
                 * step 2 , Insert new vertex index
                 */
                if (nReader == nullptr) {
                    nReader = RowReader::getTagPropReader(this->env_->schemaMan_,
                                                          spaceId_,
                                                          tagId,
                                                          v.second);
                    if (nReader == nullptr) {
                        LOG(ERROR) << "Bad format row";
                        return folly::none;
                    }
                }
                auto ni = indexKey(partId, vId.str(), nReader.get(), index);
                if (!ni.empty()) {
                    batchHolder->put(std::move(ni), "");
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

folly::Optional<std::string>
AddVerticesProcessor::findObsoleteIndex(PartitionID partId, VertexID vId, TagID tagId) {
    auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen_, partId, vId, tagId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = this->env_->kvstore_->prefix(this->spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Error! ret = " << static_cast<int32_t>(ret)
                   << ", spaceId " << this->spaceId_;
        return folly::none;
    }
    if (iter && iter->valid()) {
        return iter->val().str();
    }
    return std::string();
}

std::string AddVerticesProcessor::indexKey(PartitionID partId,
                                           VertexID vId,
                                           RowReader* reader,
                                           std::shared_ptr<nebula::meta::cpp2::IndexItem> index) {
    std::vector<Value::Type> colsType;
    auto values = IndexKeyUtils::collectIndexValues(reader, index->get_fields(), colsType);
    if (!values.ok()) {
        return "";
    }
    return IndexKeyUtils::vertexIndexKey(spaceVidLen_, partId,
                                         index->get_index_id(),
                                         vId, values.value(),
                                         colsType);
}

}  // namespace storage
}  // namespace nebula
