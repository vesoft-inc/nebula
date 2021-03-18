/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <algorithm>
#include "common/time/WallClock.h"
#include "codec/RowWriterV2.h"
#include "utils/IndexKeyUtils.h"
#include "utils/NebulaKeyUtils.h"
#include "utils/OperationKeyUtils.h"
#include "storage/StorageFlags.h"
#include "storage/mutate/AddVerticesProcessor.h"

DECLARE_bool(enable_vertex_cache);

namespace nebula {
namespace storage {

ProcessorCounters kAddVerticesCounters;


void AddVerticesProcessor::process(const cpp2::AddVerticesRequest& req) {
    spaceId_ = req.get_space_id();
    const auto& partVertices = req.get_parts();

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
    if (!iRet.ok()) {
        LOG(ERROR) << iRet.status();
        for (auto& part : partVertices) {
            pushResultCode(cpp2::ErrorCode::E_SPACE_NOT_FOUND, part.first);
        }
        onFinished();
        return;
    }
    indexes_ = std::move(iRet).value();

    CHECK_NOTNULL(env_->kvstore_);
    if (indexes_.empty()) {
        doProcess(req);
    } else {
        doProcessWithIndex(req);
    }
}

void AddVerticesProcessor::doProcess(const cpp2::AddVerticesRequest& req) {
    const auto& partVertices = req.get_parts();
    const auto& propNamesMap = req.get_prop_names();
    for (auto& part : partVertices) {
        auto partId = part.first;
        const auto& vertices = part.second;

        std::vector<kvstore::KV> data;
        data.reserve(32);
        cpp2::ErrorCode code = cpp2::ErrorCode::SUCCEEDED;
        for (auto& vertex : vertices) {
            auto vid = vertex.get_id().getStr();
            const auto& newTags = vertex.get_tags();

            if (!NebulaKeyUtils::isValidVidLen(spaceVidLen_, vid)) {
                LOG(ERROR) << "Space " << spaceId_ << ", vertex length invalid, "
                           << " space vid len: " << spaceVidLen_ << ",  vid is " << vid;
                code = cpp2::ErrorCode::E_INVALID_VID;
                break;
            }

            for (auto& newTag : newTags) {
                auto tagId = newTag.get_tag_id();
                VLOG(3) << "PartitionID: " << partId << ", VertexID: " << vid
                        << ", TagID: " << tagId;

                auto schema = env_->schemaMan_->getTagSchema(spaceId_, tagId);
                if (!schema) {
                    LOG(ERROR) << "Space " << spaceId_ << ", Tag " << tagId << " invalid";
                    code = cpp2::ErrorCode::E_TAG_NOT_FOUND;
                    break;
                }

                auto key = NebulaKeyUtils::vertexKey(spaceVidLen_, partId, vid, tagId);
                auto props = newTag.get_props();
                auto iter = propNamesMap.find(tagId);
                std::vector<std::string> propNames;
                if (iter != propNamesMap.end()) {
                    propNames = iter->second;
                }

                WriteResult wRet;
                auto retEnc = encodeRowVal(schema.get(), propNames, props, wRet);
                if (!retEnc.ok()) {
                    LOG(ERROR) << retEnc.status();
                    code = writeResultTo(wRet, false);
                    break;
                }
                data.emplace_back(std::move(key), std::move(retEnc.value()));

                if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
                    vertexCache_->evict(std::make_pair(vid, tagId));
                    VLOG(3) << "Evict cache for vId " << vid
                            << ", tagId " << tagId;
                }
            }
        }
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            handleAsync(spaceId_, partId, code);
        } else {
            doPut(spaceId_, partId, std::move(data));
        }
    }
}

void AddVerticesProcessor::doProcessWithIndex(const cpp2::AddVerticesRequest& req) {
    const auto& partVertices = req.get_parts();
    const auto& propNamesMap = req.get_prop_names();
    for (auto& part : partVertices) {
        IndexCountWrapper wrapper(env_);
        std::unique_ptr<kvstore::BatchHolder> batchHolder =
        std::make_unique<kvstore::BatchHolder>();
        auto partId = part.first;
        const auto& vertices = part.second;
        std::vector<VMLI> dummyLock;
        dummyLock.reserve(vertices.size());
        cpp2::ErrorCode code = cpp2::ErrorCode::SUCCEEDED;
        for (auto& vertex : vertices) {
            auto vid = vertex.get_id().getStr();
            const auto& newTags = vertex.get_tags();

            if (!NebulaKeyUtils::isValidVidLen(spaceVidLen_, vid)) {
                LOG(ERROR) << "Space " << spaceId_ << ", vertex length invalid, "
                           << " space vid len: " << spaceVidLen_ << ",  vid is " << vid;
                code = cpp2::ErrorCode::E_INVALID_VID;
                break;
            }

            for (auto& newTag : newTags) {
                auto tagId = newTag.get_tag_id();
                VLOG(3) << "PartitionID: " << partId << ", VertexID: " << vid
                        << ", TagID: " << tagId;

                auto schema = env_->schemaMan_->getTagSchema(spaceId_, tagId);
                if (!schema) {
                    LOG(ERROR) << "Space " << spaceId_ << ", Tag " << tagId << " invalid";
                    code = cpp2::ErrorCode::E_TAG_NOT_FOUND;
                    break;
                }

                auto key = NebulaKeyUtils::vertexKey(spaceVidLen_, partId, vid, tagId);
                auto props = newTag.get_props();
                auto iter = propNamesMap.find(tagId);
                std::vector<std::string> propNames;
                if (iter != propNamesMap.end()) {
                    propNames = iter->second;
                }

                WriteResult wRet;
                auto retEnc = encodeRowVal(schema.get(), propNames, props, wRet);
                if (!retEnc.ok()) {
                    LOG(ERROR) << retEnc.status();
                    code = writeResultTo(wRet, false);
                    break;
                }

                RowReaderWrapper nReader;
                RowReaderWrapper oReader;
                auto obsIdx = findOldValue(partId, vid, tagId);
                if (nebula::ok(obsIdx)) {
                    if (!nebula::value(obsIdx).empty()) {
                        oReader = RowReaderWrapper::getTagPropReader(env_->schemaMan_,
                                                                     spaceId_,
                                                                     tagId,
                                                                     nebula::value(obsIdx));
                    }
                } else {
                    code = to(nebula::error(obsIdx));
                    break;
                }
                if (!retEnc.value().empty()) {
                    nReader = RowReaderWrapper::getTagPropReader(env_->schemaMan_,
                                                                 spaceId_,
                                                                 tagId,
                                                                 retEnc.value());
                }
                for (auto& index : indexes_) {
                    if (tagId == index->get_schema_id().get_tag_id()) {
                        /*
                        * step 1 , Delete old version index if exists.
                        */
                        if (oReader != nullptr) {
                            auto oi = indexKey(partId, vid, oReader.get(), index);
                            if (!oi.empty()) {
                                // Check the index is building for the specified partition or not.
                                auto indexState = env_->getIndexState(spaceId_, partId);
                                if (env_->checkRebuilding(indexState)) {
                                    auto delOpKey = OperationKeyUtils::deleteOperationKey(partId);
                                    batchHolder->put(std::move(delOpKey), std::move(oi));
                                } else if (env_->checkIndexLocked(indexState)) {
                                    LOG(ERROR) << "The index has been locked: "
                                               << index->get_index_name();
                                    code = cpp2::ErrorCode::E_DATA_CONFLICT_ERROR;
                                    break;
                                } else {
                                    batchHolder->remove(std::move(oi));
                                }
                            }
                        }

                        /*
                        * step 2 , Insert new vertex index
                        */
                        if (nReader != nullptr) {
                            auto ni = indexKey(partId, vid, nReader.get(), index);
                            if (!ni.empty()) {
                                // Check the index is building for the specified partition or not.
                                auto indexState = env_->getIndexState(spaceId_, partId);
                                if (env_->checkRebuilding(indexState)) {
                                    auto opKey = OperationKeyUtils::modifyOperationKey(partId, ni);
                                    batchHolder->put(std::move(opKey), "");
                                } else if (env_->checkIndexLocked(indexState)) {
                                    LOG(ERROR) << "The index has been locked: "
                                               << index->get_index_name();
                                    code = cpp2::ErrorCode::E_DATA_CONFLICT_ERROR;
                                    break;
                                } else {
                                    batchHolder->put(std::move(ni), "");
                                }
                            }
                        }
                    }
                }  // for index data
                if (code != cpp2::ErrorCode::SUCCEEDED) {
                    break;
                }
                /*
                * step 3 , Insert new vertex data
                */
                batchHolder->put(std::move(key), std::move(retEnc.value()));
                dummyLock.emplace_back(std::make_tuple(spaceId_, partId, tagId, vid));

                if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
                    vertexCache_->evict(std::make_pair(vid, tagId));
                    VLOG(3) << "Evict cache for vId " << vid
                            << ", tagId " << tagId;
                }
            }
            if (code != cpp2::ErrorCode::SUCCEEDED) {
                break;
            }
        }
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            handleAsync(spaceId_, partId, code);
            continue;
        }
        auto batch = encodeBatchValue(std::move(batchHolder)->getBatch());
        DCHECK(!batch.empty());
        nebula::MemoryLockGuard<VMLI> lg(env_->verticesML_.get(), std::move(dummyLock), true);
        if (!lg) {
            auto conflict = lg.conflictKey();
            LOG(ERROR) << "vertex conflict "
                        << std::get<0>(conflict) << ":"
                        << std::get<1>(conflict) << ":"
                        << std::get<2>(conflict) << ":"
                        << std::get<3>(conflict);
            handleAsync(spaceId_, partId, cpp2::ErrorCode::E_DATA_CONFLICT_ERROR);
            continue;
        }
        env_->kvstore_->asyncAppendBatch(spaceId_, partId, std::move(batch),
            [l = std::move(lg), partId, this](kvstore::ResultCode kvRet) {
                UNUSED(l);
                handleAsync(spaceId_, partId, kvRet);
            });
    }
}

ErrorOr<kvstore::ResultCode, std::string>
AddVerticesProcessor::findOldValue(PartitionID partId, const VertexID& vId, TagID tagId) {
    auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen_, partId, vId, tagId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env_->kvstore_->prefix(spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Error! ret = " << static_cast<int32_t>(ret)
                   << ", spaceId " << spaceId_;
        return ret;
    }
    if (iter && iter->valid()) {
        return iter->val().str();
    }
    return std::string();
}

std::string AddVerticesProcessor::indexKey(PartitionID partId,
                                           const VertexID& vId,
                                           RowReader* reader,
                                           std::shared_ptr<nebula::meta::cpp2::IndexItem> index) {
    auto values = IndexKeyUtils::collectIndexValues(reader, index->get_fields());
    if (!values.ok()) {
        return "";
    }

    return IndexKeyUtils::vertexIndexKey(spaceVidLen_, partId,
                                         index->get_index_id(),
                                         vId, std::move(values).value());
}

}  // namespace storage
}  // namespace nebula
