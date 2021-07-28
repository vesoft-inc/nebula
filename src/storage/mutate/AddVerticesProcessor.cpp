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
    ifNotExists_ = req.get_if_not_exists();
    CHECK_NOTNULL(env_->schemaMan_);
    auto ret = env_->schemaMan_->getSpaceVidLen(spaceId_);
    if (!ret.ok()) {
        LOG(ERROR) << ret.status();
        for (auto& part : partVertices) {
            pushResultCode(nebula::cpp2::ErrorCode::E_INVALID_SPACEVIDLEN, part.first);
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
            pushResultCode(nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND, part.first);
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
        auto code = nebula::cpp2::ErrorCode::SUCCEEDED;
        std::unordered_set<std::string> visited;
        visited.reserve(vertices.size());
        for (auto& vertex : vertices) {
            auto vid = vertex.get_id().getStr();
            const auto& newTags = vertex.get_tags();

            if (!NebulaKeyUtils::isValidVidLen(spaceVidLen_, vid)) {
                LOG(ERROR) << "Space " << spaceId_ << ", vertex length invalid, "
                           << " space vid len: " << spaceVidLen_ << ",  vid is " << vid;
                code = nebula::cpp2::ErrorCode::E_INVALID_VID;
                break;
            }

            for (auto& newTag : newTags) {
                auto tagId = newTag.get_tag_id();
                VLOG(3) << "PartitionID: " << partId << ", VertexID: " << vid
                        << ", TagID: " << tagId;

                auto schema = env_->schemaMan_->getTagSchema(spaceId_, tagId);
                if (!schema) {
                    LOG(ERROR) << "Space " << spaceId_ << ", Tag " << tagId << " invalid";
                    code = nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND;
                    break;
                }

                auto key = NebulaKeyUtils::vertexKey(spaceVidLen_, partId, vid, tagId);
                if (ifNotExists_) {
                    if (!visited.emplace(key).second) {
                        continue;
                    }
                    auto obsIdx = findOldValue(partId, vid, tagId);
                    if (nebula::ok(obsIdx)) {
                        if (!nebula::value(obsIdx).empty()) {
                            continue;
                        }
                    } else {
                        code = nebula::error(obsIdx);
                        break;
                    }
                }
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
            }
        }
        if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
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
        auto code = nebula::cpp2::ErrorCode::SUCCEEDED;

        // cache vertexKey
        std::unordered_set<std::string> visited;
        visited.reserve(vertices.size());
        for (auto& vertex : vertices) {
            auto vid = vertex.get_id().getStr();
            const auto& newTags = vertex.get_tags();

            if (!NebulaKeyUtils::isValidVidLen(spaceVidLen_, vid)) {
                LOG(ERROR) << "Space " << spaceId_ << ", vertex length invalid, "
                           << " space vid len: " << spaceVidLen_ << ",  vid is " << vid;
                code = nebula::cpp2::ErrorCode::E_INVALID_VID;
                break;
            }

            for (auto& newTag : newTags) {
                auto tagId = newTag.get_tag_id();
                auto l = std::make_tuple(spaceId_, partId, tagId, vid);
                if (std::find(dummyLock.begin(), dummyLock.end(), l) == dummyLock.end()) {
                    if (!env_->verticesML_->try_lock(l)) {
                        LOG(ERROR) << folly::format("The vertex locked : tag {}, vid {}",
                                                    tagId, vid);
                        code = nebula::cpp2::ErrorCode::E_DATA_CONFLICT_ERROR;
                        break;
                    }
                    dummyLock.emplace_back(std::move(l));
                }
                VLOG(3) << "PartitionID: " << partId << ", VertexID: " << vid
                        << ", TagID: " << tagId;

                auto schema = env_->schemaMan_->getTagSchema(spaceId_, tagId);
                if (!schema) {
                    LOG(ERROR) << "Space " << spaceId_ << ", Tag " << tagId << " invalid";
                    code = nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND;
                    break;
                }

                auto key = NebulaKeyUtils::vertexKey(spaceVidLen_, partId, vid, tagId);
                if (ifNotExists_ && !visited.emplace(key).second) {
                    continue;
                }
                auto props = newTag.get_props();
                auto iter = propNamesMap.find(tagId);
                std::vector<std::string> propNames;
                if (iter != propNamesMap.end()) {
                    propNames = iter->second;
                }

                RowReaderWrapper nReader;
                RowReaderWrapper oReader;
                auto obsIdx = findOldValue(partId, vid, tagId);
                if (nebula::ok(obsIdx)) {
                    if (ifNotExists_ && !nebula::value(obsIdx).empty()) {
                        continue;
                    }
                    if (!nebula::value(obsIdx).empty()) {
                        oReader = RowReaderWrapper::getTagPropReader(env_->schemaMan_,
                                                                     spaceId_,
                                                                     tagId,
                                                                     nebula::value(obsIdx));
                    }
                } else {
                    code = nebula::error(obsIdx);
                    break;
                }

                WriteResult wRet;
                auto retEnc = encodeRowVal(schema.get(), propNames, props, wRet);
                if (!retEnc.ok()) {
                    LOG(ERROR) << retEnc.status();
                    code = writeResultTo(wRet, false);
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
                                    code = nebula::cpp2::ErrorCode::E_DATA_CONFLICT_ERROR;
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
                            auto nik = indexKey(partId, vid, nReader.get(), index);
                            if (!nik.empty()) {
                                auto v = CommonUtils::ttlValue(schema.get(), nReader.get());
                                auto niv = v.ok() ?
                                    IndexKeyUtils::indexVal(std::move(v).value()) : "";
                                // Check the index is building for the specified partition or not.
                                auto indexState = env_->getIndexState(spaceId_, partId);
                                if (env_->checkRebuilding(indexState)) {
                                    auto opKey = OperationKeyUtils::modifyOperationKey(partId, nik);
                                    batchHolder->put(std::move(opKey), std::move(niv));
                                } else if (env_->checkIndexLocked(indexState)) {
                                    LOG(ERROR) << "The index has been locked: "
                                               << index->get_index_name();
                                    code = nebula::cpp2::ErrorCode::E_DATA_CONFLICT_ERROR;
                                    break;
                                } else {
                                    batchHolder->put(std::move(nik), std::move(niv));
                                }
                            }
                        }
                    }
                }  // for index data
                if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                    break;
                }
                /*
                * step 3 , Insert new vertex data
                */
                batchHolder->put(std::move(key), std::move(retEnc.value()));
            }
            if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                break;
            }
        }
        if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
            env_->verticesML_->unlockBatch(dummyLock);
            handleAsync(spaceId_, partId, code);
            continue;
        }
        auto batch = encodeBatchValue(std::move(batchHolder)->getBatch());
        DCHECK(!batch.empty());
        nebula::MemoryLockGuard<VMLI> lg(env_->verticesML_.get(),
                                         std::move(dummyLock),
                                         false,
                                         false);
        env_->kvstore_->asyncAppendBatch(spaceId_, partId, std::move(batch),
            [l = std::move(lg), icw = std::move(wrapper), partId, this] (
                nebula::cpp2::ErrorCode retCode) {
                UNUSED(l);
                UNUSED(icw);
                handleAsync(spaceId_, partId, retCode);
            });
    }
}

ErrorOr<nebula::cpp2::ErrorCode, std::string>
AddVerticesProcessor::findOldValue(PartitionID partId, const VertexID& vId, TagID tagId) {
    auto key = NebulaKeyUtils::vertexKey(spaceVidLen_, partId, vId, tagId);
    std::string val;
    auto ret = env_->kvstore_->get(spaceId_, partId, key, &val);
    if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
        return val;
    } else if (ret == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
        return std::string();
    } else {
        LOG(ERROR) << "Error! ret = " << apache::thrift::util::enumNameSafe(ret)
                   << ", spaceId " << spaceId_;
        return ret;
    }
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
