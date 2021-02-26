/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/StorageFlags.h"
#include "storage/mutate/DeleteVerticesProcessor.h"
#include "utils/IndexKeyUtils.h"
#include "utils/NebulaKeyUtils.h"
#include "utils/OperationKeyUtils.h"

namespace nebula {
namespace storage {

ProcessorCounters kDelVerticesCounters;

void DeleteVerticesProcessor::process(const cpp2::DeleteVerticesRequest& req) {
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
        // Operate every part, the graph layer guarantees the unique of the vid
        std::vector<std::string> keys;
        keys.reserve(32);
        for (auto& part : partVertices) {
            auto partId = part.first;
            const auto& vertexIds = part.second;
            keys.clear();

            for (auto& vid : vertexIds) {
                if (!NebulaKeyUtils::isValidVidLen(spaceVidLen_, vid.getStr())) {
                    LOG(ERROR) << "Space " << spaceId_ << ", vertex length invalid, "
                               << " space vid len: " << spaceVidLen_ << ",  vid is " << vid;
                    pushResultCode(cpp2::ErrorCode::E_INVALID_VID, partId);
                    onFinished();
                    return;
                }

                auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen_, partId, vid.getStr());
                std::unique_ptr<kvstore::KVIterator> iter;
                auto retRes = env_->kvstore_->prefix(spaceId_, partId, prefix, &iter);
                if (retRes != kvstore::ResultCode::SUCCEEDED) {
                    VLOG(3) << "Error! ret = " << static_cast<int32_t>(retRes)
                            << ", spaceID " << spaceId_;
                    handleErrorCode(retRes, spaceId_, partId);
                    onFinished();
                    return;
                }
                while (iter->valid()) {
                    auto key = iter->key();
                    auto tagId = NebulaKeyUtils::getTagId(spaceVidLen_, key);
                    // Evict vertices from cache
                    if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
                        VLOG(3) << "Evict vertex cache for VID " << vid
                                << ", TagID " << tagId;
                        vertexCache_->evict(std::make_pair(vid.getStr(), tagId));
                    }
                    keys.emplace_back(key.str());
                    iter->next();
                }
            }
            doRemove(spaceId_, partId, keys);
        }
    } else {
        for (auto& pv : partVertices) {
            auto partId = pv.first;
            std::vector<VMLI> dummyLock;
            auto batch = deleteVertices(partId, std::move(pv).second, dummyLock);
            if (batch == folly::none) {
                handleErrorCode(kvstore::ResultCode::ERR_INVALID_DATA, spaceId_, partId);
                onFinished();
                return;
            }
            DCHECK(!batch.value().empty());
            nebula::MemoryLockGuard<VMLI> lg(env_->verticesML_.get(), std::move(dummyLock), true);
            if (!lg) {
                auto conflict = lg.conflictKey();
                LOG(ERROR) << "vertex conflict "
                        << std::get<0>(conflict) << ":"
                        << std::get<1>(conflict) << ":"
                        << std::get<2>(conflict) << ":"
                        << std::get<3>(conflict);
                pushResultCode(cpp2::ErrorCode::E_DATA_CONFLICT_ERROR, partId);
                onFinished();
                return;
            }
            env_->kvstore_->asyncAppendBatch(spaceId_, partId, std::move(batch).value(),
                [l = std::move(lg), partId, this](kvstore::ResultCode code) {
                    UNUSED(l);
                    handleAsync(spaceId_, partId, code);
                });
        }
    }
}


folly::Optional<std::string>
DeleteVerticesProcessor::deleteVertices(PartitionID partId,
                                        const std::vector<Value>& vertices,
                                        std::vector<VMLI>& target) {
    IndexCountWrapper wrapper(env_);
    target.reserve(vertices.size());
    std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();
    for (auto& vertex : vertices) {
        auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen_, partId, vertex.getStr());
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = env_->kvstore_->prefix(spaceId_, partId, prefix, &iter);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            VLOG(3) << "Error! ret = " << static_cast<int32_t>(ret)
                    << ", spaceId " << spaceId_;
            return folly::none;
        }

        while (iter->valid()) {
            auto key = iter->key();
            auto tagId = NebulaKeyUtils::getTagId(spaceVidLen_, key);
            RowReaderWrapper reader;
            for (auto& index : indexes_) {
                if (index->get_schema_id().get_tag_id() == tagId) {
                    auto indexId = index->get_index_id();

                    if (reader == nullptr) {
                        reader = RowReaderWrapper::getTagPropReader(env_->schemaMan_,
                                                                    spaceId_,
                                                                    tagId,
                                                                    iter->val());
                        if (reader == nullptr) {
                            LOG(WARNING) << "Bad format row";
                            return folly::none;
                        }
                    }
                    const auto& cols = index->get_fields();
                    auto valuesRet = IndexKeyUtils::collectIndexValues(reader.get(),
                                                                        cols);
                    if (!valuesRet.ok()) {
                        continue;
                    }
                    auto indexKey = IndexKeyUtils::vertexIndexKey(spaceVidLen_,
                                                                    partId,
                                                                    indexId,
                                                                    vertex.getStr(),
                                                                    std::move(valuesRet).value());

                    // Check the index is building for the specified partition or not
                    auto indexState = env_->getIndexState(spaceId_, partId);
                    if (env_->checkRebuilding(indexState)) {
                        auto deleteOpKey = OperationKeyUtils::deleteOperationKey(partId);
                        batchHolder->put(std::move(deleteOpKey), std::move(indexKey));
                    } else if (env_->checkIndexLocked(indexState)) {
                        LOG(ERROR) << "The index has been locked: " << index->get_index_name();
                        return folly::none;
                    } else {
                        batchHolder->remove(std::move(indexKey));
                    }
                }
            }
            if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
                VLOG(3) << "Evict vertex cache for vertex ID " << vertex << ", tagId " << tagId;
                vertexCache_->evict(std::make_pair(vertex.getStr(), tagId));
            }
            target.emplace_back(std::make_tuple(spaceId_, partId, tagId, vertex.getStr()));
            batchHolder->remove(key.str());
            iter->next();
        }
    }

    return encodeBatchValue(batchHolder->getBatch());
}

}  // namespace storage
}  // namespace nebula
