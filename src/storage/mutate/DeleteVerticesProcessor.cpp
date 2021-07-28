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
        // Operate every part, the graph layer guarantees the unique of the vid
        std::vector<std::string> keys;
        keys.reserve(32);
        for (auto& part : partVertices) {
            auto partId = part.first;
            const auto& vertexIds = part.second;
            keys.clear();
            auto code = nebula::cpp2::ErrorCode::SUCCEEDED;
            for (auto& vid : vertexIds) {
                if (!NebulaKeyUtils::isValidVidLen(spaceVidLen_, vid.getStr())) {
                    LOG(ERROR) << "Space " << spaceId_ << ", vertex length invalid, "
                               << " space vid len: " << spaceVidLen_ << ",  vid is " << vid;
                    code = nebula::cpp2::ErrorCode::E_INVALID_VID;
                    break;
                }

                auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen_, partId, vid.getStr());
                std::unique_ptr<kvstore::KVIterator> iter;
                code = env_->kvstore_->prefix(spaceId_, partId, prefix, &iter);
                if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                    VLOG(3) << "Error! ret = " << static_cast<int32_t>(code)
                            << ", spaceID " << spaceId_;
                    break;
                }
                while (iter->valid()) {
                    auto key = iter->key();
                    keys.emplace_back(key.str());
                    iter->next();
                }
            }
            if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                handleAsync(spaceId_, partId, code);
                continue;
            }
            doRemove(spaceId_, partId, std::move(keys));
        }
    } else {
        for (auto& pv : partVertices) {
            IndexCountWrapper wrapper(env_);
            auto partId = pv.first;
            std::vector<VMLI> dummyLock;
            auto batch = deleteVertices(partId, std::move(pv).second, dummyLock);
            if (!nebula::ok(batch)) {
                env_->verticesML_->unlockBatch(dummyLock);
                handleAsync(spaceId_, partId, nebula::error(batch));
                continue;
            }
            DCHECK(!nebula::value(batch).empty());
            nebula::MemoryLockGuard<VMLI> lg(env_->verticesML_.get(),
                                             std::move(dummyLock),
                                             false,
                                             false);
            env_->kvstore_->asyncAppendBatch(spaceId_, partId, std::move(nebula::value(batch)),
                [l = std::move(lg), icw = std::move(wrapper), partId, this] (
                    nebula::cpp2::ErrorCode code) {
                    UNUSED(l);
                    UNUSED(icw);
                    handleAsync(spaceId_, partId, code);
                });
        }
    }
}


ErrorOr<nebula::cpp2::ErrorCode, std::string>
DeleteVerticesProcessor::deleteVertices(PartitionID partId,
                                        const std::vector<Value>& vertices,
                                        std::vector<VMLI>& target) {
    target.reserve(vertices.size());
    std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();
    for (auto& vertex : vertices) {
        auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen_, partId, vertex.getStr());
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = env_->kvstore_->prefix(spaceId_, partId, prefix, &iter);
        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
            VLOG(3) << "Error! ret = " << static_cast<int32_t>(ret)
                    << ", spaceId " << spaceId_;
            return ret;
        }

        while (iter->valid()) {
            auto key = iter->key();
            auto tagId = NebulaKeyUtils::getTagId(spaceVidLen_, key);
            auto l = std::make_tuple(spaceId_, partId, tagId, vertex.getStr());
            if (std::find(target.begin(), target.end(), l) == target.end()) {
                if (!env_->verticesML_->try_lock(l)) {
                    LOG(ERROR) << folly::format("The vertex locked : tag {}, vid {}",
                                                tagId, vertex.getStr());
                    return nebula::cpp2::ErrorCode::E_DATA_CONFLICT_ERROR;
                }
                target.emplace_back(std::move(l));
            }
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
                            return nebula::cpp2::ErrorCode::E_INVALID_DATA;
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
                        return nebula::cpp2::ErrorCode::E_DATA_CONFLICT_ERROR;
                    } else {
                        batchHolder->remove(std::move(indexKey));
                    }
                }
            }
            batchHolder->remove(key.str());
            iter->next();
        }
    }

    return encodeBatchValue(batchHolder->getBatch());
}

}  // namespace storage
}  // namespace nebula
