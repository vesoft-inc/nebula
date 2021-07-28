/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/mutate/DeleteEdgesProcessor.h"
#include <algorithm>
#include "utils/IndexKeyUtils.h"
#include "utils/NebulaKeyUtils.h"
#include "utils/OperationKeyUtils.h"

namespace nebula {
namespace storage {

ProcessorCounters kDelEdgesCounters;

void DeleteEdgesProcessor::process(const cpp2::DeleteEdgesRequest& req) {
    spaceId_ = req.get_space_id();
    const auto& partEdges = req.get_parts();

    CHECK_NOTNULL(env_->schemaMan_);
    auto ret = env_->schemaMan_->getSpaceVidLen(spaceId_);
    if (!ret.ok()) {
        LOG(ERROR) << ret.status();
        for (auto& part : partEdges) {
            pushResultCode(nebula::cpp2::ErrorCode::E_INVALID_SPACEVIDLEN, part.first);
        }
        onFinished();
        return;
    }
    spaceVidLen_ = ret.value();
    callingNum_ = partEdges.size();

    CHECK_NOTNULL(env_->indexMan_);
    auto iRet = env_->indexMan_->getEdgeIndexes(spaceId_);
    if (!iRet.ok()) {
        LOG(ERROR) << iRet.status();
        for (auto& part : partEdges) {
            pushResultCode(nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND, part.first);
        }
        onFinished();
        return;
    }
    indexes_ = std::move(iRet).value();

    CHECK_NOTNULL(env_->kvstore_);
    if (indexes_.empty()) {
        // Operate every part, the graph layer guarantees the unique of the edgeKey
        for (auto& part : partEdges) {
            std::vector<std::string> keys;
            keys.reserve(32);
            auto partId = part.first;
            auto code = nebula::cpp2::ErrorCode::SUCCEEDED;
            for (auto& edgeKey : part.second) {
                if (!NebulaKeyUtils::isValidVidLen(
                        spaceVidLen_,
                        (*edgeKey.src_ref()).getStr(),
                        (*edgeKey.dst_ref()).getStr())) {
                    LOG(ERROR) << "Space " << spaceId_ << " vertex length invalid, "
                               << "space vid len: " << spaceVidLen_
                               << ", edge srcVid: " << *edgeKey.src_ref()
                               << " dstVid: " << *edgeKey.dst_ref();
                    code = nebula::cpp2::ErrorCode::E_INVALID_VID;
                    break;
                }
                // todo(doodle): delete lock in toss
                auto edge = NebulaKeyUtils::edgeKey(spaceVidLen_,
                                                    partId,
                                                    (*edgeKey.src_ref()).getStr(),
                                                    *edgeKey.edge_type_ref(),
                                                    *edgeKey.ranking_ref(),
                                                    (*edgeKey.dst_ref()).getStr());
                keys.emplace_back(edge.data(), edge.size());
            }
            if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                handleAsync(spaceId_, partId, code);
                continue;
            }
            doRemove(spaceId_, partId, std::move(keys));
        }
    } else {
        for (auto& part : partEdges) {
            IndexCountWrapper wrapper(env_);
            auto partId = part.first;
            std::vector<EMLI> dummyLock;
            dummyLock.reserve(part.second.size());

            nebula::cpp2::ErrorCode err = nebula::cpp2::ErrorCode::SUCCEEDED;
            for (const auto& edgeKey : part.second) {
                auto l = std::make_tuple(spaceId_,
                                         partId,
                                         (*edgeKey.src_ref()).getStr(),
                                         *edgeKey.edge_type_ref(),
                                         *edgeKey.ranking_ref(),
                                         (*edgeKey.dst_ref()).getStr());
                if (std::find(dummyLock.begin(), dummyLock.end(), l) == dummyLock.end()) {
                    if (!env_->edgesML_->try_lock(l)) {
                        LOG(ERROR) << folly::format("The edge locked : src {}, "
                                                    "type {}, tank {}, dst {}",
                                                    (*edgeKey.src_ref()).getStr(),
                                                    *edgeKey.edge_type_ref(),
                                                    *edgeKey.ranking_ref(),
                                                    (*edgeKey.dst_ref()).getStr());
                        err = nebula::cpp2::ErrorCode::E_DATA_CONFLICT_ERROR;
                        break;
                    }
                    dummyLock.emplace_back(std::move(l));
                }
            }
            if (err != nebula::cpp2::ErrorCode::SUCCEEDED) {
                env_->edgesML_->unlockBatch(dummyLock);
                handleAsync(spaceId_, partId, err);
                continue;
            }
            auto batch = deleteEdges(partId, std::move(part.second));
            if (!nebula::ok(batch)) {
                env_->edgesML_->unlockBatch(dummyLock);
                handleAsync(spaceId_, partId, nebula::error(batch));
                continue;
            }
            DCHECK(!nebula::value(batch).empty());
            nebula::MemoryLockGuard<EMLI> lg(env_->edgesML_.get(),
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
DeleteEdgesProcessor::deleteEdges(PartitionID partId, const std::vector<cpp2::EdgeKey>& edges) {
    std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();
    for (auto& edge : edges) {
        auto type = *edge.edge_type_ref();
        auto srcId = (*edge.src_ref()).getStr();
        auto rank = *edge.ranking_ref();
        auto dstId = (*edge.dst_ref()).getStr();
        auto key = NebulaKeyUtils::edgeKey(spaceVidLen_, partId, srcId, type, rank, dstId);
        std::string val;
        auto ret = env_->kvstore_->get(spaceId_, partId, key, &val);

        if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
            /**
             * just get the latest version edge for index.
             */
            RowReaderWrapper reader;
            for (auto& index : indexes_) {
                if (type == index->get_schema_id().get_edge_type()) {
                    auto indexId = index->get_index_id();

                    if (reader == nullptr) {
                        reader = RowReaderWrapper::getEdgePropReader(env_->schemaMan_,
                                                                     spaceId_,
                                                                     type,
                                                                     val);
                        if (reader == nullptr) {
                            LOG(WARNING) << "Bad format row!";
                            return nebula::cpp2::ErrorCode::E_INVALID_DATA;
                        }
                    }
                    auto valuesRet = IndexKeyUtils::collectIndexValues(reader.get(),
                                                                       index->get_fields());
                    if (!valuesRet.ok()) {
                        continue;
                    }
                    auto indexKey = IndexKeyUtils::edgeIndexKey(spaceVidLen_, partId,
                                                                indexId,
                                                                srcId,
                                                                rank,
                                                                dstId,
                                                                std::move(valuesRet).value());

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
            batchHolder->remove(std::move(key));
        } else if (ret == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
            continue;
        } else {
            VLOG(3) << "Error! ret = " << apache::thrift::util::enumNameSafe(ret)
                    << ", spaceId " << spaceId_;
            return ret;
        }
    }

    return encodeBatchValue(batchHolder->getBatch());
}

}  // namespace storage
}  // namespace nebula

