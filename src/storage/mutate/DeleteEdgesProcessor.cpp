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

void DeleteEdgesProcessor::process(const cpp2::DeleteEdgesRequest& req) {
    spaceId_ = req.get_space_id();
    const auto& partEdges = req.get_parts();

    CHECK_NOTNULL(env_->schemaMan_);
    auto ret = env_->schemaMan_->getSpaceVidLen(spaceId_);
    if (!ret.ok()) {
        LOG(ERROR) << ret.status();
        for (auto& part : partEdges) {
            pushResultCode(cpp2::ErrorCode::E_INVALID_SPACEVIDLEN, part.first);
        }
        onFinished();
        return;
    }
    spaceVidLen_ = ret.value();
    callingNum_ = partEdges.size();

    CHECK_NOTNULL(env_->indexMan_);
    auto iRet = env_->indexMan_->getEdgeIndexes(spaceId_);
    if (iRet.ok()) {
        indexes_ = std::move(iRet).value();
    }

    CHECK_NOTNULL(env_->kvstore_);
    if (indexes_.empty()) {
        // Operate every part, the graph layer guarantees the unique of the edgeKey
        std::vector<std::string> keys;
        keys.reserve(32);
        for (auto& part : partEdges) {
            auto partId = part.first;
            keys.clear();
            for (auto& edgeKey : part.second) {
                if (!NebulaKeyUtils::isValidVidLen(spaceVidLen_, edgeKey.src, edgeKey.dst)) {
                    LOG(ERROR) << "Space " << spaceId_ << " vertex length invalid, "
                               << "space vid len: " << spaceVidLen_ << ", edge srcVid: "
                               << edgeKey.src << " dstVid: " << edgeKey.dst;
                    pushResultCode(cpp2::ErrorCode::E_INVALID_VID, partId);
                    onFinished();
                    return;
                }
                auto start = NebulaKeyUtils::edgeKey(spaceVidLen_,
                                                     partId,
                                                     edgeKey.src,
                                                     edgeKey.edge_type,
                                                     edgeKey.ranking,
                                                     edgeKey.dst,
                                                     0);
                auto end = NebulaKeyUtils::edgeKey(spaceVidLen_,
                                                   partId,
                                                   edgeKey.src,
                                                   edgeKey.edge_type,
                                                   edgeKey.ranking,
                                                   edgeKey.dst,
                                                   std::numeric_limits<int64_t>::max());
                std::unique_ptr<kvstore::KVIterator> iter;
                auto retRes = env_->kvstore_->range(spaceId_, partId, start, end, &iter);
                if (retRes != kvstore::ResultCode::SUCCEEDED) {
                    VLOG(3) << "Error! ret = " << static_cast<int32_t>(retRes)
                            << ", spaceID " << spaceId_;
                    handleErrorCode(retRes, spaceId_, partId);
                    onFinished();
                    return;
                }
                while (iter && iter->valid()) {
                    auto key = iter->key();
                    keys.emplace_back(key.data(), key.size());
                    iter->next();
                }
            }
            doRemove(spaceId_, partId, keys);
        }
    } else {
        std::for_each(partEdges.begin(), partEdges.end(), [this](auto &part) {
            auto partId = part.first;
            auto atomic = [partId, edges = std::move(part.second), this]()
                          -> folly::Optional<std::string> {
                return deleteEdges(partId, edges);
            };

            auto callback = [partId, this](kvstore::ResultCode code) {
                handleAsync(spaceId_, partId, code);
            };
            env_->kvstore_->asyncAtomicOp(spaceId_, partId, atomic, callback);
        });
    }
}


folly::Optional<std::string>
DeleteEdgesProcessor::deleteEdges(PartitionID partId,
                                  const std::vector<cpp2::EdgeKey>& edges) {
    env_->onFlyingRequest_.fetch_add(1);
    std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();
    for (auto& edge : edges) {
        auto type = edge.edge_type;
        auto srcId = edge.src;
        auto rank = edge.ranking;
        auto dstId = edge.dst;
        auto prefix = NebulaKeyUtils::edgePrefix(spaceVidLen_, partId, srcId, type, rank, dstId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = env_->kvstore_->prefix(spaceId_, partId, prefix, &iter);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            VLOG(3) << "Error! ret = " << static_cast<int32_t>(ret)
                    << ", spaceId " << spaceId_;
            return folly::none;
        }

        if (iter->valid()) {
            /**
             * just get the latest version edge for index.
             */
            std::unique_ptr<RowReader> reader;
            for (auto& index : indexes_) {
                if (type == index->get_schema_id().get_edge_type()) {
                    auto indexId = index->get_index_id();

                    if (reader == nullptr) {
                        reader = RowReader::getEdgePropReader(env_->schemaMan_,
                                                              spaceId_,
                                                              type,
                                                              iter->val());
                        if (reader == nullptr) {
                            LOG(WARNING) << "Bad format row!";
                            return folly::none;
                        }
                    }
                    std::vector<Value::Type> colsType;
                    auto valuesRet = IndexKeyUtils::collectIndexValues(reader.get(),
                                                                       index->get_fields(),
                                                                       colsType);
                    if (!valuesRet.ok()) {
                        continue;
                    }
                    auto indexKey = IndexKeyUtils::edgeIndexKey(spaceVidLen_, partId,
                                                                indexId,
                                                                srcId,
                                                                rank,
                                                                dstId,
                                                                valuesRet.value(),
                                                                colsType);

                    if (env_->checkRebuilding(spaceId_, partId, indexId)) {
                        auto deleteOpKey = OperationKeyUtils::deleteOperationKey(partId);
                        batchHolder->put(std::move(deleteOpKey), std::move(indexKey));
                    } else if (env_->checkIndexLocked(spaceId_, partId, indexId)) {
                        LOG(ERROR) << "The index has been locked: " << index->get_index_name();
                        return folly::none;
                    } else {
                        batchHolder->remove(std::move(indexKey));
                    }
                }
            }

            batchHolder->remove(iter->key().str());
            iter->next();
        }

        while (iter->valid()) {
            batchHolder->remove(iter->key().str());
            iter->next();
        }
    }

    return encodeBatchValue(batchHolder->getBatch());
}

}  // namespace storage
}  // namespace nebula

