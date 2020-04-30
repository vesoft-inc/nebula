/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "storage/mutate/DeleteEdgesProcessor.h"
#include <algorithm>
#include <limits>
#include "utils/NebulaKeyUtils.h"

namespace nebula {
namespace storage {

void DeleteEdgesProcessor::process(const cpp2::DeleteEdgesRequest& req) {
    auto spaceId = req.get_space_id();
    CHECK_NOTNULL(kvstore_);
    auto iRet = indexMan_->getEdgeIndexes(spaceId);
    if (iRet.ok()) {
        indexes_ = std::move(iRet).value();
    }

    if (indexes_.empty()) {
        std::for_each(req.parts.begin(), req.parts.end(), [this](auto &partEdges) {
            this->callingNum_ += partEdges.second.size();
        });
        std::vector<std::string> keys;
        keys.reserve(16);
        for (auto& partEdges : req.parts) {
            auto partId = partEdges.first;
            for (auto& edgeKey : partEdges.second) {
                auto start = NebulaKeyUtils::edgeKey(partId,
                                                     edgeKey.src,
                                                     edgeKey.edge_type,
                                                     edgeKey.ranking,
                                                     edgeKey.dst,
                                                     0);
                auto end = NebulaKeyUtils::edgeKey(partId,
                                                   edgeKey.src,
                                                   edgeKey.edge_type,
                                                   edgeKey.ranking,
                                                   edgeKey.dst,
                                                   std::numeric_limits<int64_t>::max());
                std::unique_ptr<kvstore::KVIterator> iter;
                auto ret = this->kvstore_->range(spaceId, partId, start, end, &iter);
                if (ret != kvstore::ResultCode::SUCCEEDED) {
                    VLOG(3) << "Error! ret = " << static_cast<int32_t>(ret)
                            << ", spaceID " << spaceId;
                    this->handleErrorCode(ret, spaceId, partId);
                    this->onFinished();
                    return;
                }
                keys.clear();
                while (iter && iter->valid()) {
                    auto key = iter->key();
                    keys.emplace_back(key.data(), key.size());
                    iter->next();
                }
                doRemove(spaceId, partId, keys);
            }
        }
    } else {
        callingNum_ = req.parts.size();
        std::for_each(req.parts.begin(), req.parts.end(), [spaceId, this](auto &partEdges) {
            auto partId = partEdges.first;
            auto atomic = [spaceId, partId, edges = std::move(partEdges.second), this]()
                          -> folly::Optional<std::string> {
                return deleteEdges(spaceId, partId, edges);
            };
            auto callback = [spaceId, partId, this](kvstore::ResultCode code) {
                handleAsync(spaceId, partId, code);
            };
            this->kvstore_->asyncAtomicOp(spaceId, partId, atomic, callback);
        });
    }
}

folly::Optional<std::string>
DeleteEdgesProcessor::deleteEdges(GraphSpaceID spaceId,
                                  PartitionID partId,
                                  const std::vector<cpp2::EdgeKey>& edges) {
    std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();
    for (auto& edge : edges) {
        auto type = edge.edge_type;
        auto srcId = edge.src;
        auto rank = edge.ranking;
        auto dstId = edge.dst;
        auto prefix = NebulaKeyUtils::edgePrefix(partId, srcId, type, rank, dstId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = this->kvstore_->prefix(spaceId, partId, prefix, &iter);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            VLOG(3) << "Error! ret = " << static_cast<int32_t>(ret)
                    << ", spaceId " << spaceId;
            return folly::none;
        }
        bool isLatestVE = true;
        while (iter->valid()) {
            /**
             * just get the latest version edge for index.
             */
            if (isLatestVE) {
                std::unique_ptr<RowReader> reader;
                for (auto& index : indexes_) {
                    auto indexId = index->get_index_id();
                    if (type == index->get_schema_id().get_edge_type()) {
                        if (reader == nullptr) {
                            reader = RowReader::getEdgePropReader(this->schemaMan_,
                                                                  iter->val(),
                                                                  spaceId,
                                                                  type);
                            if (reader == nullptr) {
                                LOG(WARNING) << "Bad format row!";
                                return folly::none;
                            }
                        }
                        auto values = collectIndexValues(reader.get(),
                                                         index->get_fields());
                        if (!values.ok()) {
                            continue;
                        }
                        auto indexKey = NebulaKeyUtils::edgeIndexKey(partId,
                                                                     indexId,
                                                                     srcId,
                                                                     rank,
                                                                     dstId,
                                                                     values.value());
                        batchHolder->remove(std::move(indexKey));
                    }
                }
                isLatestVE = false;
            }
            batchHolder->remove(iter->key().str());
            iter->next();
        }
    }
    return encodeBatchValue(batchHolder->getBatch());
}
}  // namespace storage
}  // namespace nebula

