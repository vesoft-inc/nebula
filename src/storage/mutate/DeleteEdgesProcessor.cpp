/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "storage/mutate/DeleteEdgesProcessor.h"
#include <algorithm>
#include <limits>
#include "base/NebulaKeyUtils.h"

namespace nebula {
namespace storage {

void DeleteEdgesProcessor::process(const cpp2::DeleteEdgesRequest& req) {
    auto spaceId = req.get_space_id();
    callingNum_ = req.parts.size();
    CHECK_NOTNULL(kvstore_);
    if (req.__isset.indexes) {
        indexes_ = req.get_indexes();
    }
    std::for_each(req.parts.begin(), req.parts.end(), [&](auto &partEdges) {
        auto partId = partEdges.first;
        const auto &edges = partEdges.second;
        auto atomic = [&]() -> std::string {
            return deleteEdges(spaceId, partId, edges);
        };
        auto callback = [spaceId, partId, this](kvstore::ResultCode code) {
            handleAsync(spaceId, partId, code);
        };
        this->kvstore_->asyncAtomicOp(spaceId, partId, atomic, callback);
    });
}

std::string DeleteEdgesProcessor::deleteEdges(GraphSpaceID spaceId,
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
            return "";
        }
        std::vector<std::string> indexes;
        while (iter->valid()) {
            batchHolder->remove(iter->key().str());
            if (indexes_ != nullptr) {
                std::for_each(indexes_->begin(), indexes_->end(), [&](auto& index) {
                    auto indexId = index.get_index_id();
                    if (index.get_schema() == type) {
                        auto reader = RowReader::getEdgePropReader(this->schemaMan_,
                                                                   iter->val(),
                                                                   spaceId,
                                                                   type);
                        auto values = collectIndexValues(reader.get(),
                                                         index.get_cols());
                        auto indexKey = NebulaKeyUtils::edgeIndexKey(partId,
                                                                     indexId,
                                                                     srcId,
                                                                     type,
                                                                     rank,
                                                                     dstId,
                                                                     values);
                        std::string val;
                        auto result = kvstore_->get(spaceId, partId, indexKey, &val);
                        if (result == kvstore::ResultCode::SUCCEEDED) {
                            indexes.emplace_back(std::move(indexKey));
                        }
                    }
                });
            }
            iter->next();
        }
        /**
         * There may be different versions of the same vertex,
         * so delete duplicate index keys is required.
         **/
        if (!indexes.empty()) {
            sort(indexes.begin(), indexes.end());
            indexes.erase(unique(indexes.begin(), indexes.end()), indexes.end());
            for (auto& key : indexes) {
                batchHolder->remove(std::move(key));
            }
        }
    }
    return encodeBatchValue(batchHolder->getBatch());
}
}  // namespace storage
}  // namespace nebula

