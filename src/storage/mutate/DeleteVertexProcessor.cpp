/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/mutate/DeleteVertexProcessor.h"
#include "base/NebulaKeyUtils.h"

DECLARE_bool(enable_vertex_cache);

namespace nebula {
namespace storage {

void DeleteVertexProcessor::process(const cpp2::DeleteVertexRequest& req) {
    VLOG(3) << "Receive DeleteVertexRequest...";
    callingNum_ = 1;
    auto spaceId = req.get_space_id();
    auto partId = req.get_part_id();
    auto vId = req.get_vid();
    if (req.__isset.indexes) {
        indexes_ = req.get_indexes();
    }
    auto atomic = [&]() -> std::string {
        return deleteVertex(spaceId, partId, vId);
    };
    auto callback = [spaceId, partId, this](kvstore::ResultCode code) {
        VLOG(3) << "partId:" << partId << ", code:" << static_cast<int32_t>(code);
        handleAsync(spaceId, partId, code);
    };
    this->kvstore_->asyncAtomicOp(spaceId, partId, atomic, callback);
}

std::string DeleteVertexProcessor::deleteVertex(GraphSpaceID spaceId,
                                                PartitionID partId,
                                                VertexID vId) {
    std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();
    auto prefix = NebulaKeyUtils::vertexPrefix(partId, vId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = this->kvstore_->prefix(spaceId, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        VLOG(3) << "Error! ret = " << static_cast<int32_t>(ret)
                << ", spaceId " << spaceId;
        return "";
    }
    std::vector<std::string> indexes;
    while (iter->valid()) {
        auto key = iter->key();
        auto tagId = NebulaKeyUtils::getTagId(key);
        if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
            if (NebulaKeyUtils::isVertex(key)) {
                VLOG(3) << "Evict vertex cache for vId " << vId << ", tagId " << tagId;
                vertexCache_->evict(std::make_pair(vId, tagId), partId);
            }
        }
        batchHolder->remove(key.str());
        if (indexes_ != nullptr) {
            std::for_each(indexes_->begin(), indexes_->end(), [&](auto& index) {
                auto indexId = index.get_index_id();
                if (index.get_schema() == tagId) {
                    auto reader = RowReader::getTagPropReader(this->schemaMan_,
                                                              iter->val(),
                                                              spaceId,
                                                              tagId);
                    const auto& cols = index.get_cols();
                    auto values = collectIndexValues(reader.get(), cols);
                    auto indexKey = NebulaKeyUtils::vertexIndexKey(partId,
                                                                   indexId,
                                                                   vId,
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
    return encodeBatchValue(batchHolder->getBatch());
}

}  // namespace storage
}  // namespace nebula
