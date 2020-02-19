/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "kvstore/LogEncoder.h"
#include "storage/StorageFlags.h"
#include "storage/admin/RebuildEdgeIndexProcessor.h"

namespace nebula {
namespace storage {

void RebuildEdgeIndexProcessor::process(const cpp2::RebuildIndexRequest& req) {
    CHECK_NOTNULL(kvstore_);
    auto space = req.get_space_id();
    auto parts = req.get_parts();
    callingNum_ = parts.size();
    auto indexID = req.get_index_id();
    auto itemRet = indexMan_->getEdgeIndex(space, indexID);
    if (!itemRet.ok()) {
        cpp2::ResultCode thriftRet;
        thriftRet.set_code(cpp2::ErrorCode::E_INDEX_NOT_FOUND);
        codes_.emplace_back(thriftRet);
        onFinished();
        return;
    }

    auto item = itemRet.value();
    auto edgeType = item->get_schema_id().get_edge_type();
    LOG(INFO) << "Rebuild Edge Index Space " << space << " Edge Type " << edgeType
              << " Edge Index " << indexID;

    for (PartitionID part : parts) {
        if (FLAGS_ignore_index_check_pre_insert) {
            std::unique_ptr<kvstore::KVIterator> iter;
            auto prefix = NebulaKeyUtils::prefix(part);
            auto ret = kvstore_->prefix(space, part, prefix, &iter);
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Processing Part " << part << " Failed";
                this->pushResultCode(to(ret), part);
                onFinished();
                return;
            }

            std::vector<kvstore::KV> data;
            data.reserve(FLAGS_rebuild_index_batch_size);
            int32_t batchSize = 0;
            VertexID currentSrcVertex = -1;
            VertexID currentDstVertex = -1;
            while (iter && iter->valid()) {
                if (batchSize >= FLAGS_rebuild_index_batch_size) {
                    doPut(space, part, std::move(data));
                    data.clear();
                    batchSize = 0;
                }

                auto key = iter->key();
                auto val = iter->val();

                if (!NebulaKeyUtils::isEdge(key) ||
                    NebulaKeyUtils::getEdgeType(key) != edgeType) {
                    iter->next();
                    continue;
                }

                auto source = NebulaKeyUtils::getSrcId(key);
                auto destination = NebulaKeyUtils::getDstId(key);
                if (currentSrcVertex == source || currentDstVertex == destination) {
                    iter->next();
                    continue;
                } else {
                    currentSrcVertex = source;
                    currentDstVertex = destination;
                }
                auto ranking = NebulaKeyUtils::getRank(key);
                auto reader = RowReader::getEdgePropReader(schemaMan_,
                                                        std::move(val),
                                                        space,
                                                        edgeType);
                auto values = collectIndexValues(reader.get(), item->get_fields());
                auto indexKey = NebulaKeyUtils::edgeIndexKey(part, indexID, source,
                                                            ranking, destination, values);
                data.emplace_back(std::move(indexKey), "");
                iter->next();
            }
            doPut(space, part, std::move(data));
        } else {
            auto atomic = [space, part, edgeType, item, this]() -> std::string {
                return partitionRebuildIndex(space, part, edgeType, item);
            };

            auto callback = [space, part, this](kvstore::ResultCode code) {
                handleAsync(space, part, code);
            };

            this->kvstore_->asyncAtomicOp(space, part, atomic, callback);
        }
    }
    onFinished();
}

std::string
RebuildEdgeIndexProcessor::partitionRebuildIndex(GraphSpaceID space,
                                                 PartitionID part,
                                                 EdgeType edge,
                                                 std::shared_ptr<nebula::cpp2::IndexItem> item) {
    std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();
    // firstly remove the discard index
    batchHolder->removePrefix(NebulaKeyUtils::indexPrefix(space, item->get_index_id()));

    VertexID currentSrcVertex = -1;
    VertexID currentDstVertex = -1;
    std::unique_ptr<kvstore::KVIterator> iter;
    auto prefix = NebulaKeyUtils::prefix(part);
    auto ret = kvstore_->prefix(space, part, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Processing Part " << part << " Failed";
        return "";
    }

    while (iter && iter->valid()) {
        auto key = iter->key();
        auto val = iter->val();
        if (!NebulaKeyUtils::isEdge(key) ||
            NebulaKeyUtils::getEdgeType(key) != edge) {
            iter->next();
            continue;
        }

        auto source = NebulaKeyUtils::getSrcId(key);
        auto destination = NebulaKeyUtils::getDstId(key);
        if (currentSrcVertex == source || currentDstVertex == destination) {
            iter->next();
            continue;
        } else {
            currentSrcVertex = source;
            currentDstVertex = destination;
        }

        auto ranking = NebulaKeyUtils::getRank(key);
        auto reader = RowReader::getEdgePropReader(schemaMan_,
                                                   std::move(val),
                                                   space,
                                                   edge);
        auto values = collectIndexValues(reader.get(), item->get_fields());
        auto indexKey = NebulaKeyUtils::edgeIndexKey(part, item->get_index_id(), source,
                                                     ranking, destination, values);
        batchHolder->put(std::move(indexKey), "");
        iter->next();
    }

    return encodeBatchValue(batchHolder->getBatch());
}

}  // namespace storage
}  // namespace nebula

