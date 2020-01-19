/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/RebuildEdgeIndexProcessor.h"

namespace nebula {
namespace storage {

void RebuildEdgeIndexProcessor::process(const cpp2::RebuildIndexRequest& req) {
    CHECK_NOTNULL(kvstore_);
    auto space = req.get_space_id();
    auto parts = req.get_parts();
    auto edgeType = req.get_schema_id().get_edge_type();
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
    LOG(INFO) << "Rebuild Edge Index Space " << space << " Edge Type " << edgeType
              << " Edge Index " << indexID;

    for (PartitionID part : parts) {
        // firstly remove the discard index
        auto indexPrefix = NebulaKeyUtils::indexPrefix(space, indexID);
        doRemovePrefix(space, part, std::move(indexPrefix));

        std::unique_ptr<kvstore::KVIterator> iter;
        auto prefix = NebulaKeyUtils::prefix(part);
        kvstore_->prefix(space, part, prefix, &iter);
        std::vector<kvstore::KV> data;
        data.reserve(FLAGS_build_index_batch_size);
        int32_t batchSize = 0;
        VertexID currentSrcVertex = -1;
        VertexID currentDstVertex = -1;
        while (iter && iter->valid()) {
            if (batchSize >= FLAGS_build_index_batch_size) {
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
    }
    onFinished();
}

}  // namespace storage
}  // namespace nebula
