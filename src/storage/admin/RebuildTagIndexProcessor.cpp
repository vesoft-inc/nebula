/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/RebuildTagIndexProcessor.h"

namespace nebula {
namespace storage {

void RebuildTagIndexProcessor::process(const cpp2::RebuildIndexRequest& req) {
    CHECK_NOTNULL(kvstore_);
    auto space = req.get_space_id();
    auto parts = req.get_parts();
    auto tagID = req.get_schema_id().get_tag_id();
    auto indexID = req.get_index_id();
    auto itemRet = indexMan_->getTagIndex(space, indexID);
    if (!itemRet.ok()) {
        cpp2::ResultCode thriftRet;
        thriftRet.set_code(cpp2::ErrorCode::E_INDEX_NOT_FOUND);
        codes_.emplace_back(thriftRet);
        onFinished();
        return;
    }
    auto item = itemRet.value();
    LOG(INFO) << "Rebuild Tag Index Space " << space << " Tag ID " << tagID
              << " Tag Index " << indexID;

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
        VertexID currentVertex = -1;
        while (iter && iter->valid()) {
            if (batchSize >= FLAGS_build_index_batch_size) {
                doPut(space, part, std::move(data));
                data.clear();
                batchSize = 0;
            }

            auto key = iter->key();
            auto val = iter->val();

            if (!NebulaKeyUtils::isVertex(key) ||
                NebulaKeyUtils::getTagId(key) != tagID) {
                iter->next();
                continue;
            }

            auto vertex = NebulaKeyUtils::getVertexId(key);
            if (currentVertex == vertex) {
                continue;
            } else {
                currentVertex = vertex;
            }
            auto reader = RowReader::getTagPropReader(schemaMan_,
                                                      std::move(val),
                                                      space,
                                                      tagID);
            auto values = collectIndexValues(reader.get(), item->get_fields());

            auto indexKey = NebulaKeyUtils::vertexIndexKey(part, indexID, vertex, values);
            data.emplace_back(std::move(indexKey), "");
            iter->next();
        }
        doPut(space, part, std::move(data));
    }

    onFinished();
}

}  // namespace storage
}  // namespace nebula
