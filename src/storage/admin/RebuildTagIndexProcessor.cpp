/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/RebuildTagIndexProcessor.h"

namespace nebula {
namespace storage {

void RebuildTagIndexProcessor::process(const cpp2::RebuildTagIndexRequest& req) {
    CHECK_NOTNULL(kvstore_);
    auto space = req.get_space_id();
    auto parts = req.get_parts();
    auto tagID = req.get_tag_id();
    auto tagVersion = req.get_tag_version();
    auto indexID = req.get_index_id();
    LOG(INFO) << "Rebuild Tag Index Space " << space << " Tag ID " << tagID
              << " Tag Index " << indexID << " Tag Version " << tagVersion;

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
        while (iter && iter->valid()) {
            if (batchSize >= FLAGS_build_index_batch_size) {
                doPut(space, part, std::move(data));
                data.clear();
                batchSize = 0;
            }

            auto key = iter->key();
            if (!NebulaKeyUtils::isVertex(key) ||
                NebulaKeyUtils::getTagId(key) != tagID ||
                NebulaKeyUtils::getTagVersion(key) != tagVersion) {
                continue;
            }

            auto vertex = NebulaKeyUtils::getVertexId(key);
            auto val = iter->val();
            auto reader = RowReader::getTagPropReader(schemaMan_,
                                                      std::move(val),
                                                      space,
                                                      tagID);
            std::vector<nebula::cpp2::ColumnDef> cols;
            auto values = collectIndexValues(reader.get(), cols);

            auto indexKey = NebulaKeyUtils::vertexIndexKey(part, indexID, vertex, values);
            data.emplace_back(std::move(indexKey), "");
        }
        doPut(space, part, std::move(data));
    }
    onFinished();
}

}  // namespace storage
}  // namespace nebula
