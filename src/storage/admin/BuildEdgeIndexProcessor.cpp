/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/BuildEdgeIndexProcessor.h"

namespace nebula {
namespace storage {

void BuildEdgeIndexProcessor::process(const cpp2::BuildEdgeIndexRequest& req) {
    CHECK_NOTNULL(kvstore_);
    auto space = req.get_space_id();
    auto parts = req.get_parts();
    auto edgeType = req.get_edge_type();
    auto edgeVersion = req.get_edge_version();
    auto indexID = req.get_index_id();
    LOG(INFO) << "Build Edge Index Space " << space << " Edge Type " << edgeType
              << " Edge Index " << indexID << " Edge Verison " << edgeVersion;

    for (int32_t part = 1; part <= parts; part++) {
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
            if (!NebulaKeyUtils::isEdge(key) ||
                NebulaKeyUtils::getEdgeType(key) != edgeType ||
                NebulaKeyUtils::getEdgeVersion(key) != edgeVersion) {
                continue;
            }

            auto source = NebulaKeyUtils::getSrcId(key);
            auto destination = NebulaKeyUtils::getDstId(key);
            auto ranking = NebulaKeyUtils::getRank(key);
            auto val = iter->val();
            auto reader = RowReader::getEdgePropReader(schemaMan_,
                                                       std::move(val),
                                                       space,
                                                       edgeType);
            std::vector<nebula::cpp2::ColumnDef> cols;
            auto values = collectIndexValues(reader.get(), cols);
            auto indexKey = NebulaKeyUtils::edgeIndexKey(part, indexID, source,
                                                         ranking, destination, values);
            data.emplace_back(std::move(indexKey), "");
        }
        doPut(space, part, std::move(data));
    }
}

}  // namespace storage
}  // namespace nebula
