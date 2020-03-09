/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "kvstore/LogEncoder.h"
#include "storage/StorageFlags.h"
#include "storage/admin/RebuildTagIndexProcessor.h"

namespace nebula {
namespace storage {

void RebuildTagIndexProcessor::process(const cpp2::RebuildIndexRequest& req) {
    processInternal(req);
    LOG(INFO) << "RebuildTagIndexProcessor process finished";
}

StatusOr<std::shared_ptr<nebula::cpp2::IndexItem>>
RebuildTagIndexProcessor::getIndex(GraphSpaceID space, IndexID indexID) {
    return indexMan_->getTagIndex(space, indexID);
}

void RebuildTagIndexProcessor::buildIndex(GraphSpaceID space,
                                          PartitionID part,
                                          nebula::cpp2::SchemaID schemaID,
                                          IndexID indexID,
                                          kvstore::KVIterator* iter,
                                          const std::vector<nebula::cpp2::ColumnDef>& cols) {
    auto tagID = schemaID.get_tag_id();
    std::vector<kvstore::KV> data;
    data.reserve(FLAGS_rebuild_index_batch_num);
    int32_t batchNum = 0;
    VertexID currentVertex = -1;
    while (iter && iter->valid()) {
        if (batchNum >= FLAGS_rebuild_index_batch_num) {
            auto result = doSyncPut(space, part, std::move(data));
            if (result != kvstore::ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Write Part " << part << " Index Failed";
                this->pushResultCode(to(result), part);
                onFinished();
                return;
            }

            data.reserve(FLAGS_rebuild_index_batch_num);
            batchNum = 0;
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
            iter->next();
            continue;
        } else {
            currentVertex = vertex;
        }

        auto reader = RowReader::getTagPropReader(schemaMan_,
                                                  std::move(val),
                                                  space,
                                                  tagID);
        if (reader == nullptr) {
            iter->next();
            continue;
        }
        auto valuesRet = collectIndexValues(reader.get(), cols);
        auto indexKey = NebulaKeyUtils::vertexIndexKey(part,
                                                       indexID,
                                                       vertex,
                                                       valuesRet.value());
        data.emplace_back(std::move(indexKey), "");
        batchNum += 1;
        iter->next();
    }

    auto result = doSyncPut(space, part, std::move(data));
    if (result != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Write Part " << part << " Index Failed";
        this->pushResultCode(to(result), part);
    }
}

}  // namespace storage
}  // namespace nebula

