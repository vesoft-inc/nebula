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
    CHECK_NOTNULL(kvstore_);
    auto space = req.get_space_id();
    auto parts = req.get_parts();
    auto indexID = req.get_index_id();
    auto isOffline = req.get_is_offline();
    auto itemRet = indexMan_->getTagIndex(space, indexID);
    if (!itemRet.ok()) {
        cpp2::ResultCode thriftRet;
        thriftRet.set_code(cpp2::ErrorCode::E_INDEX_NOT_FOUND);
        codes_.emplace_back(thriftRet);
        onFinished();
        return;
    }

    auto item = itemRet.value();
    auto tagID = item->get_schema_id().get_tag_id();
    LOG(INFO) << "Rebuild Tag Index Space " << space << " Tag ID " << tagID
              << " Tag Index " << indexID;
    for (PartitionID part : parts) {
        if (isOffline) {
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
                auto values = collectIndexValues(reader.get(), item->get_fields());
                if (!values.ok()) {
                    continue;
                }
                auto indexKey = NebulaKeyUtils::vertexIndexKey(part, indexID,
                                                               vertex, values.value());
                data.emplace_back(std::move(indexKey), "");
                batchNum += 1;
                iter->next();
            }

            auto result = doSyncPut(space, part, std::move(data));
            if (result != kvstore::ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Write Part " << part << " Index Failed";
                this->pushResultCode(to(result), part);
            }
        } else {
            // TODO darion Support online rebuild index
        }
    }
    onFinished();
}

}  // namespace storage
}  // namespace nebula

