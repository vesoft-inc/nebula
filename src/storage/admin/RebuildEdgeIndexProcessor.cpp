/* Copyright (c) 2020 vesoft inc. All rights reserved.
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
        if (req.get_is_offline()) {
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
            VertexID currentSrcVertex = -1;
            VertexID currentDstVertex = -1;
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
                if (reader == nullptr) {
                    iter->next();
                    continue;
                }
                auto values = collectIndexValues(reader.get(), item->get_fields());
                if (!values.ok()) {
                    continue;
                }
                auto indexKey = NebulaKeyUtils::edgeIndexKey(part, indexID, source,
                                                             ranking, destination, values.value());
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

