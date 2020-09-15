/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/StorageFlags.h"
#include "storage/admin/RebuildTagIndexTask.h"
#include "utils/IndexKeyUtils.h"

namespace nebula {
namespace storage {

StatusOr<IndexItems>
RebuildTagIndexTask::getIndexes(GraphSpaceID space) {
    return env_->indexMan_->getTagIndexes(space);
}


kvstore::ResultCode
RebuildTagIndexTask::buildIndexGlobal(GraphSpaceID space,
                                      PartitionID part,
                                      IndexID indexID,
                                      const IndexItems& items) {
    if (canceled_) {
        LOG(ERROR) << "Rebuild Tag Index is Canceled";
        return kvstore::ResultCode::SUCCEEDED;
    }

    auto vidSizeRet = env_->schemaMan_->getSpaceVidLen(space);
    if (!vidSizeRet.ok()) {
        LOG(ERROR) << "Get VID Size Failed";
        return kvstore::ResultCode::ERR_IO_ERROR;
    }

    auto vidSize = vidSizeRet.value();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto prefix = NebulaKeyUtils::partPrefix(part);
    auto ret = env_->kvstore_->prefix(space, part, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Processing Part " << part << " Failed";
        return ret;
    }

    VertexID currentVertex = "";
    std::vector<kvstore::KV> data;
    data.reserve(FLAGS_rebuild_index_batch_num);
    std::unique_ptr<RowReader> reader;
    while (iter && iter->valid()) {
        if (canceled_) {
            LOG(ERROR) << "Rebuild Tag Index is Canceled";
            return kvstore::ResultCode::SUCCEEDED;
        }

        if (static_cast<int32_t>(data.size()) == FLAGS_rebuild_index_batch_num) {
            auto result = writeData(space, part, data);
            if (result != kvstore::ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Write Part " << part << " Index Failed";
                return result;
            }

            data.clear();
        }

        auto key = iter->key();
        auto val = iter->val();
        if (!NebulaKeyUtils::isVertex(vidSize, key)) {
            iter->next();
            continue;
        }

        auto tagID = NebulaKeyUtils::getTagId(vidSize, key);
        auto indexed = [tagID](const std::shared_ptr<meta::cpp2::IndexItem> item) {
            return item->get_schema_id().get_tag_id() == tagID;
        };

        // Check this record is indexed.
        // If not built any index will skip to the next one.
        if (!std::any_of(items.begin(), items.end(), indexed)) {
            VLOG(3) << "This record is not built index.";
            iter->next();
            continue;
        }

        auto vertex = NebulaKeyUtils::getVertexId(vidSize, key);
        VLOG(3) << "Tag ID " << tagID << " Vertex ID " << vertex;
        if (currentVertex == vertex) {
            iter->next();
            continue;
        } else {
            currentVertex = vertex.data();
        }

        if (!reader) {
            reader = RowReader::getTagPropReader(env_->schemaMan_, space, tagID, val);
            if (reader == nullptr) {
                iter->next();
                continue;
            }
        } else {
            if (!reader->resetTagPropReader(env_->schemaMan_, space, tagID, val)) {
                iter->next();
                continue;
            }
        }

        for (auto& item : items) {
            if (item->get_schema_id().get_tag_id() != tagID) {
                continue;
            }
            std::vector<Value::Type> colsType;
            auto valuesRet = IndexKeyUtils::collectIndexValues(reader.get(),
                                                               item->get_fields(),
                                                               colsType);
            auto indexKey = IndexKeyUtils::vertexIndexKey(vidSize,
                                                          part,
                                                          indexID,
                                                          vertex.data(),
                                                          valuesRet.value(),
                                                          std::move(colsType));
            data.emplace_back(std::move(indexKey), "");
        }
        iter->next();
    }

    auto result = writeData(space, part, std::move(data));
    if (result != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Write Part " << part << " Index Failed";
        return kvstore::ResultCode::ERR_IO_ERROR;
    }
    return kvstore::ResultCode::SUCCEEDED;
}

}  // namespace storage
}  // namespace nebula
