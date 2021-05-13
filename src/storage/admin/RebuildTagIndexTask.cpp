/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/StorageFlags.h"
#include "storage/admin/RebuildTagIndexTask.h"
#include "utils/IndexKeyUtils.h"
#include "codec/RowReaderWrapper.h"

namespace nebula {
namespace storage {

StatusOr<IndexItems>
RebuildTagIndexTask::getIndexes(GraphSpaceID space) {
    return env_->indexMan_->getTagIndexes(space);
}

StatusOr<std::shared_ptr<meta::cpp2::IndexItem>>
RebuildTagIndexTask::getIndex(GraphSpaceID space, IndexID index) {
    return env_->indexMan_->getTagIndex(space, index);
}

nebula::cpp2::ErrorCode
RebuildTagIndexTask::buildIndexGlobal(GraphSpaceID space,
                                      PartitionID part,
                                      const IndexItems& items) {
    if (canceled_) {
        LOG(ERROR) << "Rebuild Tag Index is Canceled";
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    auto vidSizeRet = env_->schemaMan_->getSpaceVidLen(space);
    if (!vidSizeRet.ok()) {
        LOG(ERROR) << "Get VID Size Failed";
        return nebula::cpp2::ErrorCode::E_STORE_FAILURE;
    }

    std::unordered_set<TagID> tagIds;
    for (const auto& item : items) {
        tagIds.emplace(item->get_schema_id().get_tag_id());
    }

    auto vidSize = vidSizeRet.value();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto prefix = NebulaKeyUtils::vertexPrefix(part);
    auto ret = env_->kvstore_->prefix(space, part, prefix, &iter);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Processing Part " << part << " Failed";
        return ret;
    }

    VertexID currentVertex = "";
    std::vector<kvstore::KV> data;
    data.reserve(FLAGS_rebuild_index_batch_num);
    RowReaderWrapper reader;
    while (iter && iter->valid()) {
        if (canceled_) {
            LOG(ERROR) << "Rebuild Tag Index is Canceled";
            return nebula::cpp2::ErrorCode::SUCCEEDED;
        }

        if (static_cast<int32_t>(data.size()) == FLAGS_rebuild_index_batch_num) {
            auto result = writeData(space, part, data);
            if (result != nebula::cpp2::ErrorCode::SUCCEEDED) {
                LOG(ERROR) << "Write Part " << part << " Index Failed";
                return result;
            }

            data.clear();
        }

        auto key = iter->key();
        auto val = iter->val();

        auto tagID = NebulaKeyUtils::getTagId(vidSize, key);

        // Check whether this record contains the index of indexId
        if (tagIds.find(tagID) == tagIds.end()) {
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
            currentVertex = vertex.toString();
        }

        reader = RowReaderWrapper::getTagPropReader(env_->schemaMan_, space, tagID, val);
        if (reader == nullptr) {
            iter->next();
            continue;
        }

        auto schema = env_->schemaMan_->getTagSchema(space, tagID);
        if (!schema) {
            LOG(WARNING) << "Space " << space << ", tag " << tagID << " invalid";
            iter->next();
            continue;
        }

        auto ttlProp = CommonUtils::ttlProps(schema.get());
        if (ttlProp.first && CommonUtils::checkDataExpiredForTTL(schema.get(),
                                                                 reader.get(),
                                                                 ttlProp.second.second,
                                                                 ttlProp.second.first)) {
            VLOG(3) << "ttl expired : " << "Tag ID " << tagID << " Vertex ID " << vertex;
            iter->next();
            continue;
        }

        std::string indexVal = "";
        if (ttlProp.first) {
            auto ttlValRet = CommonUtils::ttlValue(schema.get(), reader.get());
            if (ttlValRet.ok()) {
                indexVal = IndexKeyUtils::indexVal(std::move(ttlValRet).value());
            }
        }

        for (const auto& item : items) {
            if (item->get_schema_id().get_tag_id() == tagID) {
                auto valuesRet =
                    IndexKeyUtils::collectIndexValues(reader.get(), item->get_fields());
                if (!valuesRet.ok()) {
                    LOG(WARNING) << "Collect index value failed";
                    continue;
                }
                auto indexKey = IndexKeyUtils::vertexIndexKey(vidSize,
                                                              part,
                                                              item->get_index_id(),
                                                              vertex.toString(),
                                                              std::move(valuesRet).value());
                data.emplace_back(std::move(indexKey), indexVal);
            }
        }
        iter->next();
    }

    auto result = writeData(space, part, std::move(data));
    if (result != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Write Part " << part << " Index Failed";
        return nebula::cpp2::ErrorCode::E_STORE_FAILURE;
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace storage
}  // namespace nebula
