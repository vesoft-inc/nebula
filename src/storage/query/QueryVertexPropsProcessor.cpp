/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/query/QueryVertexPropsProcessor.h"
#include <algorithm>
#include "time/Duration.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"

namespace nebula {
namespace storage {

void QueryVertexPropsProcessor::process(const cpp2::VertexPropRequest& vertexReq) {
    spaceId_ = vertexReq.get_space_id();
    auto colSize = vertexReq.get_return_columns().size();
    if (colSize > 0) {
        cpp2::GetNeighborsRequest req;
        req.set_space_id(spaceId_);
        req.set_parts(std::move(vertexReq.get_parts()));
        decltype(req.return_columns) tmpColumns;
        tmpColumns.reserve(colSize);
        for (auto& col : vertexReq.get_return_columns()) {
            tmpColumns.emplace_back(std::move(col));
        }
        req.set_return_columns(std::move(tmpColumns));
        this->onlyVertexProps_ = true;
        QueryBoundProcessor::process(req);
    } else {
        std::vector<cpp2::VertexData> vertices;
        for (auto& part : vertexReq.get_parts()) {
            auto partId = part.first;
            for (auto& vId : part.second) {
                cpp2::VertexData vResp;
                vResp.set_vertex_id(vId);
                std::vector<cpp2::TagData> td;
                auto ret = collectVertexProps(partId, vId, td);
                if (ret != kvstore::ResultCode::ERR_KEY_NOT_FOUND
                        && ret != kvstore::ResultCode::SUCCEEDED) {
                    if (ret == kvstore::ResultCode::ERR_LEADER_CHANGED) {
                        this->handleLeaderChanged(spaceId_, partId);
                    } else {
                        this->pushResultCode(this->to(ret), partId);
                    }
                    continue;
                }
                VLOG(3) << "Vid: " << vId << " found tag size: " << td.size();
                vResp.set_tag_data(std::move(td));
                vertices.emplace_back(std::move(vResp));
            }
        }
        VLOG(3) << "Seek vertices num: " << vertices.size();
        resp_.set_vertices(std::move(vertices));
        onFinished();
    }
}

kvstore::ResultCode QueryVertexPropsProcessor::collectVertexProps(
                            PartitionID partId,
                            VertexID vId,
                            std::vector<cpp2::TagData> &tds) {
    auto prefix = NebulaKeyUtils::vertexPrefix(partId, vId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = this->kvstore_->prefix(spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        return ret;
    }

    bool missedKey = true;
    std::unordered_set<TagID> tagIds;
    for (; iter && iter->valid(); iter->next()) {
        auto key = iter->key();
        auto val = iter->val();
        if (!NebulaKeyUtils::isVertex(key)) {
            continue;
        }
        missedKey = false;
        auto ver = RowReader::getSchemaVer(val);
        if (ver < 0) {
            LOG(ERROR) << "Found schema version negative " << ver;
            continue;
        }
        auto tagId = NebulaKeyUtils::getTagId(key);
        auto result = tagIds.emplace(tagId);
        if (!result.second) {
            // Already found the latest version.
            continue;
        }
        VLOG(3) << "Found tag " << tagId << " for vId" << vId;

        auto schema = this->schemaMan_->getTagSchema(spaceId_, tagId);
        auto reader = RowReader::getTagPropReader(this->schemaMan_, val, spaceId_, tagId);
        // Check if ttl data expired
        auto retTTL = getTagTTLInfo(tagId);
        if (retTTL.has_value() && checkDataExpiredForTTL(schema.get(),
                                                         reader.get(),
                                                         retTTL.value().first,
                                                         retTTL.value().second)) {
            continue;
        }
        auto valStr = val.str();
        if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
            vertexCache_->insert(std::make_pair(vId, tagId),
                                 valStr, partId);
            VLOG(3) << "Insert cache for vId " << vId << ", tagId " << tagId;
        }
        cpp2::TagData td;
        td.set_tag_id(tagId);
        td.set_data(std::move(valStr));
        tds.emplace_back(std::move(td));
    }
    if (missedKey) {
        VLOG(3) << "Missed partId " << partId << ", vId " << vId;
        return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
    }
    return kvstore::ResultCode::SUCCEEDED;
}


}  // namespace storage
}  // namespace nebula
