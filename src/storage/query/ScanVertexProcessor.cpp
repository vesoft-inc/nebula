/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "storage/query/ScanVertexProcessor.h"
#include "utils/NebulaKeyUtils.h"
#include <algorithm>
#include <limits>
#include "time/WallClock.h"
#include "kvstore/RocksEngine.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"
#include "meta/NebulaSchemaProvider.h"


DECLARE_int32(max_scan_block_size);

namespace nebula {
namespace storage {

void ScanVertexProcessor::process(const cpp2::ScanVertexRequest& req) {
    spaceId_ = req.get_space_id();
    partId_ = req.get_part_id();
    returnAllColumns_ = req.get_all_columns();

    auto retCode = checkAndBuildContexts(req);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        this->pushResultCode(retCode, partId_);
        this->onFinished();
        return;
    }

    std::string start;
    std::string prefix = NebulaKeyUtils::prefix(partId_);
    if (req.get_cursor() == nullptr || req.get_cursor()->empty()) {
        start = prefix;
    } else {
        start = *req.get_cursor();
    }

    std::unique_ptr<kvstore::KVIterator> iter;
    auto kvRet = doRangeWithPrefix(spaceId_, partId_, start, prefix, &iter);
    if (kvRet != kvstore::ResultCode::SUCCEEDED) {
        pushResultCode(to(kvRet), partId_);
        onFinished();
        return;
    }

    std::vector<cpp2::ScanVertex> vertexData;
    int32_t rowCount = 0;
    int32_t rowLimit = req.get_limit();
    int64_t startTime = req.get_start_time(), endTime = req.get_end_time();
    int32_t blockSize = 0;

    for (; iter->valid() && rowCount < rowLimit && blockSize < FLAGS_max_scan_block_size;
         iter->next()) {
        auto key = iter->key();
        if (!NebulaKeyUtils::isVertex(key)) {
            continue;
        }

        // only return data within time range [start, end)
        TagVersion version = folly::Endian::big(NebulaKeyUtils::getVersion(key));
        int64_t ts = std::numeric_limits<int64_t>::max() - version;
        if (ts < startTime || ts >= endTime) {
            continue;
        }

        TagID tagId = NebulaKeyUtils::getTagId(key);
        auto ctxIter = tagContexts_.find(tagId);
        if (ctxIter == tagContexts_.end()) {
            continue;
        }

        VertexID vId = NebulaKeyUtils::getVertexId(key);
        cpp2::ScanVertex data;
        data.set_vertexId(vId);
        data.set_tagId(tagId);
        auto value = iter->val();
        if (returnAllColumns_) {
            // return all columns
            data.set_value(value.str());
        } else if (!ctxIter->second.empty()) {
            // only return specified columns
            auto reader = RowReader::getTagPropReader(schemaMan_, value, spaceId_, tagId);
            if (reader == nullptr) {
                continue;
            }
            RowWriter writer;
            PropsCollector collector(&writer);
            auto& props = ctxIter->second;
            collectProps(reader.get(), props, &collector);
            data.set_value(writer.encode());
        }

        vertexData.emplace_back(std::move(data));
        rowCount++;
        blockSize += key.size() + value.size();
    }

    resp_.set_vertex_schema(std::move(tagSchema_));
    resp_.set_vertex_data(std::move(vertexData));
    if (iter->valid()) {
        resp_.set_has_next(true);
        resp_.set_next_cursor(iter->key().str());
    } else {
        resp_.set_has_next(false);
    }
    onFinished();
}

cpp2::ErrorCode ScanVertexProcessor::checkAndBuildContexts(const cpp2::ScanVertexRequest& req) {
    for (const auto& tagIter : req.get_return_columns()) {
        int32_t index = 0;
        TagID tagId = tagIter.first;
        std::vector<PropContext> propContexts;
        auto schema = this->schemaMan_->getTagSchema(spaceId_, tagId);
        if (!schema) {
            return cpp2::ErrorCode::E_TAG_NOT_FOUND;
        }

        if (returnAllColumns_) {
            // return all columns
            tagContexts_.emplace(tagId, std::move(propContexts));
            tagSchema_.emplace(tagId, schema->toSchema());
            continue;
        } else if (tagIter.second.empty()) {
            // return none columns
            tagContexts_.emplace(tagId, std::move(propContexts));
            continue;
        }

        // return specified columns
        meta::NebulaSchemaProvider retSchema(schema->getVersion());
        // return specifid columns
        for (const auto& col : tagIter.second) {
            PropContext prop;
            switch (col.owner) {
                case cpp2::PropOwner::SOURCE: {
                    auto ftype = schema->getFieldType(col.name);
                    if (UNLIKELY(ftype == CommonConstants::kInvalidValueType())) {
                        return cpp2::ErrorCode::E_IMPROPER_DATA_TYPE;
                    }
                    prop.type_ = ftype;
                    retSchema.addField(col.name, std::move(ftype));

                    prop.retIndex_ = index++;
                    prop.prop_ = std::move(col);
                    prop.returned_ = true;
                    propContexts.emplace_back(std::move(prop));
                    break;
                }
                default: {
                    continue;
                }
            }
        }
        tagContexts_.emplace(tagId, std::move(propContexts));
        tagSchema_.emplace(tagId, retSchema.toSchema());
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace storage
}  // namespace nebula
