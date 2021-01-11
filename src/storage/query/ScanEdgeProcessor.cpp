/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "storage/query/ScanEdgeProcessor.h"
#include "utils/NebulaKeyUtils.h"
#include <algorithm>
#include <limits>
#include "time/WallClock.h"
#include "kvstore/RocksEngine.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"
#include "meta/NebulaSchemaProvider.h"
#include "storage/StorageFlags.h"

DEFINE_int32(max_scan_block_size, 4 * 1024 * 1024, "Max size of a respsonse block");

namespace nebula {
namespace storage {

void ScanEdgeProcessor::process(const cpp2::ScanEdgeRequest& req) {
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
        handleErrorCode(kvRet, spaceId_, partId_);
        onFinished();
        return;
    }

    std::vector<cpp2::ScanEdge> edgeData;
    int32_t rowCount = 0;
    int32_t rowLimit = req.get_limit();
    int64_t startTime = req.get_start_time(), endTime = req.get_end_time();
    int32_t blockSize = 0;

    for (; iter->valid() && rowCount < rowLimit && blockSize < FLAGS_max_scan_block_size;
         iter->next()) {
        auto key = iter->key();
        if (!NebulaKeyUtils::isEdge(key)) {
            continue;
        }

        EdgeType edgeType = NebulaKeyUtils::getEdgeType(key);
        if (edgeType < 0) {
            continue;
        }

        // only return data within time range [start, end)
        EdgeVersion version = folly::Endian::big(NebulaKeyUtils::getVersion(key));
        int64_t ts = std::numeric_limits<int64_t>::max() - version;
        if (FLAGS_enable_multi_versions && (ts < startTime || ts >= endTime)) {
            continue;
        }

        auto ctxIter = edgeContexts_.find(edgeType);
        if (ctxIter == edgeContexts_.end()) {
            continue;
        }

        auto srcId = NebulaKeyUtils::getSrcId(key);
        auto dstId = NebulaKeyUtils::getDstId(key);
        cpp2::ScanEdge data;
        data.set_src(srcId);
        data.set_type(edgeType);
        data.set_dst(dstId);
        auto value = iter->val();
        if (returnAllColumns_) {
            // return all columns
            data.set_value(value.str());
        } else if (!ctxIter->second.empty()) {
            // only return specified columns
            auto reader = RowReader::getEdgePropReader(schemaMan_, value, spaceId_, edgeType);
            if (reader == nullptr) {
                LOG(WARNING) << "Skip the bad format row";
                continue;
            }
            RowWriter writer;
            PropsCollector collector(&writer);
            auto& props = ctxIter->second;
            collectProps(reader.get(), props, &collector);
            data.set_value(writer.encode());
        }

        edgeData.emplace_back(std::move(data));
        rowCount++;
        blockSize += key.size() + value.size();
    }

    resp_.set_edge_schema(std::move(edgeSchema_));
    resp_.set_edge_data(std::move(edgeData));
    if (iter->valid()) {
        resp_.set_has_next(true);
        resp_.set_next_cursor(iter->key().str());
    } else {
        resp_.set_has_next(false);
    }
    onFinished();
}

cpp2::ErrorCode ScanEdgeProcessor::checkAndBuildContexts(const cpp2::ScanEdgeRequest& req) {
    for (const auto& edgeIter : req.get_return_columns()) {
        int32_t index = 0;
        EdgeType edgeType = edgeIter.first;
        std::vector<PropContext> propContexts;
        auto schema = this->schemaMan_->getEdgeSchema(spaceId_, edgeType);
        if (!schema) {
            return cpp2::ErrorCode::E_EDGE_NOT_FOUND;
        }

        if (returnAllColumns_) {
            // return all columns
            edgeContexts_.emplace(edgeType, std::move(propContexts));
            edgeSchema_.emplace(edgeType, schema->toSchema());
            continue;
        } else if (edgeIter.second.empty()) {
            // return none columns
            edgeContexts_.emplace(edgeType, std::move(propContexts));
            continue;
        }

        // return specified columns
        meta::NebulaSchemaProvider retSchema(schema->getVersion());
        // return specifid columns
        for (const auto& col : edgeIter.second) {
            PropContext prop;
            switch (col.owner) {
                case cpp2::PropOwner::EDGE: {
                    if (col.id.get_edge_type() > 0) {
                        // Only outBound have properties on edge.
                        auto ftype = schema->getFieldType(col.name);
                        if (UNLIKELY(ftype == CommonConstants::kInvalidValueType())) {
                            return cpp2::ErrorCode::E_IMPROPER_DATA_TYPE;
                        }
                        prop.type_ = ftype;
                        retSchema.addField(col.name, std::move(ftype));
                    } else {
                        VLOG(3) << "InBound has none props, skip it!";
                        break;
                    }
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
        edgeContexts_.emplace(edgeType, std::move(propContexts));
        edgeSchema_.emplace(edgeType, retSchema.toSchema());
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace storage
}  // namespace nebula
