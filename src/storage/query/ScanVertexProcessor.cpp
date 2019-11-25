/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "storage/query/ScanVertexProcessor.h"
#include "base/NebulaKeyUtils.h"
#include <algorithm>
#include <limits>
#include "time/WallClock.h"
#include "kvstore/RocksEngine.h"

DECLARE_int32(max_scan_block_size);

namespace nebula {
namespace storage {

void ScanVertexProcessor::process(const cpp2::ScanVertexRequest& req) {
    auto spaceId = req.get_space_id();
    auto partId = req.get_part_id();
    std::string start;
    std::string prefix = NebulaKeyUtils::prefix(partId);

    if (req.get_cursor() == nullptr || req.get_cursor()->empty()) {
        if (req.get_vertex_id() != nullptr) {
            auto vId = *req.get_vertex_id();
            start = NebulaKeyUtils::vertexPrefix(partId, vId);
        } else {
            start = NebulaKeyUtils::prefix(partId);
        }
    } else {
        start = *req.get_cursor();
    }

    if (req.get_vertex_id() != nullptr) {
        auto vId = *req.get_vertex_id();
        prefix = NebulaKeyUtils::vertexPrefix(partId, vId);
    }

    std::unique_ptr<kvstore::KVIterator> iter;
    auto kvRet = doRangeWithPrefix(spaceId, partId, start, prefix, &iter);
    if (kvRet != kvstore::ResultCode::SUCCEEDED) {
        pushResultCode(to(kvRet), partId);
        onFinished();
        return;
    }

    std::unordered_map<TagID, nebula::cpp2::Schema> vertexSchema;
    std::vector<cpp2::ScanVertex> vertexData;
    int32_t rowCount = 0;
    int32_t rowLimit = req.get_row_limit();
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

        // schema
        if (vertexSchema.find(tagId) == vertexSchema.end()) {
            // need to handle schema change
            auto provider = schemaMan_->getTagSchema(spaceId, tagId);
            if (provider != nullptr) {
                vertexSchema.emplace(tagId, std::move(provider->toSchema()));
            } else {
                continue;
            }
        }

        auto value = iter->val();
        cpp2::ScanVertex data;
        data.set_tagId(tagId);
        data.set_key(key.str());
        data.set_value(value.str());
        vertexData.emplace_back(std::move(data));
        rowCount++;
        blockSize += key.size() + value.size();
    }

    resp_.set_vertex_schema(std::move(vertexSchema));
    resp_.set_vertex_data(std::move(vertexData));
    if (iter->valid()) {
        resp_.set_has_next(true);
        resp_.set_next_cursor(std::move(iter->key().str()));
    } else {
        resp_.set_has_next(false);
    }
    onFinished();
}

}  // namespace storage
}  // namespace nebula
