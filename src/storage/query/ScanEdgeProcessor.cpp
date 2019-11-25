/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "storage/query/ScanEdgeProcessor.h"
#include "base/NebulaKeyUtils.h"
#include <algorithm>
#include <limits>
#include "time/WallClock.h"
#include "kvstore/RocksEngine.h"

DEFINE_int32(max_scan_block_size, 4 * 1024 * 1024, "Max size of a respsonse block");

namespace nebula {
namespace storage {

void ScanEdgeProcessor::process(const cpp2::ScanEdgeRequest& req) {
    auto spaceId = req.get_space_id();
    auto partId = req.get_part_id();
    std::string start;
    std::string prefix = NebulaKeyUtils::prefix(partId);

    if (req.get_cursor() == nullptr || req.get_cursor()->empty()) {
        if (req.get_vertex_id() != nullptr) {
            auto vId = *req.get_vertex_id();
            start = NebulaKeyUtils::edgePrefix(partId, vId);
        } else {
            start = NebulaKeyUtils::prefix(partId);
        }
    } else {
        start = *req.get_cursor();
    }

    if (req.get_vertex_id() != nullptr) {
        auto vId = *req.get_vertex_id();
        prefix = NebulaKeyUtils::edgePrefix(partId, vId);
    }

    std::unique_ptr<kvstore::KVIterator> iter;
    auto kvRet = doRangeWithPrefix(spaceId, partId, start, prefix, &iter);
    if (kvRet != kvstore::ResultCode::SUCCEEDED) {
        pushResultCode(to(kvRet), partId);
        onFinished();
        return;
    }

    std::unordered_map<EdgeType, nebula::cpp2::Schema> edgeSchema;
    std::vector<cpp2::ScanEdge> edgeData;
    int32_t rowCount = 0;
    int32_t rowLimit = req.get_row_limit();
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
        if (ts < startTime || ts >= endTime) {
            continue;
        }

        // schema
        if (edgeSchema.find(edgeType) == edgeSchema.end()) {
            // need to handle schema change
            auto provider = schemaMan_->getEdgeSchema(spaceId, edgeType);
            if (provider != nullptr) {
                edgeSchema.emplace(edgeType, std::move(provider->toSchema()));
            } else {
                continue;
            }
        }

        auto value = iter->val();
        cpp2::ScanEdge data;
        data.set_type(edgeType);
        data.set_key(key.str());
        data.set_value(value.str());
        edgeData.emplace_back(std::move(data));
        rowCount++;
        blockSize += key.size() + value.size();
    }

    resp_.set_edge_schema(std::move(edgeSchema));
    resp_.set_edge_data(std::move(edgeData));
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
