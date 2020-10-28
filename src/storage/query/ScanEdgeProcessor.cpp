/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/query/ScanEdgeProcessor.h"
#include "utils/NebulaKeyUtils.h"
#include "storage/StorageFlags.h"
#include "storage/exec/QueryUtils.h"

namespace nebula {
namespace storage {

void ScanEdgeProcessor::process(const cpp2::ScanEdgeRequest& req) {
    spaceId_ = req.get_space_id();
    partId_ = req.get_part_id();
    returnNoProps_ = req.get_no_columns();

    auto retCode = getSpaceVidLen(spaceId_);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        pushResultCode(retCode, partId_);
        onFinished();
        return;
    }

    retCode = checkAndBuildContexts(req);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        pushResultCode(retCode, partId_);
        onFinished();
        return;
    }

    std::string start;
    std::string prefix = NebulaKeyUtils::partPrefix(partId_);
    if (req.get_cursor() == nullptr || req.get_cursor()->empty()) {
        start = prefix;
    } else {
        start = *req.get_cursor();
    }

    std::unique_ptr<kvstore::KVIterator> iter;
    auto kvRet = env_->kvstore_->rangeWithPrefix(spaceId_, partId_, start, prefix, &iter);
    if (kvRet != kvstore::ResultCode::SUCCEEDED) {
        pushResultCode(to(kvRet), partId_);
        onFinished();
        return;
    }

    int32_t rowLimit = req.get_limit();
    int64_t startTime = 0, endTime = std::numeric_limits<int64_t>::max();
    if (req.__isset.start_time) {
        startTime = *req.get_start_time();
    }
    if (req.__isset.end_time) {
        endTime = *req.get_end_time();
    }
    RowReaderWrapper reader;

    for (int32_t rowCount = 0; iter->valid() && rowCount < rowLimit; iter->next()) {
        auto key = iter->key();
        if (!NebulaKeyUtils::isEdge(spaceVidLen_, key)) {
            continue;
        }

        // only return data within time range [start, end)
        auto version = folly::Endian::big(NebulaKeyUtils::getVersion(spaceVidLen_, key));
        int64_t ts = std::numeric_limits<int64_t>::max() - version;
        if (FLAGS_enable_multi_versions && (ts < startTime || ts >= endTime)) {
            continue;
        }

        auto edgeType = NebulaKeyUtils::getEdgeType(spaceVidLen_, key);
        auto edgeIter = edgeContext_.indexMap_.find(edgeType);
        if (edgeIter == edgeContext_.indexMap_.end()) {
            continue;
        }

        auto val = iter->val();
        auto schemaIter = edgeContext_.schemas_.find(std::abs(edgeType));
        CHECK(schemaIter != edgeContext_.schemas_.end());
        reader.reset(schemaIter->second, val);
        if (!reader) {
            continue;
        }

        nebula::List list;
        auto srcId = NebulaKeyUtils::getSrcId(spaceVidLen_, key);
        auto rank = NebulaKeyUtils::getRank(spaceVidLen_, key);
        auto dstId = NebulaKeyUtils::getDstId(spaceVidLen_, key);
        auto src = srcId.subpiece(0, srcId.find_first_of('\0'));
        auto dst = dstId.subpiece(0, dstId.find_first_of('\0'));
        list.emplace_back(std::move(src));
        list.emplace_back(edgeType);
        list.emplace_back(rank);
        list.emplace_back(std::move(dst));

        if (!returnNoProps_) {
            auto idx = edgeIter->second;
            auto props = &(edgeContext_.propContexts_[idx].second);
            if (!QueryUtils::collectPropsInValue(reader.get(), props, list).ok()) {
                continue;
            }
        }
        resultDataSet_.rows.emplace_back(std::move(list));
        rowCount++;
    }

    if (iter->valid()) {
        resp_.set_has_next(true);
        resp_.set_next_cursor(iter->key().str());
    } else {
        resp_.set_has_next(false);
    }
    onProcessFinished();
    onFinished();
}

cpp2::ErrorCode ScanEdgeProcessor::checkAndBuildContexts(const cpp2::ScanEdgeRequest& req) {
    auto ret = getSpaceEdgeSchema();
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }

    auto returnProps = std::move(req.return_columns);
    return handleEdgeProps(returnProps, returnNoProps_);
}

void ScanEdgeProcessor::onProcessFinished() {
    resp_.set_edge_data(std::move(resultDataSet_));
}

}  // namespace storage
}  // namespace nebula
