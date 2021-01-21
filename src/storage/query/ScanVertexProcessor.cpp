/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/query/ScanVertexProcessor.h"
#include "utils/NebulaKeyUtils.h"
#include "storage/StorageFlags.h"
#include "storage/exec/QueryUtils.h"

namespace nebula {
namespace storage {

void ScanVertexProcessor::process(const cpp2::ScanVertexRequest& req) {
    spaceId_ = req.get_space_id();
    partId_ = req.get_part_id();

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
    auto kvRet = env_->kvstore_->rangeWithPrefix(
        spaceId_, partId_, start, prefix, &iter, req.get_enable_read_from_follower());
    if (kvRet != kvstore::ResultCode::SUCCEEDED) {
        handleErrorCode(kvRet, spaceId_, partId_);
        onFinished();
        return;
    }

    auto rowLimit = req.get_limit();
    int64_t startTime = 0, endTime = std::numeric_limits<int64_t>::max();
    if (req.__isset.start_time) {
        startTime = *req.get_start_time();
    }
    if (req.__isset.end_time) {
        endTime = *req.get_end_time();
    }
    RowReaderWrapper reader;

    bool onlyLatestVer = req.get_only_latest_version();
    // last valid key without version
    std::string lastValidKey;
    for (int64_t rowCount = 0; iter->valid() && rowCount < rowLimit; iter->next()) {
        auto key = iter->key();
        if (!NebulaKeyUtils::isVertex(spaceVidLen_, key)) {
            continue;
        }

        // only return data within time range [start, end)
        auto version = folly::Endian::big(NebulaKeyUtils::getVersion(spaceVidLen_, key));
        int64_t ts = std::numeric_limits<int64_t>::max() - version;
        if (FLAGS_enable_multi_versions && (ts < startTime || ts >= endTime)) {
            continue;
        }

        auto tagId = NebulaKeyUtils::getTagId(spaceVidLen_, key);
        auto tagIter = tagContext_.indexMap_.find(tagId);
        if (tagIter == tagContext_.indexMap_.end()) {
            continue;
        }

        auto val = iter->val();
        auto schemaIter = tagContext_.schemas_.find(tagId);
        CHECK(schemaIter != tagContext_.schemas_.end());
        reader.reset(schemaIter->second, val);
        if (!reader) {
            continue;
        }

        if (onlyLatestVer) {
            auto noVer = NebulaKeyUtils::keyWithNoVersion(key);
            if (noVer == lastValidKey) {
                continue;
            } else {
                lastValidKey = noVer.str();
            }
        }

        nebula::List list;
        auto idx = tagIter->second;
        auto props = &(tagContext_.propContexts_[idx].second);
        if (!QueryUtils::collectVertexProps(key, spaceVidLen_, isIntId_,
                                            reader.get(), props, list).ok()) {
            continue;
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

cpp2::ErrorCode ScanVertexProcessor::checkAndBuildContexts(const cpp2::ScanVertexRequest& req) {
    auto ret = getSpaceVertexSchema();
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }

    std::vector<cpp2::VertexProp> returnProps = {req.return_columns};
    ret = handleVertexProps(returnProps);
    buildTagColName(returnProps);
    return ret;
}

void ScanVertexProcessor::buildTagColName(const std::vector<cpp2::VertexProp>& tagProps) {
    for (const auto& tagProp : tagProps) {
        auto tagId = tagProp.tag;
        auto tagName = tagContext_.tagNames_[tagId];
        for (const auto& prop : tagProp.props) {
            resultDataSet_.colNames.emplace_back(tagName + "." + prop);
        }
    }
}

void ScanVertexProcessor::onProcessFinished() {
    resp_.set_vertex_data(std::move(resultDataSet_));
}

}  // namespace storage
}  // namespace nebula
