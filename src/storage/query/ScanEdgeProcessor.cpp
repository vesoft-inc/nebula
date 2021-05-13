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

ProcessorCounters kScanEdgeCounters;

void ScanEdgeProcessor::process(const cpp2::ScanEdgeRequest& req) {
    if (executor_ != nullptr) {
        executor_->add([req, this] () {
            this->doProcess(req);
        });
    } else {
        doProcess(req);
    }
}

void ScanEdgeProcessor::doProcess(const cpp2::ScanEdgeRequest& req) {
    spaceId_ = req.get_space_id();
    partId_ = req.get_part_id();

    auto retCode = getSpaceVidLen(spaceId_);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        pushResultCode(retCode, partId_);
        onFinished();
        return;
    }

    retCode = checkAndBuildContexts(req);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        pushResultCode(retCode, partId_);
        onFinished();
        return;
    }

    std::string start;
    std::string prefix = NebulaKeyUtils::edgePrefix(partId_);
    if (req.get_cursor() == nullptr || req.get_cursor()->empty()) {
        start = prefix;
    } else {
        start = *req.get_cursor();
    }

    std::unique_ptr<kvstore::KVIterator> iter;
    auto kvRet = env_->kvstore_->rangeWithPrefix(
        spaceId_, partId_, start, prefix, &iter, req.get_enable_read_from_follower());
    if (kvRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
        handleErrorCode(kvRet, spaceId_, partId_);
        onFinished();
        return;
    }

    auto rowLimit = req.get_limit();
    RowReaderWrapper reader;

    for (int64_t rowCount = 0; iter->valid() && rowCount < rowLimit; iter->next()) {
        auto key = iter->key();
        if (!NebulaKeyUtils::isEdge(spaceVidLen_, key)) {
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
        auto idx = edgeIter->second;
        auto props = &(edgeContext_.propContexts_[idx].second);
        if (!QueryUtils::collectEdgeProps(key, spaceVidLen_, isIntId_,
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

nebula::cpp2::ErrorCode
ScanEdgeProcessor::checkAndBuildContexts(const cpp2::ScanEdgeRequest& req) {
    auto ret = getSpaceEdgeSchema();
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }

    std::vector<cpp2::EdgeProp> returnProps = {*req.return_columns_ref()};
    ret = handleEdgeProps(returnProps);
    buildEdgeColName(returnProps);
    return ret;
}

void ScanEdgeProcessor::buildEdgeColName(const std::vector<cpp2::EdgeProp>& edgeProps) {
    for (const auto& edgeProp : edgeProps) {
        auto edgeType = edgeProp.get_type();
        auto edgeName = edgeContext_.edgeNames_[edgeType];
        for (const auto& prop : *edgeProp.props_ref()) {
            resultDataSet_.colNames.emplace_back(edgeName + "." + prop);
        }
    }
}

void ScanEdgeProcessor::onProcessFinished() {
    resp_.set_edge_data(std::move(resultDataSet_));
}

}  // namespace storage
}  // namespace nebula
