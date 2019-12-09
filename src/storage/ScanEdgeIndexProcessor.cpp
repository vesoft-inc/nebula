/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/ScanEdgeIndexProcessor.h"

namespace nebula {
namespace storage {

void ScanEdgeIndexProcessor::process(const cpp2::IndexScanRequest& req) {
    spaceId_ = req.get_space_id();
    index_ = req.get_index();
    hints_ = req.get_hints();
    DCHECK_GT(hints_.size() , 0);
    isRangeScan_ = hints_[0].get_is_range();
    auto indexId = index_.index_id;
    auto edgeType = index_.schema;

    auto hintRet = buildIndexHint();
    if (hintRet != cpp2::ErrorCode::SUCCEEDED) {
        for (auto& p : req.get_parts()) {
            this->pushResultCode(hintRet, p);
        }
        this->onFinished();
        return;
    }

    auto schemaRet = createResultSchema(true, edgeType, req.get_return_columns());
    if (schemaRet != cpp2::ErrorCode::SUCCEEDED) {
        for (auto& p : req.get_parts()) {
            this->pushResultCode(schemaRet, p);
        }
        this->onFinished();
        return;
    }

    for (const auto & col : index_.get_cols()) {
        if (col.get_type().get_type() == nebula::cpp2::SupportedType::STRING) {
            vlColNum_ += 1;
        }
    }

    std::vector<folly::Future<std::pair<PartitionID, kvstore::ResultCode>>> results;
    for (auto& part : req.get_parts()) {
        results.emplace_back(asyncProcess(part, indexId, edgeType));
    }

    folly::collectAll(results).via(executor_)
            .thenValue([&] (const std::vector<folly::Try<PartCode>>& tries) mutable {
                for (const auto& t : tries) {
                    auto ret = t.value();
                    auto part = std::get<0>(ret);
                    auto resultCode = std::get<1>(ret);
                    if (resultCode == kvstore::ResultCode::ERR_LEADER_CHANGED) {
                        this->handleLeaderChanged(spaceId_, part);
                    } else {
                        this->pushResultCode(this->to(resultCode), part);
                    }
                }
                decltype(resp_.schema) s;
                decltype(resp_.schema.columns) cols;
                for (auto i = 0; i < static_cast<int64_t>(schema_->getNumFields()); i++) {
                    cols.emplace_back(columnDef(schema_->getFieldName(i),
                                                schema_->getFieldType(i).get_type()));
                }
                s.set_columns(std::move(cols));
                resp_.set_schema(std::move(s));
                resp_.set_rows(edgeRows_);
                this->onFinished();
            });
}

folly::Future<std::pair<PartitionID, kvstore::ResultCode>>
ScanEdgeIndexProcessor::asyncProcess(PartitionID part,
                                     IndexID indexId,
                                     EdgeType edgeType) {
    folly::Promise<std::pair<PartitionID, kvstore::ResultCode>> promise;
    auto future = promise.getFuture();
    executor_->add([this, p = std::move(promise), part, indexId, edgeType] () mutable {
        kvstore::ResultCode ret = processEdges(part, indexId, edgeType);
        p.setValue(std::make_pair(part, ret));
    });
    return future;
}

kvstore::ResultCode
ScanEdgeIndexProcessor::processEdges(PartitionID part, IndexID indexId, EdgeType edgeType) {
    auto prefix = NebulaKeyUtils::indexPrefix(part, indexId);
    if (isRangeScan_) {
        std::unique_ptr<kvstore::KVIterator> iter;
        auto begin = prefix + range_.first;
        auto end = prefix + range_.second;
        auto ret = kvstore_->range(spaceId_,
                                   part,
                                   begin,
                                   end,
                                   &iter);
        if (ret == kvstore::SUCCEEDED) {
            int cnt = 0;
            for (; iter->valid() && cnt < FLAGS_max_row_returned_per_index_scan; iter->next()) {
                auto key = iter->key();
                if (!checkDataValidity(true, key)) {
                    continue;
                }
                cpp2::Edge data;
                auto rowRet = getEdgeRow(part, edgeType, key, &data);
                if (rowRet == kvstore::SUCCEEDED) {
                    std::lock_guard<std::mutex> lg(lock_);
                    edgeRows_.emplace_back(std::move(data));
                    ++cnt;
                }
            }
        }
        return ret;
    } else {
        std::unique_ptr<kvstore::KVIterator> iter;
        prefix += prefix_;
        auto ret = kvstore_->prefix(spaceId_,
                               part,
                               prefix,
                               &iter);
        if (ret == kvstore::SUCCEEDED) {
            int cnt = 0;
            for (; iter->valid() && cnt < FLAGS_max_row_returned_per_index_scan; iter->next()) {
                auto key = iter->key();
                if (!checkDataValidity(true, key)) {
                    continue;
                }
                cpp2::Edge data;
                auto rowRet = getEdgeRow(part, edgeType, key, &data);
                if (rowRet == kvstore::SUCCEEDED) {
                    std::lock_guard<std::mutex> lg(lock_);
                    edgeRows_.emplace_back(std::move(data));
                    ++cnt;
                }
            }
        }
        return ret;
    }
}

}  // namespace storage
}  // namespace nebula

