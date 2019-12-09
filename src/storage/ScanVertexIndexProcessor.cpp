/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/ScanVertexIndexProcessor.h"

namespace nebula {
namespace storage {

void ScanVertexIndexProcessor::process(const cpp2::IndexScanRequest& req) {
    spaceId_ = req.get_space_id();
    index_ = req.get_index();
    hints_ = req.get_hints();
    DCHECK_GT(hints_.size() , 0);
    isRangeScan_ = hints_[0].get_is_range();
    auto indexId = index_.index_id;
    auto tagId = index_.schema;

    auto hintRet = buildIndexHint();
    if (hintRet != cpp2::ErrorCode::SUCCEEDED) {
        for (auto& p : req.get_parts()) {
            this->pushResultCode(hintRet, p);
        }
        this->onFinished();
        return;
    }

    auto schemaRet = createResultSchema(false, tagId, req.get_return_columns());
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
        results.emplace_back(asyncProcess(part, indexId, tagId));
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
                resp_.set_rows(vertexRows_);
                this->onFinished();
            });
}

folly::Future<std::pair<PartitionID, kvstore::ResultCode>>
ScanVertexIndexProcessor::asyncProcess(PartitionID part,
                                       IndexID indexId,
                                       TagID tagId) {
    folly::Promise<std::pair<PartitionID, kvstore::ResultCode>> promise;
    auto future = promise.getFuture();
    executor_->add([this, p = std::move(promise), part, indexId, tagId] () mutable {
        kvstore::ResultCode ret = processVertices(part, indexId, tagId);
        p.setValue(std::make_pair(part, ret));
    });
    return future;
}

kvstore::ResultCode
ScanVertexIndexProcessor::processVertices(PartitionID part, IndexID indexId, TagID tagId) {
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
                if (!checkDataValidity(false, key)) {
                    continue;
                }
                cpp2::VertexIndexData data;
                auto rowRet = getVertexRow(part, tagId, key, &data);
                if (rowRet == kvstore::SUCCEEDED) {
                    std::lock_guard<std::mutex> lg(lock_);
                    vertexRows_.emplace_back(std::move(data));
                    ++cnt;
                }
            }
        }
        return ret;
    } else {
        std::unique_ptr<kvstore::KVIterator> iter;
        auto pre = prefix + prefix_;
        auto ret = kvstore_->prefix(spaceId_,
                                    part,
                                    pre,
                                    &iter);
        if (ret == kvstore::SUCCEEDED) {
            int cnt = 0;
            for (; iter->valid() && cnt < FLAGS_max_row_returned_per_index_scan; iter->next()) {
                auto key = iter->key();
                if (!checkDataValidity(false, key)) {
                    continue;
                }
                cpp2::VertexIndexData data;
                auto rowRet = getVertexRow(part, tagId, key, &data);
                if (rowRet == kvstore::SUCCEEDED) {
                    std::lock_guard<std::mutex> lg(lock_);
                    vertexRows_.emplace_back(std::move(data));
                    ++cnt;
                }
            }
        }
        return ret;
    }
}

}  // namespace storage
}  // namespace nebula

