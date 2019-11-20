/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/ScanProcessor.h"

namespace nebula {
namespace storage {

void ScanProcessor::process(const cpp2::ScanRequest& req) {
    CHECK_NOTNULL(kvstore_);
    space_ = req.get_space_id();
    std::vector<folly::Future<PartitionCode>> results;
    for (auto& part : req.get_parts()) {
        results.emplace_back(asyncProcess(part.first, part.second.start, part.second.end));
    }

    folly::collectAll(results).via(executor_)
                              .then([&] (const TryPartitionCodes& tries) mutable {
        for (const auto& t : tries) {
            auto ret = t.value();
            auto part = std::get<0>(ret);
            auto resultCode = std::get<1>(ret);
            this->pushResultCode(this->to(resultCode), part);
        }
        resp_.set_values(std::move(pairs_));
        this->onFinished();
    });
}

folly::Future<PartitionCode>
ScanProcessor::asyncProcess(PartitionID part, std::string start, std::string end) {
    folly::Promise<PartitionCode> promise;
    auto future = promise.getFuture();

    auto encodedStart = NebulaKeyUtils::kvKey(part, start);
    auto encodedEnd   = NebulaKeyUtils::kvKey(part, end);
    executor_->add([this, p = std::move(promise), part, encodedStart, encodedEnd] () mutable {
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = this->kvstore_->range(space_, part, encodedStart, encodedEnd, &iter);
        if (ret == kvstore::ResultCode::SUCCEEDED) {
            std::lock_guard<std::mutex> lg(this->lock_);
            while (iter->valid()) {
                pairs_.emplace(iter->key(), iter->val());
                iter->next();
            }
        }
        p.setValue(std::make_pair(part, ret));
    });
    return future;
}

}  // namespace storage
}  // namespace nebula
