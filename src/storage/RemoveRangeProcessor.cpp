/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/RemoveRangeProcessor.h"
#include "base/NebulaKeyUtils.h"

namespace nebula {
namespace storage {

void RemoveRangeProcessor::process(const cpp2::RemoveRangeRequest& req) {
    CHECK_NOTNULL(kvstore_);
    space_ = req.get_space_id();
    std::vector<folly::Future<PartCode>> results;
    for (auto& part : req.get_parts()) {
        auto start = part.second.start;
        auto end   = part.second.end;
        results.emplace_back(asyncProcess(part.first, start, end));
    }

    folly::collectAll(results).via(executor_)
                              .then([&] (const std::vector<folly::Try<PartCode>>& tries) mutable {
        for (const auto& t : tries) {
            auto ret = t.value();
            auto part = std::get<0>(ret);
            auto resultCode = std::get<1>(ret);
            this->pushResultCode(this->to(resultCode), part);
        }
        this->onFinished();
   });
}

folly::Future<PartCode>
RemoveRangeProcessor::asyncProcess(PartitionID part,
                                   const std::string& start,
                                   const std::string& end) {
    folly::Promise<PartCode> promise;
    auto future = promise.getFuture();

    executor_->add([this, pro = std::move(promise), part, start, end] () mutable {
        this->kvstore_->asyncRemoveRange(space_, part, start, end, [part, p = std::move(pro)]
                                         (kvstore::ResultCode code) mutable {
            p.setValue(std::make_pair(part, code));
        });
    });
    return future;
}

}  // namespace storage
}  // namespace nebula
