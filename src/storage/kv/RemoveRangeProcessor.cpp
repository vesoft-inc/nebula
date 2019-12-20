/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/kv/RemoveRangeProcessor.h"
#include "base/NebulaKeyUtils.h"

namespace nebula {
namespace storage {

void RemoveRangeProcessor::process(const cpp2::RemoveRangeRequest& req) {
    CHECK_NOTNULL(kvstore_);
    space_ = req.get_space_id();
    std::vector<folly::Future<PartitionCode>> results;
    for (auto& part : req.get_parts()) {
        auto start = part.second.start;
        auto end   = part.second.end;
        results.emplace_back(asyncProcess(part.first, start, end));
    }

    folly::collectAll(results).via(executor_)
                              .then([&] (const TryPartitionCodes& tries) mutable {
        for (const auto& t : tries) {
            auto ret = t.value();
            auto part = std::get<0>(ret);
            auto resultCode = std::get<1>(ret);
            this->pushResultCode(this->to(resultCode), part);
        }
        this->onFinished();
   });
}

folly::Future<PartitionCode>
RemoveRangeProcessor::asyncProcess(PartitionID part, std::string start, std::string end) {
    folly::Promise<PartitionCode> promise;
    auto future = promise.getFuture();

    auto startKey = NebulaKeyUtils::generalKey(part, start);
    auto endKey   = NebulaKeyUtils::generalKey(part, end);
    executor_->add([this, pro = std::move(promise), part, startKey = std::move(startKey),
                    endKey = std::move(endKey)] () mutable {
        this->kvstore_->asyncRemoveRange(space_, part, startKey, endKey,
                                         [part, p = std::move(pro)]
                                         (kvstore::ResultCode code) mutable {
            p.setValue(std::make_pair(part, code));
        });
    });
    return future;
}

}  // namespace storage
}  // namespace nebula
