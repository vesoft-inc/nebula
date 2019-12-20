/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/kv/PutProcessor.h"

namespace nebula {
namespace storage {

void PutProcessor::process(const cpp2::PutRequest& req) {
    CHECK_NOTNULL(kvstore_);
    space_ = req.get_space_id();
    const auto& parts = req.get_parts();
    callingNum_ = parts.size();

    std::vector<folly::Future<PartitionCode>> results;
    for (auto& part : parts) {
        std::vector<kvstore::KV> data;
        for (auto& pair : part.second) {
            data.emplace_back(std::move(NebulaKeyUtils::generalKey(part.first, pair.key)),
                              std::move(pair.value));
        }
        results.push_back(asyncProcess(part.first, std::move(data)));
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
PutProcessor::asyncProcess(PartitionID part, std::vector<kvstore::KV> keyValues) {
    folly::Promise<PartitionCode> promise;
    auto future = promise.getFuture();
    executor_->add([this, pro = std::move(promise), part, keyValues] () mutable {
        this->kvstore_->asyncMultiPut(space_, part, keyValues, [part, p = std::move(pro)]
                                      (kvstore::ResultCode code) mutable {
            p.setValue(std::make_pair(part, code));
        });
    });
    return future;
}

}  // namespace storage
}  // namespace nebula
