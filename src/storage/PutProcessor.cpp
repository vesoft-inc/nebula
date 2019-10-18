/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/PutProcessor.h"

namespace nebula {
namespace storage {

void PutProcessor::process(const cpp2::PutRequest& req) {
    LOG(INFO) << "PutProcessor running";
    CHECK_NOTNULL(kvstore_);
    space_ = req.get_space_id();
    LOG(INFO) << "Put Space " << space_;
    const auto& parts = req.get_parts();
    callingNum_ = parts.size();

    std::vector<folly::Future<PartCode>> results;
    for (auto& part : parts) {
        std::vector<kvstore::KV> data;
        for (auto& pair : part.second) {
            LOG(INFO) << "Put Run " << pair.key;
            data.emplace_back(std::move(pair.key), std::move(pair.value));
        }
        results.emplace_back(asyncProcess(part.first, data));
    }

    /*
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
    */

    /*
    std::for_each(pairs.begin(), pairs.end(), [&](auto& value) {
        auto part = value.first;
        std::vector<kvstore::KV> data;
        for (auto& pair : value.second) {
            data.emplace_back(pair.key, pair.value);
        }
        doPut(space, part, std::move(data));
    });
    */
}

folly::Future<PartCode>
PutProcessor::asyncProcess(PartitionID part, const std::vector<kvstore::KV>& keyValues) {
    folly::Promise<PartCode> promise;
    auto future = promise.getFuture();
    executor_->add([this, p = std::move(promise), part, keyValues] () mutable {
        this->kvstore_->asyncMultiPut(space_, part, keyValues,
                                      [part, &p] (kvstore::ResultCode code) {
            LOG(INFO) << "Part " << part << " code " << code;
            p.setValue(std::make_pair(part, code));
        });
    });
    return future;
}

}  // namespace storage
}  // namespace nebula
