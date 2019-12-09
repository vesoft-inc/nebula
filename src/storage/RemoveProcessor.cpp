/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/RemoveProcessor.h"
#include "base/NebulaKeyUtils.h"

namespace nebula {
namespace storage {

void RemoveProcessor::process(const cpp2::RemoveRequest& req) {
    CHECK_NOTNULL(kvstore_);
    space_ = req.get_space_id();
    std::vector<folly::Future<PartitionCode>> results;
    for (auto& part : req.get_parts()) {
        results.emplace_back(asyncProcess(part.first, part.second));
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
RemoveProcessor::asyncProcess(PartitionID part, std::vector<std::string> keys) {
    folly::Promise<PartitionCode> promise;
    auto future = promise.getFuture();

    std::vector<std::string> encodedKeys;
    encodedKeys.reserve(keys.size());
    std::transform(keys.begin(), keys.end(), std::back_inserter(encodedKeys),
                   [part](const auto& key) { return NebulaKeyUtils::generalKey(part, key); });

    executor_->add([this, pro = std::move(promise), part, encodedKeys] () mutable {
        this->kvstore_->asyncMultiRemove(space_, part, encodedKeys, [part, p = std::move(pro)]
                                         (kvstore::ResultCode code) mutable {
            p.setValue(std::make_pair(part, code));
        });
    });
    return future;
}

}  // namespace storage
}  // namespace nebula
