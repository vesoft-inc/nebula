/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/kv/GetProcessor.h"
#include "base/NebulaKeyUtils.h"

namespace nebula {
namespace storage {

void GetProcessor::process(const cpp2::GetRequest& req) {
    space_ = req.get_space_id();
    bool returnPartly = req.get_return_partly();
    std::vector<folly::Future<std::pair<PartitionID, kvstore::ResultCode>>> results;
    for (auto& part : req.get_parts()) {
        results.emplace_back(asyncProcess(part.first, std::move(part.second), returnPartly));
    }

    folly::collectAll(results).via(executor_)
        .thenValue([&] (const std::vector<folly::Try<PartCode>>& tries) mutable {
        for (const auto& t : tries) {
            auto ret = t.value();
            auto part = std::get<0>(ret);
            auto resultCode = std::get<1>(ret);
            if (resultCode == kvstore::ResultCode::ERR_LEADER_CHANGED) {
                this->handleLeaderChanged(space_, part);
            } else {
                this->pushResultCode(this->to(resultCode), part);
            }
        }

        resp_.set_values(std::move(pairs_));
        this->onFinished();
    });
}

folly::Future<std::pair<PartitionID, kvstore::ResultCode>>
GetProcessor::asyncProcess(PartitionID part, std::vector<std::string> keys, bool returnPartly) {
    folly::Promise<std::pair<PartitionID, kvstore::ResultCode>> promise;
    auto future = promise.getFuture();
    std::vector<std::string> kvKeys;
    kvKeys.reserve(keys.size());
    std::transform(keys.begin(), keys.end(), std::back_inserter(kvKeys),
                   [part](const auto& key) { return NebulaKeyUtils::kvKey(part, key); });

    executor_->add([this, p = std::move(promise), part, keys = std::move(keys),
                    kvKeys = std::move(kvKeys), returnPartly] () mutable {
        std::vector<std::string> values;
        if (returnPartly) {
            auto ret = this->kvstore_->tryGet(space_, part, kvKeys, &values);
            if (!ok(ret)) {
                p.setValue(std::make_pair(part, error(ret)));
            } else {
                auto status = value(ret);
                std::lock_guard<std::mutex> lg(this->lock_);
                for (size_t i = 0; i < keys.size(); i++) {
                    if (status[i].ok()) {
                        pairs_.emplace(keys[i], values[i]);
                    }
                }
                p.setValue(std::make_pair(part, kvstore::ResultCode::SUCCEEDED));
            }
        } else {
            auto ret = this->kvstore_->multiGet(space_, part, kvKeys, &values);
            if (ret == kvstore::ResultCode::SUCCEEDED) {
                std::lock_guard<std::mutex> lg(this->lock_);
                for (size_t i = 0; i < keys.size(); i++) {
                    pairs_.emplace(keys[i], values[i]);
                }
            }
            p.setValue(std::make_pair(part, ret));
        }
    });
    return future;
}

}  // namespace storage
}  // namespace nebula
