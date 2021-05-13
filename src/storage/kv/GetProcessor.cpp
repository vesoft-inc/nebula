/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/kv/GetProcessor.h"
#include "utils/NebulaKeyUtils.h"

namespace nebula {
namespace storage {

ProcessorCounters kGetCounters;

void GetProcessor::process(const cpp2::KVGetRequest& req) {
    CHECK_NOTNULL(env_->kvstore_);
    GraphSpaceID spaceId = req.get_space_id();
    bool returnPartly = req.get_return_partly();

    std::unordered_map<std::string, std::string> pairs;
    size_t size = 0;
    for (auto& part : req.get_parts()) {
        size += part.second.size();
    }
    pairs.reserve(size);

    for (auto& part : req.get_parts()) {
        auto partId = part.first;
        auto& keys = part.second;
        std::vector<std::string> kvKeys;
        kvKeys.reserve(part.second.size());
        std::transform(keys.begin(), keys.end(), std::back_inserter(kvKeys),
                       [partId] (const auto& key) { return NebulaKeyUtils::kvKey(partId, key); });
        std::vector<std::string> values;
        auto ret = env_->kvstore_->multiGet(spaceId, partId, kvKeys, &values);
        if ((ret.first == nebula::cpp2::ErrorCode::SUCCEEDED) ||
            (ret.first == nebula::cpp2::ErrorCode::E_PARTIAL_RESULT && returnPartly)) {
            auto& status = ret.second;
            for (size_t i = 0; i < kvKeys.size(); i++) {
                if (status[i].ok()) {
                    pairs.emplace(keys[i], values[i]);
                }
            }
        } else {
            handleErrorCode(ret.first, spaceId, partId);
        }
    }

    resp_.set_key_values(std::move(pairs));
    this->onFinished();
}

}  // namespace storage
}  // namespace nebula
