/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/kv/GetProcessor.h"
#include "utils/NebulaKeyUtils.h"

namespace nebula {
namespace storage {

void GetProcessor::process(const cpp2::GetRequest& req) {
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
        auto ret = this->kvstore_->multiGet(spaceId, partId, kvKeys, &values);
        if ((ret.first == kvstore::ResultCode::SUCCEEDED) ||
            (ret.first == kvstore::ResultCode::ERR_PARTIAL_RESULT && returnPartly)) {
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

    resp_.set_values(std::move(pairs));
    this->onFinished();
}

}  // namespace storage
}  // namespace nebula
