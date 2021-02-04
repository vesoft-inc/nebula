/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/kv/RemoveProcessor.h"
#include "utils/NebulaKeyUtils.h"

namespace nebula {
namespace storage {

ProcessorCounters kRemoveCounters;

void RemoveProcessor::process(const cpp2::KVRemoveRequest& req) {
    CHECK_NOTNULL(env_->kvstore_);
    const auto& pairs = req.get_parts();
    auto space = req.get_space_id();
    callingNum_ = pairs.size();

    std::for_each(pairs.begin(), pairs.end(), [&](auto& value) {
        auto part = value.first;
        std::vector<std::string> keys;
        for (auto& key : value.second) {
            keys.emplace_back(std::move(NebulaKeyUtils::kvKey(part, key)));
        }
        doRemove(space, part, std::move(keys));
    });
}

}  // namespace storage
}  // namespace nebula
