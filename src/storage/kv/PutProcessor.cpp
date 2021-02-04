/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/kv/PutProcessor.h"
#include "utils/NebulaKeyUtils.h"

namespace nebula {
namespace storage {

ProcessorCounters kPutCounters;

void PutProcessor::process(const cpp2::KVPutRequest& req) {
    CHECK_NOTNULL(env_->kvstore_);
    const auto& pairs = req.get_parts();
    auto space = req.get_space_id();
    callingNum_ = pairs.size();

    std::for_each(pairs.begin(), pairs.end(), [&](auto& value) {
        auto part = value.first;
        std::vector<kvstore::KV> data;
        for (auto& pair : value.second) {
            data.emplace_back(std::move(NebulaKeyUtils::kvKey(part, pair.key)),
                              std::move(pair.value));
        }
        doPut(space, part, std::move(data));
    });
}

}  // namespace storage
}  // namespace nebula
