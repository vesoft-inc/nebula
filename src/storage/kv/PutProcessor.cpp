/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/kv/PutProcessor.h"

namespace nebula {
namespace storage {

void PutProcessor::process(const cpp2::PutRequest& req) {
    const auto& pairs = req.get_parts();
    auto space = req.get_space_id();
    callingNum_ = pairs.size();
    CHECK_NOTNULL(kvstore_);

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
