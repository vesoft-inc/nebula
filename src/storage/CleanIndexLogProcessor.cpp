/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/CleanIndexLogProcessor.h"
#include "base/NebulaKeyUtils.h"

namespace nebula {
namespace storage {

void CleanIndexLogProcessor::process(const cpp2::CleanIndexLogReq& req) {
    const auto& parts = req.get_parts();
    spaceId_ = req.get_space_id();
    indexId_ = req.get_index_id();
    callingNum_ = static_cast<int32_t>(parts.size());
    CHECK_NOTNULL(kvstore_);

    std::for_each(parts.begin(), parts.end(), [&](auto& part){
        auto partId = part.first;
        auto prefix = NebulaKeyUtils::preIndexPrefix(partId, indexId_);
        doBatchDelete(spaceId_, partId, std::move(prefix));
    });
}
}  // namespace storage
}  // namespace nebula

