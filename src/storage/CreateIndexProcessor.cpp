/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/CreateIndexProcessor.h"
#include "base/NebulaKeyUtils.h"
#include <algorithm>
#include <limits>
#include "time/WallClock.h"

DEFINE_int32(bulk_number_per_index_creation, 100000,
             "The number of rows that data inserts in bulk during index creation");

#define ERROR_CODE_CHECK(ret)                                          \
do {                                                                   \
    if (ret != cpp2::ErrorCode::SUCCEEDED) {                           \
        std::vector<std::pair<cpp2::ErrorCode, PartitionID>> items;    \
        for (auto& part : parts) {                                     \
            items.emplace_back(ret, part.first);                       \
        }                                                              \
    pushPartsResultCode(std::move(items));                             \
    onFinished();                                                      \
    return;                                                            \
    }                                                                  \
} while (false)                                                        \

namespace nebula {
namespace storage {

void CreateIndexProcessor::process(const cpp2::BuildIndexReq& req) {
    const auto& indexItem = req.get_index_item();
    const auto& parts = req.get_parts();

    spaceId_ = req.get_space_id();
    indexId_ = indexItem.get_index_id();
    indexType_ = req.get_index_type();
    props_ = indexItem.get_props();

    callingNum_ = parts.size();
    CHECK_NOTNULL(kvstore_);

    auto ret = kvstore_->createSnapshot(spaceId_);
    ERROR_CODE_CHECK(to(ret));

    std::for_each(parts.begin(), parts.end(), [&](auto& part){
        auto partId = part.first;
        doIndexCreate(partId);
    });
}
}  // namespace storage
}  // namespace nebula

