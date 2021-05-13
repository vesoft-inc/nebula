/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <folly/container/Enumerate.h>
#include "storage/transaction/GetValueProcessor.h"
#include "storage/StorageFlags.h"

namespace nebula {
namespace storage {

ProcessorCounters kGetValueCounters;

void GetValueProcessor::process(const cpp2::GetValueRequest& req) {
    CHECK_NOTNULL(env_->kvstore_);
    auto spaceId = req.get_space_id();
    auto partId = req.get_part_id();
    auto key = req.get_key();

    std::string value;
    auto rc = env_->kvstore_->get(spaceId, partId, key, &value);
    LOG_IF(INFO, FLAGS_trace_toss) << "getValue for partId=" << partId
                                   << ", key=" << folly::hexlify(key)
                                   << ", rc=" << static_cast<int>(rc);

    if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
        handleErrorCode(rc, spaceId, partId);
    } else {
        resp_.set_value(std::move(value));
    }
    this->onFinished();
}

}  // namespace storage
}  // namespace nebula
