/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/SendBlockSignProcessor.h"


namespace nebula {
namespace storage {

void SendBlockSignProcessor::process(const cpp2::BlockingSignRequest& req) {
    CHECK_NOTNULL(kvstore_);
    auto spaceId = req.get_space_id();
    LOG(INFO) << "Receive block sign for space " << spaceId;
    auto sign = req.get_sign() == cpp2::EngineSignType::BLOCK_ON;
    if (writeBlocking(spaceId, sign)) {
        LOG(ERROR) << "Write Blocking Failed Space: " << spaceId;
    }
    onFinished();
}

}  // namespace storage
}  // namespace nebula

