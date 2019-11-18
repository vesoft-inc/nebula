/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/SendBlockSignProcessor.h"


namespace nebula {
namespace storage {

void SendBlockSignProcessor::process(const cpp2::BlockingSignRequest& req) {
    CHECK_NOTNULL(kvstore_);
    LOG(INFO) << "Receive block sign for space "
              << req.get_space_id() << ", part " << req.get_part_id();
    auto spaceId = req.get_space_id();
    auto partId = req.get_part_id();
    auto sign = req.get_sign() == cpp2::EngineSignType::BLOCK_ON;

    auto code = kvstore_->setPartBlocking(spaceId, partId, sign);
    this->pushResultCode(to(code), partId);
    onFinished();
}

}  // namespace storage
}  // namespace nebula

