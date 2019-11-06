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
    auto sign = req.get_sign() == cpp2::EngineSignType::BLOCK_ON ? true : false;

    auto ret = kvstore_->part(spaceId, partId);
    if (!ok(ret)) {
        this->pushResultCode(to(error(ret)), partId);
        onFinished();
        return;
    }
    auto part = nebula::value(ret);
    part->asyncBlockingLeader(sign,
                              [this, spaceId, partId] (kvstore::ResultCode code) {
                                  auto leaderRet = kvstore_->partLeader(spaceId, partId);
                                  CHECK(ok(leaderRet));
                                  if (code == kvstore::ResultCode::ERR_LEADER_CHANGED) {
                                      auto leader = value(std::move(leaderRet));
                                      this->pushResultCode(to(code), partId, leader);
                                  }
                                  onFinished();
                              });
}

}  // namespace storage
}  // namespace nebula

