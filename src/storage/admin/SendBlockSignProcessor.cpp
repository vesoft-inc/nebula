/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/admin/SendBlockSignProcessor.h"

namespace nebula {
namespace storage {

void SendBlockSignProcessor::process(const cpp2::BlockingSignRequest& req) {
  auto spaceIds = req.get_space_ids();
  auto sign = req.get_sign() == cpp2::EngineSignType::BLOCK_ON;

  for (auto spaceId : spaceIds) {
    LOG(INFO) << "Receive block sign for space " << spaceId << ", block: " << sign;
    auto code = env_->kvstore_->setWriteBlocking(spaceId, sign);
    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "set block sign failed, error: " << apache::thrift::util::enumNameSafe(code);
      resp_.code_ref() = code;
      onFinished();
      return;
    }
  }

  resp_.code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
  onFinished();
}

void SendBlockSignProcessor::onFinished() {
  this->promise_.setValue(std::move(resp_));
  delete this;
}

}  // namespace storage
}  // namespace nebula
