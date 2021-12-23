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
      cpp2::PartitionResult thriftRet;
      thriftRet.code_ref() = code;
      codes_.emplace_back(std::move(thriftRet));
      LOG(INFO) << "set block sign failed, error: " << apache::thrift::util::enumNameSafe(code);
      onFinished();
      return;
    }
  }

  onFinished();
}

}  // namespace storage
}  // namespace nebula
