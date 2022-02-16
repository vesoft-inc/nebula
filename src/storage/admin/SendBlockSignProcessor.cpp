/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/admin/SendBlockSignProcessor.h"

#include <thrift/lib/cpp/util/EnumUtils.h>  // for enumNameSafe
#include <thrift/lib/cpp2/FieldRef.h>       // for field_ref

#include <ostream>  // for operator<<, basic_ostream
#include <string>   // for operator<<
#include <utility>  // for move
#include <vector>   // for vector

#include "common/base/Logging.h"              // for LOG, LogMessage, _LOG_INFO
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode, ErrorCode::S...
#include "kvstore/KVStore.h"                  // for KVStore
#include "storage/CommonUtils.h"              // for StorageEnv

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
