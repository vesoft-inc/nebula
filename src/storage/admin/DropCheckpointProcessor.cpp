/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/admin/DropCheckpointProcessor.h"

#include <thrift/lib/cpp2/FieldRef.h>  // for field_ref

#include <utility>  // for move
#include <vector>   // for vector

#include "common/base/Logging.h"              // for CheckNotNull, CHECK_NOT...
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode, ErrorCode::S...
#include "kvstore/KVStore.h"                  // for KVStore
#include "storage/CommonUtils.h"              // for StorageEnv

namespace nebula {
namespace storage {

void DropCheckpointProcessor::process(const cpp2::DropCPRequest& req) {
  CHECK_NOTNULL(env_);
  auto spaceIdList = req.get_space_ids();
  auto& name = req.get_name();
  for (auto spaceId : spaceIdList) {
    auto code = env_->kvstore_->dropCheckpoint(spaceId, std::move(name));

    if (code == nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND) {
      continue;
    }

    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
      resp_.code_ref() = code;
      onFinished();
      return;
    }
  }

  resp_.code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
  onFinished();
}

void DropCheckpointProcessor::onFinished() {
  this->promise_.setValue(std::move(resp_));
  delete this;
}

}  // namespace storage
}  // namespace nebula
