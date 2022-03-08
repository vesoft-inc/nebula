/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/admin/CreateCheckpointProcessor.h"

namespace nebula {
namespace storage {

void CreateCheckpointProcessor::process(const cpp2::CreateCPRequest& req) {
  CHECK_NOTNULL(env_);
  auto spaceIdList = req.get_space_ids();
  auto& name = req.get_name();

  std::vector<nebula::cpp2::CheckpointInfo> ckInfoList;
  for (auto& spaceId : spaceIdList) {
    auto ckRet = env_->kvstore_->createCheckpoint(spaceId, name);
    if (!ok(ckRet) && error(ckRet) == nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND) {
      LOG(INFO) << folly::sformat("Space {} to create backup is not found", spaceId);
      continue;
    }

    if (!ok(ckRet)) {
      resp_.code_ref() = error(ckRet);
      onFinished();
      return;
    }

    auto spaceCkList = std::move(nebula::value(ckRet));
    ckInfoList.insert(ckInfoList.end(), spaceCkList.begin(), spaceCkList.end());
  }

  resp_.code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
  resp_.info_ref() = ckInfoList;
  onFinished();
}

void CreateCheckpointProcessor::onFinished() {
  this->promise_.setValue(std::move(resp_));
  delete this;
}

}  // namespace storage
}  // namespace nebula
