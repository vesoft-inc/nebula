/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/admin/StopAdminTaskProcessor.h"

#include "interface/gen-cpp2/common_types.h"
#include "storage/admin/AdminTaskManager.h"

namespace nebula {
namespace storage {

void StopAdminTaskProcessor::process(const cpp2::StopAdminTaskRequest& req) {
  auto taskManager = AdminTaskManager::instance();
  auto rc = taskManager->cancelJob(req.get_job_id());

  if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
    cpp2::PartitionResult thriftRet;
    thriftRet.code_ref() = rc;
    codes_.emplace_back(std::move(thriftRet));
  }

  onFinished();
}

}  // namespace storage
}  // namespace nebula
