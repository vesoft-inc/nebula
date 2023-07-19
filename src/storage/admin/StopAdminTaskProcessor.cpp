/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/admin/StopAdminTaskProcessor.h"

#include "interface/gen-cpp2/common_types.h"
#include "storage/admin/AdminTaskManager.h"

namespace nebula {
namespace storage {

void StopAdminTaskProcessor::process(const cpp2::StopTaskRequest& req) {
  auto taskManager = AdminTaskManager::instance();
  auto rc = taskManager->cancelTask(req.get_job_id());

  if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
    resp_.code_ref() = rc;
    onFinished();
    return;
  }

  resp_.code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
  onFinished();
}

void StopAdminTaskProcessor::onFinished() {
  this->promise_.setValue(std::move(resp_));
  delete this;
}

}  // namespace storage
}  // namespace nebula
