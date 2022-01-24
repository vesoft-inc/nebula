/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/admin/AdminTaskProcessor.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "interface/gen-cpp2/common_types.h"
#include "storage/admin/AdminTaskManager.h"

namespace nebula {
namespace storage {

void AdminTaskProcessor::process(const cpp2::AddTaskRequest& req) {
  auto taskManager = AdminTaskManager::instance();

  auto cb = [taskManager, jobId = req.get_job_id(), taskId = req.get_task_id()](
                nebula::cpp2::ErrorCode errCode, nebula::meta::cpp2::StatsItem& result) {
    taskManager->saveAndNotify(jobId, taskId, errCode, result);
  };

  TaskContext ctx(req, std::move(cb));
  auto task = AdminTaskFactory::createAdminTask(env_, std::move(ctx));
  if (task) {
    nebula::meta::cpp2::StatsItem statsItem;
    statsItem.status_ref() = nebula::meta::cpp2::JobStatus::RUNNING;
    // write an initial state of task
    taskManager->saveTaskStatus(
        ctx.jobId_, ctx.taskId_, nebula::cpp2::ErrorCode::E_TASK_EXECUTION_FAILED, statsItem);
    taskManager->addAsyncTask(task);
  } else {
    resp_.code_ref() = nebula::cpp2::ErrorCode::E_INVALID_TASK_PARA;
    onFinished();
    return;
  }

  resp_.code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
  onFinished();
}

void AdminTaskProcessor::onFinished() {
  this->promise_.setValue(std::move(resp_));
  delete this;
}

}  // namespace storage
}  // namespace nebula
