/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/ReportTaskProcessor.h"

#include "interface/gen-cpp2/common_types.h"
#include "meta/processors/job/JobManager.h"

namespace nebula {
namespace meta {

void ReportTaskProcessor::process(const cpp2::ReportTaskReq& req) {
  JobManager* jobMgr = JobManager::getInstance();
  auto rc = jobMgr->reportTaskFinish(req);
  if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
    handleErrorCode(rc);
  }
  onFinished();
}

}  // namespace meta
}  // namespace nebula
