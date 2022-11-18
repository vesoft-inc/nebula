/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/ReportTaskProcessor.h"

#include "meta/processors/job/JobManager.h"

namespace nebula {
namespace meta {

#include "meta/processors/job/JobManager.h"

void ReportTaskProcessor::process(const cpp2::ReportTaskReq& req) {
  CHECK_SPACE_ID_AND_RETURN(req.get_space_id());

  JobManager* jobMgr = JobManager::getInstance();
  auto rc = jobMgr->reportTaskFinish(req);
  if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
    handleErrorCode(rc);
  }
  onFinished();
}

}  // namespace meta
}  // namespace nebula
