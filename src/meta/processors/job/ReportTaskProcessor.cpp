/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/jobMan/ReportTaskProcessor.h"
#include "meta/processors/jobMan/JobManager.h"

namespace nebula {
namespace meta {

#include "meta/processors/jobMan/JobManager.h"

void ReportTaskProcessor::process(const cpp2::ReportTaskReq& req) {
    JobManager* jobMgr = JobManager::getInstance();
    auto rc = jobMgr->reportTaskFinish(req);
    if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
        handleErrorCode(rc);
    }
    onFinished();
}

}   // namespace meta
}   // namespace nebula
