/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/MetaJobExecutor.h"

#include "common/utils/Utils.h"

DECLARE_int32(heartbeat_interval_secs);
DECLARE_uint32(expired_time_factor);

namespace nebula {
namespace meta {
nebula::cpp2::ErrorCode MetaJobExecutor::check() {
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

// Prepare the Job info from the arguments.
nebula::cpp2::ErrorCode MetaJobExecutor::prepare() {
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

// The skeleton to run the job.
// You should rewrite the executeInternal to trigger the calling.
folly::Future<nebula::cpp2::ErrorCode> MetaJobExecutor::execute() {
  isRunning_.store(true);
  return executeInternal();
}

// Stop the job when the user cancel it.
nebula::cpp2::ErrorCode MetaJobExecutor::stop() {
  // By default we return not stoppable
  return nebula::cpp2::ErrorCode::E_JOB_NOT_STOPPABLE;
}

nebula::cpp2::ErrorCode MetaJobExecutor::finish(bool) {
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

bool MetaJobExecutor::isMetaJob() {
  return true;
}

nebula::cpp2::ErrorCode MetaJobExecutor::recovery() {
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode MetaJobExecutor::saveSpecialTaskStatus(const cpp2::ReportTaskReq&) {
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

folly::Future<nebula::cpp2::ErrorCode> MetaJobExecutor::executeInternal() {
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula
