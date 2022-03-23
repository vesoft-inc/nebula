/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/TaskDescription.h"

#include <folly/String.h>

#include "common/time/WallClock.h"
#include "common/utils/MetaKeyUtils.h"
#include "meta/processors/job/JobStatus.h"

namespace nebula {
namespace meta {

using Status = cpp2::JobStatus;
using Host = std::pair<int, int>;

TaskDescription::TaskDescription(GraphSpaceID space, JobID iJob, TaskID iTask, const HostAddr& dst)
    : TaskDescription(space, iJob, iTask, dst.host, dst.port) {}

TaskDescription::TaskDescription(
    GraphSpaceID space, JobID iJob, TaskID iTask, std::string addr, int32_t port)
    : space_(space),
      iJob_(iJob),
      iTask_(iTask),
      dest_(addr, port),
      status_(cpp2::JobStatus::RUNNING),
      startTime_(std::time(nullptr)),
      stopTime_(0) {}

/*
 * task key:
 * GraphSpaceID                    space_;
 * JobID                           iJob_;
 * TaskID                          iTask_;
 *
 * task val:
 * HostAddr                        dest_;
 * cpp2::JobStatus                 status_;
 * int64_t                         startTime_;
 * int64_t                         stopTime_;
 * */
TaskDescription::TaskDescription(const folly::StringPiece& key, const folly::StringPiece& val) {
  auto tupKey = MetaKeyUtils::parseTaskKey(key);
  space_ = std::get<0>(tupKey);
  iJob_ = std::get<1>(tupKey);
  iTask_ = std::get<2>(tupKey);

  auto tupVal = MetaKeyUtils::parseTaskVal(val);
  dest_ = std::get<0>(tupVal);
  status_ = std::get<1>(tupVal);
  startTime_ = std::get<2>(tupVal);
  stopTime_ = std::get<3>(tupVal);
}

/*
 * =====================================================================================
 * | Job Id(TaskId) | Command(Dest) | Status   | Start Time        | Stop Time |
 * =====================================================================================
 * | 27-0           | 192.168.8.5   | finished | 12/09/19 11:09:40 | 12/09/19
 * 11:09:40 |
 * -------------------------------------------------------------------------------------
 * */
cpp2::TaskDesc TaskDescription::toTaskDesc() {
  cpp2::TaskDesc ret;
  ret.space_id_ref() = space_;
  ret.job_id_ref() = iJob_;
  ret.task_id_ref() = iTask_;
  ret.host_ref() = dest_;
  ret.status_ref() = status_;
  ret.start_time_ref() = startTime_;
  ret.stop_time_ref() = stopTime_;
  return ret;
}

bool TaskDescription::setStatus(Status newStatus) {
  if (JobStatus::laterThan(status_, newStatus)) {
    return false;
  }
  status_ = newStatus;
  if (newStatus == Status::RUNNING) {
    startTime_ = nebula::time::WallClock::fastNowInSec();
  }

  if (JobStatus::laterThan(newStatus, Status::RUNNING)) {
    stopTime_ = nebula::time::WallClock::fastNowInSec();
  }
  return true;
}

}  // namespace meta
}  // namespace nebula
