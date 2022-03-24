/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_TASKDESCRIPTION_H_
#define META_TASKDESCRIPTION_H_

#include <folly/Range.h>
#include <gtest/gtest_prod.h>

#include <ctime>
#include <string>
#include <vector>

#include "interface/gen-cpp2/meta_types.h"
#include "meta/processors/job/JobStatus.h"

namespace nebula {
namespace meta {

/*
 * Task is described as follows:
 * =====================================================================================
 * | Job Id(TaskId) | Command(Dest) | Status   | Start Time        | Stop Time |
 * =====================================================================================
 * | 27             | flush nba     | finished | 12/09/19 11:09:40 | 12/09/19
 * 11:09:40 |
 * -------------------------------------------------------------------------------------
 * | 27-0           | 192.168.8.5   | finished | 12/09/19 11:09:40 | 12/09/19
 * 11:09:40 |
 * -------------------------------------------------------------------------------------
 * */
class TaskDescription {
  FRIEND_TEST(TaskDescriptionTest, Ctor);
  FRIEND_TEST(TaskDescriptionTest, Dump);
  FRIEND_TEST(TaskDescriptionTest, Ctor2);
  FRIEND_TEST(JobManagerTest, ShowJob);
  friend class JobManager;

 public:
  TaskDescription(GraphSpaceID space, JobID iJob, TaskID iTask, const HostAddr& dst);
  TaskDescription(GraphSpaceID space, JobID iJob, TaskID iTask, std::string addr, int32_t port);
  TaskDescription(const folly::StringPiece& key, const folly::StringPiece& val);

  /**
   * @brief
   * Write out task details in human readable strings
   * if a task is
   * =====================================================================================
   * | Job Id(TaskId) | Command(Dest) | Status   | Start Time        | Stop Time
   * |
   * =====================================================================================
   * | 27-0           | 192.168.8.5   | finished | 12/09/19 11:09:40 | 12/09/19
   * 11:09:40 |
   * -------------------------------------------------------------------------------------
   *  then the vector should be
   * {27-0, 192.168.8.5, finished, 12/09/19 11:09:40, 12/09/19 11:09:40}
   *
   * @return
   */
  cpp2::TaskDesc toTaskDesc();

  /**
   * @brief
   * Set the internal status
   * Will check if newStatus is later than curr Status
   * e.g. set running to a finished job is forbidden
   *
   * Will set start time if newStatus is running
   * Will set stop time if newStatus is finished / failed / stopped
   *
   * @param newStatus
   * @return
   */
  bool setStatus(cpp2::JobStatus newStatus);

  cpp2::JobStatus getStatus() {
    return status_;
  }

  GraphSpaceID getSpace() {
    return space_;
  }

  JobID getJobId() {
    return iJob_;
  }

  TaskID getTaskId() {
    return iTask_;
  }

  HostAddr getHost() {
    return dest_;
  }

  int64_t getStartTime() {
    return startTime_;
  }

  int64_t getStopTime() {
    return stopTime_;
  }

 private:
  GraphSpaceID space_;
  JobID iJob_;
  TaskID iTask_;
  HostAddr dest_;
  cpp2::JobStatus status_;
  int64_t startTime_;
  int64_t stopTime_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_TASKDESCRIPTION_H_
