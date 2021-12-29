/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_JOBEXECUTOR_H_
#define META_JOBEXECUTOR_H_

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "common/base/ErrorOr.h"
#include "kvstore/KVStore.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/job/JobDescription.h"

namespace nebula {
namespace meta {

class JobExecutor {
 public:
  JobExecutor() = default;
  explicit JobExecutor(kvstore::KVStore* kv) : kvstore_(kv) {}
  virtual ~JobExecutor() = default;

  // Check the arguments about the job.
  virtual bool check() = 0;

  // Prepare the Job info from the arguments.
  virtual nebula::cpp2::ErrorCode prepare() = 0;

  // The skeleton to run the job.
  // You should rewrite the executeInternal to trigger the calling.
  virtual nebula::cpp2::ErrorCode execute() = 0;

  // Stop the job when the user cancel it.
  virtual nebula::cpp2::ErrorCode stop() = 0;

  virtual nebula::cpp2::ErrorCode finish(bool) = 0;

  virtual nebula::cpp2::ErrorCode recovery() = 0;

  virtual void setSpaceId(GraphSpaceID spaceId) = 0;

  virtual bool isMetaJob() = 0;

  virtual void setFinishCallBack(
      std::function<nebula::cpp2::ErrorCode(meta::cpp2::JobStatus)> func) {
    UNUSED(func);
  }

  virtual nebula::cpp2::ErrorCode saveSpecialTaskStatus(const cpp2::ReportTaskReq&) = 0;

 protected:
  ErrorOr<nebula::cpp2::ErrorCode, GraphSpaceID> getSpaceIdFromName(const std::string& spaceName);

 protected:
  kvstore::KVStore* kvstore_{nullptr};
};

class JobExecutorFactory {
 public:
  static std::unique_ptr<JobExecutor> createJobExecutor(const JobDescription& jd,
                                                        kvstore::KVStore* store,
                                                        AdminClient* client);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_JOBEXECUTOR_H_
