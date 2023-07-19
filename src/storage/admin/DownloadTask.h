/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_ADMIN_DOWNLOADTASK_H_
#define STORAGE_ADMIN_DOWNLOADTASK_H_

#include "common/hdfs/HdfsCommandHelper.h"
#include "storage/admin/AdminTask.h"

namespace nebula {
namespace storage {

/**
 * @brief Task class to handle storage download task.
 *
 */
class DownloadTask : public AdminTask {
 public:
  DownloadTask(StorageEnv* env, TaskContext&& ctx) : AdminTask(env, std::move(ctx)) {
    helper_ = std::make_unique<hdfs::HdfsCommandHelper>();
  }

  bool check() override;

  ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>> genSubTasks() override;

 private:
  nebula::cpp2::ErrorCode subTask(GraphSpaceID space, PartitionID part);

 private:
  std::string hdfsPath_;
  std::string hdfsHost_;
  int32_t hdfsPort_;
  std::unique_ptr<nebula::hdfs::HdfsHelper> helper_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_ADMIN_DOWNLOADTASK_H_
