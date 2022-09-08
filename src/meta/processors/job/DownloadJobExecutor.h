/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_DOWNLOADJOBEXECUTOR_H_
#define META_DOWNLOADJOBEXECUTOR_H_

#include "common/hdfs/HdfsCommandHelper.h"
#include "meta/processors/job/SimpleConcurrentJobExecutor.h"

namespace nebula {
namespace meta {

/**
 * @brief Executor for download job, always called by job manager.
 */
class DownloadJobExecutor : public SimpleConcurrentJobExecutor {
  FRIEND_TEST(JobManagerTest, DownloadJob);
  FRIEND_TEST(JobManagerTest, IngestJob);

 public:
  DownloadJobExecutor(GraphSpaceID space,
                      JobID jobId,
                      kvstore::KVStore* kvstore,
                      AdminClient* adminClient,
                      const std::vector<std::string>& params);

  nebula::cpp2::ErrorCode check() override;

  nebula::cpp2::ErrorCode prepare() override;

 protected:
  folly::Future<Status> executeInternal(HostAddr&& address,
                                        std::vector<PartitionID>&& parts) override;

 private:
  std::unique_ptr<std::string> host_;
  int32_t port_;
  std::unique_ptr<std::string> path_;
  std::unique_ptr<nebula::hdfs::HdfsHelper> helper_;
  std::vector<std::string> taskParameters_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_DOWNLOADJOBEXECUTOR_H_
