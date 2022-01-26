/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_DISKBALANCEJOBEXECUTOR_H_
#define META_DISKBALANCEJOBEXECUTOR_H_

#include "meta/processors/job/BalanceJobExecutor.h"

namespace nebula {
namespace meta {

class DiskBalanceJobExecutor : public BalanceJobExecutor {
  FRIEND_TEST(BalanceTest, DetachDiskTest);
  FRIEND_TEST(BalanceTest, DetachMultiDiskTest);
  FRIEND_TEST(BalanceTest, AdditionDiskTest);
  FRIEND_TEST(BalanceTest, AdditionMultiDiskTest);

 public:
  DiskBalanceJobExecutor(JobDescription jobDescription,
                         kvstore::KVStore* kvstore,
                         AdminClient* adminClient,
                         bool dismiss,
                         const std::vector<std::string>& params)
      : BalanceJobExecutor(jobDescription.getJobId(), kvstore, adminClient, params),
        dismiss_(dismiss),
        jobDescription_(jobDescription) {}

  nebula::cpp2::ErrorCode prepare() override;

 protected:
  folly::Future<Status> executeInternal() override;

  Status buildBalancePlan() override;

  ErrorOr<nebula::cpp2::ErrorCode, DiskParts> assemblePartLocation(HostAddr host,
                                                                   GraphSpaceID spaceId,
                                                                   int32_t& totalParts);

  nebula::cpp2::ErrorCode detachDiskBalance(const HostAddr& host,
                                            GraphSpaceID spaceId,
                                            DiskParts& partsLocation,
                                            const std::vector<std::string>& paths,
                                            std::vector<BalanceTask>& tasks);

  nebula::cpp2::ErrorCode additionDiskBalance(const HostAddr& host,
                                              GraphSpaceID spaceId,
                                              DiskParts& partsLocation,
                                              const std::vector<std::string>& paths,
                                              int32_t totalParts,
                                              std::vector<BalanceTask>& tasks);

  std::vector<std::pair<std::string, int32_t>> sortedDisksByParts(const DiskParts& partsLocation);

 private:
  bool dismiss_;
  HostAddr host_;
  std::vector<std::string> paths_;
  JobDescription jobDescription_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_DISKBALANCEJOBEXECUTOR_H_
