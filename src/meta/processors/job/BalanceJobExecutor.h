 /* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_BALANCEJOBEXECUTOR_H_
#define META_BALANCEJOBEXECUTOR_H_

#include "meta/processors/admin/BalanceTask.h"
#include "meta/processors/admin/BalancePlan.h"
#include "meta/processors/jobMan/SimpleConcurrentJobExecutor.h"

namespace nebula {
namespace meta {

using HostParts = std::unordered_map<HostAddr, std::vector<PartitionID>>;
using ZoneParts = std::pair<std::string, std::vector<PartitionID>>;

/*
 * BalanceJobExecutor is use to balance data between hosts.
 */
class BalanceJobExecutor : public MetaJobExecutor {
public:
    BalanceJobExecutor(JobID jobId,
                       kvstore::KVStore* kvstore,
                       AdminClient* adminClient,
                       const std::vector<std::string>& params);

    bool check() override;

    nebula::cpp2::ErrorCode prepare() override;

    nebula::cpp2::ErrorCode stop() override;

protected:
    folly::Future<Status>
    executeInternal(HostAddr&& address, std::vector<PartitionID>&& parts) override;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_BALANCEJOBEXECUTOR_H_
