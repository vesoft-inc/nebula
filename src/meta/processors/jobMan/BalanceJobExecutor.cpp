/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/MetaServiceUtils.h"
#include "meta/processors/jobMan/BalanceJobExecutor.h"

namespace nebula {
namespace meta {

BalanceJobExecutor::BalanceJobExecutor(JobID jobId,
                                       kvstore::KVStore* kvstore,
                                       AdminClient* adminClient,
                                       const std::vector<std::string>& paras)
    : MetaJobExecutor(jobId, kvstore, adminClient, paras) {}


bool BalanceJobExecutor::check() {
    return false;
}

nebula::cpp2::ErrorCode BalanceJobExecutor::prepare() {
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode BalanceJobExecutor::stop() {
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

folly::Future<Status>
BalanceJobExecutor::executeInternal(HostAddr&& address, std::vector<PartitionID>&& parts) {
    UNUSED(address); UNUSED(parts);
    return Status::OK();
}

}  // namespace meta
}  // namespace nebula

