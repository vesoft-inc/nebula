/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_REBUILDEDGEJOBEXECUTOR_H_
#define META_REBUILDEDGEJOBEXECUTOR_H_

#include "meta/processors/jobMan/RebuildJobExecutor.h"

namespace nebula {
namespace meta {

class RebuildEdgeJobExecutor : public RebuildJobExecutor {
public:
    RebuildEdgeJobExecutor(JobID jobId,
                           kvstore::KVStore* kvstore,
                           AdminClient* adminClient,
                           const std::vector<std::string>& paras)
        : RebuildJobExecutor(jobId, kvstore, adminClient, std::move(paras)) {}

protected:
    folly::Future<Status>
    executeInternal(HostAddr&& address, std::vector<PartitionID>&& parts) override;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_REBUILDEDGEJOBEXECUTOR_H_
