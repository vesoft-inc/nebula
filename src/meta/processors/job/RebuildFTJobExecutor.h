/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_REBUILDFTJOBEXECUTOR_H_
#define META_REBUILDFTJOBEXECUTOR_H_

#include "meta/processors/jobMan/RebuildJobExecutor.h"

namespace nebula {
namespace meta {

class RebuildFTJobExecutor : public RebuildJobExecutor {
public:
    RebuildFTJobExecutor(JobID jobId,
                         kvstore::KVStore* kvstore,
                         AdminClient* adminClient,
                         const std::vector<std::string>& paras)
        : RebuildJobExecutor(jobId, kvstore, adminClient, std::move(paras)) {
            toHost_ = TargetHosts::LISTENER;
        }

protected:
    folly::Future<Status>
    executeInternal(HostAddr&& address, std::vector<PartitionID>&& parts) override;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_REBUILDFTJOBEXECUTOR_H_
