 /* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_COMPACTJOBEXECUTOR_H_
#define META_COMPACTJOBEXECUTOR_H_

#include "meta/processors/jobMan/SimpleConcurrentJobExecutor.h"

namespace nebula {
namespace meta {

class CompactJobExecutor : public SimpleConcurrentJobExecutor {
public:
    CompactJobExecutor(JobID jobId,
                       kvstore::KVStore* kvstore,
                       AdminClient* adminClient,
                       const std::vector<std::string>& params);

    folly::Future<Status> executeInternal(HostAddr&& address,
                                          std::vector<PartitionID>&& parts) override;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_COMPACTJOBEXECUTOR_H_
