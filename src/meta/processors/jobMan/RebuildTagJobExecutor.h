/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/jobMan/RebuildJobExecutor.h"

namespace nebula {
namespace meta {

class RebuildTagJobExecutor : public RebuildJobExecutor {
public:
    RebuildTagJobExecutor(int jobId,
                          nebula::cpp2::AdminCmd cmd,
                          std::vector<std::string> paras,
                          nebula::kvstore::KVStore* kvstore,
                          AdminClient* adminClient)
    : RebuildJobExecutor(jobId, cmd, paras, kvstore, adminClient) {}

private:
    folly::Future<Status> caller(const HostAddr& address,
                                 GraphSpaceID space,
                                 IndexID indexID,
                                 std::vector<PartitionID> parts,
                                 bool isOffline) override;
};

}  // namespace meta
}  // namespace nebula
