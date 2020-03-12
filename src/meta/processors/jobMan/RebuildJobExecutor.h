/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_REBUILDJOBEXECUTOR_H_
#define META_REBUILDJOBEXECUTOR_H_

#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/jobMan/MetaJobExecutor.h"
#include "interface/gen-cpp2/common_types.h"

namespace nebula {
namespace meta {

class RebuildJobExecutor : public MetaJobExecutor {
public:
    RebuildJobExecutor(int jobId,
                       nebula::cpp2::AdminCmd cmd,
                       std::vector<std::string> paras,
                       nebula::kvstore::KVStore* kvstore,
                       AdminClient* adminClient)
    : jobId_(jobId), cmd_(cmd), paras_(paras), kvstore_(kvstore), adminClient_(adminClient) {}

    ExecuteRet execute() override;

    void stop() override;

private:
    virtual folly::Future<Status> caller(const HostAddr& address,
                                         GraphSpaceID spaceId,
                                         IndexID indexID,
                                         std::vector<PartitionID> parts,
                                         bool isOffline) = 0;

protected:
    int jobId_;
    int spaceId_;
    char category_;
    nebula::cpp2::AdminCmd cmd_;
    std::vector<std::string> paras_;
    kvstore::KVStore* kvstore_{nullptr};
    AdminClient* adminClient_{nullptr};
};

}  // namespace meta
}  // namespace nebula

#endif  // META_REBUILDJOBEXECUTOR_H_
