/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_REBUILDJOBEXECUTOR_H_
#define META_REBUILDJOBEXECUTOR_H_

#include "common/interface/gen-cpp2/common_types.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/jobMan/MetaJobExecutor.h"

namespace nebula {
namespace meta {

class RebuildJobExecutor : public MetaJobExecutor {
public:
    RebuildJobExecutor(JobID jobId,
                       kvstore::KVStore* kvstore,
                       AdminClient* adminClient,
                       const std::vector<std::string>& paras)
        : MetaJobExecutor(jobId, kvstore, adminClient, paras) {
        toHost_ = TargetHosts::LEADER;
    }

    bool check() override;

    nebula::cpp2::ErrorCode prepare() override;

    nebula::cpp2::ErrorCode stop() override;

protected:
    std::vector<std::string>  taskParameters_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_REBUILDJOBEXECUTOR_H_
