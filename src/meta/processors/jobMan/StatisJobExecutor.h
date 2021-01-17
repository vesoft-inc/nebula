/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_STATISJOBEXECUTOR_H_
#define META_STATISJOBEXECUTOR_H_

#include "common/interface/gen-cpp2/meta_types.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/jobMan/MetaJobExecutor.h"

namespace nebula {
namespace meta {

class StatisJobExecutor : public MetaJobExecutor {
public:
    StatisJobExecutor(JobID jobId,
                      kvstore::KVStore* kvstore,
                      AdminClient* adminClient,
                      const std::vector<std::string>& paras)
        : MetaJobExecutor(jobId, kvstore, adminClient, paras) {
        toLeader_ = true;
    }

    bool check() override;

    cpp2::ErrorCode prepare() override;

    cpp2::ErrorCode stop() override;

    folly::Future<Status>
    executeInternal(HostAddr&& address, std::vector<PartitionID>&& parts) override;

    // Summarize the results of statisItem_
    void finish(bool ExeSuccessed) override;

    cpp2::ErrorCode saveSpecialTaskStatus(const cpp2::ReportTaskReq& req) override;

private:
    // Statis job writes an additional data.
    // The additional data is written when the statis job passes the check function.
    // Update this additional data when job finishes.
    kvstore::ResultCode save(const std::string& key, const std::string& val);

    void addStatis(cpp2::StatisItem& lhs, const cpp2::StatisItem& rhs);

    std::string toTempKey(int32_t jobId);

    void doRemove(const std::string& key);

private:
    // Statis results
    std::unordered_map<HostAddr, cpp2::StatisItem>  statisItem_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_STATISJOBEXECUTOR_H_
