/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_METAJOBEXECUTOR_H_
#define META_METAJOBEXECUTOR_H_

#include <thrift/lib/cpp/util/EnumUtils.h>
#include "common/base/ErrorOr.h"
#include "kvstore/KVStore.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/jobMan/JobDescription.h"

namespace nebula {
namespace meta {

using PartsOfHost = std::pair<HostAddr, std::vector<PartitionID>>;
using ErrOrHosts  = ErrorOr<nebula::cpp2::ErrorCode, std::vector<PartsOfHost>>;

class MetaJobExecutor {
public:
    enum class TargetHosts {
        LEADER = 0,
        LISTENER,
        DEFAULT
    };

    MetaJobExecutor(JobID jobId,
                    kvstore::KVStore* kvstore,
                    AdminClient* adminClient,
                    const std::vector<std::string>& paras)
    : jobId_(jobId),
      kvstore_(kvstore),
      adminClient_(adminClient),
      paras_(paras) {}

    virtual ~MetaJobExecutor() = default;

    // Check the arguments about the job.
    virtual bool check() = 0;

    // Prepare the Job info from the arguments.
    virtual nebula::cpp2::ErrorCode prepare() = 0;

    // The skeleton to run the job.
    // You should rewrite the executeInternal to trigger the calling.
    nebula::cpp2::ErrorCode  execute();

    void interruptExecution(JobID jobId);

    // Stop the job when the user cancel it.
    virtual nebula::cpp2::ErrorCode stop() = 0;

    virtual nebula::cpp2::ErrorCode finish(bool) {
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    void setSpaceId(GraphSpaceID spaceId) { space_ = spaceId; }

    virtual nebula::cpp2::ErrorCode
    saveSpecialTaskStatus(const cpp2::ReportTaskReq&) {
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

protected:
    ErrorOr<nebula::cpp2::ErrorCode, GraphSpaceID>
    getSpaceIdFromName(const std::string& spaceName);

    ErrOrHosts getTargetHost(GraphSpaceID space);

    ErrOrHosts getLeaderHost(GraphSpaceID space);

    ErrOrHosts getListenerHost(GraphSpaceID space, cpp2::ListenerType type);

    virtual folly::Future<Status>
    executeInternal(HostAddr&& address, std::vector<PartitionID>&& parts) = 0;

protected:
    JobID                       jobId_{INT_MIN};
    TaskID                      taskId_{0};
    kvstore::KVStore*           kvstore_{nullptr};
    AdminClient*                adminClient_{nullptr};
    GraphSpaceID                space_;
    std::vector<std::string>    paras_;
    TargetHosts                 toHost_{TargetHosts::DEFAULT};
    int32_t                     concurrency_{INT_MAX};
    bool                        stopped_{false};
    std::mutex                  muInterrupt_;
    std::condition_variable     condInterrupt_;
};

class MetaJobExecutorFactory {
public:
    static std::unique_ptr<MetaJobExecutor>
    createMetaJobExecutor(const JobDescription& jd,
                          kvstore::KVStore* store,
                          AdminClient* client);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_METAJOBEXECUTOR_H_
