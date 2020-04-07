/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_SIMPLECONCURRENTJOBEXECUTOR_H_
#define META_SIMPLECONCURRENTJOBEXECUTOR_H_

#include "meta/processors/jobMan/MetaJobExecutor.h"
#include "interface/gen-cpp2/common_types.h"
namespace nebula {
namespace meta {

class SimpleConcurrentJobExecutor : public MetaJobExecutor {
public:
    using ExecuteRet = ErrorOr<kvstore::ResultCode, std::map<HostAddr, Status>>;
    using ErrOrInt  = ErrorOr<nebula::kvstore::ResultCode, int32_t>;
    using ErrOrHostAddrVec  = ErrorOr<nebula::kvstore::ResultCode, std::vector<HostAddr>>;

    SimpleConcurrentJobExecutor(int jobId,
                                nebula::cpp2::AdminCmd cmd,
                                std::vector<std::string> params,
                                nebula::kvstore::KVStore* kvStore,
                                AdminClient* adminClient);

    ExecuteRet execute() override;
    void stop() override;

private:
    ErrOrInt getSpaceIdFromName(const std::string& spaceName);
    ErrOrHostAddrVec getTargetHost(int32_t spaceId);

private:
    int                         jobId_;
    nebula::cpp2::AdminCmd      cmd_;
    std::vector<std::string>    paras_;
    nebula::kvstore::KVStore*   kvStore_;
    AdminClient*                adminClient_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_SIMPLECONCURRENTJOBEXECUTOR_H_
