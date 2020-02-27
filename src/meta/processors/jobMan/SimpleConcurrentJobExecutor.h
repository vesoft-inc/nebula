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
    SimpleConcurrentJobExecutor(nebula::cpp2::AdminCmd cmd,
                                std::vector<std::string> params);
    // virtual void prepare() override;
    ErrorOr<nebula::kvstore::ResultCode, std::map<HostAddr, Status>>
    execute(int spaceId,
            int jobId,
            const std::vector<std::string>& jobParas,
            nebula::kvstore::KVStore* kvStore,
            nebula::thread::GenericThreadPool* pool) override;
    void stop() override;

private:
    nebula::cpp2::AdminCmd cmd_;
    std::vector<std::string> params_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_SIMPLECONCURRENTJOBEXECUTOR_H_
