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
    SimpleConcurrentJobExecutor(int jobId,
                                nebula::cpp2::AdminCmd cmd,
                                std::vector<std::string> paras,
                                nebula::kvstore::KVStore* kvStore) :
                                jobId_(jobId),
                                cmd_(cmd),
                                paras_(paras),
                                kvStore_(kvStore) {}

    ExecuteRet execute() override;

    void stop() override;

private:
    int jobId_;
    nebula::cpp2::AdminCmd cmd_;
    int spaceId_;
    std::vector<std::string> paras_;
    nebula::kvstore::KVStore* kvStore_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_SIMPLECONCURRENTJOBEXECUTOR_H_
