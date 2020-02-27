/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_METAJOBEXECUTOR_H_
#define META_METAJOBEXECUTOR_H_

#include "base/ErrorOr.h"
#include "kvstore/KVStore.h"
#include "meta/processors/jobMan/JobDescription.h"

namespace nebula {
namespace meta {

class MetaJobExecutor {
public:
    using HostAddrAndStatus = std::pair<HostAddr, Status>;

    MetaJobExecutor() = default;
    // virtual void prepare() {};
    virtual ErrorOr<nebula::kvstore::ResultCode, std::map<HostAddr, Status>>
    execute(int spaceId,
            int jobId,
            const std::vector<std::string>& jobParas,
            nebula::kvstore::KVStore* kvStore,
            nebula::thread::GenericThreadPool* pool) = 0;
    virtual void stop() = 0;
    virtual ~MetaJobExecutor() = default;

protected:
    int32_t jobId_{INT_MIN};
};

class MetaJobExecutorFactory {
public:
    static std::unique_ptr<MetaJobExecutor> createMetaJobExecutor(const JobDescription& jd);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_METAJOBEXECUTOR_H_
