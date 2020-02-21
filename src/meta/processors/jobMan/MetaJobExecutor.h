/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_METAJOBEXECUTOR_H_
#define META_METAJOBEXECUTOR_H_

#include <memory>
#include <string>
#include <vector>
#include "meta/processors/jobMan/JobDescription.h"
#include "kvstore/KVStore.h"

namespace nebula {
namespace meta {

class MetaJobExecutor {
public:
    MetaJobExecutor() = default;
    // virtual void prepare() {};
    virtual bool execute(int spaceId,
                         int jobId,
                         const std::vector<std::string>& jobParas,
                         nebula::kvstore::KVStore* kvStore,
                         nebula::thread::GenericThreadPool* pool) {
        UNUSED(spaceId);
        UNUSED(jobId);
        UNUSED(jobParas);
        UNUSED(kvStore);
        UNUSED(pool);
        return false;
    }
    virtual void stop() {}
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
