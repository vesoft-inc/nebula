/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_METAJOBEXECUTOR_H_
#define META_METAJOBEXECUTOR_H_

#include "base/ErrorOr.h"
#include "kvstore/KVStore.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/jobMan/JobDescription.h"

namespace nebula {
namespace meta {

class MetaJobExecutor {
public:
    using ExecuteRet = ErrorOr<kvstore::ResultCode, std::map<HostAddr, Status>>;

    MetaJobExecutor() = default;

    virtual ExecuteRet execute() = 0;
    virtual void stop() = 0;
    virtual ~MetaJobExecutor() = default;

protected:
    int32_t jobId_{INT_MIN};
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
