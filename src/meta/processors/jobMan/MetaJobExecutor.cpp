/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/jobMan/MetaJobExecutor.h"
#include "meta/processors/jobMan/SimpleConcurrentJobExecutor.h"
#include <memory>
#include <map>
#include "interface/gen-cpp2/common_types.h"

namespace nebula {
namespace meta {

std::unique_ptr<MetaJobExecutor>
MetaJobExecutorFactory::createMetaJobExecutor(const JobDescription& jd,  kvstore::KVStore* store) {
    std::unique_ptr<MetaJobExecutor> ret;

    auto cmd = jd.getCmd();
    switch (cmd) {
    case nebula::cpp2::AdminCmd::COMPACT:
    case nebula::cpp2::AdminCmd::FLUSH:
        ret.reset(new SimpleConcurrentJobExecutor(jd.getJobId(), cmd, jd.getParas(),
                                                  store, jd.getAdminClient()));
        break;
    case nebula::cpp2::AdminCmd::REBUILD_TAG_INDEX:
    case nebula::cpp2::AdminCmd::REBUILD_EDGE_INDEX:
        break;
    default:
        break;
    }
    return ret;
}

}  // namespace meta
}  // namespace nebula
