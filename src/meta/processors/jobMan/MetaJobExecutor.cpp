/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <memory>
#include <map>
#include "meta/processors/jobMan/MetaJobExecutor.h"
#include "meta/processors/jobMan/SimpleConcurrentJobExecutor.h"
#include "meta/processors/jobMan/RebuildTagJobExecutor.h"
#include "meta/processors/jobMan/RebuildEdgeJobExecutor.h"
#include "interface/gen-cpp2/common_types.h"

namespace nebula {
namespace meta {

enum class JobCmdEnum {
    COMPACT,
    FLUSH,
    REBUILD_TAG_INDEX,
    REBUILD_EDGE_INDEX
};

nebula::cpp2::AdminCmd toAdminCmd(const std::string& cmd) {
    static std::map<std::string, nebula::cpp2::AdminCmd> mapping {
        {"compact", nebula::cpp2::AdminCmd::COMPACT},
        {"flush", nebula::cpp2::AdminCmd::FLUSH},
    };
    if (mapping.count(cmd) == 0) {
        return nebula::cpp2::AdminCmd::INVALID;
    }
    return mapping[cmd];
}

std::unique_ptr<MetaJobExecutor>
MetaJobExecutorFactory::createMetaJobExecutor(const JobDescription& description,
                                              kvstore::KVStore* store,
                                              AdminClient* adminClient) {
    std::unique_ptr<MetaJobExecutor> executor;

    nebula::cpp2::AdminCmd cmd = toAdminCmd(description.getCmd());
    switch (cmd) {
    case nebula::cpp2::AdminCmd::COMPACT:
    case nebula::cpp2::AdminCmd::FLUSH:
        executor.reset(new SimpleConcurrentJobExecutor(description.getJobId(),
                                                       cmd,
                                                       description.getParas(),
                                                       store));
        break;
    case nebula::cpp2::AdminCmd::REBUILD_TAG_INDEX:
        executor.reset(new RebuildTagJobExecutor(description.getJobId(),
                                                 cmd,
                                                 description.getParas(),
                                                 store));
        break;
    case nebula::cpp2::AdminCmd::REBUILD_EDGE_INDEX:
        executor.reset(new RebuildEdgeJobExecutor(description.getJobId(),
                                                  cmd,
                                                  description.getParas(),
                                                  store));
        break;
    default:
        break;
    }
    return executor;
}



}  // namespace meta
}  // namespace nebula
