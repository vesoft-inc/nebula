/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/jobMan/MetaJobExecutor.h"
#include "meta/processors/jobMan/SimpleConcurrentJobExecutor.h"
#include <memory>
#include "interface/gen-cpp2/common_types.h"

namespace nebula {
namespace meta {

enum class JobCmdEnum {
    COMPACT,
    FLUSH,
    REBUILD_INDEX
};

nebula::cpp2::AdminCmd toAdminCmd(const std::string& cmd) {
    UNUSED(cmd);
    LOG(ERROR) << "this code should be removed and string cmd need to change";
    return nebula::cpp2::AdminCmd::COMPACT;
}

std::unique_ptr<MetaJobExecutor>
MetaJobExecutorFactory::createMetaJobExecutor(const JobDescription& jd) {
    std::unique_ptr<MetaJobExecutor> ret;

    nebula::cpp2::AdminCmd cmd = toAdminCmd(jd.getCmd());
    switch (cmd) {
    case nebula::cpp2::AdminCmd::COMPACT:
    case nebula::cpp2::AdminCmd::FLUSH:
        ret.reset(new SimpleConcurrentJobExecutor(cmd, jd.getParas()));
        break;
    case nebula::cpp2::AdminCmd::REBUILD_INDEX:
        break;
    default:
        break;
    }
    return ret;
}

}  // namespace meta
}  // namespace nebula
