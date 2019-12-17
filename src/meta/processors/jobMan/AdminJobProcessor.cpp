/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/jobMan/AdminJobProcessor.h"
#include "meta/processors/jobMan/JobManager.h"

namespace nebula {
namespace meta {

void AdminJobProcessor::process(const cpp2::AdminJobReq& req) {
    std::vector<std::string> paras;
    for (auto& p : req.get_paras()) {
        paras.emplace_back(p);
    }

    auto job_mgr = KVJobManager::getInstance();
    auto ret = job_mgr->runJob(req.get_op(), paras);
    if (!ok(ret)) {
        resp_.set_code(to(error(ret)));
        onFinished();
        return;
    }
    resp_.set_result(std::move(value(ret)));
    onFinished();
}

}  // namespace meta
}  // namespace nebula

