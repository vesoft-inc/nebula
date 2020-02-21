/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/jobMan/SimpleConcurrentJobExecutor.h"
#include <bits/stdint-intn.h>
#include "meta/MetaServiceUtils.h"
#include "meta/processors/Common.h"
#include "meta/processors/admin/AdminClient.h"

namespace nebula {
namespace meta {

SimpleConcurrentJobExecutor::
SimpleConcurrentJobExecutor(nebula::cpp2::AdminCmd cmd,
                            std::vector<std::string> params) :
                            cmd_(cmd),
                            params_(params) {
}

// void SimpleConcurrentJobExecutor::prepare() {}

bool SimpleConcurrentJobExecutor::execute(int spaceId,
                                          int jobId,
                                          const std::vector<std::string>& jobParas,
                                          nebula::kvstore::KVStore* kvStore,
                                          nebula::thread::GenericThreadPool* pool) {
    UNUSED(kvStore);
    if (jobParas.empty()) {
        LOG(ERROR) << "SimpleConcurrentJob should have a para";
        return false;
    }

    std::unique_ptr<kvstore::KVIterator> iter;
    auto partPrefix = MetaServiceUtils::partPrefix(spaceId);
    auto ret = kvStore->prefix(kDefaultSpaceId, kDefaultPartId, partPrefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Fetch Parts Failed";
        return false;
    }

    struct HostAddrCmp {
        bool operator()(const nebula::cpp2::HostAddr& a,
                        const nebula::cpp2::HostAddr& b) const {
            if (a.get_ip() == b.get_ip()) {
                return a.get_port() < b.get_port();
            }
            return a.get_ip() < b.get_ip();
        }
    };
    std::set<nebula::cpp2::HostAddr, HostAddrCmp> hosts;
    while (iter->valid()) {
        for (auto &host : MetaServiceUtils::parsePartVal(iter->val())) {
            hosts.insert(host);
        }
        iter->next();
    }

    std::vector<folly::SemiFuture<bool>> futures;
    size_t taskId = 0;
    for (auto& host : hosts) {
        UNUSED(host);
        // UNUSED(jobId);
        auto dispatcher = [jobId, kvStore]() {
            // static const char *tmp = "http://%s:%d/admin?op=%s&space=%s";

            std::unique_ptr<AdminClient> client(new AdminClient(kvStore));
            // auto strIP = network::NetworkUtils::intToIPv4(host.get_ip());
            // auto url = folly::stringPrintf(tmp, strIP.c_str(),
            //                                FLAGS_ws_storage_http_port,
            //                                op.c_str(),
            //                                spaceName.c_str());
            // LOG(INFO) << "make admin url: " << url << ", iTask=" << iTask;
            // TaskDescription taskDesc(iJob, iTask, host);
            // save(taskDesc.taskKey(), taskDesc.taskVal());

            // auto httpResult = nebula::http::HttpClient::get(url, "-GSs");
            // bool suc = httpResult.ok() && httpResult.value() == "ok";

            // if (suc) {
            //     taskDesc.setStatus(cpp2::JobStatus::FINISHED);
            // } else {
            //     taskDesc.setStatus(cpp2::JobStatus::FAILED);
            // }

            // save(taskDesc.taskKey(), taskDesc.taskVal());
            // return suc;
            return false;
        };
        ++taskId;
        auto future = pool->addTask(dispatcher);
        futures.push_back(std::move(future));
    }

    bool success = false;
    folly::collectAll(std::move(futures))
        .thenValue([&](const std::vector<folly::Try<bool>>& tries) {
            for (const auto& t : tries) {
                if (t.hasException()) {
                    LOG(ERROR) << "admin Failed: " << t.exception();
                    success = false;
                    break;
                }
            }
        }).thenError([&](auto&& e) {
            LOG(ERROR) << "admin Failed: " << e.what();
            success = false;
        }).wait();
    // LOG(INFO) << folly::stringPrintf("admin job %d %s, descrtion: %s %s",
    //                                  iJob,
    //                                  successfully ? "succeeded" : "failed",
    //                                  op.c_str(),
    //                                  folly::join(" ", jobDesc.getParas()).c_str());
    return success;
}

void SimpleConcurrentJobExecutor::stop() {
}

}  // namespace meta
}  // namespace nebula
