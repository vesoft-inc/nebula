/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/jobMan/SimpleConcurrentJobExecutor.h"
#include "meta/MetaServiceUtils.h"
#include "meta/processors/Common.h"
#include "meta/processors/admin/AdminClient.h"

namespace nebula {
namespace meta {

SimpleConcurrentJobExecutor::
SimpleConcurrentJobExecutor(int jobId,
                            cpp2::AdminCmd cmd,
                            std::vector<std::string> paras,
                            nebula::kvstore::KVStore* kvStore,
                            AdminClient* adminClient)
                        : jobId_(jobId)
                        , cmd_(cmd)
                        , paras_(paras)
                        , kvStore_(kvStore)
                        , adminClient_(adminClient) {}

ErrorOr<nebula::kvstore::ResultCode, std::map<HostAddr, Status>>
SimpleConcurrentJobExecutor::execute() {
    if (paras_.empty()) {
        LOG(ERROR) << "SimpleConcurrentJob should have a para";
        return nebula::kvstore::ResultCode::ERR_INVALID_ARGUMENT;
    }

    std::string spaceName = paras_.back();
    auto errOrSpaceId = getSpaceIdFromName(spaceName);
    if (!nebula::ok(errOrSpaceId)) {
        return nebula::error(errOrSpaceId);
    }
    int32_t spaceId = nebula::value(errOrSpaceId);

    auto errOrTargetHost = getTargetHost(spaceId);
    if (!nebula::ok(errOrTargetHost)) {
        return nebula::error(errOrTargetHost);
    }
    std::vector<HostAddr>& hosts = nebula::value(errOrTargetHost);
    int32_t concurrency = INT_MAX;
    if (paras_.size() > 1) {
        concurrency = std::atoi(paras_[0].c_str());
    }

    std::vector<folly::SemiFuture<Status>> futures;
    int taskId = 0;
    std::vector<PartitionID> parts;
    for (auto& host : hosts) {
        auto future = adminClient_->addTask(cmd_, jobId_, taskId++, spaceId,
                                            {host}, {}, parts, concurrency);
        futures.push_back(std::move(future));
    }

    std::vector<Status> results;
    nebula::Status errorStatus;
    folly::collectAll(std::move(futures))
        .thenValue([&](const std::vector<folly::Try<Status>>& tries) {
            Status status;
            for (const auto& t : tries) {
                if (t.hasException()) {
                    LOG(ERROR) << "admin Failed: " << t.exception();
                    results.push_back(nebula::Status::Error());
                } else {
                    results.push_back(t.value());
                }
            }
        }).thenError([&](auto&& e) {
            LOG(ERROR) << "admin Failed: " << e.what();
            errorStatus = Status::Error(e.what());
        }).wait();

    if (hosts.size() != results.size()) {
        LOG(ERROR) << "return ERR_UNKNOWN: hosts.size()=" << hosts.size()
                   << ", results.size()=" << results.size();
        return nebula::kvstore::ResultCode::ERR_UNKNOWN;
    }

    std::map<HostAddr, Status> ret;
    for (size_t i = 0; i != hosts.size(); ++i) {
        ret.insert(std::make_pair(hosts[i], results[i]));
    }
    return ret;
}

void SimpleConcurrentJobExecutor::stop() {
    std::string spaceName = paras_.back();
    auto errOrSpaceId = getSpaceIdFromName(spaceName);
    if (!nebula::ok(errOrSpaceId)) {
        return;
    }
    int32_t spaceId = nebula::value(errOrSpaceId);

    auto errOrTargetHost = getTargetHost(spaceId);
    if (!nebula::ok(errOrTargetHost)) {
        return;
    }
    std::vector<HostAddr>& hosts = nebula::value(errOrTargetHost);
    for (auto& host : hosts) {
        adminClient_->stopTask({host}, jobId_, 0);
    }
}

ErrorOr<nebula::kvstore::ResultCode, int32_t>
SimpleConcurrentJobExecutor::getSpaceIdFromName(const std::string& spaceName) {
    auto indexKey = MetaServiceUtils::indexSpaceKey(spaceName);
    std::string val;
    auto rc = kvStore_->get(kDefaultSpaceId, kDefaultPartId, indexKey, &val);
    if (rc != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "KVStore get indexKey error spaceName: " << spaceName;
        return rc;
    }
    return *reinterpret_cast<const int*>(val.c_str());
}

ErrorOr<nebula::kvstore::ResultCode, std::vector<HostAddr>>
SimpleConcurrentJobExecutor::getTargetHost(int32_t spaceId) {
    std::unique_ptr<kvstore::KVIterator> iter;
    auto partPrefix = MetaServiceUtils::partPrefix(spaceId);
    auto rc = kvStore_->prefix(kDefaultSpaceId, kDefaultPartId, partPrefix, &iter);
    if (rc != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Fetch Parts Failed";
        return rc;
    }

    // use vector instead of set because this can convient for next step
    std::vector<HostAddr> hosts;
    while (iter->valid()) {
        auto target = MetaServiceUtils::parsePartVal(iter->val());
        hosts.insert(hosts.end(), target.begin(), target.end());
        iter->next();
    }
    std::sort(hosts.begin(), hosts.end());
    auto last = std::unique(hosts.begin(), hosts.end());
    hosts.erase(last, hosts.end());
    return hosts;
}

}  // namespace meta
}  // namespace nebula
