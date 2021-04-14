/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <memory>
#include <map>
#include "common/network/NetworkUtils.h"
#include "common/interface/gen-cpp2/common_types.h"
#include "meta/ActiveHostsMan.h"
#include "meta/MetaServiceUtils.h"
#include "meta/processors/Common.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/jobMan/CompactJobExecutor.h"
#include "meta/processors/jobMan/FlushJobExecutor.h"
#include "meta/processors/jobMan/MetaJobExecutor.h"
#include "meta/processors/jobMan/RebuildTagJobExecutor.h"
#include "meta/processors/jobMan/RebuildEdgeJobExecutor.h"
#include "meta/processors/jobMan/StatisJobExecutor.h"
#include "meta/processors/jobMan/TaskDescription.h"
#include "utils/Utils.h"

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace meta {

std::unique_ptr<MetaJobExecutor>
MetaJobExecutorFactory::createMetaJobExecutor(const JobDescription& jd,
                                              kvstore::KVStore* store,
                                              AdminClient* client) {
    std::unique_ptr<MetaJobExecutor> ret;
    switch (jd.getCmd()) {
    case cpp2::AdminCmd::COMPACT:
        ret.reset(new CompactJobExecutor(jd.getJobId(),
                                         store,
                                         client,
                                         jd.getParas()));
        break;
    case cpp2::AdminCmd::FLUSH:
        ret.reset(new FlushJobExecutor(jd.getJobId(),
                                       store,
                                       client,
                                       jd.getParas()));
        break;
    case cpp2::AdminCmd::REBUILD_TAG_INDEX:
        ret.reset(new RebuildTagJobExecutor(jd.getJobId(),
                                            store,
                                            client,
                                            jd.getParas()));
        break;
    case cpp2::AdminCmd::REBUILD_EDGE_INDEX:
        ret.reset(new RebuildEdgeJobExecutor(jd.getJobId(),
                                             store,
                                             client,
                                             jd.getParas()));
        break;
    case cpp2::AdminCmd::STATS:
        ret.reset(new StatisJobExecutor(jd.getJobId(),
                                        store,
                                        client,
                                        jd.getParas()));
        break;
    default:
        break;
    }
    return ret;
}

ErrorOr<cpp2::ErrorCode, GraphSpaceID>
MetaJobExecutor::getSpaceIdFromName(const std::string& spaceName) {
    auto indexKey = MetaServiceUtils::indexSpaceKey(spaceName);
    std::string val;
    auto rc = kvstore_->get(kDefaultSpaceId, kDefaultPartId, indexKey, &val);
    if (rc != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Get space ID failed space name: " << spaceName;
        return cpp2::ErrorCode::E_NOT_FOUND;
    }
    return *reinterpret_cast<const GraphSpaceID*>(val.c_str());
}

ErrOrHosts MetaJobExecutor::getTargetHost(GraphSpaceID spaceId) {
    std::unique_ptr<kvstore::KVIterator> iter;
    auto partPrefix = MetaServiceUtils::partPrefix(spaceId);
    auto rc = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, partPrefix, &iter);
    if (rc != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Fetch Parts Failed";
        return cpp2::ErrorCode::E_NOT_FOUND;
    }

    // use vector instead of set because this can convient for next step
    std::vector<std::pair<HostAddr, std::vector<PartitionID>>> hosts;
    while (iter->valid()) {
        auto targets = MetaServiceUtils::parsePartVal(iter->val());
        for (auto& target : targets) {
            std::vector<PartitionID> parts;
            hosts.emplace_back(std::make_pair(std::move(target), std::move(parts)));
        }
        iter->next();
    }
    std::sort(hosts.begin(), hosts.end());
    auto last = std::unique(hosts.begin(), hosts.end());
    hosts.erase(last, hosts.end());
    return hosts;
}

ErrOrHosts MetaJobExecutor::getLeaderHost(GraphSpaceID space) {
    const auto& hostPrefix = MetaServiceUtils::leaderPrefix();
    std::unique_ptr<kvstore::KVIterator> leaderIter;
    auto result = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, hostPrefix, &leaderIter);
    if (result != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Get space " << space << "'s part failed";
        return cpp2::ErrorCode::E_NOT_FOUND;
    }

    std::vector<std::pair<HostAddr, std::vector<PartitionID>>> hosts;
    while (leaderIter->valid()) {
        auto hostAddr = MetaServiceUtils::parseLeaderKey(leaderIter->key());
        if (hostAddr.host == "") {
            LOG(ERROR) << "leader key parse to empty string";
            return cpp2::ErrorCode::E_INVALID_PARM;
        }

        if (ActiveHostsMan::isLived(kvstore_, hostAddr)) {
            auto leaderParts = MetaServiceUtils::parseLeaderVal(leaderIter->val());
            auto parts = leaderParts[space];
            hosts.emplace_back(std::make_pair(std::move(hostAddr), std::move(parts)));
        }
        leaderIter->next();
    }
    return hosts;
}

cpp2::ErrorCode MetaJobExecutor::execute() {
    ErrOrHosts addressesRet;
    if (toLeader_) {
        addressesRet = getLeaderHost(space_);
    } else {
        addressesRet = getTargetHost(space_);
    }

    if (!nebula::ok(addressesRet)) {
        LOG(ERROR) << "Can't get hosts";
        return cpp2::ErrorCode::E_NO_HOSTS;
    }

    std::vector<PartitionID> parts;
    auto addresses = nebula::value(addressesRet);

    // write all tasks first.
    for (auto i = 0U; i != addresses.size(); ++i) {
        TaskDescription task(jobId_, i, addresses[i].first);
        std::vector<kvstore::KV> data{{task.taskKey(), task.taskVal()}};
        folly::Baton<true, std::atomic> baton;
        auto rc = nebula::kvstore::ResultCode::SUCCEEDED;
        kvstore_->asyncMultiPut(kDefaultSpaceId,
                                kDefaultPartId,
                                std::move(data),
                                [&](nebula::kvstore::ResultCode code) {
                                    rc = code;
                                    baton.post();
                                });
        baton.wait();
        if (rc != nebula::kvstore::ResultCode::SUCCEEDED) {
            LOG(INFO) << "write to kv store failed. E_STORE_FAILURE";
            return cpp2::ErrorCode::E_STORE_FAILURE;
        }
    }

    std::vector<folly::SemiFuture<Status>> futs;
    for (auto& address : addresses) {
        // transform to the admin host
        auto h = Utils::getAdminAddrFromStoreAddr(address.first);
        futs.emplace_back(executeInternal(std::move(h), std::move(address.second)));
    }

    auto rc = cpp2::ErrorCode::SUCCEEDED;
    auto tries = folly::collectAll(std::move(futs)).get();
    for (auto& t : tries) {
        if (t.hasException()) {
            LOG(ERROR) << t.exception().what();
            rc = cpp2::ErrorCode::E_RPC_FAILURE;
            continue;
        }
        if (!t.value().ok()) {
            LOG(ERROR) << t.value().toString();
            rc = cpp2::ErrorCode::E_RPC_FAILURE;
            continue;
        }
    }
    return rc;
}

}  // namespace meta
}  // namespace nebula
