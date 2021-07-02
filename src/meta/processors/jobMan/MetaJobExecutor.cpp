/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/network/NetworkUtils.h"
#include "common/interface/gen-cpp2/common_types.h"
#include "meta/ActiveHostsMan.h"
#include "meta/common/MetaCommon.h"
#include "meta/MetaServiceUtils.h"
#include "meta/processors/Common.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/jobMan/CompactJobExecutor.h"
#include "meta/processors/jobMan/FlushJobExecutor.h"
#include "meta/processors/jobMan/MetaJobExecutor.h"
#include "meta/processors/jobMan/RebuildTagJobExecutor.h"
#include "meta/processors/jobMan/RebuildEdgeJobExecutor.h"
#include "meta/processors/jobMan/RebuildFTJobExecutor.h"
#include "meta/processors/jobMan/StatisJobExecutor.h"
#include "meta/processors/jobMan/TaskDescription.h"
#include "utils/Utils.h"

DECLARE_int32(heartbeat_interval_secs);
DECLARE_uint32(expired_time_factor);

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
    case cpp2::AdminCmd::REBUILD_FULLTEXT_INDEX:
        ret.reset(new RebuildFTJobExecutor(jd.getJobId(),
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

ErrorOr<nebula::cpp2::ErrorCode, GraphSpaceID>
MetaJobExecutor::getSpaceIdFromName(const std::string& spaceName) {
    auto indexKey = MetaServiceUtils::indexSpaceKey(spaceName);
    std::string val;
    auto retCode = kvstore_->get(kDefaultSpaceId, kDefaultPartId, indexKey, &val);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Get space failed, space name: " << spaceName << " error: "
                   << apache::thrift::util::enumNameSafe(retCode);
        return retCode;
    }
    return *reinterpret_cast<const GraphSpaceID*>(val.c_str());
}

ErrOrHosts MetaJobExecutor::getTargetHost(GraphSpaceID spaceId) {
    std::unique_ptr<kvstore::KVIterator> iter;
    const auto& partPrefix = MetaServiceUtils::partPrefix(spaceId);
    auto retCode = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, partPrefix, &iter);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Fetch Parts Failed, error: "
                   << apache::thrift::util::enumNameSafe(retCode);
        return retCode;
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
    const auto& hostPrefix = MetaServiceUtils::leaderPrefix(space);
    std::unique_ptr<kvstore::KVIterator> leaderIter;
    auto retCode = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, hostPrefix, &leaderIter);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Get space " << space << "'s part failed, error: "
                   << apache::thrift::util::enumNameSafe(retCode);
        return retCode;
    }

    std::vector<std::pair<HostAddr, std::vector<PartitionID>>> hosts;
    HostAddr host;
    nebula::cpp2::ErrorCode code;
    for (; leaderIter->valid(); leaderIter->next()) {
        auto spaceAndPart = MetaServiceUtils::parseLeaderKeyV3(leaderIter->key());
        auto partId = spaceAndPart.second;
        std::tie(host, std::ignore, code) = MetaServiceUtils::parseLeaderValV3(leaderIter->val());
        if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
            continue;
        }
        auto it = std::find_if(hosts.begin(), hosts.end(), [&](auto& item){
            return item.first == host;
        });
        if (it == hosts.end()) {
            hosts.emplace_back(std::make_pair(host, std::vector<PartitionID>{partId}));
        } else {
            it->second.emplace_back(partId);
        }
    }
    return hosts;
}

ErrOrHosts MetaJobExecutor::getListenerHost(GraphSpaceID space, cpp2::ListenerType type) {
    const auto& prefix = MetaServiceUtils::listenerPrefix(space, type);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Get space " << space << "'s listener failed, error: "
                   << apache::thrift::util::enumNameSafe(ret);
        return ret;
    }

    auto activeHostsRet = ActiveHostsMan::getActiveHosts(
        kvstore_,
        FLAGS_heartbeat_interval_secs * FLAGS_expired_time_factor,
        cpp2::HostRole::LISTENER);
    if (!nebula::ok(activeHostsRet)) {
        return nebula::error(activeHostsRet);
    }

    auto activeHosts = std::move(nebula::value(activeHostsRet));
    std::vector<std::pair<HostAddr, std::vector<PartitionID>>> hosts;

    while (iter->valid()) {
        auto host = MetaServiceUtils::deserializeHostAddr(iter->val());
        auto part = MetaServiceUtils::parseListenerPart(iter->key());
        if (std::find(activeHosts.begin(), activeHosts.end(), host) == activeHosts.end()) {
            LOG(ERROR) << "Invalid host : " << network::NetworkUtils::toHostsStr({host});
            return nebula::cpp2::ErrorCode::E_INVALID_HOST;
        }
        auto it = std::find_if(hosts.begin(), hosts.end(), [&host](auto& item){
            return item.first == host;
        });
        if (it == hosts.end()) {
            hosts.emplace_back(std::make_pair(host, std::vector<PartitionID>{part}));
        } else {
            it->second.emplace_back(part);
        }
        iter->next();
    }
    if (hosts.empty()) {
       return nebula::cpp2::ErrorCode::E_LISTENER_NOT_FOUND;
    }
    return hosts;
}

nebula::cpp2::ErrorCode MetaJobExecutor::execute() {
    ErrOrHosts addressesRet;
    switch (toHost_) {
        case TargetHosts::LEADER: {
            addressesRet = getLeaderHost(space_);
            break;
        }
        case TargetHosts::LISTENER: {
            addressesRet = getListenerHost(space_,  cpp2::ListenerType::ELASTICSEARCH);
            break;
        }
        case TargetHosts::DEFAULT: {
            addressesRet = getTargetHost(space_);
            break;
        }
    }

    if (!nebula::ok(addressesRet)) {
        LOG(ERROR) << "Can't get hosts";
        return nebula::error(addressesRet);
    }

    std::vector<PartitionID> parts;
    auto addresses = nebula::value(addressesRet);

    // write all tasks first.
    for (auto i = 0U; i != addresses.size(); ++i) {
        TaskDescription task(jobId_, i, addresses[i].first);
        std::vector<kvstore::KV> data{{task.taskKey(), task.taskVal()}};
        folly::Baton<true, std::atomic> baton;
        auto rc = nebula::cpp2::ErrorCode::SUCCEEDED;
        kvstore_->asyncMultiPut(kDefaultSpaceId,
                                kDefaultPartId,
                                std::move(data),
                                [&](nebula::cpp2::ErrorCode code) {
                                    rc = code;
                                    baton.post();
                                });
        baton.wait();
        if (rc != nebula::cpp2::ErrorCode::SUCCEEDED) {
            LOG(INFO) << "write to kv store failed, error: "
                      << apache::thrift::util::enumNameSafe(rc);
            return rc;
        }
    }

    std::vector<folly::SemiFuture<Status>> futs;
    for (auto& address : addresses) {
        // transform to the admin host
        auto h = Utils::getAdminAddrFromStoreAddr(address.first);
        futs.emplace_back(executeInternal(std::move(h), std::move(address.second)));
    }

    auto rc = nebula::cpp2::ErrorCode::SUCCEEDED;
    auto tries = folly::collectAll(std::move(futs)).get();
    for (auto& t : tries) {
        if (t.hasException()) {
            LOG(ERROR) << t.exception().what();
            rc = nebula::cpp2::ErrorCode::E_RPC_FAILURE;
            continue;
        }
        if (!t.value().ok()) {
            LOG(ERROR) << t.value().toString();
            rc = nebula::cpp2::ErrorCode::E_RPC_FAILURE;
            continue;
        }
    }
    return rc;
}

}  // namespace meta
}  // namespace nebula
