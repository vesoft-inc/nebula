/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/ActiveHostsMan.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/indexMan/RebuildTagIndexProcessor.h"

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace meta {

void RebuildTagIndexProcessor::process(const cpp2::RebuildIndexReq& req) {
    auto space = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(space);
    const auto &indexName = req.get_index_name();
    auto isOffline = req.get_is_offline();
    if (!isOffline) {
        LOG(ERROR) << "Currently not support online rebuild index";
        resp_.set_code(cpp2::ErrorCode::E_UNSUPPORTED);
        onFinished();
        return;
    }

    LOG(INFO) << "Rebuild Tag Index Space " << space << ", Index Name " << indexName;
    const auto& hostPrefix = MetaServiceUtils::leaderPrefix();
    std::unique_ptr<kvstore::KVIterator> leaderIter;
    auto kvRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, hostPrefix, &leaderIter);
    if (kvRet != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Get space " << space << "'s part failed";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    folly::SharedMutex::ReadHolder indexHolder(LockUtils::tagIndexLock());
    auto tagIndexIDResult = getIndexID(space, indexName);
    if (!tagIndexIDResult.ok()) {
        LOG(ERROR) << "Tag Index " << indexName << " Not Found";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    auto tagIndexID = tagIndexIDResult.value();
    auto statusKey = MetaServiceUtils::rebuildIndexStatus(space, 'T', indexName);
    if (!MetaCommon::saveRebuildStatus(kvstore_, statusKey, "RUNNING")) {
        LOG(ERROR) << "Save rebuild status failed";
        resp_.set_code(cpp2::ErrorCode::E_STORE_FAILURE);
        onFinished();
        return;
    }

    std::vector<folly::Future<Status>> results;
    auto activeHosts = ActiveHostsMan::getActiveHosts(kvstore_, FLAGS_heartbeat_interval_secs + 1);
    while (leaderIter->valid()) {
        auto host = MetaServiceUtils::parseLeaderKey(leaderIter->key());
        auto ip = NetworkUtils::intToIPv4(host.get_ip());
        auto port = host.get_port();
        auto hostAddrRet = NetworkUtils::toHostAddr(ip, port);
        if (!hostAddrRet.ok()) {
            LOG(ERROR) << "Can't cast to host " << ip + ":" << port;
            resp_.set_code(cpp2::ErrorCode::E_STORE_FAILURE);
            onFinished();
            return;
        }

        auto hostAddr = hostAddrRet.value();
        if (std::find(activeHosts.begin(), activeHosts.end(),
                      HostAddr(host.ip, host.port)) != activeHosts.end()) {
            auto leaderParts = MetaServiceUtils::parseLeaderVal(leaderIter->val());
            auto& partIds = leaderParts[space];
            auto future = adminClient_->rebuildTagIndex(hostAddr,
                                                        space,
                                                        tagIndexID,
                                                        partIds,
                                                        isOffline);
            results.emplace_back(std::move(future));
        }
        leaderIter->next();
    }

    handleRebuildIndexResult(std::move(results), kvstore_, std::move(statusKey));
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}

}  // namespace meta
}  // namespace nebula
