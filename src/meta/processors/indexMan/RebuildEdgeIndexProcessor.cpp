/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/ActiveHostsMan.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/indexMan/RebuildEdgeIndexProcessor.h"

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace meta {

void RebuildEdgeIndexProcessor::process(const cpp2::RebuildIndexReq& req) {
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

    LOG(INFO) << "Rebuild Edge Index Space " << space << ", Index Name " << indexName;
    const auto& hostPrefix = MetaServiceUtils::leaderPrefix();
    std::unique_ptr<kvstore::KVIterator> leaderIter;
    auto kvRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, hostPrefix, &leaderIter);
    if (kvRet != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Get space " << space << "'s part failed";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    folly::SharedMutex::ReadHolder indexHolder(LockUtils::edgeIndexLock());
    auto edgeIndexIDResult = getIndexID(space, indexName);
    if (!edgeIndexIDResult.ok()) {
        LOG(ERROR) << "Edge Index " << indexName << " Not Found";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    auto edgeIndexID = edgeIndexIDResult.value();
    auto edgeKey = MetaServiceUtils::indexKey(space, edgeIndexIDResult.value());
    auto edgeResult = doGet(edgeKey);
    if (!edgeResult.ok()) {
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    auto statusKey = MetaServiceUtils::rebuildIndexStatus(space, 'E', indexName);
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
        auto hostAddrRet = NetworkUtils::toHostAddr(NetworkUtils::intToIPv4(host.get_ip()),
                                                    host.get_port());
        if (!hostAddrRet.ok()) {
            LOG(ERROR) << "Can't cast to host " << host.get_ip() + ":" << host.get_port();
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
                                                        edgeIndexID,
                                                        partIds,
                                                        isOffline);
            results.emplace_back(std::move(future));
        }
        leaderIter->next();
     }

    folly::collectAll(results)
        .thenValue([statusKey, kv = kvstore_] (const auto& tries) mutable {
            for (const auto& t : tries) {
                if (!t.value().ok()) {
                    LOG(ERROR) << "Build Edge Index Failed";
                    if (!MetaCommon::saveRebuildStatus(kv, statusKey, "FAILED")) {
                        LOG(ERROR) << "Save rebuild status failed";
                        return;
                    }
                }
            }

            if (!MetaCommon::saveRebuildStatus(kv, std::move(statusKey), "SUCCEEDED")) {
                LOG(ERROR) << "Save rebuild status failed";
                return;
            }
        })
        .thenError([statusKey, kv = kvstore_] (auto &&e) {
            LOG(ERROR) << "Exception caught: " << e.what();
            if (!MetaCommon::saveRebuildStatus(kv, std::move(statusKey), "FAILED")) {
                LOG(ERROR) << "Save rebuild status failed";
                return;
            }
        });

    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}

}  // namespace meta
}  // namespace nebula


