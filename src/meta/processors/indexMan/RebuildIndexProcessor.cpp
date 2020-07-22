/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/ActiveHostsMan.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/indexMan/RebuildIndexProcessor.h"

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace meta {

void RebuildIndexProcessor::processInternal(const cpp2::RebuildIndexReq& req) {
    auto space = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(space);
    const auto &indexName = req.get_index_name();

    LOG(INFO) << "Rebuild Index Space " << space << ", Index Name " << indexName;
    const auto& hostPrefix = MetaServiceUtils::leaderPrefix();
    std::unique_ptr<kvstore::KVIterator> leaderIter;
    auto leaderRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, hostPrefix, &leaderIter);
    if (leaderRet != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Get space " << space << "'s part failed";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    auto indexIDResult = getIndexID(space, indexName);
    if (!indexIDResult.ok()) {
        LOG(ERROR) << "Index " << indexName << " Not Found";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    auto indexID = indexIDResult.value();
    auto statusKey = MetaServiceUtils::rebuildIndexStatus(space, category_, indexName);
    if (!MetaCommon::saveRebuildStatus(kvstore_, statusKey, "RUNNING")) {
        LOG(ERROR) << "Save rebuild status failed";
        resp_.set_code(cpp2::ErrorCode::E_STORE_FAILURE);
        onFinished();
        return;
    }

    std::vector<folly::Future<Status>> results;
    auto activeHosts = ActiveHostsMan::getActiveHosts(kvstore_, FLAGS_heartbeat_interval_secs + 1);
    while (leaderIter->valid()) {
        auto hostAddr = MetaServiceUtils::parseLeaderKey(leaderIter->key());
        if (hostAddr.host == "") {
            LOG(ERROR) << "leader key parse to empty string";
            resp_.set_code(cpp2::ErrorCode::E_STORE_FAILURE);
            onFinished();
            return;
        }

        if (std::find(activeHosts.begin(), activeHosts.end(), hostAddr) != activeHosts.end()) {
            auto leaderParts = MetaServiceUtils::parseLeaderVal(leaderIter->val());
            auto& partIds = leaderParts[space];
            auto future = caller(hostAddr, space, indexID, std::move(partIds), true);
            results.emplace_back(std::move(future));
        }
        leaderIter->next();
    }

    handleRebuildIndexResult(std::move(results), kvstore_, std::move(statusKey));
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}

void RebuildIndexProcessor::handleRebuildIndexResult(std::vector<folly::Future<Status>> results,
                                                     kvstore::KVStore* kvstore,
                                                     std::string statusKey) {
    folly::collectAll(std::move(results))
        .thenValue([statusKey, kvstore] (const auto& tries) mutable {
            for (const auto& t : tries) {
                if (!t.value().ok()) {
                    LOG(ERROR) << "Build Edge Index Failed";
                    if (!MetaCommon::saveRebuildStatus(kvstore, statusKey, "FAILED")) {
                        LOG(ERROR) << "Save rebuild status failed";
                        return;
                    }
                }
            }

            if (!MetaCommon::saveRebuildStatus(kvstore, std::move(statusKey), "SUCCEEDED")) {
                LOG(ERROR) << "Save rebuild status failed";
                return;
            }
        })
        .thenError([statusKey, kvstore] (auto &&e) {
            LOG(ERROR) << "Exception caught: " << e.what();
            if (!MetaCommon::saveRebuildStatus(kvstore, std::move(statusKey), "FAILED")) {
                LOG(ERROR) << "Save rebuild status failed";
                return;
            }
        });
}

}  // namespace meta
}  // namespace nebula
