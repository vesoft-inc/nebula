/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/ActiveHostsMan.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/indexMan/RebuildEdgeIndexProcessor.h"

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
    std::unique_ptr<AdminClient> client(new AdminClient(kvstore_));
    auto partsRet = client->getLeaderDist(space).get();
    if (!partsRet.ok()) {
        LOG(ERROR) << "Get space " << space << "'s part failed";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    auto parts = partsRet.value();

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

    std::vector<folly::Future<Status>> results;
    auto statusKey = MetaServiceUtils::rebuildIndexStatus(space, 'E', indexName);
    for (auto iter = parts.begin(); iter != parts.end(); iter++) {
        auto future = client->rebuildEdgeIndex(iter->first,
                                               space,
                                               edgeIndexID,
                                               iter->second,
                                               isOffline);
        results.emplace_back(std::move(future));
    }

    folly::collectAll(results)
        .thenValue([statusKey, kv = kvstore_] (const auto& tries) mutable {
            for (const auto& t : tries) {
                if (!t.value().ok()) {
                    LOG(ERROR) << "Build Edge Index Failed";
                    if (!MetaCommon::saveRebuildStatus(kv, std::move(statusKey), "FAILED")) {
                        return;
                    }
                }
            }

            if (!MetaCommon::saveRebuildStatus(kv, statusKey, "SUCCEEDED")) {
                return;
            }
        })
        .thenError([statusKey, kv = kvstore_] (auto &&e) {
            LOG(ERROR) << "Exception caught: " << e.what();
            if (!MetaCommon::saveRebuildStatus(kv, std::move(statusKey), "FAILED")) {
                return;
            }
        });

    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}

}  // namespace meta
}  // namespace nebula


