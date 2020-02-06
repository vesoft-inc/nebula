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
    const auto &indexName = req.get_index_name();

    LOG(INFO) << "Rebuild Edge Index Space " << space << ", Index Name " << indexName;

    folly::SharedMutex::ReadHolder rHolder(LockUtils::edgeIndexLock());
    std::unique_ptr<AdminClient> client(new AdminClient(kvstore_));
    auto spaceKey = MetaServiceUtils::spaceKey(space);
    auto spaceRet = doGet(spaceKey);
    if (!spaceRet.ok()) {
        LOG(ERROR) << "Space " << space << " Not Found";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    auto partsRet = client->getPartsDist(space).get();
    if (!partsRet.ok()) {
        LOG(ERROR) << "Get space " << space << "'s part failed";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    auto parts = partsRet.value();
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

    auto blockingStatus = client->blockingWrites(space, SignType::BLOCK_ON).get();
    if (!blockingStatus.ok()) {
        resp_.set_code(cpp2::ErrorCode::E_BLOCK_WRITE_FAILURE);
        onFinished();
        return;
    }

    auto statusKey = MetaServiceUtils::rebuildIndexStatus(space, 'E', indexName);
    std::vector<kvstore::KV> status{std::make_pair(statusKey, "RUNNING")};
    doSyncPut(status);

    std::vector<folly::Future<Status>> results;
    for (auto iter = parts.begin(); iter != parts.end(); iter++) {
        auto future = client->rebuildEdgeIndex(iter->first,
                                               space,
                                               edgeIndexID,
                                               iter->second);
        results.emplace_back(std::move(future));
    }

    folly::collectAll(results)
        .thenValue([&] (const std::vector<folly::Try<Status>>& tries) mutable {
            for (const auto& t : tries) {
                if (!t.value().ok()) {
                    LOG(ERROR) << "Build Edge Index Failed";
                    std::vector<kvstore::KV> failedStatus{std::make_pair(statusKey, "FAILED")};
                    doSyncPut(failedStatus);
                    resp_.set_code(cpp2::ErrorCode::E_BLOCK_WRITE_FAILURE);
                    onFinished();
                    return;
                }
            }
        });

    auto unblockStatus = client->blockingWrites(space, SignType::BLOCK_OFF).get();
    if (!unblockStatus.ok()) {
        std::vector<kvstore::KV> failedStatus{std::make_pair(statusKey, "FAILED")};
        doSyncPut(failedStatus);
        resp_.set_code(cpp2::ErrorCode::E_BLOCK_WRITE_FAILURE);
        onFinished();
        return;
    }

    std::vector<kvstore::KV> successedStatus{std::make_pair(statusKey, "SUCCESSED")};
    doSyncPut(successedStatus);
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}

}  // namespace meta
}  // namespace nebula


