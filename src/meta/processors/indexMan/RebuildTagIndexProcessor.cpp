/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/ActiveHostsMan.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/indexMan/RebuildTagIndexProcessor.h"

namespace nebula {
namespace meta {

void RebuildTagIndexProcessor::process(const cpp2::RebuildIndexReq& req) {
    auto space = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(space);
    const auto &indexName = req.get_index_name();
    folly::SharedMutex::ReadHolder schemaHolder(LockUtils::tagLock());
    folly::SharedMutex::ReadHolder indexHolder(LockUtils::tagIndexLock());

    LOG(INFO) << "Rebuild Tag Index Space " << space << ", Index Name " << indexName;
    std::unique_ptr<AdminClient> client(new AdminClient(kvstore_));

    auto partsRet = client->getLeaderDist(space).get();
    if (!partsRet.ok()) {
        LOG(ERROR) << "Get space " << space << "'s part failed";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    auto parts = partsRet.value();
    auto tagIndexIDResult = getIndexID(space, indexName);
    if (!tagIndexIDResult.ok()) {
        LOG(ERROR) << "Tag Index " << indexName << " Not Found";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    auto tagIndexID = tagIndexIDResult.value();
    auto tagKey = MetaServiceUtils::indexKey(space, tagIndexIDResult.value());
    auto tagResult = doGet(tagKey);
    if (!tagResult.ok()) {
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    auto statusKey = MetaServiceUtils::rebuildIndexStatus(space, 'T', indexName);
    std::vector<kvstore::KV> status{std::make_pair(statusKey, "RUNNING")};
    doSyncPut(status);

    std::vector<folly::Future<Status>> results;
    for (auto iter = parts.begin(); iter != parts.end(); iter++) {
        auto future = client->rebuildTagIndex(iter->first,
                                              space,
                                              tagIndexID,
                                              iter->second);
        results.emplace_back(std::move(future));
    }

    folly::collectAll(results)
        .thenValue([&] (const std::vector<folly::Try<Status>>& tries) mutable {
            for (const auto& t : tries) {
                if (!t.value().ok()) {
                    LOG(ERROR) << "Build Tag Index Failed";
                    std::vector<kvstore::KV> failedStatus{std::make_pair(statusKey, "FAILED")};
                    doSyncPut(failedStatus);
                    resp_.set_code(cpp2::ErrorCode::E_REBUILD_INDEX_FAILURE);
                    onFinished();
                    return;
                }
            }
        });

    std::vector<kvstore::KV> successedStatus{std::make_pair(statusKey, "SUCCESSED")};
    doSyncPut(successedStatus);

    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}

}  // namespace meta
}  // namespace nebula
