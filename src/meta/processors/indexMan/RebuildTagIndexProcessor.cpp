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

void RebuildTagIndexProcessor::process(const cpp2::RebuildTagIndexReq& req) {
    auto space = req.get_space_id();
    const auto &indexName = req.get_index_name();
    auto tagID = req.get_tag_id();
    auto version = req.get_tag_version();

    LOG(INFO) << "Rebuild Tag Index Space " << space << ", Index Name " << indexName;
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
    auto tagIndexIDResult = getTagIndexID(space, indexName);
    if (!tagIndexIDResult.ok()) {
        LOG(ERROR) << "Tag Index " << indexName << " Not Found";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    auto tagIndexID = tagIndexIDResult.value();
    auto tagKey = MetaServiceUtils::tagIndexKey(space, tagIndexIDResult.value());
    auto tagResult = doGet(tagKey);
    if (!tagResult.ok()) {
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    auto blockingStatus = client->blockingWrites(space, SignType::BLOCK_ON).get();
    if (!blockingStatus.ok()) {
        LOG(ERROR) << "Block Write Open Failed";
        resp_.set_code(cpp2::ErrorCode::E_BLOCK_WRITE_FAILURE);
        onFinished();
        return;
    }

    auto statusKey = MetaServiceUtils::rebuildIndexStatus(space, 'T', indexName);
    std::vector<kvstore::KV> status{std::make_pair(statusKey, "RUNNING")};
    doPut(status);

    std::vector<folly::Future<Status>> results;
    for (auto iter = parts.begin(); iter != parts.end(); iter++) {
        auto future = client->rebuildTagIndex(iter->first,
                                              space,
                                              tagID,
                                              tagIndexID,
                                              version,
                                              iter->second);
        results.emplace_back(std::move(future));
    }

    folly::collectAll(results)
        .thenValue([&] (const std::vector<folly::Try<Status>>& tries) mutable {
            for (const auto& t : tries) {
                if (!t.value().ok()) {
                    LOG(ERROR) << "Build Tag Index Failed";
                    std::vector<kvstore::KV> failedStatus{std::make_pair(statusKey, "FAILED")};
                    doPut(failedStatus);
                    resp_.set_code(cpp2::ErrorCode::E_BLOCK_WRITE_FAILURE);
                    onFinished();
                    return;
                }
            }
        });

    std::vector<kvstore::KV> successedStatus{std::make_pair(statusKey, "SUCCESSED")};
    doPut(successedStatus);

    auto unblockStatus = client->blockingWrites(space, SignType::BLOCK_OFF).get();
    if (!unblockStatus.ok()) {
        LOG(ERROR) << "Block Write Close Failed";
        resp_.set_code(cpp2::ErrorCode::E_BLOCK_WRITE_FAILURE);
        onFinished();
        return;
    }

    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}

}  // namespace meta
}  // namespace nebula
