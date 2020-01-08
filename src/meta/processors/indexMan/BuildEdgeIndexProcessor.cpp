/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/ActiveHostsMan.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/indexMan/BuildEdgeIndexProcessor.h"

namespace nebula {
namespace meta {

void BuildEdgeIndexProcessor::process(const cpp2::BuildEdgeIndexReq& req) {
    auto space = req.get_space_id();
    const auto &indexName = req.get_index_name();
    auto edgeType = req.get_edge_type();
    auto version = req.get_edge_version();

    LOG(INFO) << "Build Edge Index Space " << space << ", Index Name " << indexName;

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

    auto properties = MetaServiceUtils::parseSpace(spaceRet.value());
    auto parts = properties.get_partition_num();

    auto edgeIndexIDResult = getEdgeIndexID(space, indexName);
    if (!edgeIndexIDResult.ok()) {
        LOG(ERROR) << "Edge Index " << indexName << " Not Found";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    auto edgeIndexID = edgeIndexIDResult.value();
    auto edgeKey = MetaServiceUtils::edgeIndexKey(space, edgeIndexIDResult.value());
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

    auto statusKey = MetaServiceUtils::buildIndexStatus(space, 'E', indexName);
    std::vector<kvstore::KV> status{std::make_pair(statusKey, "RUNNING")};
    doPut(status);
    auto buildIndexStatus = client->buildEdgeIndex(space, edgeType, edgeIndexID,
                                                   version, parts).get();
    if (!buildIndexStatus.ok()) {
        std::vector<kvstore::KV> failedStatus{std::make_pair(statusKey, "FAILED")};
        doPut(failedStatus);
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    std::vector<kvstore::KV> successedStatus{std::make_pair(statusKey, "SUCCESSED")};
    doPut(successedStatus);
    auto unblockStatus = client->blockingWrites(space, SignType::BLOCK_OFF).get();
    if (!unblockStatus.ok()) {
        resp_.set_code(cpp2::ErrorCode::E_BLOCK_WRITE_FAILURE);
        onFinished();
        return;
    }

    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}

}  // namespace meta
}  // namespace nebula


