/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/ActiveHostsMan.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/indexMan/BuildTagIndexProcessor.h"

namespace nebula {
namespace meta {

void BuildTagIndexProcessor::process(const cpp2::BuildTagIndexReq& req) {
    auto space = req.get_space_id();
    const auto &indexName = req.get_index_name();
    auto tagID = req.get_tag_id();
    auto version = req.get_tag_version();

    LOG(INFO) << "Build Tag Index Space " << space << ", Index Name " << indexName;
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

    auto buildIndexStatus = client->buildTagIndex(space, tagID, tagIndexID,
                                                  version, parts).get();
    if (!blockingStatus.ok()) {
        LOG(ERROR) << "Build Tag Index Failed";
        resp_.set_code(cpp2::ErrorCode::E_BLOCK_WRITE_FAILURE);
        onFinished();
        return;
    }

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
