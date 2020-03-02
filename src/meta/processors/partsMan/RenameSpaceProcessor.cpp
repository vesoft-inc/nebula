/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "meta/processors/partsMan/RenameSpaceProcessor.h"

namespace nebula {
namespace meta {

void RenameSpaceProcessor::process(const cpp2::RenameSpaceReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::spaceLock());
    auto fromSpace = req.get_from_space();
    if (fromSpace == "") {
        LOG(ERROR) << "The from_space name is empty";
        resp_.set_code(cpp2::ErrorCode::E_INVALID_PARM);
        onFinished();
        return;
    }

    auto toSpace = req.get_to_space();
    if (toSpace == "") {
        LOG(ERROR) << "The to_space name is empty";
        resp_.set_code(cpp2::ErrorCode::E_INVALID_PARM);
        onFinished();
        return;
    }

    auto spaceRet = getSpaceId(toSpace);
    if (spaceRet.ok()) {
        LOG(ERROR) << "The to_space " << toSpace << " has existed!";
        resp_.set_code(cpp2::ErrorCode::E_EXISTED);
        onFinished();
        return;
    }

    spaceRet = getSpaceId(fromSpace);
    if (!spaceRet.ok()) {
        LOG(ERROR) << "Get '" << fromSpace << "' spaceId failed";
        resp_.set_code(MetaCommon::to(spaceRet.status()));
        onFinished();
        return;
    }

    auto spaceId = spaceRet.value();
    std::string spaceKey = MetaServiceUtils::spaceKey(spaceId);
    auto ret = doGet(spaceKey);
    if (!ret.ok()) {
        LOG(ERROR) << "Get space spaceName: " << fromSpace << " not found";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    auto properties = MetaServiceUtils::parseSpace(ret.value());
    properties.set_space_name(toSpace);

    // add new space index
    std::vector<kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::indexSpaceKey(toSpace),
                      std::string(reinterpret_cast<const char*>(&spaceId), sizeof(spaceId)));
    data.emplace_back(MetaServiceUtils::spaceKey(spaceId),
                      MetaServiceUtils::spaceVal(properties));
    if (doSyncPut(std::move(data)) != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Add space: " << toSpace << " index failed";
        resp_.set_code(cpp2::ErrorCode::E_STORE_FAILURE);
        onFinished();
        return;
    }

    std::vector<std::string> deleteKey;
    deleteKey.emplace_back(MetaServiceUtils::indexSpaceKey(fromSpace));
    doSyncMultiRemoveAndUpdate(std::move(deleteKey));
}

}  // namespace meta
}  // namespace nebula

