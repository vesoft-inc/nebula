/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/indexMan/FTServiceProcessor.h"

namespace nebula {
namespace meta {

void SignInFTServiceProcessor::process(const cpp2::SignInFTServiceReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::fulltextServicesLock());
    auto serviceKey = MetaServiceUtils::fulltextServiceKey();
    auto ret = doGet(serviceKey);
    if (ret.ok()) {
        handleErrorCode(cpp2::ErrorCode::E_EXISTED);
        onFinished();
        return;
    }

    std::vector<kvstore::KV> data;
    data.emplace_back(std::move(serviceKey),
                      MetaServiceUtils::fulltextServiceVal(req.get_type(), req.get_clients()));
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    doSyncPutAndUpdate(std::move(data));
}

void SignOutFTServiceProcessor::process(const cpp2::SignOutFTServiceReq&) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::fulltextServicesLock());
    auto serviceKey = MetaServiceUtils::fulltextServiceKey();
    auto ret = doGet(serviceKey);
    if (!ret.ok()) {
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    doSyncMultiRemoveAndUpdate({std::move(serviceKey)});
}

void ListFTClientsProcessor::process(const cpp2::ListFTClientsReq&) {
    folly::SharedMutex::WriteHolder rHolder(LockUtils::fulltextServicesLock());
    auto prefix = MetaServiceUtils::fulltextServiceKey();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Can't find any full text service.";
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    std::vector<nebula::meta::cpp2::FTClient> clients;
    if (iter->valid()) {
        clients = MetaServiceUtils::parseFTClients(iter->val());
    }
    resp_.set_clients(std::move(clients));
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}

}  // namespace meta
}  // namespace nebula
