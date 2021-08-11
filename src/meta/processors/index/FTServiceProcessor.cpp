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
    if (nebula::ok(ret)) {
        LOG(ERROR) << "Fulltext already exists.";
        handleErrorCode(nebula::cpp2::ErrorCode::E_EXISTED);
        onFinished();
        return;
    } else {
        auto retCode = nebula::error(ret);
        if (retCode != nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
            LOG(ERROR) << "Sign in fulltext failed, error: "
                       << apache::thrift::util::enumNameSafe(retCode);
            handleErrorCode(retCode);
            onFinished();
            return;
        }
    }

    std::vector<kvstore::KV> data;
    data.emplace_back(std::move(serviceKey),
                      MetaServiceUtils::fulltextServiceVal(req.get_type(), req.get_clients()));
    doSyncPutAndUpdate(std::move(data));
}

void SignOutFTServiceProcessor::process(const cpp2::SignOutFTServiceReq&) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::fulltextServicesLock());
    auto serviceKey = MetaServiceUtils::fulltextServiceKey();
    auto ret = doGet(serviceKey);
    if (!nebula::ok(ret)) {
        auto retCode = nebula::error(ret);
        if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
            LOG(ERROR) << "Sign out fulltext failed, Fulltext not exists.";
        } else {
            LOG(ERROR) << "Sign out fulltext failed, error: "
                       << apache::thrift::util::enumNameSafe(retCode);
        }
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    doSyncMultiRemoveAndUpdate({std::move(serviceKey)});
}

void ListFTClientsProcessor::process(const cpp2::ListFTClientsReq&) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::fulltextServicesLock());
    const auto& prefix = MetaServiceUtils::fulltextServiceKey();
    auto iterRet = doPrefix(prefix);
    if (!nebula::ok(iterRet)) {
        auto retCode = nebula::error(iterRet);
        LOG(ERROR) << "List fulltext failed, error: "
                   << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    auto iter = nebula::value(iterRet).get();
    std::vector<nebula::meta::cpp2::FTClient> clients;
    if (iter->valid()) {
        clients = MetaServiceUtils::parseFTClients(iter->val());
    }
    resp_.set_clients(std::move(clients));
    handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}

}  // namespace meta
}  // namespace nebula
