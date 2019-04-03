/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_METASERVICEHANDLER_H_
#define META_METASERVICEHANDLER_H_

#include "base/Base.h"
#include <mutex>
#include "interface/gen-cpp2/MetaService.h"
#include "kvstore/KVStore.h"

namespace nebula {
namespace meta {

class MetaServiceHandler final : public cpp2::MetaServiceSvIf {
public:
    explicit MetaServiceHandler(kvstore::KVStore* kv)
                : kvstore_(kv) {}

    folly::Future<cpp2::ExecResp>
    future_createSpace(const cpp2::CreateSpaceReq& req) override;

    folly::Future<cpp2::ListSpacesResp>
    future_listSpaces(const cpp2::ListSpacesReq& req) override;

    folly::Future<cpp2::ExecResp>
    future_addHosts(const cpp2::AddHostsReq& req) override;

    folly::Future<cpp2::ListHostsResp>
    future_listHosts(const cpp2::ListHostsReq& req) override;

    folly::Future<cpp2::GetPartsAllocResp>
    future_getPartsAlloc(const cpp2::GetPartsAllocReq& req) override;

    // User management and authentication
    folly::Future<cpp2::ExecResp>
    future_createUser(const cpp2::CreateUserReq& req) override;

    folly::Future<cpp2::ExecResp>
    future_dropUser(const cpp2::DropUserReq& req) override;

    folly::Future<cpp2::ExecResp>
    future_alterUser(const cpp2::AlterUserReq& req) override;

    folly::Future<cpp2::ExecResp>
    future_grantToUser(const cpp2::GrantToUserReq& req) override;

    folly::Future<cpp2::ExecResp>
    future_revokeFromUser(const cpp2::RevokeFromUserReq& req) override;

    folly::Future<cpp2::ListUsersResp>
    future_listUsers(const cpp2::ListUsersReq& req) override;

private:
    kvstore::KVStore* kvstore_ = nullptr;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_METASERVICEHANDLER_H_
