/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#include "meta/MetaServiceHandler.h"
#include "meta/processors/CreateSpaceProcessor.h"
#include "meta/processors/AddHostsProcessor.h"
#include "meta/processors/ListHostsProcessor.h"
#include "meta/processors/ListSpacesProcessor.h"
#include "meta/processors/GetPartsAllocProcessor.h"
#include <meta/processors/AuthenticationProcessor.h>

#define RETURN_FUTURE(processor) \
    auto f = processor->getFuture(); \
    processor->process(req); \
    return f;

namespace nebula {
namespace meta {

folly::Future<cpp2::ExecResp>
MetaServiceHandler::future_createSpace(const cpp2::CreateSpaceReq& req) {
    auto* processor = CreateSpaceProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ListSpacesResp>
MetaServiceHandler::future_listSpaces(const cpp2::ListSpacesReq& req) {
    auto* processor = ListSpacesProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp>
MetaServiceHandler::future_addHosts(const cpp2::AddHostsReq& req) {
    auto* processor = AddHostsProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ListHostsResp>
MetaServiceHandler::future_listHosts(const cpp2::ListHostsReq& req) {
    auto* processor = ListHostsProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::GetPartsAllocResp>
MetaServiceHandler::future_getPartsAlloc(const cpp2::GetPartsAllocReq& req) {
    auto* processor = GetPartsAllocProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp>
MetaServiceHandler::future_createUser(const cpp2::CreateUserReq& req) {
    auto* processor = CreateUserProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp>
MetaServiceHandler::future_dropUser(const cpp2::DropUserReq& req) {
    auto* processor = DropUserProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp>
MetaServiceHandler::future_alterUser(const cpp2::AlterUserReq& req) {
    auto* processor = AlterUserProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp>
MetaServiceHandler::future_grantToUser(const cpp2::GrantToUserReq& req) {
    auto* processor = GrantToUserProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResp>
MetaServiceHandler::future_revokeFromUser(const cpp2::RevokeFromUserReq& req) {
    auto* processor = RevokeFromUserProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ListUsersResp>
MetaServiceHandler::future_listUsers(const cpp2::ListUsersReq& req) {
    auto* processor = ListUsersProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

}  // namespace meta
}  // namespace nebula
