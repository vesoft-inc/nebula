/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/AuthenticationProcessor.h"

namespace nebula {
namespace meta {

void CreateUserProcessor::process(const cpp2::CreateUserReq& req) {
    if (spaceExist(req.get_space_id()) == Status::SpaceNotFound()) {
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    folly::SharedMutex::WriteHolder wHolder(LockUtils::userLock());
    std::vector<kvstore::KV> data;
    auto userKey = MetaUtils::userKey(req.get_space_id(), req.get_user_name());
    std::string userVal;
    auto ret = kvstore_->get(kDefaultSpaceId_, kDefaultPartId_, userKey, &userVal);
    if (ret == kvstore::ResultCode::SUCCEEDED) {
        if (req.get_missing_ok()) {
            resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
        } else {
            resp_.set_code(cpp2::ErrorCode::E_USER_EXISTED);
        }
        onFinished();
        return;
    }
    userVal = MetaUtils::userVal(req.get_role(), req.get_password());
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    data.emplace_back(userKey, userVal);
    kvstore_->asyncMultiPut(kDefaultSpaceId_, kDefaultPartId_, std::move(data),
                            [this] (kvstore::ResultCode code, HostAddr leader) {
        UNUSED(leader);
        this->resp_.set_code(to(code));
        this->onFinished();
    });
}

void DropUserProcessor::process(const cpp2::DropUserReq& req) {
    LOCK_CHECK(user);
    auto userKey = MetaUtils::userKey(req.get_space_id(), req.get_user_name());
    std::string userVal;
    auto ret = kvstore_->get(kDefaultSpaceId_, kDefaultPartId_, userKey, &userVal);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        if (ret == kvstore::ResultCode::ERR_KEY_NOT_FOUND && req.get_missing_ok()) {
            resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
        } else {
            resp_.set_code(to(ret));
        }
        userLock.unlock_shared();
        onFinished();
        return;
    }
    kvstore_->asyncRemove(kDefaultSpaceId_, kDefaultPartId_, std::move(userKey),
                            [&] (kvstore::ResultCode code, HostAddr leader) {
        UNUSED(leader);
        resp_.set_code(to(code));
        userLock.unlock_shared();
        onFinished();
    });
}

void AlterUserProcessor::process(const cpp2::AlterUserReq& req) {
    LOCK_CHECK(user);
    auto userKey = MetaUtils::userKey(req.get_space_id(), req.get_user_name());
    std::string userVal;
    USER_CHECK(userKey, userVal);
    std::vector<kvstore::KV> data;
    auto role = *reinterpret_cast<const RoleType *>(userVal.data());
    userVal = MetaUtils::userVal(role, req.get_password());
    data.emplace_back(userKey, userVal);
    doPut(data, userLock);
}

void GrantToUserProcessor::process(const cpp2::GrantToUserReq& req) {
    LOCK_CHECK(user);
    auto userKey = MetaUtils::userKey(req.get_space_id(), req.get_user_name());
    std::string userVal;
    USER_CHECK(userKey, userVal);
    std::vector<kvstore::KV> data;
    std::string pwd = userVal.substr(sizeof(RoleType), userVal.size() - sizeof(RoleType));
    data.emplace_back(userKey, MetaUtils::userVal(req.get_role(), pwd));
    doPut(data, userLock);
}

void RevokeFromUserProcessor::process(const cpp2::RevokeFromUserReq& req) {
    LOCK_CHECK(user);
    auto userKey = MetaUtils::userKey(req.get_space_id(), req.get_user_name());
    std::string userVal;
    USER_CHECK(userKey, userVal);
    std::vector<kvstore::KV> data;
    auto role = *reinterpret_cast<const RoleType *>(userVal.data());
    if (role != req.get_role()) {
        this->resp_.set_code(cpp2::ErrorCode::E_ROLE_ERROR);
        userLock.unlock_shared();
        this->onFinished();
        return;
    }

    std::string pwd = userVal.substr(sizeof(RoleType), userVal.size() - sizeof(RoleType));
    data.emplace_back(userKey, MetaUtils::userVal(RoleTypeE::T_DEFAULT, pwd));
    doPut(data, userLock);
}

void ListUsersProcessor::process(const cpp2::ListUsersReq& req) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::userLock());
    auto prefix = MetaUtils::userPrefix(req.get_space_id());
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId_, kDefaultPartId_, prefix, &iter);
    resp_.set_code(to(ret));
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        onFinished();
        return;
    }
    decltype(resp_.users) users;
    while (iter->valid()) {
        auto key = iter->key();
        auto val = iter->val();
        users.emplace_back(MetaUtils::parseUserItem(key.str(), val.str()));
        iter->next();
    }
    resp_.set_users(std::move(users));
    onFinished();
}

}  // namespace meta
}  // namespace nebula
