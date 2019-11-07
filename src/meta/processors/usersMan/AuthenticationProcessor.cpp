/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/usersMan/AuthenticationProcessor.h"

namespace nebula {
namespace meta {

void CreateUserProcessor::process(const cpp2::CreateUserReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::userLock());
    const auto& user = req.get_user();
    auto ret = getUserId(user.get_account());
    if (ret.ok()) {
        if (req.get_missing_ok()) {
            resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
        } else {
            LOG(ERROR) << "Create User Failed :" << user.get_account() << " have existed";
            resp_.set_code(cpp2::ErrorCode::E_EXISTED);
        }
        resp_.set_id(to(ret.value(), EntryType::USER));
        onFinished();
        return;
    }

    auto userRet = autoIncrementId();
    if (!nebula::ok(userRet)) {
        LOG(ERROR) << "Create User Failed : Get user id failed";
        resp_.set_code(nebula::error(userRet));
        onFinished();
        return;
    }
    auto userId = nebula::value(userRet);
    std::vector<kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::indexUserKey(user.get_account()),
                      std::string(reinterpret_cast<const char*>(&userId), sizeof(userId)));
    LOG(INFO) << "Create User " << user.get_account() << ", userId " << userId;
    data.emplace_back(MetaServiceUtils::userKey(userId),
                      MetaServiceUtils::userVal(req.get_encoded_pwd(), user));
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_id(to(userId, EntryType::USER));
    doPut(std::move(data));
}


void AlterUserProcessor::process(const cpp2::AlterUserReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::userLock());
    const auto& user = req.get_user_item();
    auto ret = getUserId(user.get_account());
    if (!ret.ok()) {
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    UserID userId = ret.value();
    auto userKey = MetaServiceUtils::userKey(userId);
    std::string val;
    auto result = kvstore_->get(kDefaultSpaceId, kDefaultPartId, userKey, &val);
    if (result != kvstore::ResultCode::SUCCEEDED) {
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    std::vector<kvstore::KV> data;
    data.emplace_back(std::move(userKey),
                      MetaServiceUtils::replaceUserVal(user, val));
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_id(to(userId, EntryType::USER));
    doPut(std::move(data));
}


void DropUserProcessor::process(const cpp2::DropUserReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::userLock());
    auto ret = getUserId(req.get_account());
    if (!ret.ok()) {
        if (req.get_missing_ok()) {
            resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
        } else {
            LOG(ERROR) << "Drop User Failed :" << req.get_account() << " not found.";
            resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        }
        onFinished();
        return;
    }
    std::vector<std::string> keys;
    keys.emplace_back(MetaServiceUtils::indexUserKey(req.get_account()));
    keys.emplace_back(MetaServiceUtils::userKey(ret.value()));

    // Collect related roles by user.
    std::unique_ptr<kvstore::KVIterator> iter;
    auto prefix = MetaServiceUtils::rolesPrefix();
    auto userRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (userRet == kvstore::ResultCode::SUCCEEDED) {
        while (iter->valid()) {
            auto key = iter->key();
            auto userId = MetaServiceUtils::parseRoleUserId(key);
            if (userId == ret.value()) {
                keys.emplace_back(std::move(key));
            }
            iter->next();
        }
    }

    resp_.set_id(to(ret.value(), EntryType::USER));
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    LOG(INFO) << "Drop User " << req.get_account();
    doMultiRemove(std::move(keys));
}


void GrantProcessor::process(const cpp2::GrantRoleReq& req) {
    const auto& roleItem = req.get_role_item();
    CHECK_SPACE_ID_AND_RETURN(roleItem.get_space_id());
    CHECK_USER_ID_AND_RETURN(roleItem.get_user_id());
    folly::SharedMutex::WriteHolder wHolder(LockUtils::userLock());
    std::vector<kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::roleKey(roleItem.get_space_id(), roleItem.get_user_id()),
                      MetaServiceUtils::roleVal(roleItem.get_role_type()));
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    doPut(std::move(data));
}


void RevokeProcessor::process(const cpp2::RevokeRoleReq& req) {
    const auto& roleItem = req.get_role_item();
    CHECK_SPACE_ID_AND_RETURN(roleItem.get_space_id());
    CHECK_USER_ID_AND_RETURN(roleItem.get_user_id());
    folly::SharedMutex::WriteHolder wHolder(LockUtils::userLock());
    auto roleKey = MetaServiceUtils::roleKey(roleItem.get_space_id(), roleItem.get_user_id());
    resp_.set_id(to(roleItem.get_user_id(), EntryType::USER));
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    doRemove(std::move(roleKey));
}


void ChangePasswordProcessor::process(const cpp2::ChangePasswordReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::userLock());
    auto userRet = getUserId(req.get_account());
    if (!userRet.ok()) {
        resp_.set_code(to(userRet.status()));
        onFinished();
        return;
    }

    // If the user role is god, the option old_encoded_pwd will not be set.
    if (req.__isset.old_encoded_pwd) {
        if (!checkPassword(userRet.value(), req.get_old_encoded_pwd())) {
            resp_.set_code(cpp2::ErrorCode::E_INVALID_PASSWORD);
            onFinished();
            return;
        }
    }

    auto userKey = MetaServiceUtils::userKey(userRet.value());
    std::string val;
    auto result = kvstore_->get(kDefaultSpaceId, kDefaultPartId, userKey, &val);
    if (result != kvstore::ResultCode::SUCCEEDED) {
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    std::vector<kvstore::KV> data;
    data.emplace_back(std::move(userKey),
                      MetaServiceUtils::changePassword(val, req.get_new_encoded_pwd()));
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    doPut(std::move(data));
}


void GetUserProcessor::process(const cpp2::GetUserReq& req) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::userLock());
    auto userRet = getUserId(req.get_account());
    if (!userRet.ok()) {
        LOG(ERROR) << "User " << req.get_account() << " not found.";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    auto userKey = MetaServiceUtils::userKey(userRet.value());
    std::string val;
    auto result = kvstore_->get(kDefaultSpaceId, kDefaultPartId, userKey, &val);
    if (result != kvstore::ResultCode::SUCCEEDED) {
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    decltype(resp_.user_item) user = MetaServiceUtils::parseUserItem(val);
    resp_.set_user_item(user);
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}


void ListUsersProcessor::process(const cpp2::ListUsersReq& req) {
    UNUSED(req);
    folly::SharedMutex::ReadHolder rHolder(LockUtils::userLock());
    std::unique_ptr<kvstore::KVIterator> iter;
    std::string prefix = "__users__";
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Can't find any users.";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    decltype(resp_.users) users;
    while (iter->valid()) {
        cpp2::UserItem user = MetaServiceUtils::parseUserItem(iter->val());
        auto userId = MetaServiceUtils::parseUserId(iter->key());
        users.emplace(userId, std::move(user));
        iter->next();
    }
    resp_.set_users(users);
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}


void CheckPasswordProcessor::process(const cpp2::CheckPasswordReq& req) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::userLock());
    auto userRet = getUserId(req.get_account());
    if (!userRet.ok()) {
        resp_.set_code(to(userRet.status()));
        onFinished();
        return;
    }

    if (!checkPassword(userRet.value(), req.get_encoded_pwd())) {
        resp_.set_code(cpp2::ErrorCode::E_INVALID_PASSWORD);
        onFinished();
        return;
    }
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_id(to(userRet.value(), EntryType::USER));
    onFinished();
}


void ListRolesProcessor::process(const cpp2::ListRolesReq& req) {
    auto spaceId = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(spaceId);
    folly::SharedMutex::ReadHolder rHolder(LockUtils::userLock());
    auto prefix = MetaServiceUtils::roleSpacePrefix(spaceId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Can't find any roles by space id  " << spaceId;
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    decltype(resp_.roles) roles;
    while (iter->valid()) {
        auto userId = MetaServiceUtils::parseRoleUserId(iter->key());
        auto val = iter->val();
        auto account = getUserAccount(userId);
        if (!account.ok()) {
            LOG(ERROR) << "Get user account failling by id : " << userId;
            resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
            onFinished();
            return;
        }
        cpp2::RoleItem role;
        role.set_user_id(userId);
        role.set_space_id(spaceId);
        role.set_role_type(*reinterpret_cast<const cpp2::RoleType *>(val.begin()));
        roles.emplace_back(std::move(role));
        iter->next();
    }
    resp_.set_roles(roles);
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}
}  // namespace meta
}  // namespace nebula
