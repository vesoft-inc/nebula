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
    const auto& user = req.get_user_item();
    const auto& account = user.get_account();
    auto ret = getUserId(account);
    if (ret.ok()) {
        cpp2::ErrorCode code;
        if (req.get_if_not_exists()) {
            code = cpp2::ErrorCode::SUCCEEDED;
        } else {
            LOG(ERROR) << "Create User Failed : User " << account
                       << " have existed!";
            code = cpp2::ErrorCode::E_EXISTED;
        }

        resp_.set_id(to(ret.value(), EntryType::USER));
        handleErrorCode(code);
        onFinished();
        return;
    }

    auto idRet = autoIncrementId();
    if (!nebula::ok(idRet)) {
        LOG(ERROR) << "Create user failed : Get user id failed";
        handleErrorCode(nebula::error(idRet));
        onFinished();
        return;
    }
    auto userId = nebula::value(idRet);
    std::vector<kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::indexUserKey(account),
                      std::string(reinterpret_cast<const char*>(&userId), sizeof(userId)));
    data.emplace_back(MetaServiceUtils::userKey(userId),
                      MetaServiceUtils::userVal(user));
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_id(to(userId, EntryType::USER));
    doPut(std::move(data));
}


void AlterUserProcessor::process(const cpp2::AlterUserReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::userLock());
    const auto& user = req.get_user_item();
    const auto& account = user.get_account();
    auto ret = getUserId(account);
    if (!ret.ok()) {
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    UserID userId = ret.value();
    auto userKey = MetaServiceUtils::userKey(userId);
    std::string val;
    auto result = kvstore_->get(kDefaultSpaceId, kDefaultPartId, userKey, &val);
    if (result != kvstore::ResultCode::SUCCEEDED) {
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    std::vector<kvstore::KV> data;
    data.emplace_back(std::move(userKey),
                      MetaServiceUtils::replaceUserVal(user, val));
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_id(to(userId, EntryType::USER));
    doPut(std::move(data));
}


void DropUserProcessor::process(const cpp2::DropUserReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::userLock());
    const auto& account = req.get_account();
    auto ret = getUserId(account);
    if (!ret.ok()) {
        if (req.get_if_exists()) {
            handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
        } else {
            LOG(ERROR) << "Drop User Failed :" << account << " not found.";
            handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        }
        onFinished();
        return;
    }
    std::vector<std::string> keys;
    keys.emplace_back(MetaServiceUtils::indexUserKey(account));
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
                keys.emplace_back(key);
            }
            iter->next();
        }
    }

    resp_.set_id(to(ret.value(), EntryType::USER));
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    LOG(INFO) << "Drop User " << req.get_account();
    doMultiRemove(std::move(keys));
}


void GrantProcessor::process(const cpp2::GrantRoleReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::userLock());
    GraphSpaceID spaceId;
    const auto& roleItem = req.get_role_item();
    auto userRet = getUserId(roleItem.get_user());
    if (!userRet.ok()) {
        handleErrorCode(MetaCommon::to(userRet.status()));
        onFinished();
        return;
    }
    if (roleItem.get_role_type() == nebula::cpp2::RoleType::GOD) {
        spaceId = kDefaultSpaceId;
    } else {
        auto spaceRet = getSpaceId(roleItem.get_space());
        if (!spaceRet.ok()) {
            handleErrorCode(MetaCommon::to(spaceRet.status()));
            onFinished();
            return;
        }
        spaceId = spaceRet.value();
    }
    std::vector<kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::roleKey(spaceId, userRet.value()),
                      MetaServiceUtils::roleVal(roleItem.get_role_type()));
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    doPut(std::move(data));
}


void RevokeProcessor::process(const cpp2::RevokeRoleReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::userLock());
    const auto& roleItem = req.get_role_item();
    auto userRet = getUserId(roleItem.get_user());
    if (!userRet.ok()) {
        handleErrorCode(MetaCommon::to(userRet.status()));
        onFinished();
        return;
    }
    auto spaceRet = getSpaceId(roleItem.get_space());
    if (!spaceRet.ok()) {
        handleErrorCode(MetaCommon::to(spaceRet.status()));
        onFinished();
        return;
    }

    auto roleKey = MetaServiceUtils::roleKey(spaceRet.value(), userRet.value());
    auto result = doGet(roleKey);
    if (!result.ok()) {
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    auto val = result.value();
    const auto role = *reinterpret_cast<const nebula::cpp2::RoleType *>(val.c_str());
    if (role != roleItem.get_role_type()) {
        handleErrorCode(cpp2::ErrorCode::E_INPROPER_ROLE);
        onFinished();
        return;
    }
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    doRemove(roleKey);
}


void ChangePasswordProcessor::process(const cpp2::ChangePasswordReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::userLock());
    auto userRet = getUserId(req.get_account());
    if (!userRet.ok()) {
        handleErrorCode(MetaCommon::to(userRet.status()));
        onFinished();
        return;
    }

    if (!checkPassword(userRet.value(), req.get_old_encoded_pwd())) {
        handleErrorCode(cpp2::ErrorCode::E_INVALID_PASSWORD);
        onFinished();
        return;
    }

    auto userKey = MetaServiceUtils::userKey(userRet.value());
    std::string val;
    auto result = kvstore_->get(kDefaultSpaceId, kDefaultPartId, userKey, &val);
    if (result != kvstore::ResultCode::SUCCEEDED) {
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    std::vector<kvstore::KV> data;
    data.emplace_back(std::move(userKey),
                      MetaServiceUtils::changePassword(val, req.get_new_encoded_pwd()));
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    doPut(std::move(data));
}


void GetUserProcessor::process(const cpp2::GetUserReq& req) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::userLock());
    auto userRet = getUserId(req.get_account());
    if (!userRet.ok()) {
        LOG(ERROR) << "User " << req.get_account() << " not found.";
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    auto userKey = MetaServiceUtils::userKey(userRet.value());
    std::string val;
    auto result = kvstore_->get(kDefaultSpaceId, kDefaultPartId, userKey, &val);
    if (result != kvstore::ResultCode::SUCCEEDED) {
        handleErrorCode(cpp2::ErrorCode::E_CONFLICT);
        onFinished();
        return;
    }
    decltype(resp_.user_item) user = MetaServiceUtils::parseUserItem(val);
    resp_.set_user_item(user);
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
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
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    decltype(resp_.users) users;
    while (iter->valid()) {
        nebula::cpp2::UserItem user = MetaServiceUtils::parseUserItem(iter->val());
        auto userId = MetaServiceUtils::parseUserId(iter->key());
        users.emplace(userId, std::move(user));
        iter->next();
    }
    resp_.set_users(users);
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}


void ListRolesProcessor::process(const cpp2::ListRolesReq& req) {
    auto spaceRet = getSpaceId(req.get_space());
    if (!spaceRet.ok()) {
        LOG(ERROR) << "space dose not found";
        handleErrorCode(MetaCommon::to(spaceRet.status()));
        onFinished();
        return;
    }
    folly::SharedMutex::ReadHolder rHolder(LockUtils::userLock());
    auto prefix = MetaServiceUtils::roleSpacePrefix(spaceRet.value());
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Can't find any roles by space id  " << spaceRet.value();
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    decltype(resp_.roles) roles;
    while (iter->valid()) {
        auto userId = MetaServiceUtils::parseRoleUserId(iter->key());
        auto val = iter->val();
        auto account = getUserAccount(userId);
        if (!account.ok()) {
            LOG(ERROR) << "Get user account failing by id : " << userId;
            handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
            onFinished();
            return;
        }
        nebula::cpp2::RoleItem role;
        role.set_user(account.value());
        role.set_space(req.get_space());
        role.set_role_type(*reinterpret_cast<const nebula::cpp2::RoleType *>(val.begin()));
        roles.emplace_back(role);
        iter->next();
    }
    resp_.set_roles(roles);
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}
}  // namespace meta
}  // namespace nebula
