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

    UserID userId = autoIncrementId();
    std::vector<kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::indexUserKey(user.get_account()),
                      std::string(reinterpret_cast<const char*>(&userId), sizeof(userId)));
    LOG(INFO) << "Create User " << user.get_account() << ", userId " << userId;
    auto schema = getUserSchema();
    if (!schema.ok()) {
        resp_.set_code(to(ret.status()));
        onFinished();
        return;
    }
    data.emplace_back(MetaServiceUtils::userKey(userId),
                      MetaServiceUtils::userVal(req.get_encoded_pwd(), user, schema.value()));
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
    auto result = kvstore_->get(kDefaultSpaceId_, kDefaultPartId_, userKey, &val);
    if (result != kvstore::ResultCode::SUCCEEDED) {
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    auto schema = getUserSchema();
    if (!schema.ok()) {
        resp_.set_code(to(ret.status()));
        onFinished();
        return;
    }
    std::vector<kvstore::KV> data;
    data.emplace_back(std::move(userKey),
                      MetaServiceUtils::replaceUserVal(user, val, schema.value()));
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
    std::string prefix = "__roles__";
    auto userRet = kvstore_->prefix(kDefaultSpaceId_, kDefaultPartId_, prefix, &iter);
    if (userRet == kvstore::ResultCode::SUCCEEDED) {
        while (iter->valid()) {
            auto key = iter->key();
            auto userId = MetaServiceUtils::parseUserId(key);
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
    folly::SharedMutex::WriteHolder wHolder(LockUtils::userLock());
    const auto& roleItem = req.get_role_item();
    auto spaceRet = getSpaceId(roleItem.get_space());
    if (!spaceRet.ok()) {
        resp_.set_code(to(spaceRet.status()));
        onFinished();
        return;;
    }
    auto userRet = getUserId(roleItem.get_account());
    if (!userRet.ok()) {
        resp_.set_code(to(userRet.status()));
        onFinished();
        return;
    }
    std::vector<kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::roleKey(spaceRet.value(), userRet.value()),
                      MetaServiceUtils::roleVal(roleItem.get_RoleType()));
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    doPut(std::move(data));
}


void RevokeProcessor::process(const cpp2::RevokeRoleReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::userLock());
    const auto& roleItem = req.get_role_item();
    auto spaceRet = getSpaceId(roleItem.get_space());
    if (!spaceRet.ok()) {
        resp_.set_code(to(spaceRet.status()));
        onFinished();
        return;;
    }
    auto userRet = getUserId(roleItem.get_account());
    if (!userRet.ok()) {
        resp_.set_code(to(userRet.status()));
        onFinished();
        return;
    }
    auto roleKey = MetaServiceUtils::roleKey(spaceRet.value(), userRet.value());
    resp_.set_id(to(userRet.value(), EntryType::USER));
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
    auto result = kvstore_->get(kDefaultSpaceId_, kDefaultPartId_, userKey, &val);
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
        onFinished();
        return;
    }
    auto userKey = MetaServiceUtils::userKey(userRet.value());
    std::string val;
    auto result = kvstore_->get(kDefaultSpaceId_, kDefaultPartId_, userKey, &val);
    if (result != kvstore::ResultCode::SUCCEEDED) {
        onFinished();
        return;
    }
    auto schema = getUserSchema();
    if (!schema.ok()) {
        LOG(ERROR) << "Global schema " << GLOBAL_USER_SCHEMA_TAG << " not found.";
        onFinished();
        return;
    }
    decltype(resp_.user_item) user = MetaServiceUtils::parseUserItem(val, schema.value());
    resp_.set_user_item(user);
    onFinished();
}


void ListUsersProcessor::process(const cpp2::ListUsersReq& req) {
    UNUSED(req);
    folly::SharedMutex::ReadHolder rHolder(LockUtils::userLock());
    std::unique_ptr<kvstore::KVIterator> iter;
    std::string prefix = "__user__";
    auto ret = kvstore_->prefix(kDefaultSpaceId_, kDefaultPartId_, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Can't find any users.";
        onFinished();
        return;
    }
    auto schema = getUserSchema();
    if (!schema.ok()) {
        LOG(ERROR) << "Global schema " << GLOBAL_USER_SCHEMA_TAG << " not found.";
        onFinished();
        return;
    }
    decltype(resp_.users) users;
    while (iter->valid()) {
        cpp2::UserItem user = MetaServiceUtils::parseUserItem(iter->val(), schema.value());
        users.emplace_back(std::move(user));
        iter->next();
    }
    resp_.set_users(users);
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
    folly::SharedMutex::ReadHolder rHolder(LockUtils::userLock());
    auto spaceRet = getSpaceId(req.get_space());
    if (!spaceRet.ok()) {
        onFinished();
        return;;
    }
    auto prefix = MetaServiceUtils::roleSpacePrefix(spaceRet.value());
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId_, kDefaultPartId_, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Can't find any roles by space " << req.get_space();
        onFinished();
        return;
    }

    decltype(resp_.roles) roles;
    while (iter->valid()) {
        auto userId = MetaServiceUtils::parseUserId(iter->key());
        auto val = iter->val();
        auto account = getUserAccount(userId);
        if (!account.ok()) {
            LOG(ERROR) << "Get user account failling by id : " << userId;
            onFinished();
            return;
        }
        cpp2::RoleItem role(apache::thrift::FragileConstructor::FRAGILE,
                            account.value(), ""/*space name can be ignore at here*/,
                            *reinterpret_cast<const cpp2::RoleType *>(val.begin()));
        roles.emplace_back(std::move(role));
        iter->next();
    }
    resp_.set_roles(roles);
    onFinished();
}
}  // namespace meta
}  // namespace nebula
