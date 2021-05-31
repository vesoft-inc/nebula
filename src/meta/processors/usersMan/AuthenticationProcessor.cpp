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
    const auto& account = req.get_account();
    const auto& password = req.get_encoded_pwd();
    auto ret = userExist(account);
    if (ret.ok()) {
        cpp2::ErrorCode code;
        if (req.get_if_not_exists()) {
            code = cpp2::ErrorCode::SUCCEEDED;
        } else {
            LOG(ERROR) << "Create User Failed : User " << account
                       << " already existed!";
            code = cpp2::ErrorCode::E_EXISTED;
        }
        handleErrorCode(code);
        onFinished();
        return;
    }

    std::vector<kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::userKey(account),
                      MetaServiceUtils::userVal(password));
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    doSyncPutAndUpdate(std::move(data));
}


void AlterUserProcessor::process(const cpp2::AlterUserReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::userLock());
    const auto& account = req.get_account();
    const auto& password = req.get_encoded_pwd();
    auto userKey = MetaServiceUtils::userKey(account);
    auto userVal = MetaServiceUtils::userVal(password);
    std::string val;
    auto result = kvstore_->get(kDefaultSpaceId, kDefaultPartId, userKey, &val);
    if (result != kvstore::ResultCode::SUCCEEDED) {
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    std::vector<kvstore::KV> data;
    data.emplace_back(std::move(userKey), std::move(userVal));
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    doSyncPutAndUpdate(std::move(data));
}


void DropUserProcessor::process(const cpp2::DropUserReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::userLock());
    const auto& account = req.get_account();
    auto ret = userExist(account);
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
    keys.emplace_back(MetaServiceUtils::userKey(account));

    // Collect related roles by user.
    std::unique_ptr<kvstore::KVIterator> iter;
    auto prefix = MetaServiceUtils::rolesPrefix();
    auto userRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (userRet == kvstore::ResultCode::SUCCEEDED) {
        while (iter->valid()) {
            auto key = iter->key();
            auto user = MetaServiceUtils::parseRoleUser(key);
            if (user == account) {
                keys.emplace_back(key);
            }
            iter->next();
        }
    }

    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    LOG(INFO) << "Drop User " << req.get_account();
    doSyncMultiRemoveAndUpdate({std::move(keys)});
}


void GrantProcessor::process(const cpp2::GrantRoleReq& req) {
    folly::SharedMutex::WriteHolder userHolder(LockUtils::userLock());
    folly::SharedMutex::ReadHolder spaceHolder(LockUtils::spaceLock());
    const auto& roleItem = req.get_role_item();
    auto spaceId = roleItem.get_space_id();
    /**
     *  for cloud authority, need init a god user by this interface. the god user default grant to
     *  meta space (kDefaultSpaceId). so skip the space check.
     *  Should be reject the grant operation and return a error 
     *  when grant a user to GOD through graph layer.
    */
    if (!(spaceId == kDefaultSpaceId && roleItem.get_role_type() == nebula::cpp2::RoleType::GOD)) {
        CHECK_SPACE_ID_AND_RETURN(spaceId);
    }
    auto userRet = userExist(roleItem.get_user());
    if (!userRet.ok()) {
        handleErrorCode(MetaCommon::to(userRet));
        onFinished();
        return;
    }

    std::vector<kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::roleKey(spaceId, roleItem.get_user()),
                      MetaServiceUtils::roleVal(roleItem.get_role_type()));
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    doSyncPutAndUpdate(std::move(data));
}


void RevokeProcessor::process(const cpp2::RevokeRoleReq& req) {
    folly::SharedMutex::WriteHolder userHolder(LockUtils::userLock());
    folly::SharedMutex::ReadHolder spaceHolder(LockUtils::spaceLock());
    const auto& roleItem = req.get_role_item();
    auto spaceId = roleItem.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(spaceId);
    auto userRet = userExist(roleItem.get_user());
    if (!userRet.ok()) {
        handleErrorCode(MetaCommon::to(userRet));
        onFinished();
        return;
    }

    auto roleKey = MetaServiceUtils::roleKey(spaceId, roleItem.get_user());
    auto result = doGet(roleKey);
    if (!result.ok()) {
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    auto val = result.value();
    const auto role = *reinterpret_cast<const nebula::cpp2::RoleType *>(val.c_str());
    if (role != roleItem.get_role_type()) {
        handleErrorCode(cpp2::ErrorCode::E_IMPROPER_ROLE);
        onFinished();
        return;
    }
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    doSyncMultiRemoveAndUpdate({std::move(roleKey)});
}


void ChangePasswordProcessor::process(const cpp2::ChangePasswordReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::userLock());
    auto userRet = userExist(req.get_account());
    if (!userRet.ok()) {
        handleErrorCode(MetaCommon::to(userRet));
        onFinished();
        return;
    }

    if (!checkPassword(req.get_account(), req.get_old_encoded_pwd())) {
        handleErrorCode(cpp2::ErrorCode::E_INVALID_PASSWORD);
        onFinished();
        return;
    }

    auto userKey = MetaServiceUtils::userKey(req.get_account());
    auto userVal = MetaServiceUtils::userVal(req.get_new_encoded_pwd());
    std::vector<kvstore::KV> data;
    data.emplace_back(std::move(userKey), std::move(userVal));
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    doSyncPutAndUpdate(std::move(data));
}


void ListUsersProcessor::process(const cpp2::ListUsersReq& req) {
    UNUSED(req);
    folly::SharedMutex::ReadHolder rHolder(LockUtils::userLock());
    std::unique_ptr<kvstore::KVIterator> iter;
    std::string prefix = "__users__";
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "List users fail.";
        handleErrorCode(MetaCommon::to(ret));
        onFinished();
        return;
    }
    decltype(resp_.users) users;
    while (iter->valid()) {
        auto account = MetaServiceUtils::parseUser(iter->key());
        auto password = MetaServiceUtils::parseUserPwd(iter->val());
        users.emplace(std::pair<std::string, std::string>(std::move(account), std::move(password)));
        iter->next();
    }
    resp_.set_users(users);
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
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
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    decltype(resp_.roles) roles;
    while (iter->valid()) {
        auto account = MetaServiceUtils::parseRoleUser(iter->key());
        auto val = iter->val();
        nebula::cpp2::RoleItem role;
        role.set_user(std::move(account));
        role.set_space_id(spaceId);
        role.set_role_type(*reinterpret_cast<const nebula::cpp2::RoleType *>(val.begin()));
        roles.emplace_back(role);
        iter->next();
    }
    resp_.set_roles(roles);
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}

void GetUserRolesProcessor::process(const cpp2::GetUserRolesReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::userLock());
    auto prefix = MetaServiceUtils::rolesPrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Can't find any roles by user  " << req.get_account();
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    decltype(resp_.roles) roles;
    while (iter->valid()) {
        auto account = MetaServiceUtils::parseRoleUser(iter->key());
        auto spaceId = MetaServiceUtils::parseRoleSpace(iter->key());
        if (account == req.get_account()) {
            auto val = iter->val();
            nebula::cpp2::RoleItem role;
            role.set_user(std::move(account));
            role.set_space_id(spaceId);
            role.set_role_type(*reinterpret_cast<const nebula::cpp2::RoleType *>(val.begin()));
            roles.emplace_back(role);
        }
        iter->next();
    }
    resp_.set_roles(roles);
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}

}  // namespace meta
}  // namespace nebula
