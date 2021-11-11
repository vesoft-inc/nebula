/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/user/AuthenticationProcessor.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

namespace nebula {
namespace meta {

void CreateUserProcessor::process(const cpp2::CreateUserReq& req) {
  folly::SharedMutex::WriteHolder wHolder(LockUtils::userLock());
  const auto& account = req.get_account();
  const auto& password = req.get_encoded_pwd();

  auto retCode = userExist(account);
  if (retCode != nebula::cpp2::ErrorCode::E_USER_NOT_FOUND) {
    if (retCode == nebula::cpp2::ErrorCode::SUCCEEDED) {
      if (!req.get_if_not_exists()) {
        LOG(ERROR) << "Create User Failed : User " << account << " already existed!";
        retCode = nebula::cpp2::ErrorCode::E_EXISTED;
      }
    } else {
      LOG(ERROR) << "Create User Failed : User " << account
                 << " error: " << apache::thrift::util::enumNameSafe(retCode);
    }
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  std::vector<kvstore::KV> data;
  data.emplace_back(MetaKeyUtils::userKey(account), MetaKeyUtils::userVal(password));
  doSyncPutAndUpdate(std::move(data));
}

void AlterUserProcessor::process(const cpp2::AlterUserReq& req) {
  folly::SharedMutex::WriteHolder wHolder(LockUtils::userLock());
  const auto& account = req.get_account();
  const auto& password = req.get_encoded_pwd();
  auto userKey = MetaKeyUtils::userKey(account);
  auto userVal = MetaKeyUtils::userVal(password);

  auto iRet = doGet(userKey);
  if (!nebula::ok(iRet)) {
    auto errCode = nebula::error(iRet);
    if (errCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      errCode = nebula::cpp2::ErrorCode::E_USER_NOT_FOUND;
    }
    LOG(ERROR) << "Get User Failed : User " << account
               << " error: " << apache::thrift::util::enumNameSafe(errCode);
    handleErrorCode(errCode);
    onFinished();
    return;
  }

  std::vector<kvstore::KV> data;
  data.emplace_back(std::move(userKey), std::move(userVal));
  doSyncPutAndUpdate(std::move(data));
}

void DropUserProcessor::process(const cpp2::DropUserReq& req) {
  folly::SharedMutex::WriteHolder wHolder(LockUtils::userLock());
  const auto& account = req.get_account();

  auto retCode = userExist(account);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    if (retCode == nebula::cpp2::ErrorCode::E_USER_NOT_FOUND) {
      if (req.get_if_exists()) {
        retCode = nebula::cpp2::ErrorCode::SUCCEEDED;
      } else {
        LOG(ERROR) << "Drop User Failed: " << account << " not found.";
      }
    } else {
      LOG(ERROR) << "Drop User Failed, User " << account
                 << " error: " << apache::thrift::util::enumNameSafe(retCode);
    }
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  std::vector<std::string> keys;
  keys.emplace_back(MetaKeyUtils::userKey(account));

  // Collect related roles by user.
  auto prefix = MetaKeyUtils::rolesPrefix();
  auto iterRet = doPrefix(prefix);
  if (!nebula::ok(iterRet)) {
    retCode = nebula::error(iterRet);
    // The error of prefix is leader change
    LOG(ERROR) << "Drop User Failed, User " << account
               << " error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto iter = nebula::value(iterRet).get();
  while (iter->valid()) {
    auto key = iter->key();
    auto user = MetaKeyUtils::parseRoleUser(key);
    if (user == account) {
      keys.emplace_back(key);
    }
    iter->next();
  }

  LOG(INFO) << "Drop User " << account;
  doSyncMultiRemoveAndUpdate({std::move(keys)});
}

void GrantProcessor::process(const cpp2::GrantRoleReq& req) {
  folly::SharedMutex::WriteHolder userHolder(LockUtils::userLock());
  folly::SharedMutex::ReadHolder spaceHolder(LockUtils::spaceLock());
  const auto& roleItem = req.get_role_item();
  auto spaceId = roleItem.get_space_id();
  const auto& account = roleItem.get_user_id();

  /**
   *  for cloud authority, need init a god user by this interface. the god user
   * default grant to meta space (kDefaultSpaceId). so skip the space check.
   *  Should be reject the grant operation and return a error
   *  when grant a user to GOD through graph layer.
   */
  if (!(spaceId == kDefaultSpaceId && roleItem.get_role_type() == cpp2::RoleType::GOD)) {
    CHECK_SPACE_ID_AND_RETURN(spaceId);
  }
  auto retCode = userExist(account);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "Grant User Failed : User " << account
               << " error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  std::vector<kvstore::KV> data;
  data.emplace_back(MetaKeyUtils::roleKey(spaceId, account),
                    MetaKeyUtils::roleVal(roleItem.get_role_type()));
  doSyncPutAndUpdate(std::move(data));
}

void RevokeProcessor::process(const cpp2::RevokeRoleReq& req) {
  folly::SharedMutex::WriteHolder userHolder(LockUtils::userLock());
  folly::SharedMutex::ReadHolder spaceHolder(LockUtils::spaceLock());
  const auto& roleItem = req.get_role_item();
  auto spaceId = roleItem.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(spaceId);
  const auto& account = roleItem.get_user_id();

  auto userRet = userExist(account);
  if (userRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "Revoke User Failed : User " << account
               << " error: " << apache::thrift::util::enumNameSafe(userRet);
    handleErrorCode(userRet);
    onFinished();
    return;
  }

  auto roleKey = MetaKeyUtils::roleKey(spaceId, account);
  auto result = doGet(roleKey);
  if (!nebula::ok(result)) {
    userRet = nebula::error(result);
    if (userRet == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      userRet = nebula::cpp2::ErrorCode::E_ROLE_NOT_FOUND;
    }
    LOG(ERROR) << "Get Role User Failed : User " << account
               << " error: " << apache::thrift::util::enumNameSafe(userRet);
    handleErrorCode(userRet);
    onFinished();
    return;
  }
  auto val = nebula::value(result);
  const auto role = *reinterpret_cast<const cpp2::RoleType*>(val.c_str());
  if (role != roleItem.get_role_type()) {
    LOG(ERROR) << "Revoke User Failed : User " << account << " error: "
               << apache::thrift::util::enumNameSafe(nebula::cpp2::ErrorCode::E_IMPROPER_ROLE);
    handleErrorCode(nebula::cpp2::ErrorCode::E_IMPROPER_ROLE);
    onFinished();
    return;
  }
  doSyncMultiRemoveAndUpdate({std::move(roleKey)});
}

void ChangePasswordProcessor::process(const cpp2::ChangePasswordReq& req) {
  folly::SharedMutex::WriteHolder wHolder(LockUtils::userLock());
  const auto& account = req.get_account();
  auto userRet = userExist(account);
  if (userRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(ERROR) << "Change password Failed, get user " << account << " failed, "
               << " error: " << apache::thrift::util::enumNameSafe(userRet);
    handleErrorCode(userRet);
    onFinished();
    return;
  }

  auto checkRet = checkPassword(account, req.get_old_encoded_pwd());
  if (!nebula::ok(checkRet)) {
    auto retCode = nebula::error(checkRet);
    if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      retCode = nebula::cpp2::ErrorCode::E_USER_NOT_FOUND;
    }
    LOG(ERROR) << "Get user " << account << " failed, "
               << " error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  } else {
    if (!nebula::value(checkRet)) {
      auto retCode = nebula::cpp2::ErrorCode::E_INVALID_PASSWORD;
      handleErrorCode(retCode);
      LOG(ERROR) << "Change password failed, user " << account
                 << apache::thrift::util::enumNameSafe(retCode);
      onFinished();
      return;
    }
  }

  auto userKey = MetaKeyUtils::userKey(account);
  auto userVal = MetaKeyUtils::userVal(req.get_new_encoded_pwd());
  std::vector<kvstore::KV> data;
  data.emplace_back(std::move(userKey), std::move(userVal));
  doSyncPutAndUpdate(std::move(data));
}

void ListUsersProcessor::process(const cpp2::ListUsersReq& req) {
  UNUSED(req);
  folly::SharedMutex::ReadHolder rHolder(LockUtils::userLock());
  std::string prefix = "__users__";
  auto ret = doPrefix(prefix);
  if (!nebula::ok(ret)) {
    auto retCode = nebula::error(ret);
    LOG(ERROR) << "List User failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto iter = nebula::value(ret).get();
  std::unordered_map<std::string, std::string> users;
  while (iter->valid()) {
    auto account = MetaKeyUtils::parseUser(iter->key());
    auto password = MetaKeyUtils::parseUserPwd(iter->val());
    users.emplace(std::move(account), std::move(password));
    iter->next();
  }
  resp_.set_users(std::move(users));
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  onFinished();
}

void ListRolesProcessor::process(const cpp2::ListRolesReq& req) {
  auto spaceId = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(spaceId);

  folly::SharedMutex::ReadHolder rHolder(LockUtils::userLock());
  auto prefix = MetaKeyUtils::roleSpacePrefix(spaceId);
  auto ret = doPrefix(prefix);
  if (!nebula::ok(ret)) {
    auto retCode = nebula::error(ret);
    LOG(ERROR) << "List roles failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto iter = nebula::value(ret).get();
  std::vector<nebula::meta::cpp2::RoleItem> roles;
  while (iter->valid()) {
    auto account = MetaKeyUtils::parseRoleUser(iter->key());
    auto val = iter->val();
    cpp2::RoleItem role;
    role.set_user_id(std::move(account));
    role.set_space_id(spaceId);
    role.set_role_type(*reinterpret_cast<const cpp2::RoleType*>(val.begin()));
    roles.emplace_back(std::move(role));
    iter->next();
  }
  resp_.set_roles(std::move(roles));
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  onFinished();
}

void GetUserRolesProcessor::process(const cpp2::GetUserRolesReq& req) {
  folly::SharedMutex::WriteHolder wHolder(LockUtils::userLock());
  const auto& act = req.get_account();

  auto prefix = MetaKeyUtils::rolesPrefix();
  auto ret = doPrefix(prefix);
  if (!nebula::ok(ret)) {
    auto retCode = nebula::error(ret);
    LOG(ERROR) << "Prefix roles failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto iter = nebula::value(ret).get();
  std::vector<nebula::meta::cpp2::RoleItem> roles;
  while (iter->valid()) {
    auto account = MetaKeyUtils::parseRoleUser(iter->key());
    auto spaceId = MetaKeyUtils::parseRoleSpace(iter->key());
    if (account == act) {
      auto val = iter->val();
      cpp2::RoleItem role;
      role.set_user_id(std::move(account));
      role.set_space_id(spaceId);
      role.set_role_type(*reinterpret_cast<const cpp2::RoleType*>(val.begin()));
      roles.emplace_back(std::move(role));
    }
    iter->next();
  }
  resp_.set_roles(std::move(roles));
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
