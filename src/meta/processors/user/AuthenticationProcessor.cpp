/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/user/AuthenticationProcessor.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "kvstore/LogEncoder.h"

namespace nebula {
namespace meta {

void CreateUserProcessor::process(const cpp2::CreateUserReq& req) {
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  const auto& account = req.get_account();
  const auto& password = req.get_encoded_pwd();

  auto retCode = userExist(account);
  if (retCode != nebula::cpp2::ErrorCode::E_USER_NOT_FOUND) {
    if (retCode == nebula::cpp2::ErrorCode::SUCCEEDED) {
      if (!req.get_if_not_exists()) {
        LOG(INFO) << "Create User Failed : User " << account << " already existed!";
        retCode = nebula::cpp2::ErrorCode::E_EXISTED;
      }
    } else {
      LOG(INFO) << "Create User Failed : User " << account
                << " error: " << apache::thrift::util::enumNameSafe(retCode);
    }
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  LOG(INFO) << "Create User " << account;
  std::vector<kvstore::KV> data;
  data.emplace_back(MetaKeyUtils::userKey(account), MetaKeyUtils::userVal(password));
  auto timeInMilliSec = time::WallClock::fastNowInMilliSec();
  LastUpdateTimeMan::update(data, timeInMilliSec);
  auto ret = doSyncPut(std::move(data));
  handleErrorCode(ret);
  onFinished();
}

void AlterUserProcessor::process(const cpp2::AlterUserReq& req) {
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
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
    LOG(INFO) << "Get User Failed : User " << account
              << " error: " << apache::thrift::util::enumNameSafe(errCode);
    handleErrorCode(errCode);
    onFinished();
    return;
  }

  LOG(INFO) << "Alter User " << account;
  std::vector<kvstore::KV> data;
  data.emplace_back(std::move(userKey), std::move(userVal));
  auto timeInMilliSec = time::WallClock::fastNowInMilliSec();
  LastUpdateTimeMan::update(data, timeInMilliSec);
  auto ret = doSyncPut(std::move(data));
  handleErrorCode(ret);
  onFinished();
}

void DropUserProcessor::process(const cpp2::DropUserReq& req) {
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  const auto& account = req.get_account();

  auto retCode = userExist(account);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    if (retCode == nebula::cpp2::ErrorCode::E_USER_NOT_FOUND) {
      if (req.get_if_exists()) {
        retCode = nebula::cpp2::ErrorCode::SUCCEEDED;
      } else {
        LOG(INFO) << "Drop User Failed: " << account << " not found.";
      }
    } else {
      LOG(INFO) << "Drop User Failed, User " << account
                << " error: " << apache::thrift::util::enumNameSafe(retCode);
    }
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto batchHolder = std::make_unique<kvstore::BatchHolder>();
  batchHolder->remove(MetaKeyUtils::userKey(account));

  // Collect related roles by user.
  auto prefix = MetaKeyUtils::rolesPrefix();
  auto iterRet = doPrefix(prefix);
  if (!nebula::ok(iterRet)) {
    retCode = nebula::error(iterRet);
    // The error of prefix is leader change
    LOG(INFO) << "Drop User Failed, User " << account
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
      batchHolder->remove(key.str());
    }
    iter->next();
  }

  LOG(INFO) << "Drop User " << account;
  auto timeInMilliSec = time::WallClock::fastNowInMilliSec();
  LastUpdateTimeMan::update(batchHolder.get(), timeInMilliSec);
  auto batch = encodeBatchValue(std::move(batchHolder)->getBatch());
  doBatchOperation(std::move(batch));
}

void GrantProcessor::process(const cpp2::GrantRoleReq& req) {
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
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
    LOG(INFO) << "Grant User Failed : User " << account
              << " error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  LOG(INFO) << "Grant User " << account
            << " role:" << apache::thrift::util::enumNameSafe(roleItem.get_role_type());
  std::vector<kvstore::KV> data;
  data.emplace_back(MetaKeyUtils::roleKey(spaceId, account),
                    MetaKeyUtils::roleVal(roleItem.get_role_type()));
  auto timeInMilliSec = time::WallClock::fastNowInMilliSec();
  LastUpdateTimeMan::update(data, timeInMilliSec);
  auto ret = doSyncPut(std::move(data));
  handleErrorCode(ret);
  onFinished();
}

void RevokeProcessor::process(const cpp2::RevokeRoleReq& req) {
  folly::SharedMutex::ReadHolder holder(LockUtils::lock());
  const auto& roleItem = req.get_role_item();
  auto spaceId = roleItem.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(spaceId);
  const auto& account = roleItem.get_user_id();

  auto userRet = userExist(account);
  if (userRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Revoke User Failed : User " << account
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
    LOG(INFO) << "Get Role User Failed : User " << account
              << " error: " << apache::thrift::util::enumNameSafe(userRet);
    handleErrorCode(userRet);
    onFinished();
    return;
  }
  auto val = nebula::value(result);
  const auto role = *reinterpret_cast<const cpp2::RoleType*>(val.c_str());
  if (role != roleItem.get_role_type()) {
    LOG(INFO) << "Revoke User Failed : User " << account << " error: "
              << apache::thrift::util::enumNameSafe(nebula::cpp2::ErrorCode::E_IMPROPER_ROLE);
    handleErrorCode(nebula::cpp2::ErrorCode::E_IMPROPER_ROLE);
    onFinished();
    return;
  }

  LOG(INFO) << "Revoke user " << account
            << "'s role: " << apache::thrift::util::enumNameSafe(roleItem.get_role_type());

  auto batchHolder = std::make_unique<kvstore::BatchHolder>();
  batchHolder->remove(std::move(roleKey));
  auto timeInMilliSec = time::WallClock::fastNowInMilliSec();
  LastUpdateTimeMan::update(batchHolder.get(), timeInMilliSec);
  auto batch = encodeBatchValue(std::move(batchHolder)->getBatch());
  doBatchOperation(std::move(batch));
}

void ChangePasswordProcessor::process(const cpp2::ChangePasswordReq& req) {
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  const auto& account = req.get_account();
  auto userRet = userExist(account);
  if (userRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Change password Failed, get user " << account << " failed, "
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
    LOG(INFO) << "Get user " << account << " failed, "
              << " error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  } else {
    if (!nebula::value(checkRet)) {
      auto retCode = nebula::cpp2::ErrorCode::E_INVALID_PASSWORD;
      handleErrorCode(retCode);
      LOG(INFO) << "Change password failed, user " << account
                << apache::thrift::util::enumNameSafe(retCode);
      onFinished();
      return;
    }
  }

  LOG(INFO) << "Change password for user " << account;
  auto userKey = MetaKeyUtils::userKey(account);
  auto userVal = MetaKeyUtils::userVal(req.get_new_encoded_pwd());
  std::vector<kvstore::KV> data;
  data.emplace_back(std::move(userKey), std::move(userVal));
  auto timeInMilliSec = time::WallClock::fastNowInMilliSec();
  LastUpdateTimeMan::update(data, timeInMilliSec);
  auto ret = doSyncPut(std::move(data));
  handleErrorCode(ret);
  onFinished();
}

void ListUsersProcessor::process(const cpp2::ListUsersReq&) {
  folly::SharedMutex::ReadHolder holder(LockUtils::lock());
  std::string prefix = MetaKeyUtils::userPrefix();
  auto ret = doPrefix(prefix);
  if (!nebula::ok(ret)) {
    auto retCode = nebula::error(ret);
    LOG(INFO) << "List User failed, error: " << apache::thrift::util::enumNameSafe(retCode);
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

  VLOG(2) << "List all users, user count: " << users.size();
  resp_.users_ref() = std::move(users);
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  onFinished();
}

void ListRolesProcessor::process(const cpp2::ListRolesReq& req) {
  auto spaceId = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(spaceId);

  folly::SharedMutex::ReadHolder holder(LockUtils::lock());
  auto prefix = MetaKeyUtils::roleSpacePrefix(spaceId);
  auto ret = doPrefix(prefix);
  if (!nebula::ok(ret)) {
    auto retCode = nebula::error(ret);
    LOG(INFO) << "List roles failed, error: " << apache::thrift::util::enumNameSafe(retCode);
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
    role.user_id_ref() = std::move(account);
    role.space_id_ref() = spaceId;
    role.role_type_ref() = *reinterpret_cast<const cpp2::RoleType*>(val.begin());
    roles.emplace_back(std::move(role));
    iter->next();
  }

  VLOG(2) << "List all user roles for space:" << spaceId << ", user role count: " << roles.size();
  resp_.roles_ref() = std::move(roles);
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  onFinished();
}

void GetUserRolesProcessor::process(const cpp2::GetUserRolesReq& req) {
  folly::SharedMutex::ReadHolder holder(LockUtils::lock());
  const auto& act = req.get_account();

  auto retCode = userExist(act);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    if (retCode == nebula::cpp2::ErrorCode::E_USER_NOT_FOUND) {
      LOG(INFO) << "Get User Roles Failed: " << act << " not found.";
    } else {
      LOG(INFO) << "Get User Roles Failed, User " << act
                << " error: " << apache::thrift::util::enumNameSafe(retCode);
    }
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto prefix = MetaKeyUtils::rolesPrefix();
  auto ret = doPrefix(prefix);
  if (!nebula::ok(ret)) {
    retCode = nebula::error(ret);
    LOG(INFO) << "Prefix roles failed, error: " << apache::thrift::util::enumNameSafe(retCode);
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
      role.user_id_ref() = std::move(account);
      role.space_id_ref() = spaceId;
      role.role_type_ref() = *reinterpret_cast<const cpp2::RoleType*>(val.begin());
      roles.emplace_back(std::move(role));
    }
    iter->next();
  }

  VLOG(2) << "Get user:" << act << " roles, its count: " << roles.size();
  resp_.roles_ref() = std::move(roles);
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
