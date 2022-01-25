/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_AUTHENTICATIONPROCESSOR_H
#define META_AUTHENTICATIONPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Create user with account and password
 *
 */
class CreateUserProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static CreateUserProcessor* instance(kvstore::KVStore* kvstore) {
    return new CreateUserProcessor(kvstore);
  }

  void process(const cpp2::CreateUserReq& req);

 private:
  explicit CreateUserProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

/**
 * @brief Update given user's password, return error if user
 *        not exist. It will override old password without checking.
 *
 */
class AlterUserProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static AlterUserProcessor* instance(kvstore::KVStore* kvstore) {
    return new AlterUserProcessor(kvstore);
  }

  void process(const cpp2::AlterUserReq& req);

 private:
  explicit AlterUserProcessor(kvstore::KVStore* kvstore) : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

/**
 * @brief Drop user and revoke related roles.
 *
 */
class DropUserProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static DropUserProcessor* instance(kvstore::KVStore* kvstore) {
    return new DropUserProcessor(kvstore);
  }

  void process(const cpp2::DropUserReq& req);

 private:
  explicit DropUserProcessor(kvstore::KVStore* kvstore) : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

/**
 * @brief Grant user space's given role permission(RoleType:GOD, ADMIN, DBA, USER, GUEST)
 *
 */
class GrantProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static GrantProcessor* instance(kvstore::KVStore* kvstore) {
    return new GrantProcessor(kvstore);
  }

  void process(const cpp2::GrantRoleReq& req);

 private:
  explicit GrantProcessor(kvstore::KVStore* kvstore) : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

/**
 * @brief Revoke user's given role. Return error if user or role not exist.
 *
 */
class RevokeProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static RevokeProcessor* instance(kvstore::KVStore* kvstore) {
    return new RevokeProcessor(kvstore);
  }

  void process(const cpp2::RevokeRoleReq& req);

 private:
  explicit RevokeProcessor(kvstore::KVStore* kvstore) : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

/**
 * @brief Change given user's password, but it will check old password first.
 *
 */
class ChangePasswordProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static ChangePasswordProcessor* instance(kvstore::KVStore* kvstore) {
    return new ChangePasswordProcessor(kvstore);
  }

  void process(const cpp2::ChangePasswordReq& req);

 private:
  explicit ChangePasswordProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

/**
 * @brief Get all user's accounts and passwords.
 *
 */
class ListUsersProcessor : public BaseProcessor<cpp2::ListUsersResp> {
 public:
  static ListUsersProcessor* instance(kvstore::KVStore* kvstore) {
    return new ListUsersProcessor(kvstore);
  }

  void process(const cpp2::ListUsersReq& req);

 private:
  explicit ListUsersProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ListUsersResp>(kvstore) {}
};

/**
 * @brief List all user roles granted to given space.
 *
 */
class ListRolesProcessor : public BaseProcessor<cpp2::ListRolesResp> {
 public:
  static ListRolesProcessor* instance(kvstore::KVStore* kvstore) {
    return new ListRolesProcessor(kvstore);
  }

  void process(const cpp2::ListRolesReq& req);

 private:
  explicit ListRolesProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ListRolesResp>(kvstore) {}
};

/**
 * @brief Get given user's all roles and relative spaces
 *
 */
class GetUserRolesProcessor : public BaseProcessor<cpp2::ListRolesResp> {
 public:
  static GetUserRolesProcessor* instance(kvstore::KVStore* kvstore) {
    return new GetUserRolesProcessor(kvstore);
  }

  void process(const cpp2::GetUserRolesReq& req);

 private:
  explicit GetUserRolesProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ListRolesResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_AUTHENTICATIONPROCESSOR_H
