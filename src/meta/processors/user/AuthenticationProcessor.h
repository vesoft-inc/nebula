/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_AUTHENTICATIONPROCESSOR_H
#define META_AUTHENTICATIONPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

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

class AlterUserProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static AlterUserProcessor* instance(kvstore::KVStore* kvstore) {
    return new AlterUserProcessor(kvstore);
  }

  void process(const cpp2::AlterUserReq& req);

 private:
  explicit AlterUserProcessor(kvstore::KVStore* kvstore) : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

class DropUserProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static DropUserProcessor* instance(kvstore::KVStore* kvstore) {
    return new DropUserProcessor(kvstore);
  }

  void process(const cpp2::DropUserReq& req);

 private:
  explicit DropUserProcessor(kvstore::KVStore* kvstore) : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

class GrantProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static GrantProcessor* instance(kvstore::KVStore* kvstore) { return new GrantProcessor(kvstore); }

  void process(const cpp2::GrantRoleReq& req);

 private:
  explicit GrantProcessor(kvstore::KVStore* kvstore) : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

class RevokeProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static RevokeProcessor* instance(kvstore::KVStore* kvstore) {
    return new RevokeProcessor(kvstore);
  }

  void process(const cpp2::RevokeRoleReq& req);

 private:
  explicit RevokeProcessor(kvstore::KVStore* kvstore) : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

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
