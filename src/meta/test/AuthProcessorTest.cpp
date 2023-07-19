/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "meta/processors/parts/CreateSpaceProcessor.h"
#include "meta/processors/parts/DropSpaceProcessor.h"
#include "meta/processors/user/AuthenticationProcessor.h"
#include "meta/processors/zone/AddHostsProcessor.h"
#include "meta/test/TestUtils.h"

namespace nebula {
namespace meta {

TEST(AuthProcessorTest, CreateUserTest) {
  fs::TempDir rootPath("/tmp/CreateUserTest.XXXXXX");
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
  {
    cpp2::CreateUserReq req;
    req.if_not_exists_ref() = false;
    req.account_ref() = "user1";
    req.encoded_pwd_ref() = "password";
    auto* processor = CreateUserProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

    // Check user data has been inserted.
    std::string userVal;
    std::unique_ptr<kvstore::KVIterator> iter;
    auto retCode =
        kv->get(kDefaultSpaceId, kDefaultPartId, MetaKeyUtils::userKey("user1"), &userVal);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, retCode);
  }
  // Test user exists and param 'if_not_exists' == false;
  {
    cpp2::CreateUserReq req;
    req.if_not_exists_ref() = false;
    req.account_ref() = "user1";
    req.encoded_pwd_ref() = "password";
    auto* processor = CreateUserProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_EXISTED, resp.get_code());
  }
  // Test user exists and param 'if_not_exists' == true;
  {
    cpp2::CreateUserReq req;
    req.if_not_exists_ref() = true;
    req.account_ref() = "user1";
    req.encoded_pwd_ref() = "password";
    auto* processor = CreateUserProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
}

TEST(AuthProcessorTest, AlterUserTest) {
  fs::TempDir rootPath("/tmp/AlterUserTest.XXXXXX");
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
  // create a user.
  {
    cpp2::CreateUserReq req;
    req.if_not_exists_ref() = false;
    req.account_ref() = "user1";
    req.encoded_pwd_ref() = "password";
    auto* processor = CreateUserProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // Simple alter user.
  {
    cpp2::AlterUserReq req;
    req.account_ref() = "user1";
    req.encoded_pwd_ref() = "password_1";
    auto* processor = AlterUserProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // If user not exists
  {
    cpp2::AlterUserReq req;
    req.account_ref() = "user2";
    req.encoded_pwd_ref() = "user3";
    auto* processor = AlterUserProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_USER_NOT_FOUND, resp.get_code());
  }
}

TEST(AuthProcessorTest, DropUserTest) {
  fs::TempDir rootPath("/tmp/AlterUserTest.XXXXXX");
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
  // create a user.
  {
    cpp2::CreateUserReq req;
    req.if_not_exists_ref() = false;
    req.account_ref() = "user1";
    req.encoded_pwd_ref() = "password";
    auto* processor = CreateUserProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // User not exists and 'if_exists' = false.
  {
    cpp2::DropUserReq req;
    req.account_ref() = "user2";
    req.if_exists_ref() = false;
    auto* processor = DropUserProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_USER_NOT_FOUND, resp.get_code());
  }
  // User not exists and 'if_exists' = true.
  {
    cpp2::DropUserReq req;
    req.account_ref() = "user2";
    req.if_exists_ref() = true;
    auto* processor = DropUserProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // User exists.
  {
    cpp2::DropUserReq req;
    req.account_ref() = "user1";
    req.if_exists_ref() = false;
    auto* processor = DropUserProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

    // Check user dropped.
    std::string userVal;
    std::unique_ptr<kvstore::KVIterator> iter;
    auto retCode =
        kv->get(kDefaultSpaceId, kDefaultPartId, MetaKeyUtils::userKey("user1"), &userVal);
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND, retCode);
  }
}

TEST(AuthProcessorTest, GrantRevokeTest) {
  fs::TempDir rootPath("/tmp/GrantRevokeTest.XXXXXX");
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
  {
    cpp2::AddHostsReq req;
    std::vector<HostAddr> hosts = {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}};
    req.hosts_ref() = std::move(hosts);
    auto* processor = AddHostsProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    std::vector<HostAddr> hosts = {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}};
    TestUtils::registerHB(kv.get(), hosts);
  }
  GraphSpaceID space1, space2;
  // create space1
  {
    cpp2::SpaceDesc properties;
    properties.space_name_ref() = "space1";
    properties.partition_num_ref() = 1;
    properties.replica_factor_ref() = 3;
    properties.charset_name_ref() = "utf8";
    properties.collate_name_ref() = "utf8_bin";
    cpp2::CreateSpaceReq req;
    req.properties_ref() = std::move(properties);
    auto* processor = CreateSpaceProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    space1 = resp.get_id().get_space_id();
  }
  // create space2
  {
    cpp2::SpaceDesc properties;
    properties.space_name_ref() = "space2";
    properties.partition_num_ref() = 1;
    properties.replica_factor_ref() = 3;
    properties.charset_name_ref() = "utf8";
    properties.collate_name_ref() = "utf8_bin";
    cpp2::CreateSpaceReq req;
    req.properties_ref() = std::move(properties);
    auto* processor = CreateSpaceProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    space2 = resp.get_id().get_space_id();
  }
  // create a user1.
  {
    cpp2::CreateUserReq req;
    req.if_not_exists_ref() = false;
    req.account_ref() = "user1";
    req.encoded_pwd_ref() = "password";
    auto* processor = CreateUserProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // create a user2.
  {
    cpp2::CreateUserReq req;
    req.if_not_exists_ref() = false;
    req.account_ref() = "user2";
    req.encoded_pwd_ref() = "password";
    auto* processor = CreateUserProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // create a user3.
  {
    cpp2::CreateUserReq req;
    req.if_not_exists_ref() = false;
    req.account_ref() = "user3";
    req.encoded_pwd_ref() = "password";
    auto* processor = CreateUserProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // grant role to user for space1, user not exists.
  {
    cpp2::GrantRoleReq req;
    nebula::meta::cpp2::RoleItem role;
    role.user_id_ref() = "user";
    role.space_id_ref() = space1;
    role.role_type_ref() = cpp2::RoleType::GUEST;
    req.role_item_ref() = role;
    auto* processor = GrantProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_USER_NOT_FOUND, resp.get_code());
  }
  // grant role to user1 for space, space not exists.
  {
    cpp2::GrantRoleReq req;
    nebula::meta::cpp2::RoleItem role;
    role.user_id_ref() = "user1";
    role.space_id_ref() = -1;
    role.role_type_ref() = cpp2::RoleType::GUEST;
    req.role_item_ref() = role;
    auto* processor = GrantProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND, resp.get_code());
  }
  // grant role to user1 for space1.
  {
    cpp2::GrantRoleReq req;
    nebula::meta::cpp2::RoleItem role;
    role.user_id_ref() = "user1";
    role.space_id_ref() = space1;
    role.role_type_ref() = cpp2::RoleType::GUEST;
    req.role_item_ref() = role;
    auto* processor = GrantProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // grant role to user2 for space1.
  {
    cpp2::GrantRoleReq req;
    nebula::meta::cpp2::RoleItem role;
    role.user_id_ref() = "user2";
    role.space_id_ref() = space1;
    role.role_type_ref() = cpp2::RoleType::ADMIN;
    req.role_item_ref() = role;
    auto* processor = GrantProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // grant role to user2 for space2.
  {
    cpp2::GrantRoleReq req;
    nebula::meta::cpp2::RoleItem role;
    role.user_id_ref() = "user2";
    role.space_id_ref() = space2;
    role.role_type_ref() = cpp2::RoleType::DBA;
    req.role_item_ref() = role;
    auto* processor = GrantProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // list roles.
  {
    cpp2::ListRolesReq req;
    req.space_id_ref() = space1;
    auto* processor = ListRolesProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    std::vector<nebula::meta::cpp2::RoleItem> expectRoles;
    cpp2::RoleItem role;
    role.space_id_ref() = space1;
    role.user_id_ref() = "user1";
    role.role_type_ref() = cpp2::RoleType::GUEST;
    expectRoles.emplace_back(std::move(role));
    role.space_id_ref() = space1;
    role.user_id_ref() = "user2";
    role.role_type_ref() = cpp2::RoleType::ADMIN;
    expectRoles.emplace_back(std::move(role));
    ASSERT_EQ(expectRoles, resp.get_roles());
  }
  // list roles.
  {
    cpp2::ListRolesReq req;
    req.space_id_ref() = space2;
    auto* processor = ListRolesProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    std::vector<nebula::meta::cpp2::RoleItem> expectRoles;
    cpp2::RoleItem role;
    role.space_id_ref() = space2;
    role.user_id_ref() = "user2";
    role.role_type_ref() = cpp2::RoleType::DBA;
    expectRoles.emplace_back(std::move(role));
    ASSERT_EQ(expectRoles, resp.get_roles());
  }
  // user not exists.
  {
    cpp2::RevokeRoleReq req;
    nebula::meta::cpp2::RoleItem role;
    role.user_id_ref() = "user";
    role.space_id_ref() = space2;
    req.role_item_ref() = role;
    auto* processor = RevokeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_USER_NOT_FOUND, resp.get_code());
  }
  // space not exists.
  {
    cpp2::RevokeRoleReq req;
    nebula::meta::cpp2::RoleItem role;
    role.user_id_ref() = "user1";
    role.space_id_ref() = -1;
    req.role_item_ref() = role;
    auto* processor = RevokeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND, resp.get_code());
  }
  // actual role is GUEST, but revoke role ADMIN, expect error.
  {
    cpp2::RevokeRoleReq req;
    nebula::meta::cpp2::RoleItem role;
    role.user_id_ref() = "user1";
    role.space_id_ref() = space1;
    role.role_type_ref() = cpp2::RoleType::ADMIN;
    req.role_item_ref() = role;
    auto* processor = RevokeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_IMPROPER_ROLE, resp.get_code());
  }
  // actual role is GUEST, but revoke unknown, expect error.
  {
    cpp2::RevokeRoleReq req;
    nebula::meta::cpp2::RoleItem role;
    role.user_id_ref() = "user1";
    role.space_id_ref() = space1;
    req.role_item_ref() = role;
    auto* processor = RevokeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_IMPROPER_ROLE, resp.get_code());
  }
  // revoke
  {
    cpp2::RevokeRoleReq req;
    nebula::meta::cpp2::RoleItem role;
    role.user_id_ref() = "user1";
    role.space_id_ref() = space1;
    role.role_type_ref() = cpp2::RoleType::GUEST;
    req.role_item_ref() = role;
    auto* processor = RevokeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // list roles.
  {
    cpp2::ListRolesReq req;
    req.space_id_ref() = space1;
    auto* processor = ListRolesProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    std::vector<nebula::meta::cpp2::RoleItem> expectRoles;
    cpp2::RoleItem role;
    role.space_id_ref() = space1;
    role.user_id_ref() = "user2";
    role.role_type_ref() = cpp2::RoleType::ADMIN;
    expectRoles.emplace_back(std::move(role));
    ASSERT_EQ(expectRoles, resp.get_roles());
  }
  // list roles.
  {
    cpp2::ListRolesReq req;
    req.space_id_ref() = space2;
    auto* processor = ListRolesProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    std::vector<nebula::meta::cpp2::RoleItem> expectRoles;
    cpp2::RoleItem role;
    role.space_id_ref() = space2;
    role.user_id_ref() = "user2";
    role.role_type_ref() = cpp2::RoleType::DBA;
    expectRoles.emplace_back(std::move(role));
    ASSERT_EQ(expectRoles, resp.get_roles());
  }
  // role not exists.
  {
    cpp2::RevokeRoleReq req;
    nebula::meta::cpp2::RoleItem role;
    role.user_id_ref() = "user1";
    role.space_id_ref() = space1;
    req.role_item_ref() = role;
    auto* processor = RevokeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_ROLE_NOT_FOUND, resp.get_code());
  }
  // list users
  {
    cpp2::ListUsersReq req;
    auto* processor = ListUsersProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(3, resp.get_users().size());
  }
  // role deleted after drop user
  {
    cpp2::DropUserReq req;
    req.account_ref() = "user2";
    req.if_exists_ref() = false;
    auto* processor = DropUserProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // list roles.
  {
    cpp2::ListRolesReq req;
    req.space_id_ref() = space2;
    auto* processor = ListRolesProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(0, resp.get_roles().size());
  }
  // grant role to user2 for space1.
  {
    cpp2::GrantRoleReq req;
    nebula::meta::cpp2::RoleItem role;
    role.user_id_ref() = "user1";
    role.space_id_ref() = space1;
    role.role_type_ref() = cpp2::RoleType::ADMIN;
    req.role_item_ref() = role;
    auto* processor = GrantProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    cpp2::ListRolesReq req;
    req.space_id_ref() = space1;
    auto* processor = ListRolesProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(1, resp.get_roles().size());
  }
  {
    cpp2::DropSpaceReq req;
    req.space_name_ref() = "space1";
    req.if_exists_ref() = false;
    auto* processor = DropSpaceProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

    // check role deleted after drop space
    auto rolePrefix = MetaKeyUtils::roleSpacePrefix(space1);
    std::unique_ptr<kvstore::KVIterator> roleIter;
    auto roleRet = kv->prefix(kDefaultSpaceId, kDefaultPartId, rolePrefix, &roleIter);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, roleRet);
    auto roleCount = 0;
    while (roleIter->valid()) {
      roleCount++;
      roleIter->next();
    }
    ASSERT_EQ(0, roleCount);
  }
}

TEST(AuthProcessorTest, ChangePasswordTest) {
  fs::TempDir rootPath("/tmp/ChangePasswordTest.XXXXXX");
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
  // create a user.
  {
    cpp2::CreateUserReq req;
    req.if_not_exists_ref() = false;
    req.account_ref() = "user1";
    req.encoded_pwd_ref() = "password";
    auto* processor = CreateUserProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // change password, user dose not exists.
  {
    cpp2::ChangePasswordReq req;
    req.account_ref() = "user";
    req.new_encoded_pwd_ref() = "pwd1";
    req.old_encoded_pwd_ref() = "pwd1";
    auto* processor = ChangePasswordProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_USER_NOT_FOUND, resp.get_code());
  }
  // change password, old password is not valid.
  {
    cpp2::ChangePasswordReq req;
    req.account_ref() = "user1";
    req.new_encoded_pwd_ref() = "pwd1";
    req.old_encoded_pwd_ref() = "pwd1";
    auto* processor = ChangePasswordProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_INVALID_PASSWORD, resp.get_code());
  }
  // change password, old password is valid.
  {
    cpp2::ChangePasswordReq req;
    req.account_ref() = "user1";
    req.new_encoded_pwd_ref() = "pwd1";
    req.old_encoded_pwd_ref() = "password";
    auto* processor = ChangePasswordProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // change password, old password is not need check.
  {
    cpp2::ChangePasswordReq req;
    req.account_ref() = "user1";
    req.new_encoded_pwd_ref() = "pwdpwd";
    auto* processor = ChangePasswordProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_INVALID_PASSWORD, resp.get_code());
  }
}

}  // namespace meta
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}
