/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include <gtest/gtest.h>
#include "meta/test/TestUtils.h"
#include "meta/processors/usersMan/AuthenticationProcessor.h"
#include "meta/processors/partsMan/CreateSpaceProcessor.h"
#include "meta/processors/partsMan/DropSpaceProcessor.h"

namespace nebula {
namespace meta {

TEST(AuthProcessorTest, CreateUserTest) {
    fs::TempDir rootPath("/tmp/CreateUserTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    {
        cpp2::CreateUserReq req;
        req.set_if_not_exists(false);
        req.set_account("user1");
        req.set_encoded_pwd("password");
        auto* processor = CreateUserProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

        // Check user data has been inserted.
        std::string userVal;
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retCode = kv->get(kDefaultSpaceId, kDefaultPartId,
                               MetaServiceUtils::userKey("user1"),
                               &userVal);
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, retCode);
    }
    // Test user exists and param 'if_not_exists' == false;
    {
        cpp2::CreateUserReq req;
        req.set_if_not_exists(false);
        req.set_account("user1");
        req.set_encoded_pwd("password");
        auto* processor = CreateUserProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_EXISTED, resp.get_code());
    }
    // Test user exists and param 'if_not_exists' == true;
    {
        cpp2::CreateUserReq req;
        req.set_if_not_exists(true);
        req.set_account("user1");
        req.set_encoded_pwd("password");
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
        req.set_if_not_exists(false);
        req.set_account("user1");
        req.set_encoded_pwd("password");
        auto* processor = CreateUserProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Simple alter user.
    {
        cpp2::AlterUserReq req;
        req.set_account("user1");
        req.set_encoded_pwd("password_1");
        auto* processor = AlterUserProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // If user not exists
    {
        cpp2::AlterUserReq req;
        req.set_account("user2");
        req.set_encoded_pwd("user3");
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
        req.set_if_not_exists(false);
        req.set_account("user1");
        req.set_encoded_pwd("password");
        auto* processor = CreateUserProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // User not exists and 'if_exists' = false.
    {
        cpp2::DropUserReq req;
        req.set_account("user2");
        req.set_if_exists(false);
        auto* processor = DropUserProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_USER_NOT_FOUND, resp.get_code());
    }
    // User not exists and 'if_exists' = true.
    {
        cpp2::DropUserReq req;
        req.set_account("user2");
        req.set_if_exists(true);
        auto* processor = DropUserProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // User exists.
    {
        cpp2::DropUserReq req;
        req.set_account("user1");
        req.set_if_exists(false);
        auto* processor = DropUserProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

        // Check user dropped.
        std::string userVal;
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retCode = kv->get(kDefaultSpaceId, kDefaultPartId,
                               MetaServiceUtils::userKey("user1"),
                               &userVal);
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND, retCode);
    }
}

TEST(AuthProcessorTest, GrantRevokeTest) {
    fs::TempDir rootPath("/tmp/GrantRevokeTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    TestUtils::createSomeHosts(kv.get());
    GraphSpaceID space1, space2;
    // create space1
    {
        cpp2::SpaceDesc properties;
        properties.set_space_name("space1");
        properties.set_partition_num(1);
        properties.set_replica_factor(3);
        properties.set_charset_name("utf8");
        properties.set_collate_name("utf8_bin");
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));
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
        properties.set_space_name("space2");
        properties.set_partition_num(1);
        properties.set_replica_factor(3);
        properties.set_charset_name("utf8");
        properties.set_collate_name("utf8_bin");
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));
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
        req.set_if_not_exists(false);
        req.set_account("user1");
        req.set_encoded_pwd("password");
        auto* processor = CreateUserProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // create a user2.
    {
        cpp2::CreateUserReq req;
        req.set_if_not_exists(false);
        req.set_account("user2");
        req.set_encoded_pwd("password");
        auto* processor = CreateUserProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // create a user3.
    {
        cpp2::CreateUserReq req;
        req.set_if_not_exists(false);
        req.set_account("user3");
        req.set_encoded_pwd("password");
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
        role.set_user_id("user");
        role.set_space_id(space1);
        role.set_role_type(cpp2::RoleType::GUEST);
        req.set_role_item(role);
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
        role.set_user_id("user1");
        role.set_space_id(-1);
        role.set_role_type(cpp2::RoleType::GUEST);
        req.set_role_item(role);
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
        role.set_user_id("user1");
        role.set_space_id(space1);
        role.set_role_type(cpp2::RoleType::GUEST);
        req.set_role_item(role);
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
        role.set_user_id("user2");
        role.set_space_id(space1);
        role.set_role_type(cpp2::RoleType::ADMIN);
        req.set_role_item(role);
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
        role.set_user_id("user2");
        role.set_space_id(space2);
        role.set_role_type(cpp2::RoleType::DBA);
        req.set_role_item(role);
        auto* processor = GrantProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // list roles.
    {
        cpp2::ListRolesReq req;
        req.set_space_id(space1);
        auto* processor = ListRolesProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        std::vector<nebula::meta::cpp2::RoleItem> expectRoles;
        cpp2::RoleItem role;
        role.set_space_id(space1);
        role.set_user_id("user1");
        role.set_role_type(cpp2::RoleType::GUEST);
        expectRoles.emplace_back(std::move(role));
        role.set_space_id(space1);
        role.set_user_id("user2");
        role.set_role_type(cpp2::RoleType::ADMIN);
        expectRoles.emplace_back(std::move(role));
        ASSERT_EQ(expectRoles, resp.get_roles());
    }
    // list roles.
    {
        cpp2::ListRolesReq req;
        req.set_space_id(space2);
        auto* processor = ListRolesProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        std::vector<nebula::meta::cpp2::RoleItem> expectRoles;
        cpp2::RoleItem role;
        role.set_space_id(space2);
        role.set_user_id("user2");
        role.set_role_type(cpp2::RoleType::DBA);
        expectRoles.emplace_back(std::move(role));
        ASSERT_EQ(expectRoles, resp.get_roles());
    }
    // user not exists.
    {
        cpp2::RevokeRoleReq req;
        nebula::meta::cpp2::RoleItem role;
        role.set_user_id("user");
        role.set_space_id(space2);
        req.set_role_item(role);
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
        role.set_user_id("user1");
        role.set_space_id(-1);
        req.set_role_item(role);
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
        role.set_user_id("user1");
        role.set_space_id(space1);
        role.set_role_type(cpp2::RoleType::ADMIN);
        req.set_role_item(role);
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
        role.set_user_id("user1");
        role.set_space_id(space1);
        req.set_role_item(role);
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
        role.set_user_id("user1");
        role.set_space_id(space1);
        role.set_role_type(cpp2::RoleType::GUEST);
        req.set_role_item(role);
        auto* processor = RevokeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // list roles.
    {
        cpp2::ListRolesReq req;
        req.set_space_id(space1);
        auto* processor = ListRolesProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        std::vector<nebula::meta::cpp2::RoleItem> expectRoles;
        cpp2::RoleItem role;
        role.set_space_id(space1);
        role.set_user_id("user2");
        role.set_role_type(cpp2::RoleType::ADMIN);
        expectRoles.emplace_back(std::move(role));
        ASSERT_EQ(expectRoles, resp.get_roles());
    }
    // list roles.
    {
        cpp2::ListRolesReq req;
        req.set_space_id(space2);
        auto* processor = ListRolesProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        std::vector<nebula::meta::cpp2::RoleItem> expectRoles;
        cpp2::RoleItem role;
        role.set_space_id(space2);
        role.set_user_id("user2");
        role.set_role_type(cpp2::RoleType::DBA);
        expectRoles.emplace_back(std::move(role));
        ASSERT_EQ(expectRoles, resp.get_roles());
    }
    // role not exists.
    {
        cpp2::RevokeRoleReq req;
        nebula::meta::cpp2::RoleItem role;
        role.set_user_id("user1");
        role.set_space_id(space1);
        req.set_role_item(role);
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
        req.set_account("user2");
        req.set_if_exists(false);
        auto* processor = DropUserProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // list roles.
    {
        cpp2::ListRolesReq req;
        req.set_space_id(space2);
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
        role.set_user_id("user1");
        role.set_space_id(space1);
        role.set_role_type(cpp2::RoleType::ADMIN);
        req.set_role_item(role);
        auto* processor = GrantProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::ListRolesReq req;
        req.set_space_id(space1);
        auto* processor = ListRolesProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(1, resp.get_roles().size());
    }
    {
        cpp2::DropSpaceReq req;
        req.set_space_name("space1");
        req.set_if_exists(false);
        auto* processor = DropSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

        // check role deleted after drop space
        auto rolePrefix = MetaServiceUtils::roleSpacePrefix(space1);
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
        req.set_if_not_exists(false);
        req.set_account("user1");
        req.set_encoded_pwd("password");
        auto* processor = CreateUserProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // change password, user dose not exists.
    {
        cpp2::ChangePasswordReq req;
        req.set_account("user");
        req.set_new_encoded_pwd("pwd1");
        req.set_old_encoded_pwd("pwd1");
        auto* processor = ChangePasswordProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_USER_NOT_FOUND, resp.get_code());
    }
    // change password, old password is not valid.
    {
        cpp2::ChangePasswordReq req;
        req.set_account("user1");
        req.set_new_encoded_pwd("pwd1");
        req.set_old_encoded_pwd("pwd1");
        auto* processor = ChangePasswordProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_INVALID_PASSWORD, resp.get_code());
    }
    // change password, old password is valid.
    {
        cpp2::ChangePasswordReq req;
        req.set_account("user1");
        req.set_new_encoded_pwd("pwd1");
        req.set_old_encoded_pwd("password");
        auto* processor = ChangePasswordProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // change password, old password is not need check.
    {
        cpp2::ChangePasswordReq req;
        req.set_account("user1");
        req.set_new_encoded_pwd("pwdpwd");
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
