/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <folly/String.h>
#include <fstream>
#include "fs/TempDir.h"
#include "meta/test/TestUtils.h"
#include "meta/processors/usersMan/AuthenticationProcessor.h"
#include "meta/processors/partsMan/CreateSpaceProcessor.h"

namespace nebula {
namespace meta {

using apache::thrift::FragileConstructor::FRAGILE;

TEST(AuthProcessorTest, CreateUserTest) {
    fs::TempDir rootPath("/tmp/CreateUserTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    // Simple test
    auto code = TestUtils::createUser(kv.get(), false, "user1", "pwd",
                                      false, 0, 0, 0, 0);
    ASSERT_TRUE(code.ok());

    /**
     * missing_ok should be turn on when "IF NOT EXISTS" is set.
     * the result will be SUCCEEDED whether the user exists or not.
     *
     * missing_ok should be turn off when without "IF NOT EXISTS".
     * the result will be EXISTED if the user exists.
     **/

    code = TestUtils::createUser(kv.get(), false, "user1", "pwd",
                                 false, 0, 0, 0, 0);
    ASSERT_FALSE(code.ok());

    code = TestUtils::createUser(kv.get(), true, "user1", "pwd",
                                 false, 0, 0, 0, 0);
    ASSERT_TRUE(code.ok());
}


TEST(AuthProcessorTest, AlterUserTest) {
    fs::TempDir rootPath("/tmp/AlterUserTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    // Setup
    {
        auto code = TestUtils::createUser(kv.get(), false, "user1", "pwd",
                                          false, 1, 2, 3, 4);
        ASSERT_TRUE(code.ok());
    }
    // Alter a few attributes
    {
        cpp2::AlterUserReq req;
        decltype(req.user_item) newUser;
        newUser.set_account("user1");
        newUser.set_is_lock(true);
        req.set_user_item(std::move(newUser));
        auto* processor = AlterUserProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Test user does not exist
    {
        cpp2::AlterUserReq req;
        decltype(req.user_item) newUser;
        newUser.set_account("user");
        req.set_user_item(std::move(newUser));
        auto* processor = AlterUserProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.get_code());
    }
    // Alter all attributes
    {
        cpp2::AlterUserReq req;
        decltype(req.user_item) newUser;
        newUser.set_account("user1");
        newUser.set_max_queries_per_hour(10);
        newUser.set_max_updates_per_hour(20);
        newUser.set_max_connections_per_hour(30);
        newUser.set_max_user_connections(40);
        newUser.set_is_lock(false);
        req.set_user_item(std::move(newUser));
        auto* processor = AlterUserProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Test GetUser
    {
        cpp2::GetUserReq req;
        req.set_account("user1");
        auto* processor = GetUserProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        decltype(resp.user_item) user;
        user.set_account("user1");
        user.set_max_queries_per_hour(10);
        user.set_max_updates_per_hour(20);
        user.set_max_connections_per_hour(30);
        user.set_max_user_connections(40);
        user.set_is_lock(false);
        ASSERT_EQ(user, resp.get_user_item());
    }
}

TEST(AuthProcessorTest, DropUserTest) {
    fs::TempDir rootPath("/tmp/DropUserTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    // Setup
    {
        auto code = TestUtils::createUser(kv.get(), false, "user1", "pwd",
                                          false, 1, 2, 3, 4);
        ASSERT_TRUE(code.ok());
    }
    // Simple drop.
    {
        cpp2::DropUserReq req;
        req.set_account("user1");
        req.set_missing_ok(false);
        auto* processor = DropUserProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }

    /**
     * missing_ok should be turn on when "IF EXISTS" is set.
     * the result will be SUCCEEDED whether the user exists or not.
     *
     * missing_ok should be turn off when without "IF EXISTS" .
     * the result will be NOT_FOUND if the user does not exist.
     **/
    // missing_ok = false , And user does net exist.
    {
        cpp2::DropUserReq req;
        req.set_account("user");
        req.set_missing_ok(false);
        auto* processor = DropUserProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.get_code());
    }
    // missing_ok = true , And user does net exist.
    {
        cpp2::DropUserReq req;
        req.set_account("user");
        req.set_missing_ok(true);
        auto* processor = DropUserProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
}

TEST(AuthProcessorTest, PasswordTest) {
    fs::TempDir rootPath("/tmp/PasswordTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    // Setup
    {
        auto code = TestUtils::createUser(kv.get(), false, "user1", "pwd",
                                          false, 4, 3, 2, 1);
        ASSERT_TRUE(code.ok());
    }
    // verify password.
    {
        cpp2::CheckPasswordReq req;
        req.set_account("user1");
        req.set_encoded_pwd("pwd");
        auto* processor = CheckPasswordProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // verify password.
    {
        cpp2::CheckPasswordReq req;
        req.set_account("user1");
        req.set_encoded_pwd("pwd1");
        auto* processor = CheckPasswordProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_INVALID_PASSWORD, resp.get_code());
    }
    // Change password for god role.
    {
        cpp2::ChangePasswordReq req;
        req.set_account("user1");
        req.set_new_encoded_pwd("password");
        auto* processor = ChangePasswordProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // If the user role is not GOD, the old password must be set.
    {
        cpp2::ChangePasswordReq req;
        req.set_account("user1");
        req.set_new_encoded_pwd("password");
        req.set_old_encoded_pwd("password1");
        auto* processor = ChangePasswordProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_INVALID_PASSWORD, resp.get_code());
    }
    // verify password.
    {
        cpp2::CheckPasswordReq req;
        req.set_account("user1");
        req.set_encoded_pwd("password");
        auto* processor = CheckPasswordProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
}

TEST(AuthProcessorTest, GrantRevokeTest) {
    fs::TempDir rootPath("/tmp/GrantRevokeTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    auto ret = TestUtils::createUser(kv.get(), false, "user1", "pwd",
                                      false, 1, 2, 3, 4);
    ASSERT_TRUE(ret.ok());
    auto userId = ret.value();

    // grant test : space does not exist
    {
        cpp2::GrantRoleReq req;
        decltype(req.role_item) role;
        role.set_user_id(userId);
        role.set_space_id(100);
        role.set_role_type(cpp2::RoleType::USER);
        req.set_role_item(std::move(role));
        auto* processor = GrantProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.get_code());
    }

    // revoke test : space does not exist
    {
        cpp2::RevokeRoleReq req;
        decltype(req.role_item) role;
        role.set_user_id(userId);
        role.set_space_id(100);
        role.set_role_type(cpp2::RoleType::USER);
        req.set_role_item(std::move(role));
        auto* processor = RevokeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.get_code());
    }
    // setup space
    GraphSpaceID spaceId;
    {
        TestUtils::createSomeHosts(kv.get());
        cpp2::SpaceProperties sp;
        sp.set_space_name("test_space");
        sp.set_replica_factor(1);
        sp.set_partition_num(1);
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(sp));
        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        spaceId = resp.get_id().get_space_id();
    }
    // grant test
    {
        cpp2::GrantRoleReq req;
        decltype(req.role_item) role;
        role.set_user_id(userId);
        role.set_space_id(spaceId);
        role.set_role_type(cpp2::RoleType::USER);

        req.set_role_item(std::move(role));
        auto* processor = GrantProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // List acl by space name.
    {
        cpp2::ListRolesReq req;
        req.set_space_id(spaceId);
        auto* processor = ListRolesProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        decltype(resp.roles) rolesList;
        rolesList.emplace_back(FRAGILE, userId, spaceId, cpp2::RoleType::USER);
        ASSERT_EQ(rolesList, resp.get_roles());
    }
    // revoke test
    {
        cpp2::RevokeRoleReq req;
        decltype(req.role_item) role;
        role.set_user_id(userId);
        role.set_space_id(spaceId);
        role.set_role_type(cpp2::RoleType::USER);
        req.set_role_item(std::move(role));
        auto* processor = RevokeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        std::unique_ptr<kvstore::KVIterator> iter;
        auto prefix = MetaServiceUtils::rolesPrefix();
        auto code = kv.get()->prefix(0, 0, prefix, &iter);
        ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, code);
        ASSERT_FALSE(iter->valid());
    }
    // List Users
    {
        cpp2::ListUsersReq req;
        auto* processor = ListUsersProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        cpp2::UserItem user;
        user.set_account("user1");
        user.set_max_queries_per_hour(1);
        user.set_max_updates_per_hour(2);
        user.set_max_connections_per_hour(3);
        user.set_max_user_connections(4);
        user.set_is_lock(false);

        decltype(resp.users) users;
        users.emplace(userId, std::move(user));
        ASSERT_EQ(users, resp.get_users());
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
