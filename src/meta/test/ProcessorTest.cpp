/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <folly/String.h>
#include <fstream>
#include "fs/TempDir.h"
#include "meta/MetaUtils.h"
#include "meta/test/TestUtils.h"
#include "meta/processors/CreateSpaceProcessor.h"
#include "meta/processors/GetPartsAllocProcessor.h"
#include "meta/processors/ListSpacesProcessor.h"

namespace nebula {
namespace meta {
using apache::thrift::FragileConstructor::FRAGILE;
TEST(ProcessorTest, AddHostsTest) {
    fs::TempDir rootPath("/tmp/AddHostsTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    {
        std::vector<nebula::cpp2::HostAddr> thriftHosts;
        for (auto i = 0; i < 10; i++) {
            thriftHosts.emplace_back(FRAGILE, i, i);
        }
        cpp2::AddHostsReq req;
        req.set_hosts(std::move(thriftHosts));
        auto* processor = AddHostsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        EXPECT_EQ(resp.code, cpp2::ErrorCode::SUCCEEDED);
    }
    {
        cpp2::ListHostsReq req;
        auto* processor = ListHostsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        EXPECT_EQ(10, resp.hosts.size());
        for (auto i = 0; i < 10; i++) {
            EXPECT_EQ(i, resp.hosts[i].ip);
            EXPECT_EQ(i, resp.hosts[i].port);
        }
    }
    {
        std::vector<nebula::cpp2::HostAddr> thriftHosts;
        for (auto i = 10; i < 20; i++) {
            thriftHosts.emplace_back(FRAGILE, i, i);
        }
        cpp2::AddHostsReq req;
        req.set_hosts(std::move(thriftHosts));
        auto* processor = AddHostsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        EXPECT_EQ(resp.code, cpp2::ErrorCode::SUCCEEDED);
    }
    {
        cpp2::ListHostsReq req;
        auto* processor = ListHostsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        EXPECT_EQ(20, resp.hosts.size());
        for (auto i = 0; i < 20; i++) {
            EXPECT_EQ(i, resp.hosts[i].ip);
            EXPECT_EQ(i, resp.hosts[i].port);
        }
    }
}

TEST(ProcessorTest, CreateSpaceTest) {
    fs::TempDir rootPath("/tmp/CreateSpaceTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    auto hostsNum = TestUtils::createSomeHosts(kv.get());
    {
        cpp2::CreateSpaceReq req;
        req.set_space_name("default_space");
        req.set_parts_num(9);
        req.set_replica_factor(3);

        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(resp.code, cpp2::ErrorCode::SUCCEEDED);
        ASSERT_EQ(resp.get_id().get_space_id(), 1);
    }
    {
        cpp2::ListSpacesReq req;
        auto* processor = ListSpacesProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(resp.code, cpp2::ErrorCode::SUCCEEDED);
        ASSERT_EQ(resp.spaces.size(), 1);
        ASSERT_EQ(resp.spaces[0].id.get_space_id(), 1);
        ASSERT_EQ(resp.spaces[0].name, "default_space");
    }
    // Check the result. The dispatch way from part to hosts is in a round robin fashion.
    {
        cpp2::GetPartsAllocReq req;
        req.set_space_id(1);
        auto* processor = GetPartsAllocProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(resp.code, cpp2::ErrorCode::SUCCEEDED);
        for (auto& p : resp.get_parts()) {
            auto startIndex = p.first;
            for (auto& h : p.second) {
                ASSERT_EQ(startIndex++ % hostsNum, h.get_ip());
                ASSERT_EQ(h.get_ip(), h.get_port());
            }
        }
    }
}

TEST(ProcessorTest, CreateUserTest) {
    fs::TempDir rootPath("/tmp/CreateUserTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    ASSERT_TRUE(TestUtils::assembleSpace(kv.get(), 1));
    {
        ASSERT_EQ(TestUtils::createUser(kv.get(), 1, true, "nebula", "nebula", RoleTypeE::T_ADMIN),
                cpp2::ErrorCode::SUCCEEDED);
        ASSERT_EQ(TestUtils::createUser(kv.get(), 1, true, "nebula", "nebula", RoleTypeE::T_ADMIN),
                  cpp2::ErrorCode::SUCCEEDED);
        ASSERT_EQ(TestUtils::createUser(kv.get(), 1, false, "nebula", "nebula", RoleTypeE::T_ADMIN),
                  cpp2::ErrorCode::E_USER_EXISTED);
    }

    {
        auto key = MetaUtils::userKey(1, "nebula");
        std::string val;
        auto ret = kv.get()->get(0, 0, std::move(key), &val);
        ASSERT_TRUE(ret == kvstore::ResultCode::SUCCEEDED);
        auto role = *reinterpret_cast<const nebula::cpp2::RoleType*>(val.data());
        ASSERT_EQ(role, RoleTypeE::T_ADMIN);
        ASSERT_EQ(folly::StringPiece(reinterpret_cast<const char*>(val.data() + sizeof(RoleType))),
                folly::StringPiece(MetaUtils::encPassword("nebula").data()));
    }
}

TEST(ProcessorTest, DropUserTest) {
    fs::TempDir rootPath("/tmp/DropUserTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    ASSERT_TRUE(TestUtils::assembleSpace(kv.get(), 1));
    {
        ASSERT_EQ(TestUtils::createUser(kv.get(), 1, true, "nebula", "nebula", RoleTypeE::T_ADMIN),
                  cpp2::ErrorCode::SUCCEEDED);
    }
    {
        cpp2::DropUserReq req(FRAGILE, 1, true, "nebula");
        auto* processor = DropUserProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(resp.code, cpp2::ErrorCode::SUCCEEDED);
    }
    {
        cpp2::DropUserReq req(FRAGILE, 1, false, "nebula");
        auto* processor = DropUserProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(resp.code, cpp2::ErrorCode::E_NOT_FOUND);
    }
    {
        auto key = MetaUtils::userKey(1, "nebula");
        std::string val;
        auto ret = kv.get()->get(0, 0, std::move(key), &val);
        ASSERT_TRUE(ret == kvstore::ResultCode::ERR_KEY_NOT_FOUND);
    }
}

TEST(ProcessorTest, AlterUserTest) {
    fs::TempDir rootPath("/tmp/AlterUserTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    ASSERT_TRUE(TestUtils::assembleSpace(kv.get(), 1));
    {
        ASSERT_EQ(TestUtils::createUser(kv.get(), 1, true, "nebula", "nebula", RoleTypeE::T_ADMIN),
                  cpp2::ErrorCode::SUCCEEDED);
    }
    {
        cpp2::AlterUserReq req(FRAGILE, 1, "nebula", "123456");
        auto* processor = AlterUserProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(resp.code, cpp2::ErrorCode::SUCCEEDED);
        auto key = MetaUtils::userKey(1, "nebula");
        std::string val;
        auto ret = kv.get()->get(0, 0, std::move(key), &val);
        ASSERT_TRUE(ret == kvstore::ResultCode::SUCCEEDED);
        ASSERT_EQ(folly::StringPiece(reinterpret_cast<const char*>(val.data() + sizeof(RoleType))),
                  folly::StringPiece(MetaUtils::encPassword("123456").data()));
    }
    {
        cpp2::AlterUserReq req(FRAGILE, 1, "nebula1", "123456");
        auto* processor = AlterUserProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(resp.code, cpp2::ErrorCode::E_NOT_FOUND);
    }
}

TEST(ProcessorTest, GrantToUserTest) {
    fs::TempDir rootPath("/tmp/GrantToUserTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    ASSERT_TRUE(TestUtils::assembleSpace(kv.get(), 1));
    {
        ASSERT_EQ(TestUtils::createUser(kv.get(), 1, true,
                "nebula", "nebula", RoleTypeE::T_DEFAULT),
                  cpp2::ErrorCode::SUCCEEDED);
    }
    {
        cpp2::GrantToUserReq req(FRAGILE, 1, "nebula", RoleTypeE::T_ADMIN);
        auto* processor = GrantToUserProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(resp.code, cpp2::ErrorCode::SUCCEEDED);
        auto key = MetaUtils::userKey(1, "nebula");
        std::string val;
        auto ret = kv.get()->get(0, 0, std::move(key), &val);
        ASSERT_TRUE(ret == kvstore::ResultCode::SUCCEEDED);
        auto role = *reinterpret_cast<const nebula::cpp2::RoleType*>(val.data());
        ASSERT_EQ(role, RoleTypeE::T_ADMIN);
    }
}

TEST(ProcessorTest, RevokeFromUserTest) {
    fs::TempDir rootPath("/tmp/RevokeFromUserTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    ASSERT_TRUE(TestUtils::assembleSpace(kv.get(), 1));
    {
        ASSERT_EQ(TestUtils::createUser(kv.get(), 1, true, "nebula", "nebula", RoleTypeE::T_ADMIN),
                  cpp2::ErrorCode::SUCCEEDED);
    }
    {
        cpp2::RevokeFromUserReq req(FRAGILE, 1, "nebula", RoleTypeE::T_ADMIN);
        auto* processor = RevokeFromUserProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(resp.code, cpp2::ErrorCode::SUCCEEDED);
        auto key = MetaUtils::userKey(1, "nebula");
        std::string val;
        auto ret = kv.get()->get(0, 0, std::move(key), &val);
        ASSERT_TRUE(ret == kvstore::ResultCode::SUCCEEDED);
        auto role = *reinterpret_cast<const nebula::cpp2::RoleType*>(val.data());
        ASSERT_EQ(role, RoleTypeE::T_DEFAULT);
    }
}

TEST(ProcessorTest, ListUsersTest) {
    fs::TempDir rootPath("/tmp/ListUsersTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    ASSERT_TRUE(TestUtils::assembleSpace(kv.get(), 1));
    {
        for (int8_t i = 0; i < 10; i++) {
            nebula::cpp2::RoleType type = i%4;
            ASSERT_EQ(TestUtils::createUser(kv.get(), 1, false,
                    folly::stringPrintf("user_%d", i), folly::stringPrintf("password_%d", i), type),
                      cpp2::ErrorCode::SUCCEEDED);
        }
    }
    {
        cpp2::ListUsersReq req(FRAGILE, 1);
        auto* processor = ListUsersProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(resp.code, cpp2::ErrorCode::SUCCEEDED);
        decltype(resp.users) users;
        users = resp.get_users();
        ASSERT_EQ(users.size(), 10);
        for (int8_t i = 0; i < 10; i++) {
            auto u = users[i];
            ASSERT_EQ(u.get_role(), i%4);
            ASSERT_EQ(u.get_user_name(), folly::stringPrintf("user_%d", i));
            ASSERT_EQ(u.get_user_pwd(),
                    MetaUtils::encPassword(folly::stringPrintf("password_%d", i)));
        }
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


