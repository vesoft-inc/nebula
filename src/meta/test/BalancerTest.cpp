/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <folly/synchronization/Baton.h>
#include "meta/processors/admin/Balancer.h"
#include "meta/test/TestUtils.h"
#include "meta/test/MockAdminClient.h"
#include "meta/processors/partsMan/CreateSpaceProcessor.h"

DECLARE_uint32(task_concurrency);
DECLARE_int32(heartbeat_interval_secs);
DECLARE_uint32(expired_time_factor);
DECLARE_double(leader_balance_deviation);

namespace nebula {
namespace meta {

using ::testing::_;
using ::testing::AtLeast;
using ::testing::ByMove;
using ::testing::Return;
using ::testing::DefaultValue;
using ::testing::NiceMock;
using ::testing::NaggyMock;
using ::testing::StrictMock;
using ::testing::SetArgPointee;

TEST(BalanceTest, BalanceTaskTest) {
    fs::TempDir rootPath("/tmp/SimpleTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    HostAddr src("0", 0);
    HostAddr dst("1", 1);
    TestUtils::registerHB(kv, {src, dst});

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    {
        StrictMock<MockAdminClient> client;
        EXPECT_CALL(client, checkPeers(0, 0)).Times(2);
        EXPECT_CALL(client, transLeader(0, 0, src, _)).Times(1);
        EXPECT_CALL(client, addPart(0, 0, dst, true)).Times(1);
        EXPECT_CALL(client, addLearner(0, 0, dst)).Times(1);
        EXPECT_CALL(client, waitingForCatchUpData(0, 0, dst)).Times(1);
        EXPECT_CALL(client, memberChange(0, 0, dst, true)).Times(1);
        EXPECT_CALL(client, memberChange(0, 0, src, false)).Times(1);
        EXPECT_CALL(client, updateMeta(0, 0, src, dst)).Times(1);
        EXPECT_CALL(client, removePart(0, 0, src)).Times(1);

        folly::Baton<true, std::atomic> b;
        BalanceTask task(0, 0, 0, src, dst, kv, &client);
        task.onFinished_ = [&]() {
            LOG(INFO) << "Task finished!";
            EXPECT_EQ(BalanceTaskResult::SUCCEEDED, task.ret_);
            EXPECT_EQ(BalanceTaskStatus::END, task.status_);
            b.post();
        };
        task.onError_ = []() {
            LOG(FATAL) << "We should not reach here!";
        };
        task.invoke();
        b.wait();
    }
    {
        NiceMock<MockAdminClient> client;
        EXPECT_CALL(client, transLeader(_, _, _, _))
            .Times(1)
            .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("Transfer failed")))));

        folly::Baton<true, std::atomic> b;
        BalanceTask task(0, 0, 0, src, dst, kv, &client);
        task.onFinished_ = []() {
            LOG(FATAL) << "We should not reach here!";
        };
        task.onError_ = [&]() {
            LOG(INFO) << "Error happens!";
            EXPECT_EQ(BalanceTaskResult::FAILED, task.ret_);
            EXPECT_EQ(BalanceTaskStatus::CHANGE_LEADER, task.status_);
            b.post();
        };
        task.invoke();
        b.wait();
    }
    LOG(INFO) << "Test finished!";
}

void showHostLoading(kvstore::KVStore* kv, GraphSpaceID spaceId) {
    auto prefix = MetaServiceUtils::partPrefix(spaceId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
    HostParts hostPart;
    while (iter->valid()) {
        auto key = iter->key();
        PartitionID partId;
        memcpy(&partId, key.data() + prefix.size(), sizeof(PartitionID));
        auto hs = MetaServiceUtils::parsePartVal(iter->val());
        for (auto h : hs) {
            hostPart[h].emplace_back(partId);
        }
        iter->next();
    }

    for (auto it = hostPart.begin(); it != hostPart.end(); it++) {
        std::stringstream ss;
        for (auto part : it->second) {
            ss << part << " ";
        }
        LOG(INFO) << "Host: " << it->first << " parts: " << ss.str();
    }
}

HostParts assignHostParts(kvstore::KVStore* kv, GraphSpaceID spaceId) {
    auto prefix = MetaServiceUtils::partPrefix(spaceId);
    std::unique_ptr<kvstore::KVIterator> iter;
    kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    HostParts hostPart;
    while (iter->valid()) {
        auto key = iter->key();
        PartitionID partId;
        memcpy(&partId, key.data() + prefix.size(), sizeof(PartitionID));
        auto hs = MetaServiceUtils::parsePartVal(iter->val());
        for (auto h : hs) {
            hostPart[h].emplace_back(partId);
        }
        iter->next();
    }
    return hostPart;
}

TEST(BalanceTest, SimpleTestWithZone) {
    fs::TempDir rootPath("/tmp/SimpleTestWithZone.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 1;
    {
        std::vector<HostAddr> hosts;
        for (int i = 0; i < 4; i++) {
            hosts.emplace_back(std::to_string(i), i);
        }
        TestUtils::createSomeHosts(kv, hosts);
        TestUtils::registerHB(kv, hosts);

        // create zone and group
        ZoneInfo zoneInfo = {
            {"zone_0", {{"0", 0}}},
            {"zone_1", {{"1", 1}}},
            {"zone_2", {{"2", 2}}},
            {"zone_3", {{"3", 3}}}
        };
        GroupInfo groupInfo = {
            {"group_0", {"zone_0", "zone_1", "zone_2", "zone_3"}}
        };
        TestUtils::assembleGroupAndZone(kv, zoneInfo, groupInfo);
    }
    {
        cpp2::SpaceDesc properties;
        properties.set_space_name("default_space");
        properties.set_partition_num(4);
        properties.set_replica_factor(3);
        properties.set_group_name("group_0");
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));
        auto* processor = CreateSpaceProcessor::instance(kv);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(1, resp.get_id().get_space_id());
    }
    sleep(1);
    {
        HostParts hostParts;
        hostParts.emplace(HostAddr("0", 0), std::vector<PartitionID>{1, 2, 3, 4});
        hostParts.emplace(HostAddr("1", 0), std::vector<PartitionID>{1, 2, 3, 4});
        hostParts.emplace(HostAddr("2", 0), std::vector<PartitionID>{1, 2, 3, 4});
        hostParts.emplace(HostAddr("3", 0), std::vector<PartitionID>{});
        int32_t totalParts = 12;
        std::vector<BalanceTask> tasks;
        NiceMock<MockAdminClient> client;
        Balancer balancer(kv, &client);
        balancer.balanceParts(0, 0, hostParts, totalParts, tasks);
        for (auto it = hostParts.begin(); it != hostParts.end(); it++) {
            EXPECT_EQ(3, it->second.size());
        }
        EXPECT_EQ(3, tasks.size());
    }
}

TEST(BalanceTest, ExpansionZoneTest) {
    fs::TempDir rootPath("/tmp/ExpansionZoneTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 1;
    {
        std::vector<HostAddr> hosts;
        for (int i = 0; i < 3; i++) {
            hosts.emplace_back(std::to_string(i), i);
        }
        TestUtils::createSomeHosts(kv, hosts);
        TestUtils::registerHB(kv, hosts);

        // create zone and group
        ZoneInfo zoneInfo = {
            {"zone_0", {{"0", 0}}},
            {"zone_1", {{"1", 1}}},
            {"zone_2", {{"2", 2}}}
        };
        GroupInfo groupInfo = {
            {"default_group", {"zone_0", "zone_1", "zone_2"}}
        };
        TestUtils::assembleGroupAndZone(kv, zoneInfo, groupInfo);
    }
    {
        cpp2::SpaceDesc properties;
        properties.set_space_name("default_space");
        properties.set_partition_num(4);
        properties.set_replica_factor(3);
        properties.set_group_name("default_group");
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));
        auto* processor = CreateSpaceProcessor::instance(kv);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(1, resp.get_id().get_space_id());
    }

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;
    Balancer balancer(kv, &client);
    auto ret = balancer.balance();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_BALANCED, error(ret));
    {
        std::vector<HostAddr> hosts;
        for (int i = 0; i < 4; i++) {
            hosts.emplace_back(std::to_string(i), i);
        }
        TestUtils::createSomeHosts(kv, hosts);
        TestUtils::registerHB(kv, hosts);
        ZoneInfo zoneInfo = {
            {"zone_0", {{"0", 0}}},
            {"zone_1", {{"1", 1}}},
            {"zone_2", {{"2", 2}}},
            {"zone_3", {{"3", 3}}}
        };
        GroupInfo groupInfo = {
            {"default_group", {"zone_0", "zone_1", "zone_2", "zone_3"}}
        };
        TestUtils::assembleGroupAndZone(kv, zoneInfo, groupInfo);
    }
    {
        HostParts hostParts;
        hostParts.emplace(HostAddr("0", 0), std::vector<PartitionID>{1, 2, 3, 4});
        hostParts.emplace(HostAddr("1", 0), std::vector<PartitionID>{1, 2, 3, 4});
        hostParts.emplace(HostAddr("2", 0), std::vector<PartitionID>{1, 2, 3, 4});
        hostParts.emplace(HostAddr("3", 0), std::vector<PartitionID>{});
        int32_t totalParts = 12;
        std::vector<BalanceTask> tasks;
        balancer.balanceParts(0, 0, hostParts, totalParts, tasks);
        for (auto it = hostParts.begin(); it != hostParts.end(); it++) {
            EXPECT_EQ(3, it->second.size());
        }
        EXPECT_EQ(3, tasks.size());
    }
}

TEST(BalanceTest, ExpansionHostIntoZoneTest) {
    fs::TempDir rootPath("/tmp/ExpansionHostIntoZoneTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 1;
    {
        std::vector<HostAddr> hosts;
        for (int i = 0; i < 3; i++) {
            hosts.emplace_back(std::to_string(i), i);
        }
        TestUtils::createSomeHosts(kv, hosts);
        TestUtils::registerHB(kv, hosts);

        // create zone and group
        ZoneInfo zoneInfo = {
            {"zone_0", {{"0", 0}}},
            {"zone_1", {{"1", 1}}},
            {"zone_2", {{"2", 2}}}
        };
        GroupInfo groupInfo = {
            {"default_group", {"zone_0", "zone_1", "zone_2"}}
        };
        TestUtils::assembleGroupAndZone(kv, zoneInfo, groupInfo);
    }
    {
        cpp2::SpaceDesc properties;
        properties.set_space_name("default_space");
        properties.set_partition_num(4);
        properties.set_replica_factor(3);
        properties.set_group_name("default_group");
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));
        auto* processor = CreateSpaceProcessor::instance(kv);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(1, resp.get_id().get_space_id());
    }

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;
    Balancer balancer(kv, &client);
    auto ret = balancer.balance();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_BALANCED, error(ret));
    {
        std::vector<HostAddr> hosts;
        for (int i = 0; i < 6; i++) {
            hosts.emplace_back(std::to_string(i), i);
        }
        TestUtils::createSomeHosts(kv, hosts);
        TestUtils::registerHB(kv, hosts);
        ZoneInfo zoneInfo = {
            {"zone_0", {{"0", 0}, {"3", 3}}},
            {"zone_1", {{"1", 1}, {"4", 4}}},
            {"zone_2", {{"2", 2}, {"5", 5}}}
        };
        GroupInfo groupInfo = {
            {"default_group", {"zone_0", "zone_1", "zone_2"}}
        };
        TestUtils::assembleGroupAndZone(kv, zoneInfo, groupInfo);
    }
    {
        HostParts hostParts;
        hostParts.emplace(HostAddr("0", 0), std::vector<PartitionID>{1, 2, 3, 4});
        hostParts.emplace(HostAddr("1", 1), std::vector<PartitionID>{1, 2, 3, 4});
        hostParts.emplace(HostAddr("2", 2), std::vector<PartitionID>{1, 2, 3, 4});
        hostParts.emplace(HostAddr("3", 3), std::vector<PartitionID>{});
        hostParts.emplace(HostAddr("4", 4), std::vector<PartitionID>{});
        hostParts.emplace(HostAddr("5", 5), std::vector<PartitionID>{});

        int32_t totalParts = 12;
        std::vector<BalanceTask> tasks;
        balancer.balanceParts(0, 0, hostParts, totalParts, tasks);
        for (auto it = hostParts.begin(); it != hostParts.end(); it++) {
            EXPECT_EQ(2, it->second.size());
        }
        EXPECT_EQ(6, tasks.size());
    }
}

TEST(BalanceTest, ShrinkZoneTest) {
    fs::TempDir rootPath("/tmp/ShrinkZoneTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 1;
    {
        std::vector<HostAddr> hosts;
        for (int i = 0; i < 4; i++) {
            hosts.emplace_back(std::to_string(i), i);
        }

        TestUtils::createSomeHosts(kv, hosts);
        TestUtils::registerHB(kv, hosts);
        // create zone and group
        ZoneInfo zoneInfo = {
            {"zone_0", {{"0", 0}}},
            {"zone_1", {{"1", 1}}},
            {"zone_2", {{"2", 2}}},
            {"zone_3", {{"3", 3}}}
        };
        GroupInfo groupInfo = {
            {"default_group", {"zone_0", "zone_1", "zone_2", "zone_3"}}
        };
        TestUtils::assembleGroupAndZone(kv, zoneInfo, groupInfo);
    }
    {
        cpp2::SpaceDesc properties;
        properties.set_space_name("default_space");
        properties.set_partition_num(4);
        properties.set_replica_factor(3);
        properties.set_group_name("default_group");
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));
        auto* processor = CreateSpaceProcessor::instance(kv);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(1, resp.get_id().get_space_id());
    }

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;
    Balancer balancer(kv, &client);
    auto ret = balancer.balance();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_BALANCED, error(ret));
    {
        ZoneInfo zoneInfo = {
            {"zone_0", {{"0", 0}}},
            {"zone_1", {{"1", 1}}},
            {"zone_2", {{"2", 2}}},
        };
        GroupInfo groupInfo = {
            {"default_group", {"zone_0", "zone_1", "zone_2"}}
        };
        TestUtils::assembleGroupAndZone(kv, zoneInfo, groupInfo);
    }
    ret = balancer.balance({{"3", 3}});
    ASSERT_TRUE(ok(ret));
}

TEST(BalanceTest, ShrinkHostFromZoneTest) {
    fs::TempDir rootPath("/tmp/ShrinkHostFromZoneTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 1;
    {
        std::vector<HostAddr> hosts;
        for (int i = 0; i < 6; i++) {
            hosts.emplace_back(std::to_string(i), i);
        }
        TestUtils::createSomeHosts(kv, hosts);
        TestUtils::registerHB(kv, hosts);

        // create zone and group
        ZoneInfo zoneInfo = {
            {"zone_0", {{"0", 0}, {"3", 3}}},
            {"zone_1", {{"1", 1}, {"4", 4}}},
            {"zone_2", {{"2", 2}, {"5", 5}}}
        };
        GroupInfo groupInfo = {
            {"default_group", {"zone_0", "zone_1", "zone_2"}}
        };
        TestUtils::assembleGroupAndZone(kv, zoneInfo, groupInfo);
    }
    {
        cpp2::SpaceDesc properties;
        properties.set_space_name("default_space");
        properties.set_partition_num(4);
        properties.set_replica_factor(3);
        properties.set_group_name("default_group");
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));
        auto* processor = CreateSpaceProcessor::instance(kv);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(1, resp.get_id().get_space_id());
    }

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;
    Balancer balancer(kv, &client);
    auto ret = balancer.balance();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_BALANCED, error(ret));
    showHostLoading(kv, 1);

    {
        ZoneInfo zoneInfo = {
            {"zone_0", {{"0", 0}}},
            {"zone_1", {{"1", 1}, {"4", 4}}},
            {"zone_2", {{"2", 2}, {"5", 5}}}
        };
        GroupInfo groupInfo = {
            {"default_group", {"zone_0", "zone_1", "zone_2"}}
        };
        TestUtils::assembleGroupAndZone(kv, zoneInfo, groupInfo);
    }
    ret = balancer.balance({{"3", 3}});
    ASSERT_TRUE(ok(ret));
}

TEST(BalanceTest, BalanceWithComplexZoneTest) {
    fs::TempDir rootPath("/tmp/LeaderBalanceWithComplexZoneTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 1;
    std::vector<HostAddr> hosts;
    for (int i = 0; i < 18; i++) {
        hosts.emplace_back(std::to_string(i), i);
    }
    TestUtils::createSomeHosts(kv, hosts);
    TestUtils::registerHB(kv, hosts);

    {
        ZoneInfo zoneInfo = {
            {"zone_0", {HostAddr("0", 0), HostAddr("1", 1)}},
            {"zone_1", {HostAddr("2", 2), HostAddr("3", 3)}},
            {"zone_2", {HostAddr("4", 4), HostAddr("5", 5)}},
            {"zone_3", {HostAddr("6", 6), HostAddr("7", 7)}},
            {"zone_4", {HostAddr("8", 8), HostAddr("9", 9)}},
            {"zone_5", {HostAddr("10", 10), HostAddr("11", 11)}},
            {"zone_6", {HostAddr("12", 12), HostAddr("13", 13)}},
            {"zone_7", {HostAddr("14", 14), HostAddr("15", 15)}},
            {"zone_8", {HostAddr("16", 16), HostAddr("17", 17)}},
        };
        {
            GroupInfo groupInfo = {
                {"group_0", {"zone_0", "zone_1", "zone_2", "zone_3", "zone_4"}}
            };
            TestUtils::assembleGroupAndZone(kv, zoneInfo, groupInfo);
        }
        {
            GroupInfo groupInfo = {
                {"group_1", {"zone_0", "zone_1", "zone_2", "zone_3", "zone_4",
                             "zone_5", "zone_6", "zone_7", "zone_8"}}
            };
            TestUtils::assembleGroupAndZone(kv, zoneInfo, groupInfo);
        }
    }
    {
        {
            cpp2::SpaceDesc properties;
            properties.set_space_name("default_space");
            properties.set_partition_num(18);
            properties.set_replica_factor(3);
            cpp2::CreateSpaceReq req;
            req.set_properties(std::move(properties));
            auto* processor = CreateSpaceProcessor::instance(kv);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
            ASSERT_EQ(1, resp.get_id().get_space_id());
            LOG(INFO) << "Show host about space " << resp.get_id().get_space_id();
            showHostLoading(kv, resp.get_id().get_space_id());
        }
        {
            cpp2::SpaceDesc properties;
            properties.set_space_name("space_on_group_0");
            properties.set_partition_num(64);
            properties.set_replica_factor(3);
            properties.set_group_name("group_0");
            cpp2::CreateSpaceReq req;
            req.set_properties(std::move(properties));
            auto* processor = CreateSpaceProcessor::instance(kv);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
            ASSERT_EQ(2, resp.get_id().get_space_id());
            LOG(INFO) << "Show host about space " << resp.get_id().get_space_id();
            showHostLoading(kv, resp.get_id().get_space_id());
        }
        {
            cpp2::SpaceDesc properties;
            properties.set_space_name("space_on_group_1");
            properties.set_partition_num(81);
            properties.set_replica_factor(3);
            properties.set_group_name("group_1");
            cpp2::CreateSpaceReq req;
            req.set_properties(std::move(properties));
            auto* processor = CreateSpaceProcessor::instance(kv);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
            ASSERT_EQ(3, resp.get_id().get_space_id());
            LOG(INFO) << "Show host about space " << resp.get_id().get_space_id();
            showHostLoading(kv, resp.get_id().get_space_id());
        }
    }
    sleep(1);

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;
    Balancer balancer(kv, &client);

    {
        int32_t totalParts = 18 * 3;
        std::vector<BalanceTask> tasks;
        auto hostParts = assignHostParts(kv, 1);
        balancer.balanceParts(0, 1, hostParts, totalParts, tasks);
    }
    {
        int32_t totalParts = 64 * 3;
        std::vector<BalanceTask> tasks;
        auto hostParts = assignHostParts(kv, 2);
        balancer.balanceParts(0, 2, hostParts, totalParts, tasks);
    }
    {
        auto dump = [](const HostParts& hostParts,
                       const std::vector<BalanceTask>& tasks) {
            for (auto it = hostParts.begin(); it != hostParts.end(); it++) {
                std::stringstream ss;
                ss << it->first << ": ";
                for (auto partId : it->second) {
                    ss << partId << ", ";
                }
                LOG(INFO) << ss.str() << " size " << it->second.size();
            }
            for (const auto& task : tasks) {
                LOG(INFO) << task.taskIdStr();
            }
        };

        HostParts hostParts;
        std::vector<PartitionID> parts;
        for (int32_t i = 0; i< 81; i++) {
            parts.emplace_back(i);
        }

        for (int32_t i = 0; i< 18; i++) {
            if (i == 10 || i == 12 || i == 14) {
                hostParts.emplace(HostAddr(std::to_string(i), i), parts);
            } else {
                hostParts.emplace(HostAddr(std::to_string(i), i), std::vector<PartitionID>{});
            }
        }

        LOG(INFO) << "=== original map ====";
        int32_t totalParts = 243;
        std::vector<BalanceTask> tasks;
        dump(hostParts, tasks);
        balancer.balanceParts(0, 3, hostParts, totalParts, tasks);

        LOG(INFO) << "=== new map ====";
        dump(hostParts, tasks);
        for (auto it = hostParts.begin(); it != hostParts.end(); it++) {
            EXPECT_GE(it->second.size(), 5);
            EXPECT_LE(it->second.size(), 24);

            LOG(INFO) << "Host " << it->first << " Part Size " << it->second.size();
        }
        showHostLoading(kv, 3);
    }
}


TEST(BalanceTest, BalancePartsTest) {
    fs::TempDir rootPath("/tmp/BalancePartsTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;

    auto dump = [](const HostParts& hostParts,
                   const std::vector<BalanceTask>& tasks) {
        for (auto it = hostParts.begin(); it != hostParts.end(); it++) {
            std::stringstream ss;
            ss << it->first << ": ";
            for (auto partId : it->second) {
                ss << partId << ", ";
            }
            VLOG(1) << ss.str();
        }
        for (const auto& task : tasks) {
            VLOG(1) << task.taskIdStr();
        }
    };
    {
        HostParts hostParts;
        hostParts.emplace(HostAddr("0", 0), std::vector<PartitionID>{1, 2, 3, 4});
        hostParts.emplace(HostAddr("1", 0), std::vector<PartitionID>{1, 2, 3, 4});
        hostParts.emplace(HostAddr("2", 0), std::vector<PartitionID>{1, 2, 3, 4});
        hostParts.emplace(HostAddr("3", 0), std::vector<PartitionID>{});
        int32_t totalParts = 12;
        std::vector<BalanceTask> tasks;
        VLOG(1) << "=== original map ====";
        dump(hostParts, tasks);
        Balancer balancer(kv, &client);
        balancer.balanceParts(0, 0, hostParts, totalParts, tasks);
        VLOG(1) << "=== new map ====";
        dump(hostParts, tasks);
        for (auto it = hostParts.begin(); it != hostParts.end(); it++) {
            EXPECT_EQ(3, it->second.size());
        }
        EXPECT_EQ(3, tasks.size());
    }
    {
        HostParts hostParts;
        hostParts.emplace(HostAddr("0", 0), std::vector<PartitionID>{1, 2, 3, 4, 5});
        hostParts.emplace(HostAddr("1", 0), std::vector<PartitionID>{1, 2, 4, 5});
        hostParts.emplace(HostAddr("2", 0), std::vector<PartitionID>{2, 3, 4, 5});
        hostParts.emplace(HostAddr("3", 0), std::vector<PartitionID>{1, 3});
        int32_t totalParts = 15;
        std::vector<BalanceTask> tasks;
        VLOG(1) << "=== original map ====";
        dump(hostParts, tasks);
        Balancer balancer(kv, &client);
        balancer.balanceParts(0, 0, hostParts, totalParts, tasks);
        VLOG(1) << "=== new map ====";
        dump(hostParts, tasks);
        EXPECT_EQ(4, hostParts[HostAddr("0", 0)].size());
        EXPECT_EQ(4, hostParts[HostAddr("1", 0)].size());
        EXPECT_EQ(4, hostParts[HostAddr("2", 0)].size());
        EXPECT_EQ(3, hostParts[HostAddr("3", 0)].size());
        EXPECT_EQ(1, tasks.size());
    }
    {
        HostParts hostParts;
        hostParts.emplace(HostAddr("0", 0), std::vector<PartitionID>{1, 2, 3, 4});
        hostParts.emplace(HostAddr("1", 0), std::vector<PartitionID>{1, 2, 4, 5});
        hostParts.emplace(HostAddr("2", 0), std::vector<PartitionID>{2, 3, 4, 5});
        hostParts.emplace(HostAddr("3", 0), std::vector<PartitionID>{1, 3, 5});
        int32_t totalParts = 15;
        std::vector<BalanceTask> tasks;
        VLOG(1) << "=== original map ====";
        dump(hostParts, tasks);
        Balancer balancer(kv, &client);
        balancer.balanceParts(0, 0, hostParts, totalParts, tasks);
        VLOG(1) << "=== new map ====";
        dump(hostParts, tasks);
        EXPECT_EQ(4, hostParts[HostAddr("0", 0)].size());
        EXPECT_EQ(4, hostParts[HostAddr("1", 0)].size());
        EXPECT_EQ(4, hostParts[HostAddr("2", 0)].size());
        EXPECT_EQ(3, hostParts[HostAddr("3", 0)].size());
        EXPECT_EQ(0, tasks.size());
    }
    {
        HostParts hostParts;
        hostParts.emplace(HostAddr("0", 0), std::vector<PartitionID>{1, 2, 3, 4, 5, 6, 7, 8, 9});
        hostParts.emplace(HostAddr("1", 0), std::vector<PartitionID>{1, 2, 3, 4, 5, 6, 7, 8, 9});
        hostParts.emplace(HostAddr("2", 0), std::vector<PartitionID>{1, 2, 3, 4, 5, 6, 7, 8, 9});
        hostParts.emplace(HostAddr("3", 0), std::vector<PartitionID>{});
        hostParts.emplace(HostAddr("4", 0), std::vector<PartitionID>{});
        hostParts.emplace(HostAddr("5", 0), std::vector<PartitionID>{});
        hostParts.emplace(HostAddr("6", 0), std::vector<PartitionID>{});
        hostParts.emplace(HostAddr("7", 0), std::vector<PartitionID>{});
        hostParts.emplace(HostAddr("8", 0), std::vector<PartitionID>{});
        int32_t totalParts = 27;
        std::vector<BalanceTask> tasks;
        VLOG(1) << "=== original map ====";
        dump(hostParts, tasks);
        Balancer balancer(kv, &client);
        balancer.balanceParts(0, 0, hostParts, totalParts, tasks);
        VLOG(1) << "=== new map ====";
        dump(hostParts, tasks);
        for (auto it = hostParts.begin(); it != hostParts.end(); it++) {
            EXPECT_EQ(3, it->second.size());
        }
        EXPECT_EQ(18, tasks.size());
    }
    {
        HostParts hostParts;
        hostParts.emplace(HostAddr("0", 0), std::vector<PartitionID>{1, 2, 3, 4, 5, 6, 7, 8, 9});
        hostParts.emplace(HostAddr("1", 0), std::vector<PartitionID>{1, 2, 3, 4, 5, 6, 7, 8, 9});
        hostParts.emplace(HostAddr("2", 0), std::vector<PartitionID>{1, 2, 3, 4, 5, 6, 7, 8, 9});
        hostParts.emplace(HostAddr("3", 0), std::vector<PartitionID>{});
        hostParts.emplace(HostAddr("4", 0), std::vector<PartitionID>{});
        hostParts.emplace(HostAddr("5", 0), std::vector<PartitionID>{});
        hostParts.emplace(HostAddr("6", 0), std::vector<PartitionID>{});
        hostParts.emplace(HostAddr("7", 0), std::vector<PartitionID>{});
        int32_t totalParts = 27;
        std::vector<BalanceTask> tasks;
        VLOG(1) << "=== original map ====";
        dump(hostParts, tasks);
        Balancer balancer(kv, &client);
        balancer.balanceParts(0, 0, hostParts, totalParts, tasks);
        VLOG(1) << "=== new map ====";
        dump(hostParts, tasks);
        for (auto it = hostParts.begin(); it != hostParts.end(); it++) {
            EXPECT_GE(4, it->second.size());
            EXPECT_LE(3, it->second.size());
        }
        EXPECT_EQ(18, tasks.size());
    }
}

TEST(BalanceTest, DispatchTasksTest) {
    {
        FLAGS_task_concurrency = 10;
        BalancePlan plan(0L, nullptr, nullptr);
        for (int i = 0; i < 20; i++) {
            BalanceTask task(0, 0, 0, HostAddr(std::to_string(i), 0),
                             HostAddr(std::to_string(i), 1), nullptr, nullptr);
            plan.addTask(std::move(task));
        }
        plan.dispatchTasks();
        // All tasks is about space 0, part 0.
        // So they will be dispatched into the same bucket.
        ASSERT_EQ(1, plan.buckets_.size());
        ASSERT_EQ(20, plan.buckets_[0].size());
    }
    {
        FLAGS_task_concurrency = 10;
        BalancePlan plan(0L, nullptr, nullptr);
        for (int i = 0; i < 5; i++) {
            BalanceTask task(0, 0, i, HostAddr(std::to_string(i), 0),
                             HostAddr(std::to_string(i), 1), nullptr, nullptr);
            plan.addTask(std::move(task));
        }
        plan.dispatchTasks();
        ASSERT_EQ(5, plan.buckets_.size());
        for (auto& bucket : plan.buckets_) {
            ASSERT_EQ(1, bucket.size());
        }
    }
    {
        FLAGS_task_concurrency = 20;
        BalancePlan plan(0L, nullptr, nullptr);
        for (int i = 0; i < 5; i++) {
            BalanceTask task(0, 0, i, HostAddr(std::to_string(i), 0),
                             HostAddr(std::to_string(i), 1), nullptr, nullptr);
            plan.addTask(std::move(task));
        }
        for (int i = 0; i < 10; i++) {
            BalanceTask task(0, 0, i, HostAddr(std::to_string(i), 2),
                             HostAddr(std::to_string(i), 3), nullptr, nullptr);
            plan.addTask(std::move(task));
        }
        plan.dispatchTasks();
        ASSERT_EQ(10, plan.buckets_.size());
        int32_t total = 0;
        for (auto i = 0; i < 10; i++) {
            ASSERT_LE(1, plan.buckets_[i].size());
            ASSERT_GE(2, plan.buckets_[i].size());
            total += plan.buckets_[i].size();
        }
        ASSERT_EQ(15, total);
    }
}

TEST(BalanceTest, BalancePlanTest) {
    fs::TempDir rootPath("/tmp/BalancePlanTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    std::vector<HostAddr> hosts;
    for (int i = 0; i < 10; i++) {
        hosts.emplace_back(std::to_string(i), 0);
        hosts.emplace_back(std::to_string(i), 1);
    }

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    {
        LOG(INFO) << "Test with all tasks succeeded, only one bucket!";
        NiceMock<MockAdminClient> client;
        BalancePlan plan(0L, kv, &client);
        TestUtils::registerHB(kv, hosts);

        for (int i = 0; i < 10; i++) {
            BalanceTask task(0, 0, 0, HostAddr(std::to_string(i), 0),
                             HostAddr(std::to_string(i), 1), kv, &client);
            plan.addTask(std::move(task));
        }
        folly::Baton<true, std::atomic> b;
        plan.onFinished_ = [&plan, &b] () {
            ASSERT_EQ(BalanceStatus::SUCCEEDED, plan.status_);
            ASSERT_EQ(10, plan.finishedTaskNum_);
            b.post();
        };
        plan.invoke();
        b.wait();
        // All tasks is about space 0, part 0.
        // So they will be dispatched into the same bucket.
        ASSERT_EQ(1, plan.buckets_.size());
        ASSERT_EQ(10, plan.buckets_[0].size());
    }
    {
        LOG(INFO) << "Test with all tasks succeeded, 10 buckets!";
        NiceMock<MockAdminClient> client;
        BalancePlan plan(0L, kv, &client);
        TestUtils::registerHB(kv, hosts);

        for (int i = 0; i < 10; i++) {
            BalanceTask task(0, 0, i, HostAddr(std::to_string(i), 0),
                             HostAddr(std::to_string(i), 1), kv, &client);
            plan.addTask(std::move(task));
        }
        folly::Baton<true, std::atomic> b;
        plan.onFinished_ = [&plan, &b] () {
            ASSERT_EQ(BalanceStatus::SUCCEEDED, plan.status_);
            ASSERT_EQ(10, plan.finishedTaskNum_);
            b.post();
        };
        plan.invoke();
        b.wait();
        // All tasks is about different parts.
        // So they will be dispatched into different buckets.
        ASSERT_EQ(10, plan.buckets_.size());
        for (auto i = 0; i < 10; i++) {
            ASSERT_EQ(1, plan.buckets_[1].size());
        }
    }
    {
        LOG(INFO) << "Test with one task failed, 10 buckets";
        BalancePlan plan(0L, kv, nullptr);
        NiceMock<MockAdminClient> client1, client2;
        {
            for (int i = 0; i < 9; i++) {
                BalanceTask task(0, 0, i, HostAddr(std::to_string(i), 0),
                                 HostAddr(std::to_string(i), 1), kv, &client1);
                plan.addTask(std::move(task));
            }
        }
        {
            EXPECT_CALL(client2, transLeader(_, _, _, _))
                .Times(1)
                .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("Transfer failed")))));
            BalanceTask task(0, 0, 9, HostAddr("9", 0), HostAddr("9", 1), kv, &client2);
            plan.addTask(std::move(task));
        }
        TestUtils::registerHB(kv, hosts);
        folly::Baton<true, std::atomic> b;
        plan.onFinished_ = [&plan, &b] () {
            ASSERT_EQ(BalanceStatus::FAILED, plan.status_);
            ASSERT_EQ(10, plan.finishedTaskNum_);
            b.post();
        };
        plan.invoke();
        b.wait();
    }
}

int32_t verifyBalancePlan(kvstore::KVStore* kv,
                          BalanceID balanceId,
                          BalanceStatus balanceStatus) {
    const auto& prefix = MetaServiceUtils::balancePlanPrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    EXPECT_EQ(retcode, nebula::cpp2::ErrorCode::SUCCEEDED);
    int32_t num = 0;
    while (iter->valid()) {
        auto id = MetaServiceUtils::parseBalanceID(iter->key());
        auto status = MetaServiceUtils::parseBalanceStatus(iter->val());
        EXPECT_EQ(balanceId, id);
        EXPECT_EQ(balanceStatus, status);
        num++;
        iter->next();
    }
    return num;
}

void verifyBalanceTask(kvstore::KVStore* kv,
                       BalanceID balanceId,
                       BalanceTaskStatus status,
                       BalanceTaskResult result,
                       std::unordered_map<HostAddr, int32_t>& partCount,
                       int32_t exceptNumber = 0) {
    const auto& prefix = MetaServiceUtils::balanceTaskPrefix(balanceId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto code = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    ASSERT_EQ(code, nebula::cpp2::ErrorCode::SUCCEEDED);
    int32_t num = 0;
    while (iter->valid()) {
        auto keyTuple = MetaServiceUtils::parseBalanceTaskKey(iter->key());
        ASSERT_EQ(balanceId, std::get<0>(keyTuple));
        ASSERT_EQ(1, std::get<1>(keyTuple));
        partCount[std::get<3>(keyTuple)]--;
        partCount[std::get<4>(keyTuple)]++;
        auto valueTuple = MetaServiceUtils::parseBalanceTaskVal(iter->val());
        ASSERT_EQ(status, std::get<0>(valueTuple));
        ASSERT_EQ(result, std::get<1>(valueTuple));
        ASSERT_LT(0, std::get<2>(valueTuple));
        ASSERT_LT(0, std::get<3>(valueTuple));
        num++;
        iter->next();
    }
    if (exceptNumber != 0) {
        ASSERT_EQ(exceptNumber, num);
    }
}

TEST(BalanceTest, NormalTest) {
    fs::TempDir rootPath("/tmp/NormalTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 1;
    TestUtils::createSomeHosts(kv);
    TestUtils::assembleSpace(kv, 1, 8, 3, 4);
    std::unordered_map<HostAddr, int32_t> partCount;

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;
    Balancer balancer(kv, &client);
    auto ret = balancer.balance();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_BALANCED, error(ret));

    sleep(FLAGS_heartbeat_interval_secs * FLAGS_expired_time_factor + 1);
    LOG(INFO) << "Now, we lost host " << HostAddr("3", 3);
    TestUtils::registerHB(kv, {{"0", 0}, {"1", 1}, {"2", 2}});
    ret = balancer.balance();
    ASSERT_TRUE(ok(ret));
    auto balanceId = value(ret);
    sleep(1);

    LOG(INFO) << "Rebalance finished!";
    ASSERT_EQ(1, verifyBalancePlan(kv, balanceId, BalanceStatus::SUCCEEDED));
    verifyBalanceTask(kv, balanceId,
                      BalanceTaskStatus::END,
                      BalanceTaskResult::SUCCEEDED,
                      partCount, 6);
}

TEST(BalanceTest, SpecifyHostTest) {
    fs::TempDir rootPath("/tmp/SpecifyHostTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 1;
    TestUtils::createSomeHosts(kv, {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}});
    TestUtils::assembleSpace(kv, 1, 8, 3, 4);
    std::unordered_map<HostAddr, int32_t> partCount;

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;
    Balancer balancer(kv, &client);

    sleep(1);
    LOG(INFO) << "Now, we remove host {3, 3}";
    TestUtils::registerHB(kv, {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}});
    auto ret = balancer.balance({{"3", 3}});
    ASSERT_TRUE(ok(ret));
    auto balanceId = value(ret);
    sleep(1);
    LOG(INFO) << "Rebalance finished!";
    ASSERT_EQ(1, verifyBalancePlan(kv, balanceId, BalanceStatus::SUCCEEDED));
    verifyBalanceTask(kv, balanceId,
                      BalanceTaskStatus::END,
                      BalanceTaskResult::SUCCEEDED,
                      partCount, 6);
}

TEST(BalanceTest, SpecifyMultiHostTest) {
    fs::TempDir rootPath("/tmp/SpecifyMultiHostTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 1;
    TestUtils::createSomeHosts(kv, {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}, {"4", 4}, {"5", 5}});
    TestUtils::assembleSpace(kv, 1, 12, 3, 6);
    std::unordered_map<HostAddr, int32_t> partCount;
    for (int32_t i = 0; i < 6; i++) {
        partCount[HostAddr(std::to_string(i), i)] = 6;
    }

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;
    Balancer balancer(kv, &client);

    sleep(FLAGS_heartbeat_interval_secs * FLAGS_expired_time_factor + 1);
    LOG(INFO) << "Now, we want to remove host {2, 2}/{3, 3}";
    // If {"2", 2} and {"3", 3} are both dead, minority hosts for some part are alive,
    // it would lead to a fail
    TestUtils::registerHB(kv, {{"0", 0}, {"1", 1}, {"4", 4}, {"5", 5}});
    auto ret = balancer.balance({{"2", 2}, {"3", 3}});
    ASSERT_FALSE(ok(ret));
    EXPECT_EQ(nebula::cpp2::ErrorCode::E_NO_VALID_HOST, error(ret));
    // If {"2", 2} is dead, {"3", 3} still alive, each part has majority hosts alive
    TestUtils::registerHB(kv, {{"0", 0}, {"1", 1}, {"3", 3}, {"4", 4}, {"5", 5}});
    ret = balancer.balance({{"2", 2}, {"3", 3}});
    ASSERT_TRUE(ok(ret));
    auto balanceId = value(ret);
    sleep(1);
    LOG(INFO) << "Rebalance finished!";
    ASSERT_EQ(1, verifyBalancePlan(kv, balanceId, BalanceStatus::SUCCEEDED));

    // In theory, there should be only 12 tasks, but in some environment, 13 tasks is generated.
    // A parition is moved more than once from A -> B -> C, actually A -> C is enough.
    verifyBalanceTask(kv, balanceId,
                      BalanceTaskStatus::END,
                      BalanceTaskResult::SUCCEEDED,
                      partCount);
    ASSERT_EQ(9, partCount[HostAddr("0", 0)]);
    ASSERT_EQ(9, partCount[HostAddr("1", 1)]);
    ASSERT_EQ(0, partCount[HostAddr("2", 2)]);
    ASSERT_EQ(0, partCount[HostAddr("3", 3)]);
    ASSERT_EQ(9, partCount[HostAddr("4", 4)]);
    ASSERT_EQ(9, partCount[HostAddr("5", 5)]);
}

TEST(BalanceTest, MockReplaceMachineTest) {
    fs::TempDir rootPath("/tmp/MockReplaceMachineTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 1;
    TestUtils::createSomeHosts(kv, {{"0", 0}, {"1", 1}, {"2", 2}});
    TestUtils::assembleSpace(kv, 1, 12, 3, 3);

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;
    Balancer balancer(kv, &client);

    // add a new machine
    TestUtils::createSomeHosts(kv, {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}});
    LOG(INFO) << "Now, we want to replace host {2, 2} with {3, 3}";
    // Because for all parts majority hosts still alive, we could balance
    sleep(FLAGS_heartbeat_interval_secs * FLAGS_expired_time_factor + 1);
    // {2, 2} should be offline now
    TestUtils::registerHB(kv, {{"0", 0}, {"1", 1}, {"3", 3}});
    auto ret = balancer.balance();
    ASSERT_TRUE(ok(ret));
    auto balanceId = value(ret);
    sleep(1);
    LOG(INFO) << "Rebalance finished!";
    ASSERT_EQ(1, verifyBalancePlan(kv, balanceId, BalanceStatus::SUCCEEDED));

    std::unordered_map<HostAddr, int32_t> partCount;
    verifyBalanceTask(kv, balanceId,
                      BalanceTaskStatus::END,
                      BalanceTaskResult::SUCCEEDED,
                      partCount, 12);
}

TEST(BalanceTest, SingleReplicaTest) {
    fs::TempDir rootPath("/tmp/SingleReplicaTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 1;
    TestUtils::createSomeHosts(kv, {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}, {"4", 4},
                                          {"5", 5}});
    TestUtils::assembleSpace(kv, 1, 12, 1, 6);
    std::unordered_map<HostAddr, int32_t> partCount;
    for (int32_t i = 0; i < 6; i++) {
        partCount[HostAddr(std::to_string(i), i)] = 2;
    }

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;
    Balancer balancer(kv, &client);

    sleep(1);
    LOG(INFO) << "Now, we want to remove host {2, 2} and {3, 3}";
    TestUtils::registerHB(kv, {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}, {"4", 4}, {"5", 5}});
    auto ret = balancer.balance({{"2", 2}, {"3", 3}});
    ASSERT_TRUE(ok(ret));
    auto balanceId = value(ret);
    sleep(1);
    LOG(INFO) << "Rebalance finished!";
    ASSERT_EQ(1, verifyBalancePlan(kv, balanceId, BalanceStatus::SUCCEEDED));

    verifyBalanceTask(kv, balanceId,
                      BalanceTaskStatus::END,
                      BalanceTaskResult::SUCCEEDED,
                      partCount, 4);
    ASSERT_EQ(3, partCount[HostAddr("0", 0)]);
    ASSERT_EQ(3, partCount[HostAddr("1", 1)]);
    ASSERT_EQ(0, partCount[HostAddr("2", 2)]);
    ASSERT_EQ(0, partCount[HostAddr("3", 3)]);
    ASSERT_EQ(3, partCount[HostAddr("4", 4)]);
    ASSERT_EQ(3, partCount[HostAddr("5", 5)]);
}

TEST(BalanceTest, TryToRecoveryTest) {
    fs::TempDir rootPath("/tmp/TryToRecoveryTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 1;
    TestUtils::createSomeHosts(kv);
    TestUtils::assembleSpace(kv, 1, 8, 3, 4);

    sleep(FLAGS_heartbeat_interval_secs * FLAGS_expired_time_factor + 1);
    LOG(INFO) << "Now, we lost host " << HostAddr("3", 3);
    TestUtils::registerHB(kv, {{"0", 0}, {"1", 1}, {"2", 2}});

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;
    // first 6 call is the failed case, since we can't recover the plan, so only 6 call
    EXPECT_CALL(client, waitingForCatchUpData(_, _, _))
        .Times(6)
        .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("catch up failed")))))
        .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("catch up failed")))))
        .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("catch up failed")))))
        .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("catch up failed")))))
        .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("catch up failed")))))
        .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("catch up failed")))));

    Balancer balancer(kv, &client);
    auto ret = balancer.balance();
    CHECK(ok(ret));
    auto balanceId = value(ret);
    sleep(1);

    ASSERT_EQ(1, verifyBalancePlan(kv, balanceId, BalanceStatus::FAILED));
    std::unordered_map<HostAddr, int32_t> partCount;
    verifyBalanceTask(kv, balanceId,
                      BalanceTaskStatus::CATCH_UP_DATA,
                      BalanceTaskResult::FAILED,
                      partCount, 6);

    sleep(FLAGS_heartbeat_interval_secs * FLAGS_expired_time_factor + 1);
    LOG(INFO) << "Now let's try to recovery it. Sinc eall host would be regarded as offline, "
              << "so all task will be invalid";
    ret = balancer.balance();
    CHECK(ok(ret));
    balanceId = value(ret);
    sleep(1);
    ASSERT_EQ(1, verifyBalancePlan(kv, balanceId, BalanceStatus::SUCCEEDED));
    verifyBalanceTask(kv, balanceId,
                      BalanceTaskStatus::START,
                      BalanceTaskResult::INVALID,
                      partCount, 6);
}

TEST(BalanceTest, RecoveryTest) {
    FLAGS_task_concurrency = 1;
    fs::TempDir rootPath("/tmp/RecoveryTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 1;
    TestUtils::createSomeHosts(kv);
    TestUtils::assembleSpace(kv, 1, 8, 3, 4);

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;
    // first 6 call is the failed case, the later call will return default value
    // In gtest release 1.8.0 we can only write as follows:
    EXPECT_CALL(client, waitingForCatchUpData(_, _, _))
        .Times(AtLeast(12))
        .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("catch up failed")))))
        .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("catch up failed")))))
        .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("catch up failed")))))
        .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("catch up failed")))))
        .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("catch up failed")))))
        .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("catch up failed")))));

    sleep(FLAGS_heartbeat_interval_secs * FLAGS_expired_time_factor + 1);
    LOG(INFO) << "Now, we lost host " << HostAddr("3", 3);
    TestUtils::registerHB(kv, {{"0", 0}, {"1", 1}, {"2", 2}});
    Balancer balancer(kv, &client);
    auto ret = balancer.balance();
    ASSERT_TRUE(ok(ret));
    auto balanceId = value(ret);
    sleep(1);

    ASSERT_EQ(1, verifyBalancePlan(kv, balanceId, BalanceStatus::FAILED));
    std::unordered_map<HostAddr, int32_t> partCount;
    verifyBalanceTask(kv, balanceId,
                      BalanceTaskStatus::CATCH_UP_DATA,
                      BalanceTaskResult::FAILED,
                      partCount, 6);

    // register hb again to prevent from regarding src as offline
    TestUtils::registerHB(kv, {{"0", 0}, {"1", 1}, {"2", 2}});
    LOG(INFO) << "Now let's try to recovery it.";
    ret = balancer.balance();
    ASSERT_TRUE(ok(ret));
    balanceId = value(ret);
    sleep(1);

    ASSERT_EQ(1, verifyBalancePlan(kv, balanceId, BalanceStatus::SUCCEEDED));
    verifyBalanceTask(kv, balanceId,
                      BalanceTaskStatus::END,
                      BalanceTaskResult::SUCCEEDED,
                      partCount, 6);
}

TEST(BalanceTest, StopPlanTest) {
    FLAGS_task_concurrency = 1;
    fs::TempDir rootPath("/tmp/StopAndRecoverTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 1;
    TestUtils::createSomeHosts(kv);
    TestUtils::assembleSpace(kv, 1, 8, 3, 4);

    // {3, 3} is lost for now
    sleep(FLAGS_heartbeat_interval_secs * FLAGS_expired_time_factor + 1);
    TestUtils::registerHB(kv, {{"0", 0}, {"1", 1}, {"2", 2}});

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> delayClient;
    EXPECT_CALL(delayClient, waitingForCatchUpData(_, _, _))
        // first task in first plan will be blocked, all other tasks will be skipped,
        .Times(1)
        .WillOnce(Return(
            ByMove(folly::makeFuture<Status>(Status::OK()).delayed(std::chrono::seconds(3)))));

    Balancer balancer(kv, &delayClient);
    auto ret = balancer.balance();
    CHECK(ok(ret));
    auto balanceId = value(ret);

    sleep(1);
    LOG(INFO) << "Rebalance should still in progress";
    ASSERT_EQ(1, verifyBalancePlan(kv, balanceId, BalanceStatus::IN_PROGRESS));

    TestUtils::registerHB(kv, {{"0", 0}, {"1", 1}, {"2", 2}});
    auto stopRet = balancer.stop();
    CHECK(nebula::ok(stopRet));
    ASSERT_EQ(nebula::value(stopRet), balanceId);

    // wait until the only IN_PROGRESS task finished;
    sleep(3);
    {
        const auto& prefix = MetaServiceUtils::balanceTaskPrefix(balanceId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, nebula::cpp2::ErrorCode::SUCCEEDED);
        int32_t taskEnded = 0;
        int32_t taskStopped = 0;
        while (iter->valid()) {
            BalanceTask task;
            // PartitionID partId = std::get<2>(BalanceTask::MetaServiceUtils(iter->key()));
            {
                auto tup = MetaServiceUtils::parseBalanceTaskVal(iter->val());
                task.status_ = std::get<0>(tup);
                task.ret_ = std::get<1>(tup);
                task.startTimeMs_ = std::get<2>(tup);
                task.endTimeMs_ = std::get<3>(tup);

                if (task.status_ == BalanceTaskStatus::END) {
                    taskEnded++;
                } else {
                    taskStopped++;
                }
            }
            iter->next();
        }
        ASSERT_EQ(1, taskEnded);
        ASSERT_EQ(5, taskStopped);
    }

    TestUtils::registerHB(kv, {{"0", 0}, {"1", 1}, {"2", 2}});
    NiceMock<MockAdminClient> normalClient;

    balancer.client_ = &normalClient;
    ret = balancer.balance();
    CHECK(ok(ret));
    ASSERT_NE(value(ret), balanceId);
    sleep(1);
}

TEST(BalanceTest, CleanLastInvalidBalancePlanTest) {
    FLAGS_task_concurrency = 1;
    fs::TempDir rootPath("/tmp/CleanLastInvalidBalancePlanTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 1;
    TestUtils::createSomeHosts(kv);
    TestUtils::assembleSpace(kv, 1, 8, 3, 4);

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;

    // concurrency = 1, we could only block first task
    EXPECT_CALL(client, waitingForCatchUpData(_, _, _))
        .Times(AtLeast(12))
        .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("catch up failed")))));

    sleep(FLAGS_heartbeat_interval_secs * FLAGS_expired_time_factor + 1);
    TestUtils::registerHB(kv, {{"0", 0}, {"1", 1}, {"2", 2}});
    Balancer balancer(kv, &client);
    auto ret = balancer.balance();
    CHECK(ok(ret));
    auto balanceId = value(ret);

    // wait until the plan finished, no running plan for now, only one task has been executed,
    // so the task will be failed, try to clean the invalid plan
    sleep(3);
    TestUtils::registerHB(kv, {{"0", 0}, {"1", 1}, {"2", 2}});
    auto cleanRet = balancer.cleanLastInValidPlan();
    CHECK(ok(cleanRet));
    ASSERT_EQ(value(cleanRet), balanceId);

    {
        const auto& prefix = MetaServiceUtils::balancePlanPrefix();
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, nebula::cpp2::ErrorCode::SUCCEEDED);
        int num = 0;
        while (iter->valid()) {
            num++;
            iter->next();
        }
        ASSERT_EQ(0, num);
    }

    // start a new balance plan
    TestUtils::registerHB(kv, {{"0", 0}, {"1", 1}, {"2", 2}});
    ret = balancer.balance();
    CHECK(ok(ret));
    ASSERT_NE(balanceId, value(ret));
    sleep(1);
    ASSERT_EQ(1, verifyBalancePlan(kv, value(ret), BalanceStatus::SUCCEEDED));
}

void verifyLeaderBalancePlan(HostLeaderMap& hostLeaderMap,
                             const LeaderBalancePlan& plan,
                             size_t minLoad, size_t maxLoad) {
    for (const auto& task : plan) {
        auto space = std::get<0>(task);
        auto part = std::get<1>(task);

        auto& fromParts = hostLeaderMap[std::get<2>(task)][space];
        auto it = std::find(fromParts.begin(), fromParts.end(), part);
        ASSERT_TRUE(it != fromParts.end());
        fromParts.erase(it);

        auto& toParts = hostLeaderMap[std::get<3>(task)][space];
        toParts.emplace_back(part);
    }

    for (const auto& hostEntry : hostLeaderMap) {
        for (const auto& entry : hostEntry.second) {
            EXPECT_GE(entry.second.size(), minLoad);
            EXPECT_LE(entry.second.size(), maxLoad);
        }
    }
}

TEST(BalanceTest, SimpleLeaderBalancePlanTest) {
    fs::TempDir rootPath("/tmp/SimpleLeaderBalancePlanTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    std::vector<HostAddr> hosts = {{"0", 0}, {"1", 1}, {"2", 2}};
    TestUtils::createSomeHosts(kv, hosts);
    // 9 partition in space 1, 3 replica, 3 hosts
    TestUtils::assembleSpace(kv, 1, 9, 3, 3);

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;
    Balancer balancer(kv, &client);
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr("0", 0)][1] = {1, 2, 3, 4, 5};
        hostLeaderMap[HostAddr("1", 1)][1] = {6, 7, 8};
        hostLeaderMap[HostAddr("2", 2)][1] = {9};
        auto tempMap = hostLeaderMap;

        LeaderBalancePlan plan;
        auto leaderBalanceResult = balancer.buildLeaderBalancePlan(&hostLeaderMap, 1, 3,
                                                                   false, plan, false);
        ASSERT_TRUE(nebula::ok(leaderBalanceResult) && nebula::value(leaderBalanceResult));
        verifyLeaderBalancePlan(hostLeaderMap, plan, 3, 3);

        // check two plan build are same
        LeaderBalancePlan tempPlan;
        auto tempLeaderBalanceResult = balancer.buildLeaderBalancePlan(&tempMap, 1, 3, false,
                                                                       tempPlan, false);
        ASSERT_TRUE(nebula::ok(tempLeaderBalanceResult) &&  nebula::value(tempLeaderBalanceResult));
        verifyLeaderBalancePlan(tempMap, tempPlan, 3, 3);

        EXPECT_EQ(plan.size(), tempPlan.size());
        for (size_t i = 0; i < plan.size(); i++) {
            EXPECT_EQ(plan[i], tempPlan[i]);
        }
    }
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr("0", 0)][1] = {1, 2, 3, 4};
        hostLeaderMap[HostAddr("1", 1)][1] = {5, 6, 7, 8};
        hostLeaderMap[HostAddr("2", 2)][1] = {9};

        LeaderBalancePlan plan;
        auto leaderBalanceResult = balancer.buildLeaderBalancePlan(&hostLeaderMap, 1, 3,
                                                                   false, plan, false);
        ASSERT_TRUE(nebula::ok(leaderBalanceResult) && nebula::value(leaderBalanceResult));
        verifyLeaderBalancePlan(hostLeaderMap, plan, 3, 3);
    }
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr("0", 0)][1] = {};
        hostLeaderMap[HostAddr("1", 1)][1] = {};
        hostLeaderMap[HostAddr("2", 2)][1] = {1, 2, 3, 4, 5, 6, 7, 8, 9};

        LeaderBalancePlan plan;
        auto leaderBalanceResult = balancer.buildLeaderBalancePlan(&hostLeaderMap, 1, 3,
                                                                   false, plan, false);
        ASSERT_TRUE(nebula::ok(leaderBalanceResult) && nebula::value(leaderBalanceResult));
        verifyLeaderBalancePlan(hostLeaderMap, plan, 3, 3);
    }
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr("0", 0)][1] = {1, 2, 3};
        hostLeaderMap[HostAddr("1", 1)][1] = {4, 5, 6};
        hostLeaderMap[HostAddr("2", 2)][1] = {7, 8, 9};

        LeaderBalancePlan plan;
        auto leaderBalanceResult = balancer.buildLeaderBalancePlan(&hostLeaderMap, 1, 3,
                                                                   false, plan, false);
        ASSERT_TRUE(nebula::ok(leaderBalanceResult) && nebula::value(leaderBalanceResult));
        verifyLeaderBalancePlan(hostLeaderMap, plan, 3, 3);
    }
}

TEST(BalanceTest, IntersectHostsLeaderBalancePlanTest) {
    fs::TempDir rootPath("/tmp/IntersectHostsLeaderBalancePlanTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    std::vector<HostAddr> hosts = {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}, {"4", 4}, {"5", 5}};
    TestUtils::createSomeHosts(kv, hosts);
    // 7 partition in space 1, 3 replica, 6 hosts, so not all hosts have intersection parts
    TestUtils::assembleSpace(kv, 1, 7, 3, 6);

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;
    Balancer balancer(kv, &client);

    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr("0", 0)][1] = {4, 5, 6};
        hostLeaderMap[HostAddr("1", 1)][1] = {};
        hostLeaderMap[HostAddr("2", 2)][1] = {};
        hostLeaderMap[HostAddr("3", 3)][1] = {1, 2, 3, 7};
        hostLeaderMap[HostAddr("4", 4)][1] = {};
        hostLeaderMap[HostAddr("5", 5)][1] = {};
        showHostLoading(kv, 1);

        LeaderBalancePlan plan;
        auto leaderBalanceResult = balancer.buildLeaderBalancePlan(&hostLeaderMap, 1, 3,
                                                                   false, plan, false);
        ASSERT_TRUE(nebula::ok(leaderBalanceResult) && nebula::value(leaderBalanceResult));
        verifyLeaderBalancePlan(hostLeaderMap, plan, 1, 2);
    }
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr("0", 0)][1] = {};
        hostLeaderMap[HostAddr("1", 1)][1] = {5, 6, 7};
        hostLeaderMap[HostAddr("2", 2)][1] = {};
        hostLeaderMap[HostAddr("3", 3)][1] = {1, 2};
        hostLeaderMap[HostAddr("4", 4)][1] = {};
        hostLeaderMap[HostAddr("5", 5)][1] = {3, 4};

        LeaderBalancePlan plan;
        auto leaderBalanceResult = balancer.buildLeaderBalancePlan(&hostLeaderMap, 1, 3,
                                                                   false, plan, false);
        ASSERT_TRUE(nebula::ok(leaderBalanceResult) && nebula::value(leaderBalanceResult));
        verifyLeaderBalancePlan(hostLeaderMap, plan, 1, 2);
    }
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr("0", 0)][1] = {};
        hostLeaderMap[HostAddr("1", 1)][1] = {1, 5};
        hostLeaderMap[HostAddr("2", 2)][1] = {2, 6};
        hostLeaderMap[HostAddr("3", 3)][1] = {3, 7};
        hostLeaderMap[HostAddr("4", 4)][1] = {4};
        hostLeaderMap[HostAddr("5", 5)][1] = {};

        LeaderBalancePlan plan;
        auto leaderBalanceResult = balancer.buildLeaderBalancePlan(&hostLeaderMap, 1, 3,
                                                                   false, plan, false);
        ASSERT_TRUE(nebula::ok(leaderBalanceResult) && nebula::value(leaderBalanceResult));
        verifyLeaderBalancePlan(hostLeaderMap, plan, 1, 2);
    }
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr("0", 0)][1] = {5, 6};
        hostLeaderMap[HostAddr("1", 1)][1] = {1, 7};
        hostLeaderMap[HostAddr("2", 2)][1] = {};
        hostLeaderMap[HostAddr("3", 3)][1] = {};
        hostLeaderMap[HostAddr("4", 4)][1] = {2, 3, 4};
        hostLeaderMap[HostAddr("5", 5)][1] = {};

        LeaderBalancePlan plan;
        auto leaderBalanceResult = balancer.buildLeaderBalancePlan(&hostLeaderMap, 1, 3,
                                                                   false, plan, false);
        ASSERT_TRUE(nebula::ok(leaderBalanceResult) && nebula::value(leaderBalanceResult));
        verifyLeaderBalancePlan(hostLeaderMap, plan, 1, 2);
    }
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr("0", 0)][1] = {6};
        hostLeaderMap[HostAddr("1", 1)][1] = {1, 7};
        hostLeaderMap[HostAddr("2", 2)][1] = {2};
        hostLeaderMap[HostAddr("3", 3)][1] = {3};
        hostLeaderMap[HostAddr("4", 4)][1] = {4};
        hostLeaderMap[HostAddr("5", 5)][1] = {5};

        LeaderBalancePlan plan;
        auto leaderBalanceResult = balancer.buildLeaderBalancePlan(&hostLeaderMap, 1, 3,
                                                                   false, plan, false);
        ASSERT_TRUE(nebula::ok(leaderBalanceResult) && nebula::value(leaderBalanceResult));
        verifyLeaderBalancePlan(hostLeaderMap, plan, 1, 2);
    }
}

TEST(BalanceTest, ManyHostsLeaderBalancePlanTest) {
    fs::TempDir rootPath("/tmp/SimpleLeaderBalancePlanTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 1;

    int partCount = 99999;
    int replica = 3;
    int hostCount = 100;
    std::vector<HostAddr> hosts;
    for (int i = 0; i < hostCount; i++) {
        hosts.emplace_back(std::to_string(i), i);
    }
    TestUtils::createSomeHosts(kv, hosts);
    TestUtils::assembleSpace(kv, 1, partCount, replica, hostCount);

    float avgLoad = static_cast<float>(partCount) / hostCount;
    int32_t minLoad = std::floor(avgLoad * (1 - FLAGS_leader_balance_deviation));
    int32_t maxLoad = std::ceil(avgLoad * (1 + FLAGS_leader_balance_deviation));

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;
    Balancer balancer(kv, &client);

    // chcek several times if they are balanced
    for (int count = 0; count < 1; count++) {
        HostLeaderMap hostLeaderMap;
        // all part will random choose a leader
        for (int partId = 1; partId <= partCount; partId++) {
            std::vector<HostAddr> peers;
            size_t idx = partId;
            for (int32_t i = 0; i < replica; i++, idx++) {
                peers.emplace_back(hosts[idx % hostCount]);
            }
            ASSERT_EQ(peers.size(), replica);
            auto leader = peers[folly::Random::rand32(peers.size())];
            hostLeaderMap[leader][1].emplace_back(partId);
        }

        LeaderBalancePlan plan;
        auto leaderBalanceResult = balancer.buildLeaderBalancePlan(&hostLeaderMap, 1, 3,
                                                                   false, plan);
        ASSERT_TRUE(nebula::ok(leaderBalanceResult) && nebula::value(leaderBalanceResult));
        verifyLeaderBalancePlan(hostLeaderMap, plan, minLoad, maxLoad);
    }
}

TEST(BalanceTest, LeaderBalanceTest) {
    fs::TempDir rootPath("/tmp/LeaderBalanceTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    std::vector<HostAddr> hosts = {{"0", 0}, {"1", 1}, {"2", 2}};
    TestUtils::createSomeHosts(kv, hosts);
    TestUtils::assembleSpace(kv, 1, 9, 3, 3);

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;
    HostLeaderMap dist;
    dist[HostAddr("0", 0)][1] = {1, 2, 3, 4, 5};
    dist[HostAddr("1", 1)][1] = {6, 7, 8};
    dist[HostAddr("2", 2)][1] = {9};
    EXPECT_CALL(client, getLeaderDist(_))
        .WillOnce(
            DoAll(SetArgPointee<0>(dist), Return(ByMove(folly::Future<Status>(Status::OK())))));

    Balancer balancer(kv, &client);
    auto ret = balancer.leaderBalance();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
}

TEST(BalanceTest, LeaderBalanceWithZoneTest) {
    fs::TempDir rootPath("/tmp/LeaderBalanceWithZone.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 1;
    std::vector<HostAddr> hosts;
    for (int i = 0; i < 9; i++) {
        hosts.emplace_back(std::to_string(i), i);
    }
    TestUtils::createSomeHosts(kv, hosts);
    TestUtils::registerHB(kv, hosts);

    // create zone and group
    {
        ZoneInfo zoneInfo = {
            {"zone_0", {HostAddr("0", 0), HostAddr("1", 1)}},
            {"zone_1", {HostAddr("2", 2), HostAddr("3", 3)}},
            {"zone_2", {HostAddr("4", 4), HostAddr("5", 5)}}
        };
        GroupInfo groupInfo = {
            {"default_group", {"zone_0", "zone_1", "zone_2"}}
        };
        TestUtils::assembleGroupAndZone(kv, zoneInfo, groupInfo);
    }
    {
        cpp2::SpaceDesc properties;
        properties.set_space_name("default_space");
        properties.set_partition_num(8);
        properties.set_replica_factor(3);
        properties.set_group_name("default_group");
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));
        auto* processor = CreateSpaceProcessor::instance(kv);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(1, resp.get_id().get_space_id());
    }

    sleep(1);
    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;
    Balancer balancer(kv, &client);

    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr("0", 0)][1] = {1, 3, 5, 7};
        hostLeaderMap[HostAddr("1", 1)][1] = {2, 4, 6, 8};
        hostLeaderMap[HostAddr("2", 2)][1] = {};
        hostLeaderMap[HostAddr("3", 3)][1] = {};
        hostLeaderMap[HostAddr("4", 4)][1] = {};
        hostLeaderMap[HostAddr("5", 5)][1] = {};

        LeaderBalancePlan plan;
        auto leaderBalanceResult = balancer.buildLeaderBalancePlan(&hostLeaderMap, 1, 3,
                                                                   true, plan, true);
        ASSERT_TRUE(nebula::ok(leaderBalanceResult) && nebula::value(leaderBalanceResult));
        verifyLeaderBalancePlan(hostLeaderMap, plan, 1, 2);
    }
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr("0", 0)][1] = {1, 3};
        hostLeaderMap[HostAddr("1", 1)][1] = {2, 4};
        hostLeaderMap[HostAddr("2", 2)][1] = {5, 7};
        hostLeaderMap[HostAddr("3", 3)][1] = {6, 8};
        hostLeaderMap[HostAddr("4", 4)][1] = {};
        hostLeaderMap[HostAddr("5", 5)][1] = {};

        LeaderBalancePlan plan;
        auto leaderBalanceResult = balancer.buildLeaderBalancePlan(&hostLeaderMap, 1, 3,
                                                                   true, plan, true);
        ASSERT_TRUE(nebula::ok(leaderBalanceResult) && nebula::value(leaderBalanceResult));
        verifyLeaderBalancePlan(hostLeaderMap, plan, 1, 2);
    }
}

TEST(BalanceTest, LeaderBalanceWithLargerZoneTest) {
    fs::TempDir rootPath("/tmp/LeaderBalanceWithLargerZoneTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 1;
    std::vector<HostAddr> hosts;
    for (int i = 0; i < 15; i++) {
        hosts.emplace_back(std::to_string(i), i);
    }
    TestUtils::createSomeHosts(kv, hosts);
    TestUtils::registerHB(kv, hosts);

    // create zone and group
    {
        ZoneInfo zoneInfo = {
            {"zone_0", {HostAddr("0", 0), HostAddr("1", 1)}},
            {"zone_1", {HostAddr("2", 2), HostAddr("3", 3)}},
            {"zone_2", {HostAddr("4", 4), HostAddr("5", 5)}},
            {"zone_3", {HostAddr("6", 6), HostAddr("7", 7)}},
            {"zone_4", {HostAddr("8", 8), HostAddr("9", 9)}},
        };
        GroupInfo groupInfo = {
            {"default_group", {"zone_0", "zone_1", "zone_2", "zone_3", "zone_4"}}
        };
        TestUtils::assembleGroupAndZone(kv, zoneInfo, groupInfo);
    }
    {
        cpp2::SpaceDesc properties;
        properties.set_space_name("default_space");
        properties.set_partition_num(8);
        properties.set_replica_factor(3);
        properties.set_group_name("default_group");
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));
        auto* processor = CreateSpaceProcessor::instance(kv);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(1, resp.get_id().get_space_id());
    }

    sleep(1);
    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;
    Balancer balancer(kv, &client);
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr("0", 0)][1] = {1, 5, 8};
        hostLeaderMap[HostAddr("1", 1)][1] = {3, 6, 7};
        hostLeaderMap[HostAddr("2", 2)][1] = {2};
        hostLeaderMap[HostAddr("3", 3)][1] = {4};
        hostLeaderMap[HostAddr("4", 4)][1] = {};
        hostLeaderMap[HostAddr("5", 5)][1] = {};
        hostLeaderMap[HostAddr("6", 6)][1] = {};
        hostLeaderMap[HostAddr("7", 7)][1] = {};
        hostLeaderMap[HostAddr("8", 8)][1] = {};

        LeaderBalancePlan plan;
        auto leaderBalanceResult = balancer.buildLeaderBalancePlan(&hostLeaderMap, 1, 3,
                                                                   true, plan, true);
        ASSERT_TRUE(nebula::ok(leaderBalanceResult) && nebula::value(leaderBalanceResult));
        verifyLeaderBalancePlan(hostLeaderMap, plan, 0, 1);
    }
}

TEST(BalanceTest, LeaderBalanceWithComplexZoneTest) {
    fs::TempDir rootPath("/tmp/LeaderBalanceWithComplexZoneTest.XXXXXX");
    auto store = MockCluster::initMetaKV(rootPath.path());
    auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
    FLAGS_heartbeat_interval_secs = 1;
    std::vector<HostAddr> hosts;
    for (int i = 0; i < 18; i++) {
        hosts.emplace_back(std::to_string(i), i);
    }
    TestUtils::createSomeHosts(kv, hosts);
    TestUtils::registerHB(kv, hosts);

    {
        ZoneInfo zoneInfo = {
            {"zone_0", {HostAddr("0", 0), HostAddr("1", 1)}},
            {"zone_1", {HostAddr("2", 2), HostAddr("3", 3)}},
            {"zone_2", {HostAddr("4", 4), HostAddr("5", 5)}},
            {"zone_3", {HostAddr("6", 6), HostAddr("7", 7)}},
            {"zone_4", {HostAddr("8", 8), HostAddr("9", 9)}},
            {"zone_5", {HostAddr("10", 10), HostAddr("11", 11)}},
            {"zone_6", {HostAddr("12", 12), HostAddr("13", 13)}},
            {"zone_7", {HostAddr("14", 14), HostAddr("15", 15)}},
            {"zone_8", {HostAddr("16", 16), HostAddr("17", 17)}},
        };
        {
            GroupInfo groupInfo = {
                {"group_0", {"zone_0", "zone_1", "zone_2", "zone_3", "zone_4"}}
            };
            TestUtils::assembleGroupAndZone(kv, zoneInfo, groupInfo);
        }
        {
            GroupInfo groupInfo = {
                {"group_1", {"zone_0", "zone_1", "zone_2", "zone_3", "zone_4",
                             "zone_5", "zone_6", "zone_7", "zone_8"}}
            };
            TestUtils::assembleGroupAndZone(kv, zoneInfo, groupInfo);
        }
    }
    {
        {
            cpp2::SpaceDesc properties;
            properties.set_space_name("default_space");
            properties.set_partition_num(9);
            properties.set_replica_factor(3);
            cpp2::CreateSpaceReq req;
            req.set_properties(std::move(properties));
            auto* processor = CreateSpaceProcessor::instance(kv);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
            ASSERT_EQ(1, resp.get_id().get_space_id());
            showHostLoading(kv, resp.get_id().get_space_id());
        }
        {
            cpp2::SpaceDesc properties;
            properties.set_space_name("space_on_group_0");
            properties.set_partition_num(64);
            properties.set_replica_factor(3);
            properties.set_group_name("group_0");
            cpp2::CreateSpaceReq req;
            req.set_properties(std::move(properties));
            auto* processor = CreateSpaceProcessor::instance(kv);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
            ASSERT_EQ(2, resp.get_id().get_space_id());
            showHostLoading(kv, resp.get_id().get_space_id());
        }
        {
            cpp2::SpaceDesc properties;
            properties.set_space_name("space_on_group_1");
            properties.set_partition_num(81);
            properties.set_replica_factor(3);
            properties.set_group_name("group_1");
            cpp2::CreateSpaceReq req;
            req.set_properties(std::move(properties));
            auto* processor = CreateSpaceProcessor::instance(kv);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
            ASSERT_EQ(3, resp.get_id().get_space_id());
            showHostLoading(kv, resp.get_id().get_space_id());
        }
    }
    showHostLoading(kv, 3);
    sleep(1);
    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;
    Balancer balancer(kv, &client);

    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr("0", 0)][3] = {};
        hostLeaderMap[HostAddr("1", 1)][3] = {};
        hostLeaderMap[HostAddr("2", 2)][3] = {};
        hostLeaderMap[HostAddr("3", 3)][3] = {};
        hostLeaderMap[HostAddr("4", 4)][3] = {62, 68, 74, 80};
        hostLeaderMap[HostAddr("5", 5)][3] = {};
        hostLeaderMap[HostAddr("6", 6)][3] = {};
        hostLeaderMap[HostAddr("7", 7)][3] = {};
        hostLeaderMap[HostAddr("8", 8)][3] = {};
        hostLeaderMap[HostAddr("8", 8)][3] = {};
        hostLeaderMap[HostAddr("9", 9)][3] = {59, 65, 71, 77};
        hostLeaderMap[HostAddr("10", 10)][3] = {61, 67, 73, 79};
        hostLeaderMap[HostAddr("11", 11)][3] = {29, 34, 37, 42, 45, 50, 53, 58, 64, 70, 76};
        hostLeaderMap[HostAddr("12", 12)][3] = {1, 3, 6, 8, 11, 14, 16, 19, 22,
                                                24, 27, 30, 46, 48, 51, 54};
        hostLeaderMap[HostAddr("13", 13)][3] = {10, 15, 18, 31, 52, 69, 81};
        hostLeaderMap[HostAddr("14", 14)][3] = {5, 13, 21, 32, 35, 40, 43, 56, 60, 66, 72, 78};
        hostLeaderMap[HostAddr("15", 15)][3] = {2, 4, 7, 9, 12, 17, 20, 23, 25, 28, 33,
                                                39, 41, 44, 47, 49, 55, 57, 63, 75};
        hostLeaderMap[HostAddr("16", 16)][3] = {26};
        hostLeaderMap[HostAddr("17", 17)][3] = {36, 38};

        LeaderBalancePlan plan;
        auto leaderBalanceResult = balancer.buildLeaderBalancePlan(&hostLeaderMap, 3, 3,
                                                                   true, plan, true);
        ASSERT_TRUE(nebula::ok(leaderBalanceResult) && nebula::value(leaderBalanceResult));
        verifyLeaderBalancePlan(hostLeaderMap, plan, 1, 9);
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
