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
DECLARE_int32(expired_threshold_sec);
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

TEST(BalanceTaskTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/BalanceTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    HostAddr src("0", 0);
    HostAddr dst("1", 1);
    TestUtils::registerHB(kv.get(), {src, dst});

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
        BalanceTask task(0, 0, 0, src, dst, kv.get(), &client);
        folly::Baton<true, std::atomic> b;
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
        BalanceTask task(0, 0, 0, src, dst, kv.get(), &client);
        folly::Baton<true, std::atomic> b;
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

TEST(BalanceTest, BalancePartsTest) {
    std::unique_ptr<Balancer> balancer(new Balancer(nullptr, nullptr));
    auto dump = [](const std::unordered_map<HostAddr, std::vector<PartitionID>>& hostParts,
                   const std::vector<BalanceTask>& tasks) {
        for (auto it = hostParts.begin(); it != hostParts.end(); it++) {
            std::stringstream ss;
            ss << it->first << ":";
            for (auto partId : it->second) {
                ss << partId << ",";
            }
            VLOG(1) << ss.str();
        }
        for (auto& task : tasks) {
            VLOG(1) << task.taskIdStr();
        }
    };
    {
        std::unordered_map<HostAddr, std::vector<PartitionID>> hostParts;
        hostParts.emplace(HostAddr("0", 0), std::vector<PartitionID>{1, 2, 3, 4});
        hostParts.emplace(HostAddr("1", 0), std::vector<PartitionID>{1, 2, 3, 4});
        hostParts.emplace(HostAddr("2", 0), std::vector<PartitionID>{1, 2, 3, 4});
        hostParts.emplace(HostAddr("3", 0), std::vector<PartitionID>{});
        int32_t totalParts = 12;
        std::vector<BalanceTask> tasks;
        VLOG(1) << "=== original map ====";
        dump(hostParts, tasks);
        balancer->balanceParts(0, 0, hostParts, totalParts, tasks);
        VLOG(1) << "=== new map ====";
        dump(hostParts, tasks);
        for (auto it = hostParts.begin(); it != hostParts.end(); it++) {
            EXPECT_EQ(3, it->second.size());
        }
        EXPECT_EQ(3, tasks.size());
    }
    {
        std::unordered_map<HostAddr, std::vector<PartitionID>> hostParts;
        hostParts.emplace(HostAddr("0", 0), std::vector<PartitionID>{1, 2, 3, 4, 5});
        hostParts.emplace(HostAddr("1", 0), std::vector<PartitionID>{1, 2, 4, 5});
        hostParts.emplace(HostAddr("2", 0), std::vector<PartitionID>{2, 3, 4, 5});
        hostParts.emplace(HostAddr("3", 0), std::vector<PartitionID>{1, 3});
        int32_t totalParts = 15;
        std::vector<BalanceTask> tasks;
        VLOG(1) << "=== original map ====";
        dump(hostParts, tasks);
        balancer->balanceParts(0, 0, hostParts, totalParts, tasks);
        VLOG(1) << "=== new map ====";
        dump(hostParts, tasks);
        EXPECT_EQ(4, hostParts[HostAddr("0", 0)].size());
        EXPECT_EQ(4, hostParts[HostAddr("1", 0)].size());
        EXPECT_EQ(4, hostParts[HostAddr("2", 0)].size());
        EXPECT_EQ(3, hostParts[HostAddr("3", 0)].size());
        EXPECT_EQ(1, tasks.size());
    }
    {
        std::unordered_map<HostAddr, std::vector<PartitionID>> hostParts;
        hostParts.emplace(HostAddr("0", 0), std::vector<PartitionID>{1, 2, 3, 4});
        hostParts.emplace(HostAddr("1", 0), std::vector<PartitionID>{1, 2, 4, 5});
        hostParts.emplace(HostAddr("2", 0), std::vector<PartitionID>{2, 3, 4, 5});
        hostParts.emplace(HostAddr("3", 0), std::vector<PartitionID>{1, 3, 5});
        int32_t totalParts = 15;
        std::vector<BalanceTask> tasks;
        VLOG(1) << "=== original map ====";
        dump(hostParts, tasks);
        balancer->balanceParts(0, 0, hostParts, totalParts, tasks);
        VLOG(1) << "=== new map ====";
        dump(hostParts, tasks);
        EXPECT_EQ(4, hostParts[HostAddr("0", 0)].size());
        EXPECT_EQ(4, hostParts[HostAddr("1", 0)].size());
        EXPECT_EQ(4, hostParts[HostAddr("2", 0)].size());
        EXPECT_EQ(3, hostParts[HostAddr("3", 0)].size());
        EXPECT_EQ(0, tasks.size());
    }
    {
        std::unordered_map<HostAddr, std::vector<PartitionID>> hostParts;
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
        balancer->balanceParts(0, 0, hostParts, totalParts, tasks);
        VLOG(1) << "=== new map ====";
        dump(hostParts, tasks);
        for (auto it = hostParts.begin(); it != hostParts.end(); it++) {
            EXPECT_EQ(3, it->second.size());
        }
        EXPECT_EQ(18, tasks.size());
    }
    {
        std::unordered_map<HostAddr, std::vector<PartitionID>> hostParts;
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
        balancer->balanceParts(0, 0, hostParts, totalParts, tasks);
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
    fs::TempDir rootPath("/tmp/BalanceTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
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
        BalancePlan plan(0L, kv.get(), &client);
        TestUtils::registerHB(kv.get(), hosts);

        for (int i = 0; i < 10; i++) {
            BalanceTask task(0, 0, 0, HostAddr(std::to_string(i), 0),
                             HostAddr(std::to_string(i), 1), kv.get(), &client);
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
        BalancePlan plan(0L, kv.get(), &client);
        TestUtils::registerHB(kv.get(), hosts);

        for (int i = 0; i < 10; i++) {
            BalanceTask task(0, 0, i, HostAddr(std::to_string(i), 0),
                             HostAddr(std::to_string(i), 1), kv.get(), &client);
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
        BalancePlan plan(0L, kv.get(), nullptr);
        NiceMock<MockAdminClient> client1, client2;
        {
            for (int i = 0; i < 9; i++) {
                BalanceTask task(0, 0, i, HostAddr(std::to_string(i), 0),
                                 HostAddr(std::to_string(i), 1), kv.get(), &client1);
                plan.addTask(std::move(task));
            }
        }
        {
            EXPECT_CALL(client2, transLeader(_, _, _, _))
                .Times(1)
                .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("Transfer failed")))));
            BalanceTask task(0, 0, 9, HostAddr("9", 0), HostAddr("9", 1), kv.get(), &client2);
            plan.addTask(std::move(task));
        }
        TestUtils::registerHB(kv.get(), hosts);
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

TEST(BalanceTest, NormalTest) {
    fs::TempDir rootPath("/tmp/BalanceTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    FLAGS_expired_threshold_sec = 1;
    TestUtils::createSomeHosts(kv.get());
    TestUtils::assembleSpace(kv.get(), 1, 8, 3, 4);

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;
    Balancer balancer(kv.get(), &client);
    auto ret = balancer.balance();
    ASSERT_EQ(cpp2::ErrorCode::E_BALANCED, error(ret));

    sleep(1);
    LOG(INFO) << "Now, we lost host " << HostAddr("3", 3);
    TestUtils::registerHB(kv.get(), {{"0", 0}, {"1", 1}, {"2", 2}});
    ret = balancer.balance();
    CHECK(ok(ret));
    auto balanceId = value(ret);
    sleep(1);
    LOG(INFO) << "Rebalance finished!";
    {
        const auto& prefix = MetaServiceUtils::balancePlanPrefix();
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int num = 0;
        while (iter->valid()) {
            auto id = MetaServiceUtils::parseBalanceID(iter->key());
            auto status = MetaServiceUtils::parseBalanceStatus(iter->val());
            ASSERT_EQ(balanceId, id);
            ASSERT_EQ(BalanceStatus::SUCCEEDED, status);
            num++;
            iter->next();
        }
        ASSERT_EQ(1, num);
    }
    {
        const auto& prefix = MetaServiceUtils::balanceTaskPrefix(balanceId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int32_t num = 0;
        while (iter->valid()) {
            BalanceTask task;
            {
                auto tup = MetaServiceUtils::parseBalanceTaskKey(iter->key());
                task.balanceId_ = std::get<0>(tup);
                ASSERT_EQ(balanceId, task.balanceId_);
                task.spaceId_ = std::get<1>(tup);
                ASSERT_EQ(1, task.spaceId_);
                task.src_ = std::get<3>(tup);
                ASSERT_EQ(HostAddr("3", 3), task.src_);
            }
            {
                auto tup = MetaServiceUtils::parseBalanceTaskVal(iter->val());
                task.status_ = std::get<0>(tup);
                ASSERT_EQ(BalanceTaskStatus::END, task.status_);
                task.ret_ = std::get<1>(tup);
                ASSERT_EQ(BalanceTaskResult::SUCCEEDED, task.ret_);
                task.startTimeMs_ = std::get<2>(tup);
                ASSERT_GT(task.startTimeMs_, 0);
                task.endTimeMs_ = std::get<3>(tup);
                ASSERT_GT(task.endTimeMs_, 0);
            }
            num++;
            iter->next();
        }
        ASSERT_EQ(6, num);
    }
}

TEST(BalanceTest, SpecifyHostTest) {
    fs::TempDir rootPath("/tmp/BalanceTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    FLAGS_expired_threshold_sec = 1;
    TestUtils::createSomeHosts(kv.get(), {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}});
    TestUtils::assembleSpace(kv.get(), 1, 8, 3, 4);

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;
    Balancer balancer(kv.get(), &client);

    sleep(1);
    LOG(INFO) << "Now, we remove host {3, 3}";
    TestUtils::registerHB(kv.get(), {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}});
    auto ret = balancer.balance({{"3", 3}});
    CHECK(ok(ret));
    auto balanceId = value(ret);
    sleep(1);
    LOG(INFO) << "Rebalance finished!";
    {
        const auto& prefix = MetaServiceUtils::balancePlanPrefix();
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int num = 0;
        while (iter->valid()) {
            auto id = MetaServiceUtils::parseBalanceID(iter->key());
            auto status = MetaServiceUtils::parseBalanceStatus(iter->val());
            ASSERT_EQ(balanceId, id);
            ASSERT_EQ(BalanceStatus::SUCCEEDED, status);
            num++;
            iter->next();
        }
        ASSERT_EQ(1, num);
    }
    {
        const auto& prefix = MetaServiceUtils::balanceTaskPrefix(balanceId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int32_t num = 0;
        while (iter->valid()) {
            BalanceTask task;
            {
                auto tup = MetaServiceUtils::parseBalanceTaskKey(iter->key());
                task.balanceId_ = std::get<0>(tup);
                ASSERT_EQ(balanceId, task.balanceId_);
                task.spaceId_ = std::get<1>(tup);
                ASSERT_EQ(1, task.spaceId_);
                task.src_ = std::get<3>(tup);
                ASSERT_EQ(HostAddr("3", 3), task.src_);
            }
            {
                auto tup = MetaServiceUtils::parseBalanceTaskVal(iter->val());
                task.status_ = std::get<0>(tup);
                ASSERT_EQ(BalanceTaskStatus::END, task.status_);
                task.ret_ = std::get<1>(tup);
                ASSERT_EQ(BalanceTaskResult::SUCCEEDED, task.ret_);
                task.startTimeMs_ = std::get<2>(tup);
                ASSERT_GT(task.startTimeMs_, 0);
                task.endTimeMs_ = std::get<3>(tup);
                ASSERT_GT(task.endTimeMs_, 0);
            }
            num++;
            iter->next();
        }
        ASSERT_EQ(6, num);
    }
}

TEST(BalanceTest, SpecifyMultiHostTest) {
    fs::TempDir rootPath("/tmp/BalanceTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    FLAGS_expired_threshold_sec = 1;
    TestUtils::createSomeHosts(kv.get(), {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}, {"4", 4},
                                         {"5", 5}});
    TestUtils::assembleSpace(kv.get(), 1, 12, 3, 6);
    std::unordered_map<HostAddr, int32_t> partCount;
    for (int32_t i = 0; i < 6; i++) {
        partCount[HostAddr(std::to_string(i), i)] = 6;
    }

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;
    Balancer balancer(kv.get(), &client);

    sleep(1);
    LOG(INFO) << "Now, we want to remove host {2, 2}/{3, 3}";
    // If {"2", 2} and {"3", 3} are both dead, minority hosts for some part are alive,
    // it would lead to a fail
    TestUtils::registerHB(kv.get(), {{"0", 0}, {"1", 1}, {"4", 4}, {"5", 5}});
    auto ret = balancer.balance({{"2", 2}, {"3", 3}});
    CHECK(!ok(ret));
    EXPECT_EQ(cpp2::ErrorCode::E_NO_VALID_HOST, error(ret));
    // If {"2", 2} is dead, {"3", 3} stiil alive, each part has majority hosts alive
    TestUtils::registerHB(kv.get(), {{"0", 0}, {"1", 1}, {"3", 3}, {"4", 4}, {"5", 5}});
    ret = balancer.balance({{"2", 2}, {"3", 3}});
    CHECK(ok(ret));
    auto balanceId = value(ret);
    sleep(1);
    LOG(INFO) << "Rebalance finished!";
    {
        const auto& prefix = MetaServiceUtils::balancePlanPrefix();
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int num = 0;
        while (iter->valid()) {
            auto id = MetaServiceUtils::parseBalanceID(iter->key());
            auto status = MetaServiceUtils::parseBalanceStatus(iter->val());
            ASSERT_EQ(balanceId, id);
            ASSERT_EQ(BalanceStatus::SUCCEEDED, status);
            num++;
            iter->next();
        }
        ASSERT_EQ(1, num);
    }
    {
        const auto& prefix = MetaServiceUtils::balanceTaskPrefix(balanceId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int32_t num = 0;
        while (iter->valid()) {
            BalanceTask task;
            {
                auto tup = MetaServiceUtils::parseBalanceTaskKey(iter->key());
                task.balanceId_ = std::get<0>(tup);
                ASSERT_EQ(balanceId, task.balanceId_);
                task.spaceId_ = std::get<1>(tup);
                ASSERT_EQ(1, task.spaceId_);
                task.src_ = std::get<3>(tup);
                task.dst_ = std::get<4>(tup);
                partCount[task.src_]--;
                partCount[task.dst_]++;
            }
            {
                auto tup = MetaServiceUtils::parseBalanceTaskVal(iter->val());
                task.status_ = std::get<0>(tup);
                ASSERT_EQ(BalanceTaskStatus::END, task.status_);
                task.ret_ = std::get<1>(tup);
                ASSERT_EQ(BalanceTaskResult::SUCCEEDED, task.ret_);
                task.startTimeMs_ = std::get<2>(tup);
                ASSERT_GT(task.startTimeMs_, 0);
                task.endTimeMs_ = std::get<3>(tup);
                ASSERT_GT(task.endTimeMs_, 0);
            }
            num++;
            iter->next();
        }
    }
    ASSERT_EQ(9, partCount[HostAddr("0", 0)]);
    ASSERT_EQ(9, partCount[HostAddr("1", 1)]);
    ASSERT_EQ(0, partCount[HostAddr("2", 2)]);
    ASSERT_EQ(0, partCount[HostAddr("3", 3)]);
    ASSERT_EQ(9, partCount[HostAddr("4", 4)]);
    ASSERT_EQ(9, partCount[HostAddr("5", 5)]);
}

TEST(BalanceTest, MockReplaceMachineTest) {
    fs::TempDir rootPath("/tmp/BalanceTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    FLAGS_expired_threshold_sec = 1;
    TestUtils::createSomeHosts(kv.get(), {{"0", 0}, {"1", 1}, {"2", 2}});
    TestUtils::assembleSpace(kv.get(), 1, 12, 3, 3);

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;
    Balancer balancer(kv.get(), &client);

    // add a new machine
    TestUtils::createSomeHosts(kv.get(), {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}});
    LOG(INFO) << "Now, we want to replace host {2, 2} with {3, 3}";
    // Because for all parts majority hosts still alive, we could balance
    sleep(1);
    TestUtils::registerHB(kv.get(), {{"0", 0}, {"1", 1}, {"3", 3}});
    auto ret = balancer.balance();
    CHECK(ok(ret));
    auto balanceId = value(ret);
    sleep(1);
    LOG(INFO) << "Rebalance finished!";
    {
        const auto& prefix = MetaServiceUtils::balancePlanPrefix();
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int num = 0;
        while (iter->valid()) {
            auto id = MetaServiceUtils::parseBalanceID(iter->key());
            auto status = MetaServiceUtils::parseBalanceStatus(iter->val());
            ASSERT_EQ(balanceId, id);
            ASSERT_EQ(BalanceStatus::SUCCEEDED, status);
            num++;
            iter->next();
        }
        ASSERT_EQ(1, num);
    }
    {
        const auto& prefix = MetaServiceUtils::balanceTaskPrefix(balanceId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int32_t num = 0;
        while (iter->valid()) {
            BalanceTask task;
            {
                auto tup = MetaServiceUtils::parseBalanceTaskKey(iter->key());
                task.balanceId_ = std::get<0>(tup);
                ASSERT_EQ(balanceId, task.balanceId_);
                task.spaceId_ = std::get<1>(tup);
                ASSERT_EQ(1, task.spaceId_);
                task.src_ = std::get<3>(tup);
                task.dst_ = std::get<4>(tup);
                ASSERT_EQ(HostAddr("2", 2), task.src_);
                ASSERT_EQ(HostAddr("3", 3), task.dst_);
            }
            {
                auto tup = MetaServiceUtils::parseBalanceTaskVal(iter->val());
                task.status_ = std::get<0>(tup);
                ASSERT_EQ(BalanceTaskStatus::END, task.status_);
                task.ret_ = std::get<1>(tup);
                ASSERT_EQ(BalanceTaskResult::SUCCEEDED, task.ret_);
                task.startTimeMs_ = std::get<2>(tup);
                ASSERT_GT(task.startTimeMs_, 0);
                task.endTimeMs_ = std::get<3>(tup);
                ASSERT_GT(task.endTimeMs_, 0);
            }
            num++;
            iter->next();
        }
        ASSERT_EQ(12, num);
    }
}

TEST(BalanceTest, SingleReplicaTest) {
    fs::TempDir rootPath("/tmp/BalanceTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    FLAGS_expired_threshold_sec = 1;
    TestUtils::createSomeHosts(kv.get(), {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}, {"4", 4},
                                          {"5", 5}});
    TestUtils::assembleSpace(kv.get(), 1, 12, 1, 6);
    std::unordered_map<HostAddr, int32_t> partCount;
    for (int32_t i = 0; i < 6; i++) {
        partCount[HostAddr(std::to_string(i), i)] = 2;
    }

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;
    Balancer balancer(kv.get(), &client);

    sleep(1);
    LOG(INFO) << "Now, we want to remove host {2, 2} and {3, 3}";
    TestUtils::registerHB(kv.get(), {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}, {"4", 4}, {"5", 5}});
    auto ret = balancer.balance({{"2", 2}, {"3", 3}, {"3", 3}});
    CHECK(ok(ret));
    auto balanceId = value(ret);
    sleep(1);
    LOG(INFO) << "Rebalance finished!";
    {
        const auto& prefix = MetaServiceUtils::balancePlanPrefix();
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int num = 0;
        while (iter->valid()) {
            auto id = MetaServiceUtils::parseBalanceID(iter->key());
            auto status = MetaServiceUtils::parseBalanceStatus(iter->val());
            ASSERT_EQ(balanceId, id);
            ASSERT_EQ(BalanceStatus::SUCCEEDED, status);
            num++;
            iter->next();
        }
        ASSERT_EQ(1, num);
    }
    {
        const auto& prefix = MetaServiceUtils::balanceTaskPrefix(balanceId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int32_t num = 0;
        while (iter->valid()) {
            BalanceTask task;
            {
                auto tup = MetaServiceUtils::parseBalanceTaskKey(iter->key());
                task.balanceId_ = std::get<0>(tup);
                ASSERT_EQ(balanceId, task.balanceId_);
                task.spaceId_ = std::get<1>(tup);
                ASSERT_EQ(1, task.spaceId_);
                task.src_ = std::get<3>(tup);
                task.dst_ = std::get<4>(tup);
                partCount[task.src_]--;
                partCount[task.dst_]++;
            }
            {
                auto tup = MetaServiceUtils::parseBalanceTaskVal(iter->val());
                task.status_ = std::get<0>(tup);
                ASSERT_EQ(BalanceTaskStatus::END, task.status_);
                task.ret_ = std::get<1>(tup);
                ASSERT_EQ(BalanceTaskResult::SUCCEEDED, task.ret_);
                task.startTimeMs_ = std::get<2>(tup);
                ASSERT_GT(task.startTimeMs_, 0);
                task.endTimeMs_ = std::get<3>(tup);
                ASSERT_GT(task.endTimeMs_, 0);
            }
            num++;
            iter->next();
        }
        ASSERT_EQ(4, num);
    }
    ASSERT_EQ(3, partCount[HostAddr("0", 0)]);
    ASSERT_EQ(3, partCount[HostAddr("1", 1)]);
    ASSERT_EQ(0, partCount[HostAddr("2", 2)]);
    ASSERT_EQ(0, partCount[HostAddr("3", 3)]);
    ASSERT_EQ(3, partCount[HostAddr("4", 4)]);
    ASSERT_EQ(3, partCount[HostAddr("5", 5)]);
}

TEST(BalanceTest, TryToRecoveryTest) {
    fs::TempDir rootPath("/tmp/BalanceTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    FLAGS_expired_threshold_sec = 1;
    TestUtils::createSomeHosts(kv.get());
    TestUtils::assembleSpace(kv.get(), 1, 8, 3, 4);

    sleep(1);
    LOG(INFO) << "Now, we lost host " << HostAddr("3", 3);
    TestUtils::registerHB(kv.get(), {{"0", 0}, {"1", 1}, {"2", 2}});

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

    Balancer balancer(kv.get(), &client);
    auto ret = balancer.balance();
    CHECK(ok(ret));
    auto balanceId = value(ret);
    sleep(1);
    {
        const auto& prefix = MetaServiceUtils::balancePlanPrefix();
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int num = 0;
        while (iter->valid()) {
            auto id = MetaServiceUtils::parseBalanceID(iter->key());
            auto status = MetaServiceUtils::parseBalanceStatus(iter->val());
            ASSERT_EQ(balanceId, id);
            ASSERT_EQ(BalanceStatus::FAILED, status);
            num++;
            iter->next();
        }
        ASSERT_EQ(1, num);
    }
    {
        const auto& prefix = MetaServiceUtils::balanceTaskPrefix(balanceId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int32_t num = 0;
        while (iter->valid()) {
            BalanceTask task;
            {
                auto tup = MetaServiceUtils::parseBalanceTaskKey(iter->key());
                task.balanceId_ = std::get<0>(tup);
                ASSERT_EQ(balanceId, task.balanceId_);
                task.spaceId_ = std::get<1>(tup);
                ASSERT_EQ(1, task.spaceId_);
                task.src_ = std::get<3>(tup);
                ASSERT_EQ(HostAddr("3", 3), task.src_);
            }
            {
                auto tup = MetaServiceUtils::parseBalanceTaskVal(iter->val());
                task.status_ = std::get<0>(tup);
                ASSERT_EQ(BalanceTaskStatus::CATCH_UP_DATA, task.status_);
                task.ret_ = std::get<1>(tup);
                ASSERT_EQ(BalanceTaskResult::FAILED, task.ret_);
                task.startTimeMs_ = std::get<2>(tup);
                ASSERT_GT(task.startTimeMs_, 0);
                task.endTimeMs_ = std::get<3>(tup);
                ASSERT_GT(task.endTimeMs_, 0);
            }
            num++;
            iter->next();
        }
        ASSERT_EQ(6, num);
    }

    LOG(INFO) << "Now let's try to recovery it. Since the the host will expired in 1 second, "
              << "the src host would be regarded as offline, so all task will be invalid";
    ret = balancer.balance();
    CHECK(ok(ret));
    balanceId = value(ret);
    sleep(1);
    {
        const auto& prefix = MetaServiceUtils::balancePlanPrefix();
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int num = 0;
        while (iter->valid()) {
            auto id = MetaServiceUtils::parseBalanceID(iter->key());
            auto status = MetaServiceUtils::parseBalanceStatus(iter->val());
            ASSERT_EQ(balanceId, id);
            ASSERT_EQ(BalanceStatus::FAILED, status);
            num++;
            iter->next();
        }
        ASSERT_EQ(1, num);
    }
    {
        const auto& prefix = MetaServiceUtils::balanceTaskPrefix(balanceId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int32_t num = 0;
        while (iter->valid()) {
            BalanceTask task;
            {
                auto tup = MetaServiceUtils::parseBalanceTaskKey(iter->key());
                task.balanceId_ = std::get<0>(tup);
                ASSERT_EQ(balanceId, task.balanceId_);
                task.spaceId_ = std::get<1>(tup);
                ASSERT_EQ(1, task.spaceId_);
                task.src_ = std::get<3>(tup);
                ASSERT_EQ(HostAddr("3", 3), task.src_);
            }
            {
                auto tup = MetaServiceUtils::parseBalanceTaskVal(iter->val());
                task.status_ = std::get<0>(tup);
                ASSERT_EQ(BalanceTaskStatus::START, task.status_);
                task.ret_ = std::get<1>(tup);
                // all task is invalid because src is offline
                ASSERT_EQ(BalanceTaskResult::INVALID, task.ret_);
                task.startTimeMs_ = std::get<2>(tup);
                ASSERT_GT(task.startTimeMs_, 0);
                task.endTimeMs_ = std::get<3>(tup);
                ASSERT_GT(task.endTimeMs_, 0);
            }
            num++;
            iter->next();
        }
        ASSERT_EQ(6, num);
    }
}

TEST(BalanceTest, RecoveryTest) {
    FLAGS_task_concurrency = 1;
    fs::TempDir rootPath("/tmp/BalanceTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    FLAGS_expired_threshold_sec = 1;
    TestUtils::createSomeHosts(kv.get());
    TestUtils::assembleSpace(kv.get(), 1, 8, 3, 4);

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

    sleep(1);
    LOG(INFO) << "Now, we lost host " << HostAddr("3", 3);
    TestUtils::registerHB(kv.get(), {{"0", 0}, {"1", 1}, {"2", 2}});
    Balancer balancer(kv.get(), &client);
    auto ret = balancer.balance();
    CHECK(ok(ret));
    auto balanceId = value(ret);
    sleep(1);
    {
        const auto& prefix = MetaServiceUtils::balancePlanPrefix();
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int num = 0;
        while (iter->valid()) {
            auto id = MetaServiceUtils::parseBalanceID(iter->key());
            auto status = MetaServiceUtils::parseBalanceStatus(iter->val());
            ASSERT_EQ(balanceId, id);
            ASSERT_EQ(BalanceStatus::FAILED, status);
            num++;
            iter->next();
        }
        ASSERT_EQ(1, num);
    }
    {
        const auto& prefix = MetaServiceUtils::balanceTaskPrefix(balanceId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int32_t num = 0;
        while (iter->valid()) {
            BalanceTask task;
            {
                auto tup = MetaServiceUtils::parseBalanceTaskKey(iter->key());
                task.balanceId_ = std::get<0>(tup);
                ASSERT_EQ(balanceId, task.balanceId_);
                task.spaceId_ = std::get<1>(tup);
                ASSERT_EQ(1, task.spaceId_);
                task.src_ = std::get<3>(tup);
                ASSERT_EQ(HostAddr("3", 3), task.src_);
            }
            {
                auto tup = MetaServiceUtils::parseBalanceTaskVal(iter->val());
                task.status_ = std::get<0>(tup);
                ASSERT_EQ(BalanceTaskStatus::CATCH_UP_DATA, task.status_);
                task.ret_ = std::get<1>(tup);
                ASSERT_EQ(BalanceTaskResult::FAILED, task.ret_);
                task.startTimeMs_ = std::get<2>(tup);
                ASSERT_GT(task.startTimeMs_, 0);
                task.endTimeMs_ = std::get<3>(tup);
                ASSERT_GT(task.endTimeMs_, 0);
            }
            num++;
            iter->next();
        }
        ASSERT_EQ(6, num);
    }

    // register hb again to prevent from regarding src as offline
    TestUtils::registerHB(kv.get(), {{"0", 0}, {"1", 1}, {"2", 2}});
    LOG(INFO) << "Now let's try to recovery it.";
    ret = balancer.balance();
    CHECK(ok(ret));
    balanceId = value(ret);
    sleep(1);
    {
        const auto& prefix = MetaServiceUtils::balancePlanPrefix();
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int num = 0;
        while (iter->valid()) {
            auto id = MetaServiceUtils::parseBalanceID(iter->key());
            auto status = MetaServiceUtils::parseBalanceStatus(iter->val());
            ASSERT_EQ(balanceId, id);
            ASSERT_EQ(BalanceStatus::SUCCEEDED, status);
            num++;
            iter->next();
        }
        ASSERT_EQ(1, num);
    }
    {
        const auto& prefix = MetaServiceUtils::balanceTaskPrefix(balanceId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int32_t num = 0;
        while (iter->valid()) {
            BalanceTask task;
            {
                auto tup = MetaServiceUtils::parseBalanceTaskKey(iter->key());
                task.balanceId_ = std::get<0>(tup);
                ASSERT_EQ(balanceId, task.balanceId_);
                task.spaceId_ = std::get<1>(tup);
                ASSERT_EQ(1, task.spaceId_);
                task.src_ = std::get<3>(tup);
                ASSERT_EQ(HostAddr("3", 3), task.src_);
            }
            {
                auto tup = MetaServiceUtils::parseBalanceTaskVal(iter->val());
                task.status_ = std::get<0>(tup);
                ASSERT_EQ(BalanceTaskStatus::END, task.status_);
                task.ret_ = std::get<1>(tup);
                ASSERT_EQ(BalanceTaskResult::SUCCEEDED, task.ret_);
                task.startTimeMs_ = std::get<2>(tup);
                ASSERT_GT(task.startTimeMs_, 0);
                task.endTimeMs_ = std::get<3>(tup);
                ASSERT_GT(task.endTimeMs_, 0);
            }
            num++;
            iter->next();
        }
        ASSERT_EQ(6, num);
    }
}

TEST(BalanceTest, StopAndRecoverTest) {
    FLAGS_task_concurrency = 1;
    fs::TempDir rootPath("/tmp/BalanceTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    FLAGS_expired_threshold_sec = 1;
    TestUtils::createSomeHosts(kv.get());
    TestUtils::assembleSpace(kv.get(), 1, 8, 3, 4);

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> delayClient;
    EXPECT_CALL(delayClient, waitingForCatchUpData(_, _, _))
        // first task in first plan will be blocked, all other tasks will be skipped,
        .Times(1)
        .WillOnce(Return(
            ByMove(folly::makeFuture<Status>(Status::OK()).delayed(std::chrono::seconds(3)))));

    sleep(1);
    TestUtils::registerHB(kv.get(), {{"0", 0}, {"1", 1}, {"2", 2}});
    Balancer balancer(kv.get(), &delayClient);
    auto ret = balancer.balance();
    CHECK(ok(ret));
    auto balanceId = value(ret);

    sleep(1);
    LOG(INFO) << "Rebalance should still in progress";
    {
        const auto& prefix = MetaServiceUtils::balancePlanPrefix();
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int num = 0;
        while (iter->valid()) {
            auto id = MetaServiceUtils::parseBalanceID(iter->key());
            auto status = MetaServiceUtils::parseBalanceStatus(iter->val());
            ASSERT_EQ(balanceId, id);
            ASSERT_EQ(BalanceStatus::IN_PROGRESS, status);
            num++;
            iter->next();
        }
        ASSERT_EQ(1, num);
    }

    TestUtils::registerHB(kv.get(), {{"0", 0}, {"1", 1}, {"2", 2}});
    auto stopRet = balancer.stop();
    CHECK(stopRet.ok());
    ASSERT_EQ(stopRet.value(), balanceId);

    // wait until the only IN_PROGRESS task finished;
    sleep(3);
    {
        const auto& prefix = MetaServiceUtils::balanceTaskPrefix(balanceId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int32_t taskEnded = 0;
        int32_t taskStopped = 0;
        while (iter->valid()) {
            BalanceTask task;
            // PartitionID partId = std::get<2>(BalanceTask::MetaServiceUtils(iter->key()));
            {
                auto tup = MetaServiceUtils::parseBalancePlanVal(iter->val());
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

    TestUtils::registerHB(kv.get(), {{"0", 0}, {"1", 1}, {"2", 2}});
    NiceMock<MockAdminClient> normalClient;
    EXPECT_CALL(normalClient, waitingForCatchUpData(_, _, _))
        // there are 5 stopped task need to recover
        .Times(5);

    balancer.client_ = &normalClient;
    ret = balancer.balance();
    CHECK(ok(ret));
    ASSERT_EQ(value(ret), balanceId);
    // resume stopped plan
    sleep(1);
    {
        const auto& prefix = MetaServiceUtils::balanceTaskPrefix(balanceId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int32_t num = 0;
        while (iter->valid()) {
            BalanceTask task;
            {
                auto tup = MetaServiceUtils::parseBalancePlanVal(iter->val());
                task.status_ = std::get<0>(tup);
                ASSERT_EQ(BalanceTaskStatus::END, task.status_);
                task.ret_ = std::get<1>(tup);
                ASSERT_EQ(BalanceTaskResult::SUCCEEDED, task.ret_);
                task.startTimeMs_ = std::get<2>(tup);
                task.endTimeMs_ = std::get<3>(tup);
            }
            num++;
            iter->next();
        }
        ASSERT_EQ(6, num);
    }
}

TEST(BalanceTest, CleanLastInvalidBalancePlanTest) {
    FLAGS_task_concurrency = 1;
    fs::TempDir rootPath("/tmp/BalanceTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    FLAGS_expired_threshold_sec = 1;
    TestUtils::createSomeHosts(kv.get());
    TestUtils::assembleSpace(kv.get(), 1, 8, 3, 4);

    DefaultValue<folly::Future<Status>>::SetFactory([] {
        return folly::Future<Status>(Status::OK());
    });
    NiceMock<MockAdminClient> client;

    // concurrency = 1, we could only block first task
    EXPECT_CALL(client, waitingForCatchUpData(_, _, _))
        // first task in first plan will be blocked, all rest task will be skipped,
        // then start an new balance plan including 6 tasks, total 7
        .Times(AtLeast(7))
        .WillOnce(Return(
            ByMove(folly::makeFuture<Status>(Status::OK()).delayed(std::chrono::seconds(3)))));

    sleep(1);
    TestUtils::registerHB(kv.get(), {{"0", 0}, {"1", 1}, {"2", 2}});
    Balancer balancer(kv.get(), &client);
    auto ret = balancer.balance();
    CHECK(ok(ret));
    auto balanceId = value(ret);

    sleep(1);
    // stop the running plan
    TestUtils::registerHB(kv.get(), {{"0", 0}, {"1", 1}, {"2", 2}});
    auto stopRet = balancer.stop();
    CHECK(stopRet.ok());
    ASSERT_EQ(stopRet.value(), balanceId);

    // wait until the plan finished, no running plan for now, only one task has been executed,
    // so the task will be failed, try to clean the invalid plan
    sleep(5);
    TestUtils::registerHB(kv.get(), {{"0", 0}, {"1", 1}, {"2", 2}});
    auto cleanRet = balancer.cleanLastInValidPlan();
    CHECK(ok(cleanRet));
    ASSERT_EQ(value(cleanRet), balanceId);

    {
        const auto& prefix = MetaServiceUtils::balancePlanPrefix();
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int num = 0;
        while (iter->valid()) {
            num++;
            iter->next();
        }
        ASSERT_EQ(0, num);
    }

    // start a new balance plan
    TestUtils::registerHB(kv.get(), {{"0", 0}, {"1", 1}, {"2", 2}});
    ret = balancer.balance();
    CHECK(ok(ret));
    ASSERT_NE(balanceId, value(ret));
    sleep(1);

    {
        const auto& prefix = MetaServiceUtils::balancePlanPrefix();
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int num = 0;
        while (iter->valid()) {
            num++;
            iter->next();
        }
        ASSERT_EQ(1, num);
    }
}

void showHostLoading(kvstore::KVStore* kv) {
    auto prefix = MetaServiceUtils::partPrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
    std::unordered_map<HostAddr, std::vector<PartitionID>> hostPart;
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

void verifyLeaderBalancePlan(HostLeaderMap& hostLeaderMap,
                             const LeaderBalancePlan& plan,
                             size_t minLoad, size_t maxLoad) {
    for (const auto& task : plan) {
        auto space = std::get<0>(task);
        auto part = std::get<1>(task);

        auto& fromParts = hostLeaderMap[std::get<2>(task)][space];
        auto it = std::find(fromParts.begin(), fromParts.end(), part);
        if (it == fromParts.end()) {
            LOG(INFO) << "part " << part << " not found";
        }
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
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    std::vector<HostAddr> hosts = {{"0", 0}, {"1", 1}, {"2", 2}};
    TestUtils::createSomeHosts(kv.get(), hosts);
    // 9 partition in space 1, 3 replica, 3 hosts
    TestUtils::assembleSpace(kv.get(), 1, 9, 3, 3);

    std::unique_ptr<AdminClient> client(new AdminClient(kv.get()));
    std::unique_ptr<Balancer> balancer(new Balancer(kv.get(), client.get()));
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr("0", 0)][1] = {1, 2, 3, 4, 5};
        hostLeaderMap[HostAddr("1", 1)][1] = {6, 7, 8};
        hostLeaderMap[HostAddr("2", 2)][1] = {9};
        auto tempMap = hostLeaderMap;

        LeaderBalancePlan plan;
        auto leaderBalanceResult = balancer->buildLeaderBalancePlan(&hostLeaderMap, 1, 3,
                                                                    false, plan, false);
        ASSERT_TRUE(leaderBalanceResult);
        verifyLeaderBalancePlan(hostLeaderMap, plan, 3, 3);

        // check two plan build are same
        LeaderBalancePlan tempPlan;
        auto tempLeaderBalanceResult = balancer->buildLeaderBalancePlan(&tempMap, 1, 3, false,
                                                                        tempPlan, false);
        ASSERT_TRUE(tempLeaderBalanceResult);
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
        auto leaderBalanceResult = balancer->buildLeaderBalancePlan(&hostLeaderMap, 1, 3,
                                                                    false, plan, false);
        ASSERT_TRUE(leaderBalanceResult);
        verifyLeaderBalancePlan(hostLeaderMap, plan, 3, 3);
    }
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr("0", 0)][1] = {};
        hostLeaderMap[HostAddr("1", 1)][1] = {};
        hostLeaderMap[HostAddr("2", 2)][1] = {1, 2, 3, 4, 5, 6, 7, 8, 9};

        LeaderBalancePlan plan;
        auto leaderBalanceResult = balancer->buildLeaderBalancePlan(&hostLeaderMap, 1, 3,
                                                                    false, plan, false);
        ASSERT_TRUE(leaderBalanceResult);
        verifyLeaderBalancePlan(hostLeaderMap, plan, 3, 3);
    }
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr("0", 0)][1] = {1, 2, 3};
        hostLeaderMap[HostAddr("1", 1)][1] = {4, 5, 6};
        hostLeaderMap[HostAddr("2", 2)][1] = {7, 8, 9};

        LeaderBalancePlan plan;
        auto leaderBalanceResult = balancer->buildLeaderBalancePlan(&hostLeaderMap, 1, 3,
                                                                    false, plan, false);
        ASSERT_TRUE(leaderBalanceResult);
        verifyLeaderBalancePlan(hostLeaderMap, plan, 3, 3);
    }
}

TEST(BalanceTest, IntersectHostsLeaderBalancePlanTest) {
    fs::TempDir rootPath("/tmp/IntersectHostsLeaderBalancePlanTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    std::vector<HostAddr> hosts = {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}, {"4", 4}, {"5", 5}};
    TestUtils::createSomeHosts(kv.get(), hosts);
    // 7 partition in space 1, 3 replica, 6 hosts, so not all hosts have intersection parts
    TestUtils::assembleSpace(kv.get(), 1, 7, 3, 6);

    std::unique_ptr<AdminClient> client(new AdminClient(kv.get()));
    std::unique_ptr<Balancer> balancer(new Balancer(kv.get(), client.get()));
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr("0", 0)][1] = {4, 5, 6};
        hostLeaderMap[HostAddr("1", 1)][1] = {};
        hostLeaderMap[HostAddr("2", 2)][1] = {};
        hostLeaderMap[HostAddr("3", 3)][1] = {1, 2, 3, 7};
        hostLeaderMap[HostAddr("4", 4)][1] = {};
        hostLeaderMap[HostAddr("5", 5)][1] = {};
        showHostLoading(kv.get());

        LeaderBalancePlan plan;
        auto leaderBalanceResult = balancer->buildLeaderBalancePlan(&hostLeaderMap, 1, 3,
                                                                    false, plan, false);
        ASSERT_TRUE(leaderBalanceResult);
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
        auto leaderBalanceResult = balancer->buildLeaderBalancePlan(&hostLeaderMap, 1, 3,
                                                                    false, plan, false);
        ASSERT_TRUE(leaderBalanceResult);
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
        auto leaderBalanceResult = balancer->buildLeaderBalancePlan(&hostLeaderMap, 1, 3,
                                                                    false, plan, false);
        ASSERT_TRUE(leaderBalanceResult);
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
        auto leaderBalanceResult = balancer->buildLeaderBalancePlan(&hostLeaderMap, 1, 3,
                                                                    false, plan, false);
        ASSERT_TRUE(leaderBalanceResult);
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
        auto leaderBalanceResult = balancer->buildLeaderBalancePlan(&hostLeaderMap, 1, 3,
                                                                    false, plan, false);
        ASSERT_TRUE(leaderBalanceResult);
        verifyLeaderBalancePlan(hostLeaderMap, plan, 1, 2);
    }
}

TEST(BalanceTest, ManyHostsLeaderBalancePlanTest) {
    fs::TempDir rootPath("/tmp/SimpleLeaderBalancePlanTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    FLAGS_expired_threshold_sec = 600;

    int partCount = 99999;
    int replica = 3;
    int hostCount = 100;
    std::vector<HostAddr> hosts;
    for (int i = 0; i < hostCount; i++) {
        hosts.emplace_back(std::to_string(i), i);
    }
    TestUtils::createSomeHosts(kv.get(), hosts);
    TestUtils::assembleSpace(kv.get(), 1, partCount, replica, hostCount);

    float avgLoad = static_cast<float>(partCount) / hostCount;
    int32_t minLoad = std::floor(avgLoad * (1 - FLAGS_leader_balance_deviation));
    int32_t maxLoad = std::ceil(avgLoad * (1 + FLAGS_leader_balance_deviation));

    std::unique_ptr<AdminClient> client(new AdminClient(kv.get()));
    std::unique_ptr<Balancer> balancer(new Balancer(kv.get(), client.get()));
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
        auto leaderBalanceResult = balancer->buildLeaderBalancePlan(&hostLeaderMap, 1, 3,
                                                                    false, plan);
        ASSERT_TRUE(leaderBalanceResult);
        verifyLeaderBalancePlan(hostLeaderMap, plan, minLoad, maxLoad);
    }
}

TEST(BalanceTest, LeaderBalanceTest) {
    fs::TempDir rootPath("/tmp/LeaderBalanceTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    std::vector<HostAddr> hosts = {{"0", 0}, {"1", 1}, {"2", 2}};
    TestUtils::createSomeHosts(kv.get(), hosts);
    TestUtils::assembleSpace(kv.get(), 1, 9, 3, 3);

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

    Balancer balancer(kv.get(), &client);
    auto ret = balancer.leaderBalance();
    ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, ret);
}

TEST(BalanceTest, LeaderBalanceWithZoneTest) {
    fs::TempDir rootPath("/tmp/LeaderBalanceWithZone.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    FLAGS_expired_threshold_sec = 10;
    std::vector<HostAddr> hosts;
    for (int i = 0; i < 9; i++) {
        hosts.emplace_back(std::to_string(i), i);
    }
    TestUtils::createSomeHosts(kv.get(), hosts);
    TestUtils::registerHB(kv.get(), hosts);

    // create zone and group
    {
        ZoneInfo zoneInfo = {
            {"zone_0", {HostAddr("0", 0), HostAddr("1", 1)}},
            {"zone_1", {HostAddr("2", 2), HostAddr("3", 3)}},
            {"zone_2", {HostAddr("4", 4), HostAddr("5", 5)}}
        };
        GroupInfo groupInfo = {
            {"group_0", {"zone_0", "zone_1", "zone_2"}}
        };
        TestUtils::assembleGroupAndZone(kv.get(), zoneInfo, groupInfo);
    }
    {
        cpp2::SpaceDesc properties;
        properties.set_space_name("default_space");
        properties.set_partition_num(8);
        properties.set_replica_factor(3);
        properties.set_group_name("group_0");
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));
        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_EQ(1, resp.get_id().get_space_id());
    }

    sleep(1);
    std::unique_ptr<AdminClient> client(new AdminClient(kv.get()));
    std::unique_ptr<Balancer> balancer(new Balancer(kv.get(), client.get()));
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr("0", 0)][1] = {1, 3, 5, 7};
        hostLeaderMap[HostAddr("1", 1)][1] = {2, 4, 6, 8};
        hostLeaderMap[HostAddr("2", 2)][1] = {};
        hostLeaderMap[HostAddr("3", 3)][1] = {};
        hostLeaderMap[HostAddr("4", 4)][1] = {};
        hostLeaderMap[HostAddr("5", 5)][1] = {};

        LeaderBalancePlan plan;
        auto leaderBalanceResult = balancer->buildLeaderBalancePlan(&hostLeaderMap, 1, 3,
                                                                    true, plan, true);
        ASSERT_TRUE(leaderBalanceResult);
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
        auto leaderBalanceResult = balancer->buildLeaderBalancePlan(&hostLeaderMap, 1, 3,
                                                                    true, plan, true);
        ASSERT_TRUE(leaderBalanceResult);
        verifyLeaderBalancePlan(hostLeaderMap, plan, 1, 2);
    }
}

TEST(BalanceTest, LeaderBalanceWithLargerZoneTest) {
    fs::TempDir rootPath("/tmp/LeaderBalanceWithLargerZoneTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    FLAGS_expired_threshold_sec = 10;
    std::vector<HostAddr> hosts;
    for (int i = 0; i < 15; i++) {
        hosts.emplace_back(std::to_string(i), i);
    }
    TestUtils::createSomeHosts(kv.get(), hosts);
    TestUtils::registerHB(kv.get(), hosts);

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
            {"group_0", {"zone_0", "zone_1", "zone_2", "zone_3", "zone_4"}}
        };
        TestUtils::assembleGroupAndZone(kv.get(), zoneInfo, groupInfo);
    }
    {
        cpp2::SpaceDesc properties;
        properties.set_space_name("default_space");
        properties.set_partition_num(8);
        properties.set_replica_factor(3);
        properties.set_group_name("group_0");
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));
        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_EQ(1, resp.get_id().get_space_id());
    }

    sleep(1);
    std::unique_ptr<AdminClient> client(new AdminClient(kv.get()));
    std::unique_ptr<Balancer> balancer(new Balancer(kv.get(), client.get()));
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
        auto leaderBalanceResult = balancer->buildLeaderBalancePlan(&hostLeaderMap, 1, 3,
                                                                    true, plan, true);
        ASSERT_TRUE(leaderBalanceResult);
        verifyLeaderBalancePlan(hostLeaderMap, plan, 0, 1);
    }
}

TEST(BalanceTest, LeaderBalanceWithComplexZoneTest) {
    fs::TempDir rootPath("/tmp/LeaderBalanceWithComplexZoneTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    FLAGS_expired_threshold_sec = 10;
    std::vector<HostAddr> hosts;
    for (int i = 0; i < 18; i++) {
        hosts.emplace_back(std::to_string(i), i);
    }
    TestUtils::createSomeHosts(kv.get(), hosts);
    TestUtils::registerHB(kv.get(), hosts);

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
            TestUtils::assembleGroupAndZone(kv.get(), zoneInfo, groupInfo);
        }
        {
            GroupInfo groupInfo = {
                {"group_1", {"zone_0", "zone_1", "zone_2", "zone_3", "zone_4",
                             "zone_5", "zone_6", "zone_7", "zone_8"}}
            };
            TestUtils::assembleGroupAndZone(kv.get(), zoneInfo, groupInfo);
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
            auto* processor = CreateSpaceProcessor::instance(kv.get());
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
            ASSERT_EQ(1, resp.get_id().get_space_id());
            showHostLoading(kv.get());
        }
        {
            cpp2::SpaceDesc properties;
            properties.set_space_name("space_on_group_0");
            properties.set_partition_num(64);
            properties.set_replica_factor(3);
            properties.set_group_name("group_0");
            cpp2::CreateSpaceReq req;
            req.set_properties(std::move(properties));
            auto* processor = CreateSpaceProcessor::instance(kv.get());
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
            ASSERT_EQ(2, resp.get_id().get_space_id());
            showHostLoading(kv.get());
        }
        {
            cpp2::SpaceDesc properties;
            properties.set_space_name("space_on_group_1");
            properties.set_partition_num(81);
            properties.set_replica_factor(3);
            properties.set_group_name("group_1");
            cpp2::CreateSpaceReq req;
            req.set_properties(std::move(properties));
            auto* processor = CreateSpaceProcessor::instance(kv.get());
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
            ASSERT_EQ(3, resp.get_id().get_space_id());
            showHostLoading(kv.get());
        }
    }
    {
        auto prefix = MetaServiceUtils::partPrefix(3);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
        std::map<HostAddr, std::vector<PartitionID>> hostPart;
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
            for (auto i = it->second.begin(); i != it->second.end(); i++) {
                ss << *i << " ";
            }
            LOG(INFO) << it->first << " : " << ss.str();
        }
    }
    sleep(1);
    std::unique_ptr<AdminClient> client(new AdminClient(kv.get()));
    std::unique_ptr<Balancer> balancer(new Balancer(kv.get(), client.get()));
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
        auto leaderBalanceResult = balancer->buildLeaderBalancePlan(&hostLeaderMap, 3, 3,
                                                                    true, plan, true);
        ASSERT_TRUE(leaderBalanceResult);
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


