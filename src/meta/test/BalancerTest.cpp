/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "base/Base.h"
#include <gtest/gtest.h>
#include <folly/synchronization/Baton.h>
#include "meta/processors/admin/Balancer.h"
#include "meta/test/TestUtils.h"
#include "fs/TempDir.h"
#include "meta/processors/partsMan/CreateSpaceProcessor.h"

DECLARE_uint32(task_concurrency);
DECLARE_int32(expired_threshold_sec);
DECLARE_double(leader_balance_deviation);

namespace nebula {
namespace meta {

class TestFaultInjectorWithSleep : public TestFaultInjector {
public:
    explicit TestFaultInjectorWithSleep(std::vector<Status> sts)
        : TestFaultInjector(std::move(sts)) {}

    folly::Future<Status> waitingForCatchUpData() override {
        sleep(3);
        return response(3);
    }
};

TEST(BalanceTaskTest, SimpleTest) {
    {
        std::vector<Status> sts(9, Status::OK());
        std::unique_ptr<FaultInjector> injector(new TestFaultInjector(std::move(sts)));
        auto client = std::make_unique<AdminClient>(std::move(injector));
        BalanceTask task(0, 0, 0, HostAddr(0, 0), HostAddr(1, 1), true, nullptr, nullptr);
        folly::Baton<true, std::atomic> b;
        task.onFinished_ = [&]() {
            LOG(INFO) << "Task finished!";
            EXPECT_EQ(BalanceTask::Result::SUCCEEDED, task.ret_);
            EXPECT_EQ(BalanceTask::Status::END, task.status_);
            b.post();
        };
        task.onError_ = []() {
            LOG(FATAL) << "We should not reach here!";
        };
        task.client_ = client.get();
        task.invoke();
        b.wait();
    }
    {
        std::vector<Status> sts{Status::Error("transLeader failed!"),
                                Status::OK(),
                                Status::OK(),
                                Status::OK(),
                                Status::OK(),
                                Status::OK(),
                                Status::OK(),
                                Status::OK(),
                                Status::OK()};
        std::unique_ptr<FaultInjector> injector(new TestFaultInjector(std::move(sts)));
        auto client = std::make_unique<AdminClient>(std::move(injector));
        BalanceTask task(0, 0, 0, HostAddr(0, 0), HostAddr(1, 1), true, nullptr, nullptr);
        folly::Baton<true, std::atomic> b;
        task.onFinished_ = []() {
            LOG(FATAL) << "We should not reach here!";
        };
        task.onError_ = [&]() {
            LOG(INFO) << "Error happens!";
            EXPECT_EQ(BalanceTask::Result::FAILED, task.ret_);
            EXPECT_EQ(BalanceTask::Status::CHANGE_LEADER, task.status_);
            b.post();
        };
        task.client_ = client.get();
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
        hostParts.emplace(HostAddr(0, 0), std::vector<PartitionID>{1, 2, 3, 4});
        hostParts.emplace(HostAddr(1, 0), std::vector<PartitionID>{1, 2, 3, 4});
        hostParts.emplace(HostAddr(2, 0), std::vector<PartitionID>{1, 2, 3, 4});
        hostParts.emplace(HostAddr(3, 0), std::vector<PartitionID>{});
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
        hostParts.emplace(HostAddr(0, 0), std::vector<PartitionID>{1, 2, 3, 4, 5});
        hostParts.emplace(HostAddr(1, 0), std::vector<PartitionID>{1, 2, 4, 5});
        hostParts.emplace(HostAddr(2, 0), std::vector<PartitionID>{2, 3, 4, 5});
        hostParts.emplace(HostAddr(3, 0), std::vector<PartitionID>{1, 3});
        int32_t totalParts = 15;
        std::vector<BalanceTask> tasks;
        VLOG(1) << "=== original map ====";
        dump(hostParts, tasks);
        balancer->balanceParts(0, 0, hostParts, totalParts, tasks);
        VLOG(1) << "=== new map ====";
        dump(hostParts, tasks);
        EXPECT_EQ(4, hostParts[HostAddr(0, 0)].size());
        EXPECT_EQ(4, hostParts[HostAddr(1, 0)].size());
        EXPECT_EQ(4, hostParts[HostAddr(2, 0)].size());
        EXPECT_EQ(3, hostParts[HostAddr(3, 0)].size());
        EXPECT_EQ(1, tasks.size());
    }
    {
        std::unordered_map<HostAddr, std::vector<PartitionID>> hostParts;
        hostParts.emplace(HostAddr(0, 0), std::vector<PartitionID>{1, 2, 3, 4});
        hostParts.emplace(HostAddr(1, 0), std::vector<PartitionID>{1, 2, 4, 5});
        hostParts.emplace(HostAddr(2, 0), std::vector<PartitionID>{2, 3, 4, 5});
        hostParts.emplace(HostAddr(3, 0), std::vector<PartitionID>{1, 3, 5});
        int32_t totalParts = 15;
        std::vector<BalanceTask> tasks;
        VLOG(1) << "=== original map ====";
        dump(hostParts, tasks);
        balancer->balanceParts(0, 0, hostParts, totalParts, tasks);
        VLOG(1) << "=== new map ====";
        dump(hostParts, tasks);
        EXPECT_EQ(4, hostParts[HostAddr(0, 0)].size());
        EXPECT_EQ(4, hostParts[HostAddr(1, 0)].size());
        EXPECT_EQ(4, hostParts[HostAddr(2, 0)].size());
        EXPECT_EQ(3, hostParts[HostAddr(3, 0)].size());
        EXPECT_EQ(0, tasks.size());
    }
    {
        std::unordered_map<HostAddr, std::vector<PartitionID>> hostParts;
        hostParts.emplace(HostAddr(0, 0), std::vector<PartitionID>{1, 2, 3, 4, 5, 6, 7, 8, 9});
        hostParts.emplace(HostAddr(1, 0), std::vector<PartitionID>{1, 2, 3, 4, 5, 6, 7, 8, 9});
        hostParts.emplace(HostAddr(2, 0), std::vector<PartitionID>{1, 2, 3, 4, 5, 6, 7, 8, 9});
        hostParts.emplace(HostAddr(3, 0), std::vector<PartitionID>{});
        hostParts.emplace(HostAddr(4, 0), std::vector<PartitionID>{});
        hostParts.emplace(HostAddr(5, 0), std::vector<PartitionID>{});
        hostParts.emplace(HostAddr(6, 0), std::vector<PartitionID>{});
        hostParts.emplace(HostAddr(7, 0), std::vector<PartitionID>{});
        hostParts.emplace(HostAddr(8, 0), std::vector<PartitionID>{});
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
        hostParts.emplace(HostAddr(0, 0), std::vector<PartitionID>{1, 2, 3, 4, 5, 6, 7, 8, 9});
        hostParts.emplace(HostAddr(1, 0), std::vector<PartitionID>{1, 2, 3, 4, 5, 6, 7, 8, 9});
        hostParts.emplace(HostAddr(2, 0), std::vector<PartitionID>{1, 2, 3, 4, 5, 6, 7, 8, 9});
        hostParts.emplace(HostAddr(3, 0), std::vector<PartitionID>{});
        hostParts.emplace(HostAddr(4, 0), std::vector<PartitionID>{});
        hostParts.emplace(HostAddr(5, 0), std::vector<PartitionID>{});
        hostParts.emplace(HostAddr(6, 0), std::vector<PartitionID>{});
        hostParts.emplace(HostAddr(7, 0), std::vector<PartitionID>{});
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
    }
}

TEST(BalanceTest, DispatchTasksTest) {
    {
        FLAGS_task_concurrency = 10;
        BalancePlan plan(0L, nullptr, nullptr);
        for (int i = 0; i < 20; i++) {
            BalanceTask task(0, 0, 0, HostAddr(i, 0), HostAddr(i, 1), true, nullptr, nullptr);
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
            BalanceTask task(0, 0, i, HostAddr(i, 0), HostAddr(i, 1), true, nullptr, nullptr);
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
            BalanceTask task(0, 0, i, HostAddr(i, 0), HostAddr(i, 1), true, nullptr, nullptr);
            plan.addTask(std::move(task));
        }
        for (int i = 0; i < 10; i++) {
            BalanceTask task(0, 0, i, HostAddr(i, 2), HostAddr(i, 3), true, nullptr, nullptr);
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
    {
        LOG(INFO) << "Test with all tasks succeeded, only one bucket!";
        BalancePlan plan(0L, nullptr, nullptr);
        std::vector<Status> sts(9, Status::OK());
        std::unique_ptr<FaultInjector> injector(new TestFaultInjector(std::move(sts)));
        auto client = std::make_unique<AdminClient>(std::move(injector));

        for (int i = 0; i < 10; i++) {
            BalanceTask task(0, 0, 0, HostAddr(i, 0), HostAddr(i, 1), true, nullptr, nullptr);
            task.client_ = client.get();
            plan.addTask(std::move(task));
        }
        folly::Baton<true, std::atomic> b;
        plan.onFinished_ = [&plan, &b] () {
            ASSERT_EQ(BalancePlan::Status::SUCCEEDED, plan.status_);
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
        BalancePlan plan(0L, nullptr, nullptr);
        std::vector<Status> sts(9, Status::OK());
        std::unique_ptr<FaultInjector> injector(new TestFaultInjector(std::move(sts)));
        auto client = std::make_unique<AdminClient>(std::move(injector));

        for (int i = 0; i < 10; i++) {
            BalanceTask task(0, 0, i, HostAddr(i, 0), HostAddr(i, 1), true, nullptr, nullptr);
            task.client_ = client.get();
            plan.addTask(std::move(task));
        }
        folly::Baton<true, std::atomic> b;
        plan.onFinished_ = [&plan, &b] () {
            ASSERT_EQ(BalancePlan::Status::SUCCEEDED, plan.status_);
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
        BalancePlan plan(0L, nullptr, nullptr);
        std::unique_ptr<AdminClient> client1, client2;
        {
            std::vector<Status> sts(9, Status::OK());
            std::unique_ptr<FaultInjector> injector(new TestFaultInjector(std::move(sts)));
            client1 = std::make_unique<AdminClient>(std::move(injector));
            for (int i = 0; i < 9; i++) {
                BalanceTask task(0, 0, i, HostAddr(i, 0), HostAddr(i, 1), true, nullptr, nullptr);
                task.client_ = client1.get();
                plan.addTask(std::move(task));
            }
        }
        {
            std::vector<Status> sts {
                                Status::Error("transLeader failed!"),
                                Status::OK(),
                                Status::OK(),
                                Status::OK(),
                                Status::OK(),
                                Status::OK(),
                                Status::OK(),
                                Status::OK(),
                                Status::OK()};
            std::unique_ptr<FaultInjector> injector(new TestFaultInjector(std::move(sts)));
            client2 = std::make_unique<AdminClient>(std::move(injector));
            BalanceTask task(0, 0, 0, HostAddr(10, 0), HostAddr(10, 1), true, nullptr, nullptr);
            task.client_ = client2.get();
            plan.addTask(std::move(task));
        }
        folly::Baton<true, std::atomic> b;
        plan.onFinished_ = [&plan, &b] () {
            ASSERT_EQ(BalancePlan::Status::FAILED, plan.status_);
            ASSERT_EQ(10, plan.finishedTaskNum_);
            b.post();
        };
        plan.invoke();
        b.wait();
    }
}

TEST(BalanceTest, NormalTest) {
    fs::TempDir rootPath("/tmp/BalanceTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    FLAGS_expired_threshold_sec = 1;
    TestUtils::createSomeHosts(kv.get());
    {
        cpp2::SpaceProperties properties;
        properties.set_space_name("default_space");
        properties.set_partition_num(8);
        properties.set_replica_factor(3);
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));
        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_EQ(1, resp.get_id().get_space_id());
    }
    std::vector<Status> sts(9, Status::OK());
    std::unique_ptr<FaultInjector> injector(new TestFaultInjector(std::move(sts)));
    auto client = std::make_unique<AdminClient>(std::move(injector));
    Balancer balancer(kv.get(), std::move(client));
    auto ret = balancer.balance();
    ASSERT_EQ(cpp2::ErrorCode::E_BALANCED, error(ret));

    sleep(1);
    LOG(INFO) << "Now, we lost host " << HostAddr(3, 3);
    TestUtils::registerHB(kv.get(), {{0, 0}, {1, 1}, {2, 2}});
    ret = balancer.balance();
    CHECK(ok(ret));
    auto balanceId = value(ret);
    sleep(1);
    LOG(INFO) << "Rebalance finished!";
    {
        const auto& prefix = BalancePlan::prefix();
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int num = 0;
        while (iter->valid()) {
            auto id = BalancePlan::id(iter->key());
            auto status = BalancePlan::status(iter->val());
            ASSERT_EQ(balanceId, id);
            ASSERT_EQ(BalancePlan::Status::SUCCEEDED, status);
            num++;
            iter->next();
        }
        ASSERT_EQ(1, num);
    }
    {
        const auto& prefix = BalanceTask::prefix(balanceId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int32_t num = 0;
        while (iter->valid()) {
            BalanceTask task;
            {
                auto tup = BalanceTask::parseKey(iter->key());
                task.balanceId_ = std::get<0>(tup);
                ASSERT_EQ(balanceId, task.balanceId_);
                task.spaceId_ = std::get<1>(tup);
                ASSERT_EQ(1, task.spaceId_);
                task.src_ = std::get<3>(tup);
                ASSERT_EQ(HostAddr(3, 3), task.src_);
            }
            {
                auto tup = BalanceTask::parseVal(iter->val());
                task.status_ = std::get<0>(tup);
                ASSERT_EQ(BalanceTask::Status::END, task.status_);
                task.ret_ = std::get<1>(tup);
                ASSERT_EQ(BalanceTask::Result::SUCCEEDED, task.ret_);
                task.srcLived_ = std::get<2>(tup);
                ASSERT_FALSE(task.srcLived_);
                task.startTimeMs_ = std::get<3>(tup);
                ASSERT_GT(task.startTimeMs_, 0);
                task.endTimeMs_ = std::get<4>(tup);
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
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    FLAGS_expired_threshold_sec = 1;
    TestUtils::createSomeHosts(kv.get(), {{0, 0}, {1, 1}, {2, 2}, {3, 3}});
    {
        cpp2::SpaceProperties properties;
        properties.set_space_name("default_space");
        properties.set_partition_num(8);
        properties.set_replica_factor(3);
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));
        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_EQ(1, resp.get_id().get_space_id());
    }
    std::vector<Status> sts(9, Status::OK());
    std::unique_ptr<FaultInjector> injector(new TestFaultInjector(std::move(sts)));
    auto client = std::make_unique<AdminClient>(std::move(injector));
    Balancer balancer(kv.get(), std::move(client));

    sleep(1);
    LOG(INFO) << "Now, we remove host {3, 3}";
    TestUtils::registerHB(kv.get(), {{0, 0}, {1, 1}, {2, 2}, {3, 3}});
    auto ret = balancer.balance({{3, 3}});
    CHECK(ok(ret));
    auto balanceId = value(ret);
    sleep(1);
    LOG(INFO) << "Rebalance finished!";
    {
        const auto& prefix = BalancePlan::prefix();
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int num = 0;
        while (iter->valid()) {
            auto id = BalancePlan::id(iter->key());
            auto status = BalancePlan::status(iter->val());
            ASSERT_EQ(balanceId, id);
            ASSERT_EQ(BalancePlan::Status::SUCCEEDED, status);
            num++;
            iter->next();
        }
        ASSERT_EQ(1, num);
    }
    {
        const auto& prefix = BalanceTask::prefix(balanceId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int32_t num = 0;
        while (iter->valid()) {
            BalanceTask task;
            {
                auto tup = BalanceTask::parseKey(iter->key());
                task.balanceId_ = std::get<0>(tup);
                ASSERT_EQ(balanceId, task.balanceId_);
                task.spaceId_ = std::get<1>(tup);
                ASSERT_EQ(1, task.spaceId_);
                task.src_ = std::get<3>(tup);
                ASSERT_EQ(HostAddr(3, 3), task.src_);
            }
            {
                auto tup = BalanceTask::parseVal(iter->val());
                task.status_ = std::get<0>(tup);
                ASSERT_EQ(BalanceTask::Status::END, task.status_);
                task.ret_ = std::get<1>(tup);
                ASSERT_EQ(BalanceTask::Result::SUCCEEDED, task.ret_);
                task.srcLived_ = std::get<2>(tup);
                ASSERT_FALSE(task.srcLived_);
                task.startTimeMs_ = std::get<3>(tup);
                ASSERT_GT(task.startTimeMs_, 0);
                task.endTimeMs_ = std::get<4>(tup);
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
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    FLAGS_expired_threshold_sec = 1;
    TestUtils::createSomeHosts(kv.get(), {{0, 0}, {1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}});
    {
        cpp2::SpaceProperties properties;
        properties.set_space_name("default_space");
        properties.set_partition_num(12);
        properties.set_replica_factor(3);
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));
        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_EQ(1, resp.get_id().get_space_id());
    }
    std::unordered_map<HostAddr, int32_t> partCount;
    for (int32_t i = 0; i < 6; i++) {
        partCount[HostAddr(i, i)] = 6;
    }
    std::vector<Status> sts(9, Status::OK());
    std::unique_ptr<FaultInjector> injector(new TestFaultInjector(std::move(sts)));
    auto client = std::make_unique<AdminClient>(std::move(injector));
    Balancer balancer(kv.get(), std::move(client));

    sleep(1);
    LOG(INFO) << "Now, we want to remove host {2, 2}/{3, 3}";
    TestUtils::registerHB(kv.get(), {{0, 0}, {1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}});
    auto ret = balancer.balance({{2, 2}, {3, 3}});
    CHECK(ok(ret));
    auto balanceId = value(ret);
    sleep(1);
    LOG(INFO) << "Rebalance finished!";
    {
        const auto& prefix = BalancePlan::prefix();
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int num = 0;
        while (iter->valid()) {
            auto id = BalancePlan::id(iter->key());
            auto status = BalancePlan::status(iter->val());
            ASSERT_EQ(balanceId, id);
            ASSERT_EQ(BalancePlan::Status::SUCCEEDED, status);
            num++;
            iter->next();
        }
        ASSERT_EQ(1, num);
    }
    {
        const auto& prefix = BalanceTask::prefix(balanceId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int32_t num = 0;
        while (iter->valid()) {
            BalanceTask task;
            {
                auto tup = BalanceTask::parseKey(iter->key());
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
                auto tup = BalanceTask::parseVal(iter->val());
                task.status_ = std::get<0>(tup);
                ASSERT_EQ(BalanceTask::Status::END, task.status_);
                task.ret_ = std::get<1>(tup);
                ASSERT_EQ(BalanceTask::Result::SUCCEEDED, task.ret_);
                task.startTimeMs_ = std::get<3>(tup);
                ASSERT_GT(task.startTimeMs_, 0);
                task.endTimeMs_ = std::get<4>(tup);
                ASSERT_GT(task.endTimeMs_, 0);
            }
            num++;
            iter->next();
        }
    }
    ASSERT_EQ(9, partCount[HostAddr(0, 0)]);
    ASSERT_EQ(9, partCount[HostAddr(1, 1)]);
    ASSERT_EQ(0, partCount[HostAddr(2, 2)]);
    ASSERT_EQ(0, partCount[HostAddr(3, 3)]);
    ASSERT_EQ(9, partCount[HostAddr(4, 4)]);
    ASSERT_EQ(9, partCount[HostAddr(5, 5)]);
}

TEST(BalanceTest, RecoveryTest) {
    fs::TempDir rootPath("/tmp/BalanceTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    FLAGS_expired_threshold_sec = 1;
    TestUtils::createSomeHosts(kv.get());
    {
        cpp2::SpaceProperties properties;
        properties.set_space_name("default_space");
        properties.set_partition_num(8);
        properties.set_replica_factor(3);
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
    LOG(INFO) << "Now, we lost host " << HostAddr(3, 3);
    TestUtils::registerHB(kv.get(), {{0, 0}, {1, 1}, {2, 2}});
    std::vector<Status> sts {
                                Status::OK(),
                                Status::OK(),
                                Status::OK(),
                                Status::Error("catch up data failed!"),
                                Status::OK(),
                                Status::OK(),
                                Status::OK(),
                                Status::OK(),
                                Status::OK()};

    std::unique_ptr<FaultInjector> injector(new TestFaultInjector(std::move(sts)));
    auto client = std::make_unique<AdminClient>(std::move(injector));
    Balancer balancer(kv.get(), std::move(client));
    auto ret = balancer.balance();
    CHECK(ok(ret));
    auto balanceId = value(ret);
    sleep(1);
    {
        const auto& prefix = BalancePlan::prefix();
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int num = 0;
        while (iter->valid()) {
            auto id = BalancePlan::id(iter->key());
            auto status = BalancePlan::status(iter->val());
            ASSERT_EQ(balanceId, id);
            ASSERT_EQ(BalancePlan::Status::FAILED, status);
            num++;
            iter->next();
        }
        ASSERT_EQ(1, num);
    }
    {
        const auto& prefix = BalanceTask::prefix(balanceId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int32_t num = 0;
        while (iter->valid()) {
            BalanceTask task;
            {
                auto tup = BalanceTask::parseKey(iter->key());
                task.balanceId_ = std::get<0>(tup);
                ASSERT_EQ(balanceId, task.balanceId_);
                task.spaceId_ = std::get<1>(tup);
                ASSERT_EQ(1, task.spaceId_);
                task.src_ = std::get<3>(tup);
                ASSERT_EQ(HostAddr(3, 3), task.src_);
            }
            {
                auto tup = BalanceTask::parseVal(iter->val());
                task.status_ = std::get<0>(tup);
                ASSERT_EQ(BalanceTask::Status::CATCH_UP_DATA, task.status_);
                task.ret_ = std::get<1>(tup);
                ASSERT_EQ(BalanceTask::Result::FAILED, task.ret_);
                task.srcLived_ = std::get<2>(tup);
                ASSERT_FALSE(task.srcLived_);
                task.startTimeMs_ = std::get<3>(tup);
                ASSERT_GT(task.startTimeMs_, 0);
                task.endTimeMs_ = std::get<4>(tup);
                ASSERT_GT(task.endTimeMs_, 0);
            }
            num++;
            iter->next();
        }
        ASSERT_EQ(6, num);
    }
    LOG(INFO) << "Now let's recovery it.";
    std::vector<Status> normalSts(7, Status::OK());
    static_cast<TestFaultInjector*>(balancer.client_->faultInjector())->reset(std::move(normalSts));
    ret = balancer.balance();
    CHECK(ok(ret));
    balanceId = value(ret);
    sleep(1);
    {
        const auto& prefix = BalancePlan::prefix();
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int num = 0;
        while (iter->valid()) {
            auto id = BalancePlan::id(iter->key());
            auto status = BalancePlan::status(iter->val());
            ASSERT_EQ(balanceId, id);
            ASSERT_EQ(BalancePlan::Status::SUCCEEDED, status);
            num++;
            iter->next();
        }
        ASSERT_EQ(1, num);
    }
    {
        const auto& prefix = BalanceTask::prefix(balanceId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int32_t num = 0;
        while (iter->valid()) {
            BalanceTask task;
            {
                auto tup = BalanceTask::parseKey(iter->key());
                task.balanceId_ = std::get<0>(tup);
                ASSERT_EQ(balanceId, task.balanceId_);
                task.spaceId_ = std::get<1>(tup);
                ASSERT_EQ(1, task.spaceId_);
                task.src_ = std::get<3>(tup);
                ASSERT_EQ(HostAddr(3, 3), task.src_);
            }
            {
                auto tup = BalanceTask::parseVal(iter->val());
                task.status_ = std::get<0>(tup);
                task.ret_ = std::get<1>(tup);
                ASSERT_EQ(BalanceTask::Result::INVALID, task.ret_);
                task.srcLived_ = std::get<2>(tup);
                ASSERT_FALSE(task.srcLived_);
                task.startTimeMs_ = std::get<3>(tup);
                ASSERT_GT(task.startTimeMs_, 0);
                task.endTimeMs_ = std::get<4>(tup);
                ASSERT_GT(task.endTimeMs_, 0);
            }
            num++;
            iter->next();
        }
        ASSERT_EQ(6, num);
    }
}

TEST(BalanceTest, StopBalanceDataTest) {
    FLAGS_task_concurrency = 1;
    fs::TempDir rootPath("/tmp/BalanceTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    FLAGS_expired_threshold_sec = 1;
    TestUtils::createSomeHosts(kv.get());
    {
        cpp2::SpaceProperties properties;
        properties.set_space_name("default_space");
        properties.set_partition_num(8);
        properties.set_replica_factor(3);
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
    TestUtils::registerHB(kv.get(), {{0, 0}, {1, 1}, {2, 2}});
    std::vector<Status> sts(9, Status::OK());
    std::unique_ptr<FaultInjector> injector(new TestFaultInjectorWithSleep(std::move(sts)));
    auto client = std::make_unique<AdminClient>(std::move(injector));
    Balancer balancer(kv.get(), std::move(client));
    auto ret = balancer.balance();
    CHECK(ok(ret));
    auto balanceId = value(ret);

    sleep(1);
    LOG(INFO) << "Rebalance should still in progress";
    {
        const auto& prefix = BalancePlan::prefix();
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int num = 0;
        while (iter->valid()) {
            auto id = BalancePlan::id(iter->key());
            auto status = BalancePlan::status(iter->val());
            ASSERT_EQ(balanceId, id);
            ASSERT_EQ(BalancePlan::Status::IN_PROGRESS, status);
            num++;
            iter->next();
        }
        ASSERT_EQ(1, num);
    }

    TestUtils::registerHB(kv.get(), {{0, 0}, {1, 1}, {2, 2}});
    auto stopRet = balancer.stop();
    CHECK(stopRet.ok());
    ASSERT_EQ(stopRet.value(), balanceId);

    // wait until the only IN_PROGRESS task finished;
    sleep(3);
    {
        const auto& prefix = BalanceTask::prefix(balanceId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int32_t taskEnded = 0;
        int32_t taskStopped = 0;
        while (iter->valid()) {
            BalanceTask task;
            // PartitionID partId = std::get<2>(BalanceTask::parseKey(iter->key()));
            {
                auto tup = BalanceTask::parseVal(iter->val());
                task.status_ = std::get<0>(tup);
                task.ret_ = std::get<1>(tup);
                task.startTimeMs_ = std::get<3>(tup);
                task.endTimeMs_ = std::get<4>(tup);

                if (task.status_ == BalanceTask::Status::END) {
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

    TestUtils::registerHB(kv.get(), {{0, 0}, {1, 1}, {2, 2}});
    ret = balancer.balance();
    CHECK(ok(ret));
    ASSERT_NE(value(ret), balanceId);
    // resume stopped plan
    sleep(1);
    {
        const auto& prefix = BalanceTask::prefix(balanceId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
        ASSERT_EQ(retcode, kvstore::ResultCode::SUCCEEDED);
        int32_t num = 0;
        int32_t taskStarted = 0;
        int32_t taskEnded = 0;
        while (iter->valid()) {
            BalanceTask task;
            {
                auto tup = BalanceTask::parseVal(iter->val());
                task.status_ = std::get<0>(tup);
                task.ret_ = std::get<1>(tup);
                task.startTimeMs_ = std::get<3>(tup);
                task.endTimeMs_ = std::get<4>(tup);
                if (task.status_ == BalanceTask::Status::END) {
                    ++taskEnded;
                } else if (task.status_ == BalanceTask::Status::START) {
                    ++taskStarted;
                }
            }
            num++;
            iter->next();
        }
        ASSERT_EQ(6, num);
        EXPECT_EQ(5, taskStarted);
        EXPECT_EQ(1, taskEnded);
    }
}


void verifyLeaderBalancePlan(std::unordered_map<HostAddr, std::vector<PartitionID>> leaderCount,
        size_t minLoad, size_t maxLoad) {
    for (const auto& hostEntry : leaderCount) {
        EXPECT_GE(hostEntry.second.size(), minLoad);
        EXPECT_LE(hostEntry.second.size(), maxLoad);
    }
}

TEST(BalanceTest, SimpleLeaderBalancePlanTest) {
    fs::TempDir rootPath("/tmp/SimpleLeaderBalancePlanTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    std::vector<HostAddr> hosts = {{0, 0}, {1, 1}, {2, 2}};
    TestUtils::createSomeHosts(kv.get(), hosts);
    // 9 partition in space 1, 3 replica, 3 hosts
    TestUtils::assembleSpace(kv.get(), 1, 9, 3, 3);

    std::unique_ptr<AdminClient> client(new AdminClient(kv.get()));
    std::unique_ptr<Balancer> balancer(new Balancer(kv.get(), std::move(client)));
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr(0, 0)][1] = {1, 2, 3, 4, 5};
        hostLeaderMap[HostAddr(1, 1)][1] = {6, 7, 8};
        hostLeaderMap[HostAddr(2, 2)][1] = {9};
        auto tempMap = hostLeaderMap;

        LeaderBalancePlan plan;
        auto leaderParts = balancer->buildLeaderBalancePlan(&hostLeaderMap, 1, plan, false);
        verifyLeaderBalancePlan(leaderParts, 3, 3);

        // check two plan build are same
        LeaderBalancePlan tempPlan;
        auto tempLeaderParts = balancer->buildLeaderBalancePlan(&tempMap, 1, tempPlan, false);
        verifyLeaderBalancePlan(tempLeaderParts, 3, 3);
        EXPECT_EQ(plan.size(), tempPlan.size());
        for (size_t i = 0; i < plan.size(); i++) {
            EXPECT_EQ(plan[i], tempPlan[i]);
        }
    }
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr(0, 0)][1] = {1, 2, 3, 4};
        hostLeaderMap[HostAddr(1, 1)][1] = {5, 6, 7, 8};
        hostLeaderMap[HostAddr(2, 2)][1] = {9};

        LeaderBalancePlan plan;
        auto leaderParts = balancer->buildLeaderBalancePlan(&hostLeaderMap, 1, plan, false);
        verifyLeaderBalancePlan(leaderParts, 3, 3);
    }
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr(0, 0)][1] = {};
        hostLeaderMap[HostAddr(1, 1)][1] = {};
        hostLeaderMap[HostAddr(2, 2)][1] = {1, 2, 3, 4, 5, 6, 7, 8, 9};

        LeaderBalancePlan plan;
        auto leaderParts = balancer->buildLeaderBalancePlan(&hostLeaderMap, 1, plan, false);
        verifyLeaderBalancePlan(leaderParts, 3, 3);
    }
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr(0, 0)][1] = {1, 2, 3};
        hostLeaderMap[HostAddr(1, 1)][1] = {4, 5, 6};
        hostLeaderMap[HostAddr(2, 2)][1] = {7, 8, 9};

        LeaderBalancePlan plan;
        auto leaderParts = balancer->buildLeaderBalancePlan(&hostLeaderMap, 1, plan, false);
        verifyLeaderBalancePlan(leaderParts, 3, 3);
    }
}

TEST(BalanceTest, IntersectHostsLeaderBalancePlanTest) {
    fs::TempDir rootPath("/tmp/IntersectHostsLeaderBalancePlanTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    std::vector<HostAddr> hosts = {{0, 0}, {1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}};
    TestUtils::createSomeHosts(kv.get(), hosts);
    // 7 partition in space 1, 3 replica, 6 hosts, so not all hosts have intersection parts
    TestUtils::assembleSpace(kv.get(), 1, 7, 3, 6);

    std::unique_ptr<AdminClient> client(new AdminClient(kv.get()));
    std::unique_ptr<Balancer> balancer(new Balancer(kv.get(), std::move(client)));
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr(0, 0)][1] = {4, 5, 6};
        hostLeaderMap[HostAddr(1, 1)][1] = {};
        hostLeaderMap[HostAddr(2, 2)][1] = {};
        hostLeaderMap[HostAddr(3, 3)][1] = {1, 2, 3, 7};
        hostLeaderMap[HostAddr(4, 4)][1] = {};
        hostLeaderMap[HostAddr(5, 5)][1] = {};

        LeaderBalancePlan plan;
        auto leaderParts = balancer->buildLeaderBalancePlan(&hostLeaderMap, 1, plan, false);
        verifyLeaderBalancePlan(leaderParts, 1, 2);
    }
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr(0, 0)][1] = {};
        hostLeaderMap[HostAddr(1, 1)][1] = {5, 6, 7};
        hostLeaderMap[HostAddr(2, 2)][1] = {};
        hostLeaderMap[HostAddr(3, 3)][1] = {1, 2};
        hostLeaderMap[HostAddr(4, 4)][1] = {};
        hostLeaderMap[HostAddr(5, 5)][1] = {3, 4};

        LeaderBalancePlan plan;
        auto leaderParts = balancer->buildLeaderBalancePlan(&hostLeaderMap, 1, plan, false);
        verifyLeaderBalancePlan(leaderParts, 1, 2);
    }
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr(0, 0)][1] = {};
        hostLeaderMap[HostAddr(1, 1)][1] = {1, 5};
        hostLeaderMap[HostAddr(2, 2)][1] = {2, 6};
        hostLeaderMap[HostAddr(3, 3)][1] = {3, 7};
        hostLeaderMap[HostAddr(4, 4)][1] = {4};
        hostLeaderMap[HostAddr(5, 5)][1] = {};

        LeaderBalancePlan plan;
        auto leaderParts = balancer->buildLeaderBalancePlan(&hostLeaderMap, 1, plan, false);
        verifyLeaderBalancePlan(leaderParts, 1, 2);
    }
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr(0, 0)][1] = {5, 6};
        hostLeaderMap[HostAddr(1, 1)][1] = {1, 7};
        hostLeaderMap[HostAddr(2, 2)][1] = {};
        hostLeaderMap[HostAddr(3, 3)][1] = {};
        hostLeaderMap[HostAddr(4, 4)][1] = {2, 3, 4};
        hostLeaderMap[HostAddr(5, 5)][1] = {};

        LeaderBalancePlan plan;
        auto leaderParts = balancer->buildLeaderBalancePlan(&hostLeaderMap, 1, plan, false);
        verifyLeaderBalancePlan(leaderParts, 1, 2);
    }
    {
        HostLeaderMap hostLeaderMap;
        hostLeaderMap[HostAddr(0, 0)][1] = {6};
        hostLeaderMap[HostAddr(1, 1)][1] = {1, 7};
        hostLeaderMap[HostAddr(2, 2)][1] = {2};
        hostLeaderMap[HostAddr(3, 3)][1] = {3};
        hostLeaderMap[HostAddr(4, 4)][1] = {4};
        hostLeaderMap[HostAddr(5, 5)][1] = {5};

        LeaderBalancePlan plan;
        auto leaderParts = balancer->buildLeaderBalancePlan(&hostLeaderMap, 1, plan, false);
        verifyLeaderBalancePlan(leaderParts, 1, 2);
    }
}

TEST(BalanceTest, ManyHostsLeaderBalancePlanTest) {
    fs::TempDir rootPath("/tmp/SimpleLeaderBalancePlanTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    FLAGS_expired_threshold_sec = 600;

    int partCount = 99999;
    int replica = 3;
    int hostCount = 100;
    std::vector<HostAddr> hosts;
    for (int i = 0; i < hostCount; i++) {
        hosts.emplace_back(i, i);
    }
    TestUtils::createSomeHosts(kv.get(), hosts);
    TestUtils::assembleSpace(kv.get(), 1, partCount, replica, hostCount);

    float avgLoad = static_cast<float>(partCount) / hostCount;
    int32_t minLoad = std::floor(avgLoad * (1 - FLAGS_leader_balance_deviation));
    int32_t maxLoad = std::ceil(avgLoad * (1 + FLAGS_leader_balance_deviation));

    std::unique_ptr<AdminClient> client(new AdminClient(kv.get()));
    std::unique_ptr<Balancer> balancer(new Balancer(kv.get(), std::move(client)));
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
        auto leaderParts = balancer->buildLeaderBalancePlan(&hostLeaderMap, 1, plan);
        verifyLeaderBalancePlan(leaderParts, minLoad, maxLoad);
    }
}

TEST(BalanceTest, LeaderBalanceTest) {
    fs::TempDir rootPath("/tmp/LeaderBalanceTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    std::vector<HostAddr> hosts = {{0, 0}, {1, 1}, {2, 2}};
    TestUtils::createSomeHosts(kv.get(), hosts);
    TestUtils::assembleSpace(kv.get(), 1, 9, 3, 3);
    {
        cpp2::SpaceProperties properties;
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
    }

    std::vector<Status> sts(9, Status::OK());
    std::unique_ptr<FaultInjector> injector(new TestFaultInjector(std::move(sts)));
    auto client = std::make_unique<AdminClient>(std::move(injector));

    Balancer balancer(kv.get(), std::move(client));
    auto ret = balancer.leaderBalance();
    ASSERT_EQ(ret, cpp2::ErrorCode::SUCCEEDED);
}

}  // namespace meta
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}


