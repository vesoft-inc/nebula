/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "base/Base.h"
#include <gtest/gtest.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/synchronization/Baton.h>
#include "meta/processors/admin/Balancer.h"
#include "meta/test/TestUtils.h"
#include "fs/TempDir.h"
#include "meta/processors/partsMan/CreateSpaceProcessor.h"

DECLARE_uint32(task_concurrency);
DECLARE_int32(expired_threshold_sec);

namespace nebula {
namespace meta {

class TestFaultInjector : public FaultInjector {
public:
    explicit TestFaultInjector(std::vector<Status> sts)
        : statusArray_(std::move(sts)) {
        executor_.reset(new folly::CPUThreadPoolExecutor(1));
    }

    ~TestFaultInjector() {
    }

    folly::Future<Status> response(int index) {
        folly::Promise<Status> pro;
        auto f = pro.getFuture();
        executor_->add([this, p = std::move(pro), index]() mutable {
            p.setValue(this->statusArray_[index]);
        });
        return f;
    }

    folly::Future<Status> transLeader() override {
        return response(0);
    }

    folly::Future<Status> addPart() override {
        return response(1);
    }

    folly::Future<Status> addLearner() override {
        return response(2);
    }

    folly::Future<Status> waitingForCatchUpData() override {
        return response(3);
    }

    folly::Future<Status> memberChange() override {
        return response(4);
    }

    folly::Future<Status> updateMeta() override {
        return response(5);
    }

    folly::Future<Status> removePart() override {
        return response(6);
    }

    void reset(std::vector<Status> sts) {
        statusArray_ = std::move(sts);
    }

private:
    std::vector<Status> statusArray_;
    std::unique_ptr<folly::Executor> executor_;
};

TEST(BalanceTaskTest, SimpleTest) {
    {
        std::vector<Status> sts(7, Status::OK());
        std::unique_ptr<FaultInjector> injector(new TestFaultInjector(std::move(sts)));
        auto client = std::make_unique<AdminClient>(std::move(injector));
        BalanceTask task(0, 0, 0, HostAddr(0, 0), HostAddr(1, 1), nullptr, nullptr);
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
                                Status::OK()};
        std::unique_ptr<FaultInjector> injector(new TestFaultInjector(std::move(sts)));
        auto client = std::make_unique<AdminClient>(std::move(injector));
        BalanceTask task(0, 0, 0, HostAddr(0, 0), HostAddr(1, 1), nullptr, nullptr);
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
    auto* balancer = Balancer::instance(nullptr);
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
            BalanceTask task(0, 0, 0, HostAddr(i, 0), HostAddr(i, 1), nullptr, nullptr);
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
            BalanceTask task(0, 0, i, HostAddr(i, 0), HostAddr(i, 1), nullptr, nullptr);
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
            BalanceTask task(0, 0, i, HostAddr(i, 0), HostAddr(i, 1), nullptr, nullptr);
            plan.addTask(std::move(task));
        }
        for (int i = 0; i < 10; i++) {
            BalanceTask task(0, 0, i, HostAddr(i, 2), HostAddr(i, 3), nullptr, nullptr);
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
        std::vector<Status> sts(7, Status::OK());
        std::unique_ptr<FaultInjector> injector(new TestFaultInjector(std::move(sts)));
        auto client = std::make_unique<AdminClient>(std::move(injector));

        for (int i = 0; i < 10; i++) {
            BalanceTask task(0, 0, 0, HostAddr(i, 0), HostAddr(i, 1), nullptr, nullptr);
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
        std::vector<Status> sts(7, Status::OK());
        std::unique_ptr<FaultInjector> injector(new TestFaultInjector(std::move(sts)));
        auto client = std::make_unique<AdminClient>(std::move(injector));

        for (int i = 0; i < 10; i++) {
            BalanceTask task(0, 0, i, HostAddr(i, 0), HostAddr(i, 1), nullptr, nullptr);
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
            std::vector<Status> sts(7, Status::OK());
            std::unique_ptr<FaultInjector> injector(new TestFaultInjector(std::move(sts)));
            client1 = std::make_unique<AdminClient>(std::move(injector));
            for (int i = 0; i < 9; i++) {
                BalanceTask task(0, 0, i, HostAddr(i, 0), HostAddr(i, 1), nullptr, nullptr);
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
                                Status::OK()};
            std::unique_ptr<FaultInjector> injector(new TestFaultInjector(std::move(sts)));
            client2 = std::make_unique<AdminClient>(std::move(injector));
            BalanceTask task(0, 0, 0, HostAddr(10, 0), HostAddr(10, 1), nullptr, nullptr);
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
    std::vector<Status> sts(7, Status::OK());
    std::unique_ptr<FaultInjector> injector(new TestFaultInjector(std::move(sts)));
    auto client = std::make_unique<AdminClient>(std::move(injector));
    Balancer balancer(kv.get(), std::move(client));
    auto ret = balancer.balance();
    CHECK_EQ(Status::Error("No tasks"), ret.status());

    sleep(1);
    LOG(INFO) << "Now, we lost host " << HostAddr(3, 3);
    TestUtils::registerHB(kv.get(), {{0, 0}, {1, 1}, {2, 2}});
    ret = balancer.balance();
    CHECK(ret.ok());
    auto balanceId = ret.value();
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
                                Status::OK()};

    std::unique_ptr<FaultInjector> injector(new TestFaultInjector(std::move(sts)));
    auto client = std::make_unique<AdminClient>(std::move(injector));
    Balancer balancer(kv.get(), std::move(client));
    auto ret = balancer.balance();
    CHECK(ret.ok());
    auto balanceId = ret.value();
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
    LOG(INFO) << "Now let's recovery it.";
    std::vector<Status> normalSts(7, Status::OK());
    static_cast<TestFaultInjector*>(balancer.client_->faultInjector())->reset(std::move(normalSts));
    ret = balancer.balance();
    CHECK(ret.ok());
    balanceId = ret.value();
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
                ASSERT_EQ(BalanceTask::Status::END, task.status_);
                task.ret_ = std::get<1>(tup);
                ASSERT_EQ(BalanceTask::Result::SUCCEEDED, task.ret_);
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

}  // namespace meta
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}


