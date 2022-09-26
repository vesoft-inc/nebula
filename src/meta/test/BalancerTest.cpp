/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/synchronization/Baton.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "meta/processors/job/BalanceJobExecutor.h"
#include "meta/processors/job/DataBalanceJobExecutor.h"
#include "meta/processors/job/LeaderBalanceJobExecutor.h"
#include "meta/processors/job/ZoneBalanceJobExecutor.h"
#include "meta/processors/parts/CreateSpaceProcessor.h"
#include "meta/test/MockAdminClient.h"
#include "meta/test/TestUtils.h"

DECLARE_uint32(task_concurrency);
DECLARE_int32(heartbeat_interval_secs);
DECLARE_uint32(expired_time_factor);
DECLARE_double(leader_balance_deviation);

namespace nebula {
namespace meta {

using ::testing::_;
using ::testing::AtLeast;
using ::testing::ByMove;
using ::testing::DefaultValue;
using ::testing::NaggyMock;
using ::testing::NiceMock;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::StrictMock;

std::atomic<int32_t> testJobId = 1;

TEST(BalanceTest, BalanceTaskTest) {
  fs::TempDir rootPath("/tmp/SimpleTest.XXXXXX");
  auto store = MockCluster::initMetaKV(rootPath.path());
  auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
  HostAddr src("0", 0);
  HostAddr dst("1", 1);
  TestUtils::createSomeHosts(kv, {src, dst});

  DefaultValue<folly::Future<Status>>::SetFactory(
      [] { return folly::Future<Status>(Status::OK()); });
  GraphSpaceID space = 0;
  {
    StrictMock<MockAdminClient> client;
    EXPECT_CALL(client, checkPeers(space, 0)).Times(2);
    EXPECT_CALL(client, transLeader(space, 0, src, _)).Times(1);
    EXPECT_CALL(client, addPart(space, 0, dst, true)).Times(1);
    EXPECT_CALL(client, addLearner(space, 0, dst)).Times(1);
    EXPECT_CALL(client, waitingForCatchUpData(space, 0, dst)).Times(1);
    EXPECT_CALL(client, memberChange(space, 0, dst, true)).Times(1);
    EXPECT_CALL(client, memberChange(space, 0, src, false)).Times(1);
    EXPECT_CALL(client, updateMeta(space, 0, src, dst)).Times(1);
    EXPECT_CALL(client, removePart(space, 0, src)).Times(1);

    folly::Baton<true, std::atomic> b;
    BalanceTask task(
        testJobId.fetch_add(1, std::memory_order_relaxed), space, 0, src, dst, kv, &client);
    task.onFinished_ = [&]() {
      LOG(INFO) << "Task finished!";
      EXPECT_EQ(BalanceTaskResult::SUCCEEDED, task.ret_);
      EXPECT_EQ(BalanceTaskStatus::END, task.status_);
      b.post();
    };
    task.onError_ = []() { LOG(FATAL) << "We should not reach here!"; };
    task.invoke();
    b.wait();
  }
  {
    NiceMock<MockAdminClient> client;
    EXPECT_CALL(client, transLeader(_, _, _, _))
        .Times(1)
        .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("Transfer failed")))));

    folly::Baton<true, std::atomic> b;
    BalanceTask task(
        testJobId.fetch_add(1, std::memory_order_relaxed), space, 0, src, dst, kv, &client);
    task.onFinished_ = []() { LOG(FATAL) << "We should not reach here!"; };
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

SpaceInfo createSpaceInfo(
    const std::string& name,
    GraphSpaceID spaceId,
    int32_t replica,
    const std::vector<
        std::pair<std::string, std::vector<std::pair<HostAddr, std::vector<PartitionID>>>>>&
        zones) {
  SpaceInfo spaceInfo;
  spaceInfo.name_ = name;
  spaceInfo.spaceId_ = spaceId;
  spaceInfo.replica_ = replica;
  for (const auto& z : zones) {
    Zone zone(z.first);
    for (const auto& h : z.second) {
      Host host(h.first);
      for (const auto& p : h.second) {
        host.parts_.insert(p);
      }
      zone.hosts_.emplace(host.host_, host);
    }
    spaceInfo.zones_.emplace(zone.zoneName_, zone);
  }
  return spaceInfo;
}

void checkZoneAvg(const Zone& zone) {
  int32_t avg = zone.partNum_ / zone.hosts_.size();
  for (const auto& p : zone.hosts_) {
    EXPECT_EQ(p.second.parts_.size() - avg <= 1, true);
  }
}

void checkConflic(const Zone& zone) {
  int32_t totalNum = 0;
  for (const auto& p : zone.hosts_) {
    totalNum += p.second.parts_.size();
  }
  EXPECT_EQ(totalNum, zone.partNum_);
}

TEST(BalanceTest, RemoveZonePlanTest) {
  fs::TempDir rootPath("/tmp/RemoveZoneTest.XXXXXX");
  std::unique_ptr<kvstore::NebulaStore> store = MockCluster::initMetaKV(rootPath.path());
  SpaceInfo spaceInfo = createSpaceInfo(
      "space1",
      1,
      3,
      {{"zone1",
        {{{"127.0.0.1", 11}, {5}},
         {{"127.0.0.1", 12}, {10, 15}},
         {{"127.0.0.1", 13}, {12, 13, 14}}}},
       {"zone2",
        {{{"127.0.0.1", 21}, {3, 4}}, {{"127.0.0.1", 22}, {8}}, {{"127.0.0.1", 23}, {15}}}},
       {"zone3",
        {{{"127.0.0.1", 31}, {1, 2}},
         {{"127.0.0.1", 32}, {6, 7, 8, 9, 10}},
         {{"127.0.0.1", 33}, {11, 12}}}},
       {"zone4",
        {{{"127.0.0.1", 41}, {1, 2, 3}},
         {{"127.0.0.1", 42}, {6, 7, 11}},
         {{"127.0.0.1", 43}, {12, 13, 14}}}},
       {"zone5",
        {{{"127.0.0.1", 51}, {3, 4, 5}},
         {{"127.0.0.1", 52}, {9, 10, 11}},
         {{"127.0.0.1", 53}, {13, 14, 15}}}},
       {"zone6", {{{"127.0.0.1", 61}, {4}}, {{"127.0.0.1", 62}, {8, 9}}}},
       {"zone7",
        {{{"127.0.0.1", 71}, {1, 2}}, {{"127.0.0.1", 72}, {6, 7}}, {{"127.0.0.1", 73}, {5}}}}});
  ZoneBalanceJobExecutor balancer(JobDescription(), store.get(), nullptr, {});
  balancer.lostZones_ = {"zone6", "zone7"};
  balancer.spaceInfo_ = spaceInfo;
  Status status = balancer.buildBalancePlan();
  EXPECT_EQ(status, Status::OK());
  checkZoneAvg(balancer.spaceInfo_.zones_["zone1"]);
  checkZoneAvg(balancer.spaceInfo_.zones_["zone2"]);

  EXPECT_EQ(balancer.spaceInfo_.zones_["zone1"].partNum_, 9);
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone2"].partNum_, 9);
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone3"].partNum_, 9);
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone4"].partNum_, 9);
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone5"].partNum_, 9);
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone6"].partNum_, 0);
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone7"].partNum_, 0);
  checkConflic(balancer.spaceInfo_.zones_["zone1"]);
  checkConflic(balancer.spaceInfo_.zones_["zone2"]);
  checkConflic(balancer.spaceInfo_.zones_["zone3"]);
  checkConflic(balancer.spaceInfo_.zones_["zone4"]);
  checkConflic(balancer.spaceInfo_.zones_["zone5"]);
  checkConflic(balancer.spaceInfo_.zones_["zone6"]);
  checkConflic(balancer.spaceInfo_.zones_["zone7"]);
}

TEST(BalanceTest, BalanceZonePlanTest) {
  fs::TempDir rootPath("/tmp/BalanceZoneTest.XXXXXX");
  std::unique_ptr<kvstore::NebulaStore> store = MockCluster::initMetaKV(rootPath.path());
  SpaceInfo spaceInfo = createSpaceInfo(
      "space1",
      1,
      3,
      {
          {"zone1",
           {{{"127.0.0.1", 11}, {5}},
            {{"127.0.0.1", 12}, {10, 15}},
            {{"127.0.0.1", 13}, {12, 13, 14}}}},
          {"zone2",
           {{{"127.0.0.1", 21}, {3, 4}}, {{"127.0.0.1", 22}, {8}}, {{"127.0.0.1", 23}, {15}}}},
          {"zone3",
           {{{"127.0.0.1", 31}, {1, 2}},
            {{"127.0.0.1", 32}, {6, 7, 8, 9, 10}},
            {{"127.0.0.1", 33}, {11, 12}}}},
          {"zone4",
           {{{"127.0.0.1", 41}, {1, 2, 3, 4, 5}},
            {{"127.0.0.1", 42}, {6, 7, 8, 9, 11}},
            {{"127.0.0.1", 43}, {12, 13, 14}}}},
          {"zone5",
           {{{"127.0.0.1", 51}, {1, 2, 3, 4, 5}},
            {{"127.0.0.1", 52}, {6, 7, 9, 10, 11}},
            {{"127.0.0.1", 53}, {13, 14, 15}}}},
      });
  ZoneBalanceJobExecutor balancer(JobDescription(), store.get(), nullptr, {});
  balancer.spaceInfo_ = spaceInfo;
  Status status = balancer.buildBalancePlan();
  EXPECT_EQ(status, Status::OK());
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone1"].partNum_, 9);
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone2"].partNum_, 9);
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone3"].partNum_, 9);
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone4"].partNum_, 9);
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone5"].partNum_, 9);
  checkZoneAvg(balancer.spaceInfo_.zones_["zone1"]);
  checkZoneAvg(balancer.spaceInfo_.zones_["zone2"]);
  checkZoneAvg(balancer.spaceInfo_.zones_["zone4"]);
  checkZoneAvg(balancer.spaceInfo_.zones_["zone5"]);
  checkConflic(balancer.spaceInfo_.zones_["zone1"]);
  checkConflic(balancer.spaceInfo_.zones_["zone2"]);
  checkConflic(balancer.spaceInfo_.zones_["zone3"]);
  checkConflic(balancer.spaceInfo_.zones_["zone4"]);
  checkConflic(balancer.spaceInfo_.zones_["zone5"]);
  balancer.lostZones_ = {"zone4", "zone5"};
  status = balancer.buildBalancePlan();
  EXPECT_EQ(status, Status::OK());
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone1"].partNum_, 15);
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone2"].partNum_, 15);
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone3"].partNum_, 15);
  balancer.lostZones_ = {};
  status = balancer.buildBalancePlan();
  EXPECT_EQ(status, Status::OK());
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone1"].partNum_, 9);
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone2"].partNum_, 9);
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone3"].partNum_, 9);
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone4"].partNum_, 9);
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone5"].partNum_, 9);
}

TEST(BalanceTest, BalanceZonePlanComplexTest) {
  fs::TempDir rootPath("/tmp/BalanceZoneTest.XXXXXX");
  std::unique_ptr<kvstore::NebulaStore> store = MockCluster::initMetaKV(rootPath.path());
  std::pair<std::string, std::vector<std::pair<HostAddr, std::vector<PartitionID>>>> pair1(
      "z1", {{{"127.0.0.1", 11}, {}}});
  std::pair<std::string, std::vector<std::pair<HostAddr, std::vector<PartitionID>>>> pair2(
      "z2", {{{"127.0.0.1", 12}, {}}});
  std::pair<std::string, std::vector<std::pair<HostAddr, std::vector<PartitionID>>>> pair3(
      "z3", {{{"127.0.0.1", 13}, {}}});
  std::pair<std::string, std::vector<std::pair<HostAddr, std::vector<PartitionID>>>> pair4(
      "z4", {{{"127.0.0.1", 14}, {}}});
  std::pair<std::string, std::vector<std::pair<HostAddr, std::vector<PartitionID>>>> pair5(
      "z5", {{{"127.0.0.1", 15}, {}}});
  for (int32_t i = 1; i <= 512; i++) {
    pair1.second.front().second.push_back(i);
    pair2.second.front().second.push_back(i);
    pair3.second.front().second.push_back(i);
  }
  SpaceInfo spaceInfo = createSpaceInfo("space1", 1, 3, {pair1, pair2, pair3, pair4, pair5});
  ZoneBalanceJobExecutor balancer(JobDescription(), store.get(), nullptr, {});
  balancer.spaceInfo_ = spaceInfo;
  Status status = balancer.buildBalancePlan();
  EXPECT_EQ(status, Status::OK());
  EXPECT_EQ(balancer.spaceInfo_.zones_["z1"].partNum_, 308);
  EXPECT_EQ(balancer.spaceInfo_.zones_["z2"].partNum_, 307);
  EXPECT_EQ(balancer.spaceInfo_.zones_["z3"].partNum_, 307);
  EXPECT_EQ(balancer.spaceInfo_.zones_["z4"].partNum_, 307);
  EXPECT_EQ(balancer.spaceInfo_.zones_["z5"].partNum_, 307);
  balancer.lostZones_ = {"z1"};
  status = balancer.buildBalancePlan();
  EXPECT_EQ(status, Status::OK());
  EXPECT_EQ(balancer.plan_->tasks().size(), 389);
  auto tasks = balancer.plan_->tasks();
  EXPECT_EQ(balancer.spaceInfo_.zones_["z2"].partNum_, 384);
  EXPECT_EQ(balancer.spaceInfo_.zones_["z3"].partNum_, 384);
  EXPECT_EQ(balancer.spaceInfo_.zones_["z4"].partNum_, 384);
  EXPECT_EQ(balancer.spaceInfo_.zones_["z5"].partNum_, 384);
}

TEST(BalanceTest, BalanceZoneRemainderPlanTest) {
  fs::TempDir rootPath("/tmp/BalanceZoneTest.XXXXXX");
  std::unique_ptr<kvstore::NebulaStore> store = MockCluster::initMetaKV(rootPath.path());
  SpaceInfo spaceInfo = createSpaceInfo(
      "space1",
      1,
      3,
      {
          {"zone1",
           {{{"127.0.0.1", 11}, {5}},
            {{"127.0.0.1", 12}, {10, 15}},
            {{"127.0.0.1", 13}, {12, 13, 14}}}},
          {"zone2",
           {{{"127.0.0.1", 21}, {3, 4}}, {{"127.0.0.1", 22}, {8, 16}}, {{"127.0.0.1", 23}, {15}}}},
          {"zone3",
           {{{"127.0.0.1", 31}, {1, 2}},
            {{"127.0.0.1", 32}, {6, 7, 8, 9, 10}},
            {{"127.0.0.1", 33}, {11, 12}}}},
          {"zone4",
           {{{"127.0.0.1", 41}, {1, 2, 3, 4, 5}},
            {{"127.0.0.1", 42}, {6, 7, 8, 9, 11}},
            {{"127.0.0.1", 43}, {12, 13, 14, 16}}}},
          {"zone5",
           {{{"127.0.0.1", 51}, {1, 2, 3, 4, 5}},
            {{"127.0.0.1", 52}, {6, 7, 9, 10, 11}},
            {{"127.0.0.1", 53}, {13, 14, 15, 16}}}},
      });
  ZoneBalanceJobExecutor balancer(JobDescription(), store.get(), nullptr, {});
  balancer.spaceInfo_ = spaceInfo;
  Status status = balancer.buildBalancePlan();
  EXPECT_EQ(status, Status::OK());
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone1"].partNum_, 9);
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone2"].partNum_, 10);
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone3"].partNum_, 9);
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone4"].partNum_, 10);
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone5"].partNum_, 10);
  checkZoneAvg(balancer.spaceInfo_.zones_["zone1"]);
  checkZoneAvg(balancer.spaceInfo_.zones_["zone2"]);
  checkZoneAvg(balancer.spaceInfo_.zones_["zone4"]);
  checkZoneAvg(balancer.spaceInfo_.zones_["zone5"]);
  checkConflic(balancer.spaceInfo_.zones_["zone1"]);
  checkConflic(balancer.spaceInfo_.zones_["zone2"]);
  checkConflic(balancer.spaceInfo_.zones_["zone3"]);
  checkConflic(balancer.spaceInfo_.zones_["zone4"]);
  checkConflic(balancer.spaceInfo_.zones_["zone5"]);

  spaceInfo = createSpaceInfo(
      "space1",
      1,
      3,
      {
          {"zone1",
           {{{"127.0.0.1", 11}, {5}},
            {{"127.0.0.1", 12}, {10, 15}},
            {{"127.0.0.1", 13}, {12, 13, 14}}}},
          {"zone2",
           {{{"127.0.0.1", 21}, {3, 4}}, {{"127.0.0.1", 22}, {8}}, {{"127.0.0.1", 23}, {15}}}},
          {"zone3",
           {{{"127.0.0.1", 31}, {1, 2}},
            {{"127.0.0.1", 32}, {6, 7, 8, 9, 10}},
            {{"127.0.0.1", 33}, {11, 12, 16}}}},
          {"zone4",
           {{{"127.0.0.1", 41}, {1, 2, 3, 4, 5}},
            {{"127.0.0.1", 42}, {6, 7, 8, 9, 11}},
            {{"127.0.0.1", 43}, {12, 13, 14, 16}}}},
          {"zone5",
           {{{"127.0.0.1", 51}, {1, 2, 3, 4, 5}},
            {{"127.0.0.1", 52}, {6, 7, 9, 10, 11}},
            {{"127.0.0.1", 53}, {13, 14, 15, 16}}}},
      });
  balancer.spaceInfo_ = spaceInfo;
  status = balancer.buildBalancePlan();
  EXPECT_EQ(status, Status::OK());
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone1"].partNum_, 9);
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone2"].partNum_, 9);
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone3"].partNum_, 10);
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone4"].partNum_, 10);
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone5"].partNum_, 10);
  checkZoneAvg(balancer.spaceInfo_.zones_["zone1"]);
  checkZoneAvg(balancer.spaceInfo_.zones_["zone2"]);
  checkZoneAvg(balancer.spaceInfo_.zones_["zone4"]);
  checkZoneAvg(balancer.spaceInfo_.zones_["zone5"]);
  checkConflic(balancer.spaceInfo_.zones_["zone1"]);
  checkConflic(balancer.spaceInfo_.zones_["zone2"]);
  checkConflic(balancer.spaceInfo_.zones_["zone3"]);
  checkConflic(balancer.spaceInfo_.zones_["zone4"]);
  checkConflic(balancer.spaceInfo_.zones_["zone5"]);
  status = balancer.buildBalancePlan();
  EXPECT_EQ(status, Status::Balanced());
}

TEST(BalanceTest, BalanceDataPlanTest) {
  fs::TempDir rootPath("/tmp/BalanceZoneTest.XXXXXX");
  std::unique_ptr<kvstore::NebulaStore> store = MockCluster::initMetaKV(rootPath.path());
  SpaceInfo spaceInfo = createSpaceInfo(
      "space1",
      1,
      3,
      {
          {"zone1",
           {{{"127.0.0.1", 11}, {1, 2, 3, 53, 54}},
            {{"127.0.0.1", 12}, {4, 5}},
            {{"127.0.0.1", 13}, {6, 7, 8, 9, 10, 11, 12, 13, 14, 15}},
            {{"127.0.0.1", 14}, {16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30}},
            {{"127.0.0.1", 15}, {31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
                                 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52}}}},
      });
  DataBalanceJobExecutor balancer(JobDescription(), store.get(), nullptr, {});
  balancer.spaceInfo_ = spaceInfo;
  Status status = balancer.buildBalancePlan();
  EXPECT_EQ(status, Status::OK());
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone1"].hosts_[HostAddr("127.0.0.1", 11)].parts_.size(),
            11);
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone1"].hosts_[HostAddr("127.0.0.1", 12)].parts_.size(),
            11);
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone1"].hosts_[HostAddr("127.0.0.1", 13)].parts_.size(),
            10);
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone1"].hosts_[HostAddr("127.0.0.1", 14)].parts_.size(),
            11);
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone1"].hosts_[HostAddr("127.0.0.1", 15)].parts_.size(),
            11);

  spaceInfo = createSpaceInfo("space1",
                              1,
                              3,
                              {{"zone1",
                                {{{"127.0.0.1", 11}, {5, 6, 7, 8, 9, 10}},
                                 {{"127.0.0.1", 12}, {11, 12, 13, 17, 18, 19, 20}},
                                 {{"127.0.0.1", 13}, {21, 22, 23, 28, 29, 30}},
                                 {{"127.0.0.1", 14}, {31, 32, 33, 34, 35, 36, 37, 38, 39, 40}},
                                 {{"127.0.0.1", 15}, {41, 42, 43, 44, 45, 46, 47, 48, 49, 50}},
                                 {{"127.0.0.1", 16}, {51, 52, 53, 54, 14, 15, 16}},
                                 {{"127.0.0.1", 17}, {1, 2, 3, 4, 24, 25, 26, 27}}}}});
  balancer.spaceInfo_ = spaceInfo;
  balancer.lostHosts_ = {{"127.0.0.1", 16}, {"127.0.0.1", 17}};
  status = balancer.buildBalancePlan();
  EXPECT_EQ(status, Status::OK());
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone1"].hosts_[HostAddr("127.0.0.1", 11)].parts_.size(),
            11);
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone1"].hosts_[HostAddr("127.0.0.1", 12)].parts_.size(),
            11);
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone1"].hosts_[HostAddr("127.0.0.1", 13)].parts_.size(),
            11);
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone1"].hosts_[HostAddr("127.0.0.1", 14)].parts_.size(),
            11);
  EXPECT_EQ(balancer.spaceInfo_.zones_["zone1"].hosts_[HostAddr("127.0.0.1", 15)].parts_.size(),
            10);
  status = balancer.buildBalancePlan();
  EXPECT_EQ(status, Status::Balanced());
}

void showHostLoading(kvstore::KVStore* kv, GraphSpaceID spaceId) {
  auto prefix = MetaKeyUtils::partPrefix(spaceId);
  std::unique_ptr<kvstore::KVIterator> iter;
  auto ret = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
  ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
  HostParts hostPart;
  while (iter->valid()) {
    auto key = iter->key();
    PartitionID partId;
    memcpy(&partId, key.data() + prefix.size(), sizeof(PartitionID));
    auto hs = MetaKeyUtils::parsePartVal(iter->val());
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

TEST(BalanceTest, DispatchTasksTest) {
  GraphSpaceID space = 0;
  {
    FLAGS_task_concurrency = 10;
    JobDescription jd(
        space, testJobId.fetch_add(1, std::memory_order_relaxed), cpp2::JobType::DATA_BALANCE, {});
    BalancePlan plan(jd, nullptr, nullptr);
    for (int i = 0; i < 20; i++) {
      BalanceTask task(0,
                       space,
                       0,
                       HostAddr(std::to_string(i), 0),
                       HostAddr(std::to_string(i), 1),
                       nullptr,
                       nullptr);
      plan.addTask(std::move(task));
    }
    plan.dispatchTasks();
    // All tasks is about space 1, part 0.
    // So they will be dispatched into the same bucket.
    ASSERT_EQ(1, plan.buckets_.size());
    ASSERT_EQ(20, plan.buckets_[0].size());
  }
  {
    FLAGS_task_concurrency = 10;
    JobDescription jd(
        space, testJobId.fetch_add(1, std::memory_order_relaxed), cpp2::JobType::DATA_BALANCE, {});
    BalancePlan plan(jd, nullptr, nullptr);
    for (int i = 0; i < 5; i++) {
      BalanceTask task(0,
                       space,
                       i,
                       HostAddr(std::to_string(i), 0),
                       HostAddr(std::to_string(i), 1),
                       nullptr,
                       nullptr);
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
    JobDescription jd(
        space, testJobId.fetch_add(1, std::memory_order_relaxed), cpp2::JobType::DATA_BALANCE, {});
    BalancePlan plan(jd, nullptr, nullptr);
    for (int i = 0; i < 5; i++) {
      BalanceTask task(0,
                       space,
                       i,
                       HostAddr(std::to_string(i), 0),
                       HostAddr(std::to_string(i), 1),
                       nullptr,
                       nullptr);
      plan.addTask(std::move(task));
    }
    for (int i = 0; i < 10; i++) {
      BalanceTask task(0,
                       space,
                       i,
                       HostAddr(std::to_string(i), 2),
                       HostAddr(std::to_string(i), 3),
                       nullptr,
                       nullptr);
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
  GraphSpaceID space = 0;

  DefaultValue<folly::Future<Status>>::SetFactory(
      [] { return folly::Future<Status>(Status::OK()); });
  {
    LOG(INFO) << "Test with all tasks succeeded, only one bucket!";
    NiceMock<MockAdminClient> client;
    JobDescription jd(
        space, testJobId.fetch_add(1, std::memory_order_relaxed), cpp2::JobType::DATA_BALANCE, {});
    BalancePlan plan(jd, kv, &client);
    TestUtils::createSomeHosts(kv, hosts);
    TestUtils::registerHB(kv, hosts);

    for (int i = 0; i < 10; i++) {
      BalanceTask task(plan.id(),
                       space,
                       0,
                       HostAddr(std::to_string(i), 0),
                       HostAddr(std::to_string(i), 1),
                       kv,
                       &client);
      plan.addTask(std::move(task));
    }

    auto code = plan.invoke().get();
    ASSERT_EQ(code, meta::cpp2::JobStatus::FINISHED);

    // All tasks is about space 0, part 0.
    // So they will be dispatched into the same bucket.
    ASSERT_EQ(1, plan.buckets_.size());
    ASSERT_EQ(10, plan.buckets_[0].size());
  }
  {
    LOG(INFO) << "Test with all tasks succeeded, 10 buckets!";
    NiceMock<MockAdminClient> client;
    JobDescription jd(
        space, testJobId.fetch_add(1, std::memory_order_relaxed), cpp2::JobType::DATA_BALANCE, {});
    BalancePlan plan(jd, kv, &client);
    TestUtils::registerHB(kv, hosts);

    for (int i = 0; i < 10; i++) {
      BalanceTask task(plan.id(),
                       space,
                       i,
                       HostAddr(std::to_string(i), 0),
                       HostAddr(std::to_string(i), 1),
                       kv,
                       &client);
      plan.addTask(std::move(task));
    }

    auto code = plan.invoke().get();
    ASSERT_EQ(code, meta::cpp2::JobStatus::FINISHED);

    // All tasks is about different parts.
    // So they will be dispatched into different buckets.
    ASSERT_EQ(10, plan.buckets_.size());
    for (auto i = 0; i < 10; i++) {
      ASSERT_EQ(1, plan.buckets_[1].size());
    }
  }
  {
    LOG(INFO) << "Test with one task failed, 10 buckets";
    JobDescription jd(
        space, testJobId.fetch_add(1, std::memory_order_relaxed), cpp2::JobType::DATA_BALANCE, {});
    BalancePlan plan(jd, kv, nullptr);
    NiceMock<MockAdminClient> client1, client2;
    {
      for (int i = 0; i < 9; i++) {
        BalanceTask task(plan.id(),
                         space,
                         i,
                         HostAddr(std::to_string(i), 0),
                         HostAddr(std::to_string(i), 1),
                         kv,
                         &client1);
        plan.addTask(std::move(task));
      }
    }
    {
      EXPECT_CALL(client2, transLeader(_, _, _, _))
          .Times(1)
          .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("Transfer failed")))));
      BalanceTask task(plan.id(), space, 9, HostAddr("9", 0), HostAddr("9", 1), kv, &client2);
      plan.addTask(std::move(task));
    }
    TestUtils::registerHB(kv, hosts);

    auto code = plan.invoke().get();
    ASSERT_EQ(code, meta::cpp2::JobStatus::FAILED);
  }
}

void verifyBalanceTask(kvstore::KVStore* kv,
                       JobID jobId,
                       BalanceTaskStatus status,
                       BalanceTaskResult result,
                       std::unordered_map<HostAddr, int32_t>& partCount,
                       int32_t exceptNumber = 0) {
  const auto& prefix = MetaKeyUtils::balanceTaskPrefix(jobId);
  std::unique_ptr<kvstore::KVIterator> iter;
  auto code = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
  EXPECT_EQ(code, nebula::cpp2::ErrorCode::SUCCEEDED);
  int32_t num = 0;
  while (iter->valid()) {
    auto keyTuple = MetaKeyUtils::parseBalanceTaskKey(iter->key());
    EXPECT_EQ(jobId, std::get<0>(keyTuple));
    EXPECT_EQ(1, std::get<1>(keyTuple));
    partCount[std::get<3>(keyTuple)]--;
    partCount[std::get<4>(keyTuple)]++;
    auto valueTuple = MetaKeyUtils::parseBalanceTaskVal(iter->val());
    EXPECT_EQ(status, std::get<0>(valueTuple));
    EXPECT_EQ(result, std::get<1>(valueTuple));
    EXPECT_LT(0, std::get<2>(valueTuple));
    if (result != BalanceTaskResult::IN_PROGRESS) {
      EXPECT_LT(0, std::get<3>(valueTuple));
    }
    num++;
    iter->next();
  }
  if (exceptNumber != 0) {
    EXPECT_EQ(exceptNumber, num);
  }
}

void verifyMetaZone(kvstore::KVStore* kv,
                    GraphSpaceID spaceId,
                    const std::vector<std::string>& zones) {
  std::string spaceKey = MetaKeyUtils::spaceKey(spaceId);
  std::string spaceVal;
  kv->get(kDefaultSpaceId, kDefaultPartId, spaceKey, &spaceVal);
  meta::cpp2::SpaceDesc properties = MetaKeyUtils::parseSpace(spaceVal);
  const std::vector<std::string>& zns = properties.get_zone_names();
  std::set<std::string> zoneSet;
  for (const std::string& zoneName : zns) {
    zoneSet.emplace(zoneName);
  }
  std::set<std::string> expectZones;
  for (const std::string& zoneName : zones) {
    expectZones.emplace(zoneName);
  }
  EXPECT_EQ(zoneSet, expectZones);
}

void verifyZonePartNum(kvstore::KVStore* kv,
                       GraphSpaceID spaceId,
                       const std::map<std::string, int32_t>& zones) {
  std::map<HostAddr, std::string> hostZone;
  std::map<std::string, int32_t> zoneNum;
  std::unique_ptr<kvstore::KVIterator> iter;
  for (const auto& [zone, num] : zones) {
    std::string zoneKey = MetaKeyUtils::zoneKey(zone);
    std::string value;
    kv->get(kDefaultSpaceId, kDefaultPartId, zoneKey, &value);
    auto hosts = MetaKeyUtils::parseZoneHosts(value);
    for (auto& host : hosts) {
      hostZone[host] = zone;
    }
    zoneNum[zone] = 0;
  }
  const auto& partPrefix = MetaKeyUtils::partPrefix(spaceId);
  kv->prefix(kDefaultSpaceId, kDefaultPartId, partPrefix, &iter);
  while (iter->valid()) {
    auto hosts = MetaKeyUtils::parsePartVal(iter->val());
    for (auto& host : hosts) {
      zoneNum[hostZone[host]]++;
    }
    iter->next();
  }
  EXPECT_EQ(zoneNum, zones);
}

JobDescription makeJobDescription(GraphSpaceID space, kvstore::KVStore* kv, cpp2::JobType jobType) {
  JobDescription jd(space, testJobId.fetch_add(1, std::memory_order_relaxed), jobType, {});
  std::vector<nebula::kvstore::KV> data;
  auto jobKey = MetaKeyUtils::jobKey(jd.getSpace(), jd.getJobId());
  auto jobVal = MetaKeyUtils::jobVal(jd.getJobType(),
                                     jd.getParas(),
                                     jd.getStatus(),
                                     jd.getStartTime(),
                                     jd.getStopTime(),
                                     jd.getErrorCode());
  data.emplace_back(jobKey, jobVal);
  folly::Baton<true, std::atomic> baton;
  kv->asyncMultiPut(0, 0, std::move(data), [&](nebula::cpp2::ErrorCode code) {
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
    baton.post();
  });
  baton.wait();
  return jd;
}

TEST(BalanceTest, NormalZoneTest) {
  fs::TempDir rootPath("/tmp/NormalZoneTest.XXXXXX");
  auto store = MockCluster::initMetaKV(rootPath.path());
  auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
  GraphSpaceID space = 1;
  FLAGS_heartbeat_interval_secs = 1;
  TestUtils::assembleSpaceWithZone(kv, 1, 8, 3, 8, 24);
  std::unordered_map<HostAddr, int32_t> partCount;

  DefaultValue<folly::Future<Status>>::SetFactory(
      [] { return folly::Future<Status>(Status::OK()); });
  NiceMock<MockAdminClient> client;

  JobDescription jd = makeJobDescription(space, kv, cpp2::JobType::ZONE_BALANCE);
  ZoneBalanceJobExecutor balancer(jd, kv, &client, {});
  balancer.spaceInfo_.loadInfo(space, kv);

  auto ret = balancer.executeInternal().get();
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
  balancer.finish();
  balancer.lostZones_ = {"5", "6", "7", "8"};
  ret = balancer.executeInternal().get();
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
  verifyBalanceTask(
      kv, balancer.jobId_, BalanceTaskStatus::END, BalanceTaskResult::SUCCEEDED, partCount, 12);
}

TEST(BalanceTest, NormalDataTest) {
  fs::TempDir rootPath("/tmp/NormalDataTest.XXXXXX");
  auto store = MockCluster::initMetaKV(rootPath.path());
  auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
  FLAGS_heartbeat_interval_secs = 1;
  GraphSpaceID space = 1;

  TestUtils::assembleSpaceWithZone(kv, space, 8, 3, 1, 8);
  std::unordered_map<HostAddr, int32_t> partCount;

  DefaultValue<folly::Future<Status>>::SetFactory(
      [] { return folly::Future<Status>(Status::OK()); });
  NiceMock<MockAdminClient> client;

  JobDescription jd = makeJobDescription(space, kv, cpp2::JobType::DATA_BALANCE);
  DataBalanceJobExecutor balancer(jd, kv, &client, {});
  balancer.spaceInfo_.loadInfo(space, kv);
  auto ret = balancer.executeInternal().get();
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
  balancer.finish();

  balancer.lostHosts_ = {{"127.0.0.1", 1}, {"127.0.0.1", 8}};
  ret = balancer.executeInternal().get();
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
  verifyBalanceTask(
      kv, balancer.jobId_, BalanceTaskStatus::END, BalanceTaskResult::SUCCEEDED, partCount, 6);
}

TEST(BalanceTest, RecoveryTest) {
  fs::TempDir rootPath("/tmp/TryToRecoveryTest.XXXXXX");
  auto store = MockCluster::initMetaKV(rootPath.path());
  auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
  GraphSpaceID space = 1;

  TestUtils::assembleSpaceWithZone(kv, space, 24, 1, 1, 8);
  DefaultValue<folly::Future<Status>>::SetFactory(
      [] { return folly::Future<Status>(Status::OK()); });
  NiceMock<MockAdminClient> client;
  EXPECT_CALL(client, waitingForCatchUpData(space, _, _))
      .Times(12)
      .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("catch up failed")))))
      .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("catch up failed")))))
      .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("catch up failed")))))
      .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("catch up failed")))))
      .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("catch up failed")))))
      .WillOnce(Return(ByMove(folly::Future<Status>(Status::Error("catch up failed")))));

  JobDescription jd = makeJobDescription(space, kv, cpp2::JobType::DATA_BALANCE);
  DataBalanceJobExecutor balancer(jd, kv, &client, {});
  balancer.spaceInfo_.loadInfo(space, kv);
  balancer.lostHosts_ = {{"127.0.0.1", 1}, {"127.0.0.1", 8}};

  auto ret = balancer.executeInternal().get();
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
  std::unordered_map<HostAddr, int32_t> partCount;
  verifyBalanceTask(kv,
                    balancer.jobId_,
                    BalanceTaskStatus::CATCH_UP_DATA,
                    BalanceTaskResult::FAILED,
                    partCount,
                    6);
  balancer.recovery();
  verifyBalanceTask(
      kv, balancer.jobId_, BalanceTaskStatus::START, BalanceTaskResult::IN_PROGRESS, partCount, 6);

  ret = balancer.executeInternal().get();
  verifyBalanceTask(
      kv, balancer.jobId_, BalanceTaskStatus::END, BalanceTaskResult::SUCCEEDED, partCount, 6);
}

TEST(BalanceTest, StopPlanTest) {
  fs::TempDir rootPath("/tmp/StopAndRecoverTest.XXXXXX");
  auto store = MockCluster::initMetaKV(rootPath.path());
  auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
  FLAGS_heartbeat_interval_secs = 1;
  GraphSpaceID space = 1;

  TestUtils::createSomeHosts(kv);
  TestUtils::assembleSpaceWithZone(kv, space, 24, 3, 5, 5);
  DefaultValue<folly::Future<Status>>::SetFactory(
      [] { return folly::Future<Status>(Status::OK()); });
  NiceMock<MockAdminClient> delayClient;
  EXPECT_CALL(delayClient, waitingForCatchUpData(_, _, _))
      .Times(8)
      .WillOnce(
          Return(ByMove(folly::makeFuture<Status>(Status::OK()).delayed(std::chrono::seconds(3)))))
      .WillOnce(
          Return(ByMove(folly::makeFuture<Status>(Status::OK()).delayed(std::chrono::seconds(3)))))
      .WillOnce(
          Return(ByMove(folly::makeFuture<Status>(Status::OK()).delayed(std::chrono::seconds(3)))))
      .WillOnce(
          Return(ByMove(folly::makeFuture<Status>(Status::OK()).delayed(std::chrono::seconds(3)))))
      .WillOnce(
          Return(ByMove(folly::makeFuture<Status>(Status::OK()).delayed(std::chrono::seconds(3)))))
      .WillOnce(
          Return(ByMove(folly::makeFuture<Status>(Status::OK()).delayed(std::chrono::seconds(3)))))
      .WillOnce(
          Return(ByMove(folly::makeFuture<Status>(Status::OK()).delayed(std::chrono::seconds(3)))))
      .WillOnce(
          Return(ByMove(folly::makeFuture<Status>(Status::OK()).delayed(std::chrono::seconds(3)))));
  FLAGS_task_concurrency = 8;
  JobDescription jd = makeJobDescription(space, kv, cpp2::JobType::DATA_BALANCE);
  ZoneBalanceJobExecutor balancer(jd, kv, &delayClient, {});
  balancer.spaceInfo_.loadInfo(space, kv);
  balancer.lostZones_ = {"4", "5"};

  auto ret = balancer.executeInternal();
  auto stopRet = balancer.stop();
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, stopRet);
  ret.wait();

  const auto& prefix = MetaKeyUtils::balanceTaskPrefix(balancer.jobId_);
  std::unique_ptr<kvstore::KVIterator> iter;
  auto retcode = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
  ASSERT_EQ(retcode, nebula::cpp2::ErrorCode::SUCCEEDED);
  int32_t taskEnded = 0;
  int32_t taskStopped = 0;
  int32_t invalid = 0;
  int32_t success = 0;
  int32_t progress = 0;
  while (iter->valid()) {
    BalanceTask task;
    {
      auto tup = MetaKeyUtils::parseBalanceTaskVal(iter->val());
      task.status_ = std::get<0>(tup);
      task.ret_ = std::get<1>(tup);
      task.startTimeMs_ = std::get<2>(tup);
      task.endTimeMs_ = std::get<3>(tup);

      if (task.ret_ == BalanceTaskResult::SUCCEEDED) {
        success++;
      } else if (task.ret_ == BalanceTaskResult::INVALID) {
        invalid++;
      } else if (task.ret_ == BalanceTaskResult::IN_PROGRESS) {
        progress++;
      }

      if (task.status_ == BalanceTaskStatus::END) {
        taskEnded++;
      } else {
        taskStopped++;
      }
    }
    iter->next();
  }
  EXPECT_EQ(8, taskEnded);
  EXPECT_EQ(22, taskStopped);
  EXPECT_EQ(22, invalid);
  EXPECT_EQ(8, success);
  EXPECT_EQ(0, progress);
}

void verifyLeaderBalancePlan(HostLeaderMap& hostLeaderMap,
                             const LeaderBalancePlan& plan,
                             size_t minLoad,
                             size_t maxLoad) {
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
  GraphSpaceID space = 1;

  // 9 partition in space 1, 3 replica, 3 hosts
  TestUtils::assembleSpace(kv, space, 9, 3, 3);

  DefaultValue<folly::Future<Status>>::SetFactory(
      [] { return folly::Future<Status>(Status::OK()); });
  NiceMock<MockAdminClient> client;

  JobDescription jobDesc(
      space, testJobId.fetch_add(1, std::memory_order_relaxed), cpp2::JobType::LEADER_BALANCE);
  LeaderBalanceJobExecutor balancer(jobDesc, kv, &client, {});

  {
    HostLeaderMap hostLeaderMap;
    hostLeaderMap[HostAddr("0", 0)][1] = {1, 2, 3, 4, 5};
    hostLeaderMap[HostAddr("1", 1)][1] = {6, 7, 8};
    hostLeaderMap[HostAddr("2", 2)][1] = {9};
    auto tempMap = hostLeaderMap;

    LeaderBalancePlan plan;
    auto leaderBalanceResult =
        balancer.buildLeaderBalancePlan(&hostLeaderMap, space, 3, false, plan, false);
    ASSERT_TRUE(nebula::ok(leaderBalanceResult) && nebula::value(leaderBalanceResult));
    verifyLeaderBalancePlan(hostLeaderMap, plan, 3, 3);

    // check two plan build are same
    LeaderBalancePlan tempPlan;
    auto tempLeaderBalanceResult =
        balancer.buildLeaderBalancePlan(&tempMap, space, 3, false, tempPlan, false);
    ASSERT_TRUE(nebula::ok(tempLeaderBalanceResult) && nebula::value(tempLeaderBalanceResult));
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
    auto leaderBalanceResult =
        balancer.buildLeaderBalancePlan(&hostLeaderMap, space, 3, false, plan, false);
    ASSERT_TRUE(nebula::ok(leaderBalanceResult) && nebula::value(leaderBalanceResult));
    verifyLeaderBalancePlan(hostLeaderMap, plan, 3, 3);
  }
  {
    HostLeaderMap hostLeaderMap;
    hostLeaderMap[HostAddr("0", 0)][1] = {};
    hostLeaderMap[HostAddr("1", 1)][1] = {};
    hostLeaderMap[HostAddr("2", 2)][1] = {1, 2, 3, 4, 5, 6, 7, 8, 9};

    LeaderBalancePlan plan;
    auto leaderBalanceResult =
        balancer.buildLeaderBalancePlan(&hostLeaderMap, space, 3, false, plan, false);
    ASSERT_TRUE(nebula::ok(leaderBalanceResult) && nebula::value(leaderBalanceResult));
    verifyLeaderBalancePlan(hostLeaderMap, plan, 3, 3);
  }
  {
    HostLeaderMap hostLeaderMap;
    hostLeaderMap[HostAddr("0", 0)][1] = {1, 2, 3};
    hostLeaderMap[HostAddr("1", 1)][1] = {4, 5, 6};
    hostLeaderMap[HostAddr("2", 2)][1] = {7, 8, 9};

    LeaderBalancePlan plan;
    auto leaderBalanceResult =
        balancer.buildLeaderBalancePlan(&hostLeaderMap, space, 3, false, plan, false);
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
  GraphSpaceID space = 1;

  // 7 partition in space 1, 3 replica, 6 hosts, so not all hosts have
  // intersection parts
  TestUtils::assembleSpace(kv, space, 7, 3, 6);

  DefaultValue<folly::Future<Status>>::SetFactory(
      [] { return folly::Future<Status>(Status::OK()); });
  NiceMock<MockAdminClient> client;

  JobDescription jobDesc(
      space, testJobId.fetch_add(1, std::memory_order_relaxed), cpp2::JobType::LEADER_BALANCE);
  LeaderBalanceJobExecutor balancer(jobDesc, kv, &client, {});

  {
    HostLeaderMap hostLeaderMap;
    hostLeaderMap[HostAddr("0", 0)][1] = {4, 5, 6};
    hostLeaderMap[HostAddr("1", 1)][1] = {};
    hostLeaderMap[HostAddr("2", 2)][1] = {};
    hostLeaderMap[HostAddr("3", 3)][1] = {1, 2, 3, 7};
    hostLeaderMap[HostAddr("4", 4)][1] = {};
    hostLeaderMap[HostAddr("5", 5)][1] = {};
    showHostLoading(kv, space);

    LeaderBalancePlan plan;
    auto leaderBalanceResult =
        balancer.buildLeaderBalancePlan(&hostLeaderMap, space, 3, false, plan, false);
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
    auto leaderBalanceResult =
        balancer.buildLeaderBalancePlan(&hostLeaderMap, space, 3, false, plan, false);
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
    auto leaderBalanceResult =
        balancer.buildLeaderBalancePlan(&hostLeaderMap, space, 3, false, plan, false);
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
    auto leaderBalanceResult =
        balancer.buildLeaderBalancePlan(&hostLeaderMap, space, 3, false, plan, false);
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
    auto leaderBalanceResult =
        balancer.buildLeaderBalancePlan(&hostLeaderMap, space, 3, false, plan, false);
    ASSERT_TRUE(nebula::ok(leaderBalanceResult) && nebula::value(leaderBalanceResult));
    verifyLeaderBalancePlan(hostLeaderMap, plan, 1, 2);
  }
}

TEST(BalanceTest, ManyHostsLeaderBalancePlanTest) {
  fs::TempDir rootPath("/tmp/SimpleLeaderBalancePlanTest.XXXXXX");
  auto store = MockCluster::initMetaKV(rootPath.path());
  auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
  FLAGS_heartbeat_interval_secs = 1;
  GraphSpaceID space = 1;

  int partCount = 99999;
  int replica = 3;
  int hostCount = 100;
  std::vector<HostAddr> hosts;
  for (int i = 0; i < hostCount; i++) {
    hosts.emplace_back(std::to_string(i), i);
  }
  TestUtils::createSomeHosts(kv, hosts);
  TestUtils::assembleSpace(kv, space, partCount, replica, hostCount);

  float avgLoad = static_cast<float>(partCount) / hostCount;
  int32_t minLoad = std::floor(avgLoad * (1 - FLAGS_leader_balance_deviation));
  int32_t maxLoad = std::ceil(avgLoad * (1 + FLAGS_leader_balance_deviation));

  DefaultValue<folly::Future<Status>>::SetFactory(
      [] { return folly::Future<Status>(Status::OK()); });
  NiceMock<MockAdminClient> client;

  JobDescription jobDesc(
      space, testJobId.fetch_add(1, std::memory_order_relaxed), cpp2::JobType::LEADER_BALANCE);
  LeaderBalanceJobExecutor balancer(jobDesc, kv, &client, {});

  // check several times if they are balanced
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
    auto leaderBalanceResult =
        balancer.buildLeaderBalancePlan(&hostLeaderMap, space, 3, false, plan);
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
  GraphSpaceID space = 1;
  TestUtils::assembleSpace(kv, space, 9, 3, 3);

  DefaultValue<folly::Future<Status>>::SetFactory(
      [] { return folly::Future<Status>(Status::OK()); });
  NiceMock<MockAdminClient> client;
  HostLeaderMap dist;
  dist[HostAddr("0", 0)][1] = {1, 2, 3, 4, 5};
  dist[HostAddr("1", 1)][1] = {6, 7, 8};
  dist[HostAddr("2", 2)][1] = {9};
  EXPECT_CALL(client, getLeaderDist(_))
      .WillOnce(testing::DoAll(SetArgPointee<0>(dist),
                               Return(ByMove(folly::Future<Status>(Status::OK())))));

  JobDescription jobDesc(
      space, testJobId.fetch_add(1, std::memory_order_relaxed), cpp2::JobType::LEADER_BALANCE);
  LeaderBalanceJobExecutor balancer(jobDesc, kv, &client, {});

  auto ret = balancer.executeInternal().get();
  EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
}

TEST(BalanceTest, LeaderBalanceWithZoneTest) {
  fs::TempDir rootPath("/tmp/LeaderBalanceWithZone.XXXXXX");
  auto store = MockCluster::initMetaKV(rootPath.path());
  auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
  FLAGS_heartbeat_interval_secs = 1;
  GraphSpaceID space = 0;

  std::vector<HostAddr> hosts;
  for (int i = 0; i < 9; i++) {
    hosts.emplace_back(std::to_string(i), i);
  }
  TestUtils::createSomeHosts(kv, hosts);
  TestUtils::registerHB(kv, hosts);

  // create zone and group
  {
    ZoneInfo zoneInfo = {{"zone_0", {HostAddr("0", 0), HostAddr("1", 1)}},
                         {"zone_1", {HostAddr("2", 2), HostAddr("3", 3)}},
                         {"zone_2", {HostAddr("4", 4), HostAddr("5", 5)}}};
    TestUtils::assembleZone(kv, zoneInfo);
  }
  {
    cpp2::SpaceDesc properties;
    properties.space_name_ref() = "default_space";
    properties.partition_num_ref() = 8;
    properties.replica_factor_ref() = 3;
    std::vector<std::string> zones = {"zone_0", "zone_1", "zone_2"};
    properties.zone_names_ref() = std::move(zones);
    cpp2::CreateSpaceReq req;
    req.properties_ref() = std::move(properties);
    auto* processor = CreateSpaceProcessor::instance(kv);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    space = resp.get_id().get_space_id();
    ASSERT_EQ(1, space);
  }

  sleep(1);
  DefaultValue<folly::Future<Status>>::SetFactory(
      [] { return folly::Future<Status>(Status::OK()); });
  NiceMock<MockAdminClient> client;
  JobDescription jobDesc(
      space, testJobId.fetch_add(1, std::memory_order_relaxed), cpp2::JobType::LEADER_BALANCE);
  LeaderBalanceJobExecutor balancer(jobDesc, kv, &client, {});

  {
    HostLeaderMap hostLeaderMap;
    hostLeaderMap[HostAddr("0", 0)][1] = {1, 3, 5, 7};
    hostLeaderMap[HostAddr("1", 1)][1] = {2, 4, 6, 8};
    hostLeaderMap[HostAddr("2", 2)][1] = {};
    hostLeaderMap[HostAddr("3", 3)][1] = {};
    hostLeaderMap[HostAddr("4", 4)][1] = {};
    hostLeaderMap[HostAddr("5", 5)][1] = {};

    LeaderBalancePlan plan;
    auto leaderBalanceResult =
        balancer.buildLeaderBalancePlan(&hostLeaderMap, space, 3, true, plan, true);
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
    auto leaderBalanceResult =
        balancer.buildLeaderBalancePlan(&hostLeaderMap, space, 3, true, plan, true);
    ASSERT_TRUE(nebula::ok(leaderBalanceResult) && nebula::value(leaderBalanceResult));
    verifyLeaderBalancePlan(hostLeaderMap, plan, 1, 2);
  }
}

TEST(BalanceTest, LeaderBalanceWithLargerZoneTest) {
  fs::TempDir rootPath("/tmp/LeaderBalanceWithLargerZoneTest.XXXXXX");
  auto store = MockCluster::initMetaKV(rootPath.path());
  auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
  FLAGS_heartbeat_interval_secs = 1;
  GraphSpaceID space = 0;
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
    TestUtils::assembleZone(kv, zoneInfo);
  }
  {
    cpp2::SpaceDesc properties;
    properties.space_name_ref() = "default_space";
    properties.partition_num_ref() = 8;
    properties.replica_factor_ref() = 3;
    std::vector<std::string> zones = {"zone_0", "zone_1", "zone_2", "zone_3", "zone_4"};
    properties.zone_names_ref() = std::move(zones);
    cpp2::CreateSpaceReq req;
    req.properties_ref() = std::move(properties);
    auto* processor = CreateSpaceProcessor::instance(kv);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    space = resp.get_id().get_space_id();
    ASSERT_EQ(1, resp.get_id().get_space_id());
  }

  sleep(1);
  DefaultValue<folly::Future<Status>>::SetFactory(
      [] { return folly::Future<Status>(Status::OK()); });
  NiceMock<MockAdminClient> client;
  JobDescription jobDesc(
      space, testJobId.fetch_add(1, std::memory_order_relaxed), cpp2::JobType::LEADER_BALANCE);
  LeaderBalanceJobExecutor balancer(jobDesc, kv, &client, {});

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
    auto leaderBalanceResult =
        balancer.buildLeaderBalancePlan(&hostLeaderMap, space, 3, true, plan, true);
    ASSERT_TRUE(nebula::ok(leaderBalanceResult) && nebula::value(leaderBalanceResult));
    verifyLeaderBalancePlan(hostLeaderMap, plan, 0, 1);
  }
}

TEST(BalanceTest, LeaderBalanceWithComplexZoneTest) {
  fs::TempDir rootPath("/tmp/LeaderBalanceWithComplexZoneTest.XXXXXX");
  auto store = MockCluster::initMetaKV(rootPath.path());
  auto* kv = dynamic_cast<kvstore::KVStore*>(store.get());
  FLAGS_heartbeat_interval_secs = 1;
  GraphSpaceID space = 3;

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
    TestUtils::assembleZone(kv, zoneInfo);
  }
  {
    {
      cpp2::SpaceDesc properties;
      properties.space_name_ref() = "default_space";
      properties.partition_num_ref() = 9;
      properties.replica_factor_ref() = 3;
      cpp2::CreateSpaceReq req;
      req.properties_ref() = std::move(properties);
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
      properties.space_name_ref() = "space_on_group_0";
      properties.partition_num_ref() = 64;
      properties.replica_factor_ref() = 3;
      std::vector<std::string> zones = {"zone_0", "zone_1", "zone_2", "zone_3", "zone_4"};
      properties.zone_names_ref() = std::move(zones);
      cpp2::CreateSpaceReq req;
      req.properties_ref() = std::move(properties);
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
      properties.space_name_ref() = "space_on_group_1";
      properties.partition_num_ref() = 81;
      properties.replica_factor_ref() = 3;
      std::vector<std::string> zones = {
          "zone_0", "zone_1", "zone_2", "zone_3", "zone_4", "zone_5", "zone_6", "zone_7", "zone_8"};
      properties.zone_names_ref() = std::move(zones);
      cpp2::CreateSpaceReq req;
      req.properties_ref() = std::move(properties);
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
  DefaultValue<folly::Future<Status>>::SetFactory(
      [] { return folly::Future<Status>(Status::OK()); });
  NiceMock<MockAdminClient> client;
  JobDescription jobDesc(
      space, testJobId.fetch_add(1, std::memory_order_relaxed), cpp2::JobType::LEADER_BALANCE);
  LeaderBalanceJobExecutor balancer(jobDesc, kv, &client, {});

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
    hostLeaderMap[HostAddr("12", 12)][3] = {
        1, 3, 6, 8, 11, 14, 16, 19, 22, 24, 27, 30, 46, 48, 51, 54};
    hostLeaderMap[HostAddr("13", 13)][3] = {10, 15, 18, 31, 52, 69, 81};
    hostLeaderMap[HostAddr("14", 14)][3] = {5, 13, 21, 32, 35, 40, 43, 56, 60, 66, 72, 78};
    hostLeaderMap[HostAddr("15", 15)][3] = {2,  4,  7,  9,  12, 17, 20, 23, 25, 28,
                                            33, 39, 41, 44, 47, 49, 55, 57, 63, 75};
    hostLeaderMap[HostAddr("16", 16)][3] = {26};
    hostLeaderMap[HostAddr("17", 17)][3] = {36, 38};

    LeaderBalancePlan plan;
    auto leaderBalanceResult =
        balancer.buildLeaderBalancePlan(&hostLeaderMap, 3, 3, true, plan, true);
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
