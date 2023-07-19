/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/synchronization/Baton.h>
#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/utils/MetaKeyUtils.h"
#include "meta/ActiveHostsMan.h"
#include "meta/test/TestUtils.h"

DECLARE_int32(heartbeat_interval_secs);
DECLARE_uint32(expired_time_factor);

namespace nebula {
namespace meta {

TEST(ActiveHostsManTest, EncodeDecodeHostInfoV2) {
  auto now = time::WallClock::fastNowInMilliSec();
  auto role = cpp2::HostRole::STORAGE;
  auto strGitInfoSHA = gitInfoSha();
  {
    HostInfo hostInfo(now, role, strGitInfoSHA);
    auto encodeHostInfo = HostInfo::encodeV2(hostInfo);

    auto decodeHostInfo = HostInfo::decode(encodeHostInfo);
    ASSERT_EQ(hostInfo.lastHBTimeInMilliSec_, decodeHostInfo.lastHBTimeInMilliSec_);
    ASSERT_EQ(hostInfo.role_, decodeHostInfo.role_);
    ASSERT_EQ(hostInfo.gitInfoSha_, decodeHostInfo.gitInfoSha_);
  }
  {
    HostInfo hostInfo(now);
    auto encodeHostInfo = HostInfo::encodeV2(hostInfo);

    auto decodeHostInfo = HostInfo::decode(encodeHostInfo);
    ASSERT_EQ(hostInfo.lastHBTimeInMilliSec_, decodeHostInfo.lastHBTimeInMilliSec_);
    ASSERT_EQ(hostInfo.role_, decodeHostInfo.role_);
    ASSERT_EQ(hostInfo.gitInfoSha_, decodeHostInfo.gitInfoSha_);
  }
  {
    HostInfo hostInfo(now, role, strGitInfoSHA);
    auto encodeHostInfo = HostInfo::encodeV2(hostInfo);

    auto decodeHostInfo = HostInfo::decode(encodeHostInfo);
    ASSERT_EQ(hostInfo.lastHBTimeInMilliSec_, decodeHostInfo.lastHBTimeInMilliSec_);
  }
}

TEST(ActiveHostsManTest, NormalTest) {
  fs::TempDir rootPath("/tmp/ActiveHostsManTest.XXXXXX");
  FLAGS_heartbeat_interval_secs = 1;
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));

  std::vector<kvstore::KV> data;
  data.emplace_back(nebula::MetaKeyUtils::machineKey("0", 0), "");
  data.emplace_back(nebula::MetaKeyUtils::machineKey("0", 1), "");
  data.emplace_back(nebula::MetaKeyUtils::machineKey("0", 2), "");
  folly::Baton<true, std::atomic> baton;
  kv->asyncMultiPut(kDefaultSpaceId, kDefaultPartId, std::move(data), [&](auto) { baton.post(); });
  baton.wait();

  std::vector<kvstore::KV> times;
  auto now = time::WallClock::fastNowInMilliSec();
  HostInfo info1(now, cpp2::HostRole::STORAGE, gitInfoSha());
  ActiveHostsMan::updateHostInfo(kv.get(), HostAddr("0", 0), info1, times);
  ActiveHostsMan::updateHostInfo(kv.get(), HostAddr("0", 1), info1, times);
  ActiveHostsMan::updateHostInfo(kv.get(), HostAddr("0", 2), info1, times);
  TestUtils::doPut(kv.get(), times);
  auto hostsRet = ActiveHostsMan::getActiveHosts(kv.get());
  ASSERT_TRUE(nebula::ok(hostsRet));
  ASSERT_EQ(3, nebula::value(hostsRet).size());

  HostInfo info2(now + 2000, cpp2::HostRole::STORAGE, gitInfoSha());
  std::vector<kvstore::KV> time;
  ActiveHostsMan::updateHostInfo(kv.get(), HostAddr("0", 0), info2, time);
  TestUtils::doPut(kv.get(), time);
  hostsRet = ActiveHostsMan::getActiveHosts(kv.get());
  ASSERT_TRUE(nebula::ok(hostsRet));
  ASSERT_EQ(3, nebula::value(hostsRet).size());

  {
    const auto& prefix = MetaKeyUtils::hostPrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
    int i = 0;
    while (iter->valid()) {
      auto host = MetaKeyUtils::parseHostKey(iter->key());
      HostInfo info = HostInfo::decode(iter->val());
      ASSERT_EQ(HostAddr("0", i), HostAddr(host.host, host.port));
      if (i != 0) {
        ASSERT_EQ(info1, info);
      } else {
        ASSERT_EQ(info2, info);
      }
      iter->next();
      i++;
    }
    ASSERT_EQ(3, i);
  }

  sleep(FLAGS_heartbeat_interval_secs * FLAGS_expired_time_factor + 1);
  hostsRet = ActiveHostsMan::getActiveHosts(kv.get());
  ASSERT_TRUE(nebula::ok(hostsRet));
  ASSERT_EQ(1, nebula::value(hostsRet).size());
}

TEST(ActiveHostsManTest, LeaderTest) {
  fs::TempDir rootPath("/tmp/ActiveHostsManTest.XXXXXX");
  FLAGS_heartbeat_interval_secs = 1;
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));

  std::vector<kvstore::KV> data;
  data.emplace_back(nebula::MetaKeyUtils::machineKey("0", 0), "");
  data.emplace_back(nebula::MetaKeyUtils::machineKey("0", 1), "");
  data.emplace_back(nebula::MetaKeyUtils::machineKey("0", 2), "");
  folly::Baton<true, std::atomic> baton;
  kv->asyncMultiPut(kDefaultSpaceId, kDefaultPartId, std::move(data), [&](auto) { baton.post(); });
  baton.wait();

  auto now = time::WallClock::fastNowInMilliSec();
  HostInfo hInfo1(now, cpp2::HostRole::STORAGE, gitInfoSha());
  HostInfo hInfo2(now + 2000, cpp2::HostRole::STORAGE, gitInfoSha());
  std::vector<kvstore::KV> times;
  ActiveHostsMan::updateHostInfo(kv.get(), HostAddr("0", 0), hInfo1, times);
  ActiveHostsMan::updateHostInfo(kv.get(), HostAddr("0", 1), hInfo1, times);
  ActiveHostsMan::updateHostInfo(kv.get(), HostAddr("0", 2), hInfo1, times);
  TestUtils::doPut(kv.get(), times);
  auto hostsRet = ActiveHostsMan::getActiveHosts(kv.get());
  ASSERT_TRUE(nebula::ok(hostsRet));
  ASSERT_EQ(3, nebula::value(hostsRet).size());

  auto makePartInfo = [](int partId) {
    cpp2::LeaderInfo part;
    part.part_id_ref() = partId;
    part.term_ref() = partId * 1024;
    return part;
  };

  auto cmpPartInfo = [](auto& a, auto& b) {
    if (a.get_part_id() == b.get_part_id()) {
      return a.get_term() < b.get_term();
    }
    return a.get_part_id() < b.get_part_id();
  };

  auto part1 = makePartInfo(1);
  auto part2 = makePartInfo(2);
  auto part3 = makePartInfo(3);

  std::vector<cpp2::LeaderInfo> parts{part1, part2, part3};
  std::sort(parts.begin(), parts.end(), cmpPartInfo);

  HostAddr host("0", 0);

  std::unordered_map<GraphSpaceID, std::vector<cpp2::LeaderInfo>> leaderIds;
  leaderIds.emplace(1, std::vector<cpp2::LeaderInfo>{part1, part2});
  leaderIds.emplace(2, std::vector<cpp2::LeaderInfo>{part3});
  std::vector<kvstore::KV> time;
  ActiveHostsMan::updateHostInfo(kv.get(), host, hInfo2, time, &leaderIds);
  TestUtils::doPut(kv.get(), time);
  hostsRet = ActiveHostsMan::getActiveHosts(kv.get());
  ASSERT_TRUE(nebula::ok(hostsRet));
  ASSERT_EQ(3, nebula::value(hostsRet).size());

  using SpaceAndPart = std::pair<GraphSpaceID, PartitionID>;
  using HostAndTerm = std::pair<HostAddr, int64_t>;

  TermID term;
  nebula::cpp2::ErrorCode code;
  std::map<SpaceAndPart, HostAndTerm> results;
  {
    const auto& prefix = MetaKeyUtils::leaderPrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
    int i = 0;
    while (iter->valid()) {
      auto spaceAndPart = MetaKeyUtils::parseLeaderKeyV3(iter->key());
      std::tie(host, term, code) = MetaKeyUtils::parseLeaderValV3(iter->val());
      if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(INFO) << "error: " << apache::thrift::util::enumNameSafe(code);
        continue;
      }
      results[spaceAndPart] = std::make_pair(host, term);
      iter->next();
      i++;
    }
    EXPECT_EQ(results.size(), 3);
    EXPECT_EQ(i, 3);
  }

  sleep(FLAGS_heartbeat_interval_secs * FLAGS_expired_time_factor + 1);
  hostsRet = ActiveHostsMan::getActiveHosts(kv.get());
  ASSERT_TRUE(nebula::ok(hostsRet));
  ASSERT_EQ(1, nebula::value(hostsRet).size());
}

}  // namespace meta
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}
