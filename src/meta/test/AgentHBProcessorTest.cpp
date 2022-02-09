/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/utils/MetaKeyUtils.h"
#include "meta/processors/admin/AgentHBProcessor.h"
#include "meta/test/TestUtils.h"

namespace nebula {
namespace meta {

TEST(AgentHBProcessorTest, AgentHBTest) {
  fs::TempDir rootPath("/tmp/AgentHBTest.XXXXXX");
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));

  // mock 5 storage service in 5 hosts
  {
    // register storage machines so their heartbeat can be accepted
    std::vector<kvstore::KV> machines;
    for (auto i = 0; i < 5; i++) {
      machines.emplace_back(nebula::MetaKeyUtils::machineKey(std::to_string(i), i), "");
    }
    folly::Baton<true, std::atomic> baton;
    kv->asyncMultiPut(
        kDefaultSpaceId, kDefaultPartId, std::move(machines), [&](auto) { baton.post(); });
    baton.wait();

    // mock one heartbeat for each storage service
    const ClusterID kClusterId = 10;
    for (auto i = 0; i < 5; i++) {
      cpp2::HBReq req;
      req.host_ref() = HostAddr(std::to_string(i), i);
      req.cluster_id_ref() = kClusterId;
      req.role_ref() = cpp2::HostRole::STORAGE;
      nebula::cpp2::DirInfo dir;
      dir.root_ref() = "/tmp/nebula";
      std::vector<std::string> ds;
      ds.push_back("/tmp/nebula/data");
      dir.data_ref() = ds;
      req.dir_ref() = dir;

      auto* processor = HBProcessor::instance(kv.get(), nullptr, kClusterId);
      auto f = processor->getFuture();
      processor->process(req);
      auto resp = std::move(f).get();
      ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
  }

  // mock an agent in each host
  for (auto i = 0; i < 5; i++) {
    cpp2::AgentHBReq req;
    req.host_ref() = HostAddr(std::to_string(i), 10 + i);
    auto* processor = AgentHBProcessor::instance(kv.get(), nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(1, resp.get_service_list().size());

    auto s = resp.get_service_list()[0];
    ASSERT_EQ(cpp2::HostRole::STORAGE, s.get_role());
    ASSERT_EQ(HostAddr(std::to_string(i), i), s.get_addr());
    ASSERT_EQ(1, s.get_dir().get_data().size());
  }

  auto hostsRet = ActiveHostsMan::getActiveHosts(kv.get(), 1, cpp2::HostRole::STORAGE);
  ASSERT_TRUE(nebula::ok(hostsRet));
  ASSERT_EQ(5, nebula::value(hostsRet).size());

  hostsRet = ActiveHostsMan::getActiveHosts(kv.get(), 1, cpp2::HostRole::AGENT);
  ASSERT_TRUE(nebula::ok(hostsRet));
  ASSERT_EQ(5, nebula::value(hostsRet).size());

  sleep(3);
  hostsRet = ActiveHostsMan::getActiveHosts(kv.get(), 1, cpp2::HostRole::STORAGE);
  ASSERT_TRUE(nebula::ok(hostsRet));
  ASSERT_EQ(0, nebula::value(hostsRet).size());
  hostsRet = ActiveHostsMan::getActiveHosts(kv.get(), 1, cpp2::HostRole::AGENT);
  ASSERT_TRUE(nebula::ok(hostsRet));
  ASSERT_EQ(0, nebula::value(hostsRet).size());
}

}  // namespace meta
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}
