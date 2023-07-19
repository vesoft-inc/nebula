/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/utils/Utils.h"
#include "meta/processors/admin/ListClusterInfoProcessor.h"
#include "meta/test/TestUtils.h"

namespace nebula {
namespace meta {

namespace {
const char root_dir[] = "/tmp/create_backup_test.XXXXXX";
const char data_dir[] = "/tmp/create_backup_test.XXXXXX/data";
}  // namespace

class TestStorageService : public storage::cpp2::StorageAdminServiceSvIf {};

TEST(ProcessorTest, ListClusterInfoTest) {
  auto rpcServer = std::make_unique<mock::RpcServer>();
  auto handler = std::make_shared<TestStorageService>();
  rpcServer->start("storage-admin", 0, handler);
  LOG(INFO) << "Start storage server on " << rpcServer->port_;

  LOG(INFO) << "Now test interfaces with retry to leader!";
  std::string localIp("127.0.0.1");
  fs::TempDir rootPath(root_dir);
  HostAddr kvAddr(localIp, 10079);
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path(), kvAddr));
  HostAddr rpcHost(localIp, rpcServer->port_);

  // register storage and save its dir info
  HostAddr storageHost = Utils::getStoreAddrFromAdminAddr(rpcHost);
  std::vector<HostAddr> hosts;
  hosts.emplace_back(storageHost);
  meta::TestUtils::registerHB(kv.get(), hosts);

  std::vector<kvstore::KV> dirs;
  nebula::cpp2::DirInfo dir;
  dir.root_ref() = std::string(root_dir);
  std::vector<std::string> ds;
  ds.push_back(std::string(data_dir));
  dir.data_ref() = ds;
  dirs.emplace_back(std::make_pair(MetaKeyUtils::hostDirKey(storageHost.host, storageHost.port),
                                   MetaKeyUtils::hostDirVal(dir)));
  folly::Baton<true, std::atomic> b;
  kv->asyncMultiPut(kDefaultSpaceId, kDefaultPartId, std::move(dirs), [&](auto) { b.post(); });
  b.wait();

  // register agent
  Port randomPort = 10080;
  HostAddr agentHost(localIp, randomPort);
  hosts.clear();
  hosts.emplace_back(agentHost);
  meta::TestUtils::setupHB(kv.get(), hosts, cpp2::HostRole::AGENT, gitInfoSha());

  {
    cpp2::ListClusterInfoReq req;
    auto* processor = ListClusterInfoProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    LOG(INFO) << folly::to<int>(resp.get_code());
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

    ASSERT_EQ(resp.get_host_services().size(), 1);
    for (auto iter : resp.get_host_services()) {
      auto host = iter.first;
      auto services = iter.second;

      ASSERT_EQ(services.size(), 3);

      std::unordered_map<cpp2::HostRole, cpp2::ServiceInfo> m;
      for (auto s : services) {
        m[s.get_role()] = s;
      }
      ASSERT_NE(m.find(cpp2::HostRole::META), m.end());
      ASSERT_NE(m.find(cpp2::HostRole::STORAGE), m.end());
      ASSERT_NE(m.find(cpp2::HostRole::AGENT), m.end());
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
