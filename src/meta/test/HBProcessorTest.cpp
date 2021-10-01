/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "meta/processors/admin/HBProcessor.h"
#include "meta/test/TestUtils.h"

namespace nebula {
namespace meta {

TEST(HBProcessorTest, HBTest) {
  fs::TempDir rootPath("/tmp/HBTest.XXXXXX");
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
  const ClusterID kClusterId = 10;
  {
    for (auto i = 0; i < 5; i++) {
      cpp2::HBReq req;
      req.role_ref() = cpp2::HostRole::STORAGE;
      req.host_ref() = HostAddr(std::to_string(i), i);
      req.cluster_id_ref() = kClusterId;
      req.role_ref() = cpp2::HostRole::STORAGE;
      auto* processor = HBProcessor::instance(kv.get(), nullptr, kClusterId);
      auto f = processor->getFuture();
      processor->process(req);
      auto resp = std::move(f).get();
      ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }

    auto hostsRet = ActiveHostsMan::getActiveHosts(kv.get(), 1);
    ASSERT_TRUE(nebula::ok(hostsRet));
    ASSERT_EQ(5, nebula::value(hostsRet).size());
    sleep(3);
    hostsRet = ActiveHostsMan::getActiveHosts(kv.get(), 1);
    ASSERT_TRUE(nebula::ok(hostsRet));
    ASSERT_EQ(0, nebula::value(hostsRet).size());

    LOG(INFO) << "Test for invalid host!";
    cpp2::HBReq req;
    req.host_ref() = HostAddr(std::to_string(11), 11);
    req.cluster_id_ref() = 1;
    req.role_ref() = cpp2::HostRole::STORAGE;
    auto* processor = HBProcessor::instance(kv.get(), nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_WRONGCLUSTER, resp.get_code());
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
