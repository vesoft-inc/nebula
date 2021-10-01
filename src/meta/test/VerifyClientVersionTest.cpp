/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "meta/processors/admin/HBProcessor.h"
#include "meta/processors/admin/VerifyClientVersionProcessor.h"
#include "meta/processors/parts/ListHostsProcessor.h"
#include "meta/test/TestUtils.h"

namespace nebula {
namespace meta {

TEST(VerifyClientVersionTest, VersionTest) {
  fs::TempDir rootPath("/tmp/VersionTest.XXXXXX");
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
  {
    auto req = cpp2::VerifyClientVersionReq();
    req.client_version_ref() = "1.0.1";
    req.build_version_ref() = "1.0.1-nightly";
    auto* processor = VerifyClientVersionProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_CLIENT_SERVER_INCOMPATIBLE, resp.get_code());
  }
  {
    for (auto i = 0; i < 5; i++) {
      auto req = cpp2::VerifyClientVersionReq();
      req.host_ref() = HostAddr(std::to_string(i), i);
      auto* processor = VerifyClientVersionProcessor::instance(kv.get());
      auto f = processor->getFuture();
      processor->process(req);
      auto resp = std::move(f).get();
      ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
  }
  {
    const ClusterID kClusterId = 10;
    for (auto i = 0; i < 5; i++) {
      auto req = cpp2::HBReq();
      req.role_ref() = cpp2::HostRole::GRAPH;
      req.host_ref() = HostAddr(std::to_string(i), i);
      req.cluster_id_ref() = kClusterId;
      auto* processor = HBProcessor::instance(kv.get(), nullptr, kClusterId);
      auto f = processor->getFuture();
      processor->process(req);
      auto resp = std::move(f).get();
      ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
  }
  {
    auto req = cpp2::ListHostsReq();
    req.type_ref() = cpp2::ListHostType::GRAPH;
    auto* processor = ListHostsProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(resp.get_hosts().size(), 5);
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
