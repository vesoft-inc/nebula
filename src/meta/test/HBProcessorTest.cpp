/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/futures/Future.h>         // for Future::get
#include <folly/init/Init.h>              // for init
#include <folly/synchronization/Baton.h>  // for Baton
#include <glog/logging.h>                 // for INFO
#include <gtest/gtest.h>                  // for Message
#include <gtest/gtest.h>                  // for TestPartResult
#include <gtest/gtest.h>                  // for Message
#include <gtest/gtest.h>                  // for TestPartResult
#include <stdint.h>                       // for uint32_t
#include <thrift/lib/cpp2/FieldRef.h>     // for field_ref
#include <unistd.h>                       // for sleep

#include <atomic>       // for atomic
#include <memory>       // for unique_ptr, allocator
#include <string>       // for to_string
#include <type_traits>  // for remove_reference<>::type
#include <utility>      // for move
#include <vector>       // for vector

#include "common/base/EitherOr.h"               // for EitherOr
#include "common/base/ErrorOr.h"                // for ok, value
#include "common/base/Logging.h"                // for SetStderrLogging, LOG
#include "common/datatypes/HostAddr.h"          // for HostAddr
#include "common/fs/TempDir.h"                  // for TempDir
#include "common/thrift/ThriftTypes.h"          // for ClusterID
#include "common/utils/MetaKeyUtils.h"          // for MetaKeyUtils, kDefaul...
#include "interface/gen-cpp2/common_types.h"    // for ErrorCode, ErrorCode:...
#include "interface/gen-cpp2/meta_types.h"      // for HBReq, HBResp, HostRole
#include "kvstore/Common.h"                     // for KV
#include "kvstore/KVStore.h"                    // for KVStore
#include "kvstore/NebulaStore.h"                // for NebulaStore
#include "meta/ActiveHostsMan.h"                // for ActiveHostsMan
#include "meta/processors/admin/HBProcessor.h"  // for HBProcessor
#include "meta/test/TestUtils.h"                // for MockCluster
#include "mock/MockCluster.h"                   // for MockCluster

namespace nebula {
namespace meta {

TEST(HBProcessorTest, HBTest) {
  fs::TempDir rootPath("/tmp/HBTest.XXXXXX");
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));

  std::vector<kvstore::KV> machines;
  for (auto i = 0; i < 5; i++) {
    machines.emplace_back(nebula::MetaKeyUtils::machineKey(std::to_string(i), i), "");
  }

  folly::Baton<true, std::atomic> baton;
  kv->asyncMultiPut(
      kDefaultSpaceId, kDefaultPartId, std::move(machines), [&](auto) { baton.post(); });
  baton.wait();

  const ClusterID kClusterId = 10;
  {
    for (auto i = 0; i < 5; i++) {
      cpp2::HBReq req;
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
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_MACHINE_NOT_FOUND, resp.get_code());
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
