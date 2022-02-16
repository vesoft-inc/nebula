/* Copyright (c) 2021 vesoft inc. All rights reserved.
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
#include <thrift/lib/cpp2/FieldRef.h>     // for field_ref, optio...
#include <unistd.h>                       // for sleep

#include <atomic>       // for atomic
#include <memory>       // for unique_ptr, allo...
#include <string>       // for string, basic_st...
#include <type_traits>  // for remove_reference...
#include <utility>      // for move
#include <vector>       // for vector

#include "common/base/EitherOr.h"                    // for EitherOr
#include "common/base/ErrorOr.h"                     // for ok, value
#include "common/base/Logging.h"                     // for SetStderrLogging
#include "common/datatypes/HostAddr.h"               // for HostAddr
#include "common/fs/TempDir.h"                       // for TempDir
#include "common/thrift/ThriftTypes.h"               // for ClusterID
#include "common/utils/MetaKeyUtils.h"               // for MetaKeyUtils
#include "interface/gen-cpp2/common_types.h"         // for DirInfo, ErrorCode
#include "interface/gen-cpp2/meta_types.h"           // for HostRole, HBReq
#include "kvstore/Common.h"                          // for KV
#include "kvstore/KVStore.h"                         // for KVStore
#include "kvstore/NebulaStore.h"                     // for NebulaStore
#include "meta/ActiveHostsMan.h"                     // for ActiveHostsMan
#include "meta/processors/admin/AgentHBProcessor.h"  // for AgentHBProcessor
#include "meta/processors/admin/HBProcessor.h"       // for HBProcessor
#include "meta/test/TestUtils.h"                     // for MockCluster
#include "mock/MockCluster.h"                        // for MockCluster

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
