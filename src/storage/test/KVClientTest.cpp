/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/Try.h>                 // for Try::throwUnlessValue
#include <folly/futures/Future.h>      // for SemiFuture::get, Futu...
#include <folly/futures/Future.h>      // for Future
#include <folly/futures/Future.h>      // for SemiFuture::get, Futu...
#include <folly/futures/Future.h>      // for Future
#include <folly/futures/Promise.h>     // for PromiseException::Pro...
#include <folly/init/Init.h>           // for init
#include <gflags/gflags_declare.h>     // for DECLARE_int32, DECLAR...
#include <glog/logging.h>              // for INFO
#include <gtest/gtest.h>               // for TestPartResult
#include <gtest/gtest.h>               // for Message
#include <gtest/gtest.h>               // for TestPartResult
#include <stddef.h>                    // for size_t
#include <stdint.h>                    // for int32_t
#include <thrift/lib/cpp2/FieldRef.h>  // for field_ref

#include <memory>         // for allocator, unique_ptr
#include <ostream>        // for operator<<, basic_ost...
#include <string>         // for string, basic_string
#include <type_traits>    // for remove_reference<>::type
#include <unordered_map>  // for _Node_const_iterator
#include <utility>        // for move, pair, make_pair
#include <vector>         // for vector

#include "clients/meta/MetaClient.h"            // for MetaClientOptions
#include "clients/storage/StorageClient.h"      // for StorageClient
#include "clients/storage/StorageClientBase.h"  // for StorageRpcResponse
#include "common/base/Logging.h"                // for LOG, LogMessage, _LOG...
#include "common/base/StatusOr.h"               // for StatusOr
#include "common/datatypes/HostAddr.h"          // for HostAddr, operator<<
#include "common/datatypes/KeyValue.h"          // for KeyValue
#include "common/fs/TempDir.h"                  // for TempDir
#include "common/network/NetworkUtils.h"        // for NetworkUtils
#include "common/thrift/ThriftTypes.h"          // for GraphSpaceID
#include "interface/gen-cpp2/common_types.h"    // for ErrorCode, ErrorCode:...
#include "interface/gen-cpp2/storage_types.h"   // for KVGetResponse
#include "kvstore/NebulaStore.h"                // for NebulaStore
#include "meta/test/TestUtils.h"                // for TestUtils
#include "mock/MockCluster.h"                   // for MockCluster

DECLARE_string(meta_server_addrs);
DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace storage {

void checkResult(StorageRpcResponse<storage::cpp2::KVGetResponse>& resp, size_t expectCount) {
  size_t count = 0;
  for (const auto& result : resp.responses()) {
    count += (*result.key_values_ref()).size();
    for (const auto& pair : *result.key_values_ref()) {
      EXPECT_EQ(pair.first, pair.second);
    }
  }
  EXPECT_EQ(expectCount, count);
}

TEST(KVClientTest, SimpleTest) {
  GraphSpaceID spaceId = 1;
  fs::TempDir metaPath("/tmp/KVTest.meta.XXXXXX");
  fs::TempDir storagePath("/tmp/KVTest.storage.XXXXXX");
  mock::MockCluster cluster;
  std::string storageName{"127.0.0.1"};
  auto storagePort = network::NetworkUtils::getAvailablePort();
  HostAddr storageAddr{storageName, storagePort};

  cluster.startMeta(metaPath.path());
  meta::MetaClientOptions options;
  options.localHost_ = storageAddr;
  cluster.initMetaClient(options);
  auto* metaClient = cluster.metaClient_.get();
  {
    LOG(INFO) << "registed " << storageAddr;
    std::vector<HostAddr> hosts = {storageAddr};
    auto result = metaClient->addHosts(std::move(hosts)).get();
    EXPECT_TRUE(result.ok());
  }
  {
    std::vector<HostAddr> hosts = {storageAddr};
    nebula::meta::TestUtils::registerHB(cluster.metaKV_.get(), hosts);
  }

  cluster.startStorage(storageAddr, storagePath.path());
  auto client = cluster.initGraphStorageClient();
  // kv interface test
  {
    std::vector<nebula::KeyValue> pairs;
    for (int32_t i = 0; i < 10; i++) {
      auto key = std::to_string(i);
      auto value = std::to_string(i);
      nebula::KeyValue pair(std::make_pair(key, value));
      pairs.emplace_back(std::move(pair));
    }
    auto future = client->put(spaceId, std::move(pairs));
    auto resp = std::move(future).get();
    ASSERT_TRUE(resp.succeeded());
    LOG(INFO) << "Put Successfully";
  }
  {
    std::vector<std::string> keys;
    for (int32_t i = 0; i < 10; i++) {
      keys.emplace_back(std::to_string(i));
    }

    auto future = client->get(spaceId, std::move(keys), false);
    auto resp = std::move(future).get();
    ASSERT_TRUE(resp.succeeded());
    LOG(INFO) << "Get Successfully";
    checkResult(resp, 10);
  }
  {
    std::vector<std::string> keys;
    for (int32_t i = 0; i < 20; i++) {
      keys.emplace_back(std::to_string(i));
    }

    auto future = client->get(spaceId, std::move(keys), false);
    auto resp = std::move(future).get();
    ASSERT_FALSE(resp.succeeded());
    LOG(INFO) << "Get failed because some key not exists";
    if (!resp.failedParts().empty()) {
      for (const auto& partEntry : resp.failedParts()) {
        EXPECT_EQ(partEntry.second, nebula::cpp2::ErrorCode::E_PARTIAL_RESULT);
      }
    }
    // Can not checkResult, because some part get all keys indeed, and other
    // part return E_PARTIAL_RESULT
  }
  {
    std::vector<std::string> keys;
    for (int32_t i = 0; i < 20; i++) {
      keys.emplace_back(std::to_string(i));
    }

    auto future = client->get(spaceId, std::move(keys), true);
    auto resp = std::move(future).get();
    ASSERT_TRUE(resp.succeeded());
    LOG(INFO) << "Get Successfully";
    checkResult(resp, 10);
  }
  {
    // try to get keys all not exists
    std::vector<std::string> keys;
    for (int32_t i = 10; i < 20; i++) {
      keys.emplace_back(std::to_string(i));
    }

    auto future = client->get(spaceId, std::move(keys), true);
    auto resp = std::move(future).get();
    ASSERT_TRUE(resp.succeeded());
    LOG(INFO) << "Get failed because some key not exists";
    if (!resp.failedParts().empty()) {
      for (const auto& partEntry : resp.failedParts()) {
        EXPECT_EQ(partEntry.second, nebula::cpp2::ErrorCode::E_PARTIAL_RESULT);
      }
    }
    checkResult(resp, 0);
  }
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}
