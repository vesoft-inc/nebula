/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/futures/Future.h>  // for Future::get
#include <folly/init/Init.h>       // for init
#include <glog/logging.h>          // for INFO
#include <gtest/gtest.h>           // for TestPartResult
#include <gtest/gtest.h>           // for Message
#include <gtest/gtest.h>           // for TestPartResult

#include <memory>       // for allocator, unique_ptr
#include <type_traits>  // for remove_reference<>:...
#include <utility>      // for move
#include <vector>       // for vector

#include "common/base/Logging.h"                  // for LOG, LogMessage
#include "common/fs/TempDir.h"                    // for TempDir
#include "interface/gen-cpp2/storage_types.h"     // for AddVerticesRequest
#include "mock/MockCluster.h"                     // for MockCluster
#include "mock/MockData.h"                        // for MockData
#include "storage/mutate/AddVerticesProcessor.h"  // for AddVerticesProcessor
#include "storage/test/TestUtils.h"               // for checkAddVerticesData

namespace nebula {
namespace storage {

TEST(AddVerticesTest, SimpleTest) {
  fs::TempDir rootPath("/tmp/AddVerticesTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();

  auto* processor = AddVerticesProcessor::instance(env, nullptr);

  LOG(INFO) << "Build AddVerticesRequest...";
  cpp2::AddVerticesRequest req = mock::MockData::mockAddVerticesReq();

  LOG(INFO) << "Test AddVerticesProcessor...";
  auto fut = processor->getFuture();
  processor->process(req);
  auto resp = std::move(fut).get();
  EXPECT_EQ(0, resp.result.failed_parts.size());

  LOG(INFO) << "Check data in kv store...";
  // The number of vertices is 81
  checkAddVerticesData(req, env, 81, 0);
}

TEST(AddVerticesTest, SpecifyPropertyNameTest) {
  fs::TempDir rootPath("/tmp/AddVerticesTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();

  auto* processor = AddVerticesProcessor::instance(env, nullptr);

  LOG(INFO) << "Build AddVerticesRequest...";
  cpp2::AddVerticesRequest req = mock::MockData::mockAddVerticesSpecifiedOrderReq();

  LOG(INFO) << "Test AddVerticesProcessor...";
  auto fut = processor->getFuture();
  processor->process(req);
  auto resp = std::move(fut).get();
  EXPECT_EQ(0, resp.result.failed_parts.size());

  LOG(INFO) << "Check data in kv store...";
  // The number of vertices is 81
  checkAddVerticesData(req, env, 81, 1);
}

TEST(AddVerticesTest, MultiVersionTest) {
  fs::TempDir rootPath("/tmp/AddVerticesTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();

  LOG(INFO) << "Build AddVerticesRequest...";
  cpp2::AddVerticesRequest req = mock::MockData::mockAddVerticesReq();
  cpp2::AddVerticesRequest specifiedOrderReq = mock::MockData::mockAddVerticesSpecifiedOrderReq();

  {
    LOG(INFO) << "AddVerticesProcessor...";
    auto* processor = AddVerticesProcessor::instance(env, nullptr);
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());
  }
  {
    LOG(INFO) << "AddVerticesProcessor...";
    auto* processor = AddVerticesProcessor::instance(env, nullptr);
    auto fut = processor->getFuture();
    processor->process(specifiedOrderReq);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());
  }

  LOG(INFO) << "Check data in kv store...";
  // The number of vertices  is 162
  checkAddVerticesData(req, env, 81, 2);
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}
