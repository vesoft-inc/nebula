/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>
#include <rocksdb/db.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/utils/NebulaKeyUtils.h"
#include "interface/gen-cpp2/common_types.h"
#include "interface/gen-cpp2/storage_types.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "storage/mutate/AddVerticesProcessor.h"
#include "storage/test/TestUtils.h"

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
