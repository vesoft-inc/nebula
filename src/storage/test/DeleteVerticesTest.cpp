/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/futures/Future.h>      // for Future::get
#include <folly/init/Init.h>           // for init
#include <glog/logging.h>              // for INFO
#include <gtest/gtest.h>               // for TestPartResult
#include <gtest/gtest.h>               // for Message
#include <gtest/gtest.h>               // for TestPartResult
#include <thrift/lib/cpp2/FieldRef.h>  // for field_ref

#include <memory>       // for allocator, uniqu...
#include <ostream>      // for operator<<
#include <type_traits>  // for remove_reference...
#include <utility>      // for move
#include <vector>       // for vector

#include "common/base/Logging.h"                     // for LOG, LogMessage
#include "common/base/StatusOr.h"                    // for StatusOr
#include "common/datatypes/Value.h"                  // for Value
#include "common/fs/TempDir.h"                       // for TempDir
#include "common/meta/SchemaManager.h"               // for SchemaManager
#include "common/thrift/ThriftTypes.h"               // for PartitionID
#include "interface/gen-cpp2/storage_types.h"        // for DeleteVerticesRe...
#include "mock/MockCluster.h"                        // for MockCluster
#include "mock/MockData.h"                           // for MockData
#include "storage/CommonUtils.h"                     // for StorageEnv
#include "storage/mutate/AddVerticesProcessor.h"     // for AddVerticesProce...
#include "storage/mutate/DeleteVerticesProcessor.h"  // for DeleteVerticesPr...
#include "storage/test/TestUtils.h"                  // for checkAddVertices...

namespace nebula {
namespace storage {

TEST(DeleteVerticesTest, SimpleTest) {
  fs::TempDir rootPath("/tmp/DeleteVertexTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();

  // Add vertices
  {
    auto* processor = AddVerticesProcessor::instance(env, nullptr);

    LOG(INFO) << "Build AddVerticesRequest...";
    cpp2::AddVerticesRequest req = mock::MockData::mockAddVerticesReq();

    LOG(INFO) << "Test AddVerticesProcessor...";
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    LOG(INFO) << "Check data in kv store...";
    // The number of data in players and teams is 81
    checkAddVerticesData(req, env, 81, 0);
  }

  // Delete vertices
  {
    auto* processor = DeleteVerticesProcessor::instance(env, nullptr);

    LOG(INFO) << "Build DeleteVerticesRequest...";
    cpp2::DeleteVerticesRequest req = mock::MockData::mockDeleteVerticesReq();

    LOG(INFO) << "Test DeleteVerticesProcessor...";
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    LOG(INFO) << "Check data in kv store...";
    auto ret = env->schemaMan_->getSpaceVidLen(req.get_space_id());
    EXPECT_TRUE(ret.ok());
    auto spaceVidLen = ret.value();

    // All the added datas are deleted, the number of vertices is 0
    checkVerticesData(spaceVidLen, req.get_space_id(), *req.parts_ref(), env, 0);
  }
}

TEST(DeleteVerticesTest, MultiVersionTest) {
  fs::TempDir rootPath("/tmp/DeleteVertexTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();

  // Add vertices
  {
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
    // The number of vertices is 81
    checkAddVerticesData(req, env, 81, 2);
  }

  // Delete vertices
  {
    auto* processor = DeleteVerticesProcessor::instance(env, nullptr);

    LOG(INFO) << "Build DeleteVerticesRequest...";
    cpp2::DeleteVerticesRequest req = mock::MockData::mockDeleteVerticesReq();

    LOG(INFO) << "Test DeleteVerticesProcessor...";
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    LOG(INFO) << "Check data in kv store...";
    auto ret = env->schemaMan_->getSpaceVidLen(req.get_space_id());
    EXPECT_TRUE(ret.ok());
    auto spaceVidLen = ret.value();

    // All the added datas are deleted, the number of vertices is 0
    checkVerticesData(spaceVidLen, req.get_space_id(), *req.parts_ref(), env, 0);
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
