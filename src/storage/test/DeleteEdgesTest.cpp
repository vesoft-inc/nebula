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
#include <thrift/lib/cpp2/FieldRef.h>  // for required_field_ref

#include <memory>       // for allocator, unique_ptr
#include <ostream>      // for operator<<
#include <type_traits>  // for remove_reference<>:...
#include <utility>      // for move
#include <vector>       // for vector

#include "common/base/Logging.h"                  // for LOG, LogMessage
#include "common/base/StatusOr.h"                 // for StatusOr
#include "common/fs/TempDir.h"                    // for TempDir
#include "common/meta/SchemaManager.h"            // for SchemaManager
#include "common/thrift/ThriftTypes.h"            // for PartitionID
#include "interface/gen-cpp2/storage_types.h"     // for EdgeKey, DeleteEdge...
#include "mock/MockCluster.h"                     // for MockCluster
#include "mock/MockData.h"                        // for MockData
#include "storage/CommonUtils.h"                  // for StorageEnv
#include "storage/mutate/AddEdgesProcessor.h"     // for AddEdgesProcessor
#include "storage/mutate/DeleteEdgesProcessor.h"  // for DeleteEdgesProcessor
#include "storage/test/TestUtils.h"               // for checkAddEdgesData

namespace nebula {
namespace storage {

TEST(DeleteEdgesTest, SimpleTest) {
  fs::TempDir rootPath("/tmp/DeleteEdgesTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();

  // Add edges
  {
    auto* processor = AddEdgesProcessor::instance(env, nullptr);

    LOG(INFO) << "Build AddEdgesRequest...";
    cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq();

    LOG(INFO) << "Test AddEdgesProcessor...";
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, (*resp.result_ref()).failed_parts.size());

    LOG(INFO) << "Check data in kv store...";
    // The number of data in serve is 334
    checkAddEdgesData(req, env, 334, 0);
  }

  // Delete edges
  {
    auto* processor = DeleteEdgesProcessor::instance(env, nullptr);

    LOG(INFO) << "Build DeleteEdgesRequest...";
    cpp2::DeleteEdgesRequest req = mock::MockData::mockDeleteEdgesReq();

    LOG(INFO) << "Test DeleteEdgesProcessor...";
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, (*resp.result_ref()).failed_parts.size());

    LOG(INFO) << "Check data in kv store...";
    auto ret = env->schemaMan_->getSpaceVidLen(req.get_space_id());
    EXPECT_TRUE(ret.ok());
    auto spaceVidLen = ret.value();

    // All the added datas are deleted, the number of edge is 0
    checkEdgesData(spaceVidLen, req.get_space_id(), *req.parts_ref(), env, 0);
  }
}

TEST(DeleteEdgesTest, MultiVersionTest) {
  fs::TempDir rootPath("/tmp/DeleteEdgesTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();

  // Add edges
  {
    LOG(INFO) << "Build AddEdgesRequest...";
    cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq();
    cpp2::AddEdgesRequest specifiedOrderReq = mock::MockData::mockAddEdgesSpecifiedOrderReq();

    {
      LOG(INFO) << "AddEdgesProcessor...";
      auto* processor = AddEdgesProcessor::instance(env, nullptr);
      auto fut = processor->getFuture();
      processor->process(req);
      auto resp = std::move(fut).get();
      EXPECT_EQ(0, (*resp.result_ref()).failed_parts.size());
    }
    {
      LOG(INFO) << "AddEdgesProcessor...";
      auto* processor = AddEdgesProcessor::instance(env, nullptr);
      auto fut = processor->getFuture();
      processor->process(specifiedOrderReq);
      auto resp = std::move(fut).get();
      EXPECT_EQ(0, (*resp.result_ref()).failed_parts.size());
    }

    LOG(INFO) << "Check data in kv store...";
    // The number of data in serve is 668
    checkAddEdgesData(req, env, 334, 2);
  }

  // Delete edges
  {
    auto* processor = DeleteEdgesProcessor::instance(env, nullptr);

    LOG(INFO) << "Build DeleteEdgesRequest...";
    cpp2::DeleteEdgesRequest req = mock::MockData::mockDeleteEdgesReq();

    LOG(INFO) << "Test DeleteEdgesProcessor...";
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, (*resp.result_ref()).failed_parts.size());

    LOG(INFO) << "Check data in kv store...";
    auto ret = env->schemaMan_->getSpaceVidLen(req.get_space_id());
    EXPECT_TRUE(ret.ok());
    auto spaceVidLen = ret.value();

    // All the added datas are deleted, the number of edge is 0
    checkEdgesData(spaceVidLen, req.get_space_id(), *req.parts_ref(), env, 0);
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
