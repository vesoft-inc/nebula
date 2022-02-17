/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/String.h>              // for stringPrintf
#include <folly/futures/Future.h>      // for Future::get
#include <folly/init/Init.h>           // for init
#include <glog/logging.h>              // for INFO
#include <gtest/gtest.h>               // for Message
#include <gtest/gtest.h>               // for TestPartResult
#include <gtest/gtest.h>               // for Message
#include <gtest/gtest.h>               // for TestPartResult
#include <thrift/lib/cpp2/FieldRef.h>  // for field_ref

#include <memory>       // for allocator, uniq...
#include <string>       // for basic_string
#include <type_traits>  // for remove_referenc...
#include <utility>      // for move
#include <vector>       // for vector

#include "common/base/Logging.h"                      // for SetStderrLogging
#include "common/fs/FileUtils.h"                      // for FileUtils
#include "common/fs/TempDir.h"                        // for TempDir
#include "common/thrift/ThriftTypes.h"                // for GraphSpaceID
#include "interface/gen-cpp2/common_types.h"          // for ErrorCode, Erro...
#include "interface/gen-cpp2/storage_types.h"         // for CreateCPRequest
#include "mock/MockCluster.h"                         // for MockCluster
#include "mock/MockData.h"                            // for MockData
#include "storage/admin/CreateCheckpointProcessor.h"  // for CreateCheckpoin...
#include "storage/mutate/AddVerticesProcessor.h"      // for AddVerticesProc...

namespace nebula {
namespace storage {
TEST(CheckpointTest, simpleTest) {
  fs::TempDir dataPath("/tmp/Checkpoint_Test_src.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(dataPath.path());
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
  }

  // Begin checkpoint
  {
    auto* processor = CreateCheckpointProcessor::instance(env);
    cpp2::CreateCPRequest req;
    std::vector<GraphSpaceID> ids{1};
    req.space_ids_ref() = ids;
    req.name_ref() = "checkpoint_test";
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    auto checkpoint1 =
        folly::stringPrintf("%s/disk1/nebula/1/checkpoints/checkpoint_test/data", dataPath.path());
    auto files = fs::FileUtils::listAllFilesInDir(checkpoint1.data());
    ASSERT_EQ(4, files.size());
    files.clear();
    auto checkpoint2 =
        folly::stringPrintf("%s/disk2/nebula/1/checkpoints/checkpoint_test/data", dataPath.path());
    fs::FileUtils::listAllFilesInDir(checkpoint2.data());
    files = fs::FileUtils::listAllFilesInDir(checkpoint2.data());
    ASSERT_EQ(4, files.size());
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
