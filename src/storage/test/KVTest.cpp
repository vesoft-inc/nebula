/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/String.h>           // for stringPrintf
#include <folly/futures/Future.h>   // for Future::get
#include <folly/init/Init.h>        // for init
#include <gflags/gflags_declare.h>  // for DECLARE_int32, DECLARE...
#include <glog/logging.h>           // for INFO
#include <gtest/gtest.h>            // for Message
#include <gtest/gtest.h>            // for TestPartResult
#include <stddef.h>                 // for size_t
#include <stdint.h>                 // for int32_t

#include <memory>       // for allocator, unique_ptr
#include <string>       // for string
#include <type_traits>  // for remove_reference<>::type
#include <utility>      // for move
#include <vector>       // for vector

#include "common/base/Logging.h"               // for SetStderrLogging
#include "common/fs/TempDir.h"                 // for TempDir
#include "common/thrift/ThriftTypes.h"         // for GraphSpaceID
#include "common/utils/NebulaKeyUtils.h"       // for NebulaKeyUtils
#include "interface/gen-cpp2/common_types.h"   // for ErrorCode, ErrorCode::...
#include "interface/gen-cpp2/storage_types.h"  // for ResponseCommon, ExecRe...
#include "kvstore/NebulaStore.h"               // for NebulaStore
#include "mock/MockCluster.h"                  // for MockCluster
#include "mock/MockData.h"                     // for MockData
#include "storage/kv/GetProcessor.h"           // for GetProcessor
#include "storage/kv/PutProcessor.h"           // for PutProcessor
#include "storage/kv/RemoveProcessor.h"        // for RemoveProcessor

DECLARE_string(meta_server_addrs);
DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace storage {

TEST(KVTest, SimpleTest) {
  fs::TempDir rootPath("/tmp/KVSimpleTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  const GraphSpaceID space = 1;
  const int32_t totalParts = 6;
  {
    auto* processor = PutProcessor::instance(env, nullptr);
    auto fut = processor->getFuture();
    auto req = mock::MockData::mockKVPut();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());
  }
  {
    auto* processor = GetProcessor::instance(env, nullptr);
    auto fut = processor->getFuture();
    auto req = mock::MockData::mockKVGet();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    for (size_t part = 1; part <= totalParts; part++) {
      auto key = NebulaKeyUtils::kvKey(part, folly::stringPrintf("key_%ld", part));
      std::string value;
      auto code = cluster.storageKV_->get(space, part, key, &value);
      EXPECT_EQ(folly::stringPrintf("value_%ld", part), value);
      EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
    }
  }
  {
    auto* processor = RemoveProcessor::instance(env, nullptr);
    auto fut = processor->getFuture();
    auto req = mock::MockData::mockKVRemove();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_parts.size());

    for (size_t part = 1; part <= totalParts; part++) {
      auto key = NebulaKeyUtils::kvKey(part, folly::stringPrintf("key_%ld", part));
      std::string value;
      auto code = cluster.storageKV_->get(space, part, key, &value);
      EXPECT_EQ(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND, code);
    }
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
