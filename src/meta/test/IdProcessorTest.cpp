/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "interface/gen-cpp2/meta_types.h"
#include "meta/processors/id/GetSegmentIdProcessor.h"
#include "meta/processors/id/GetWorkerIdProcessor.h"
#include "meta/test/TestUtils.h"

namespace nebula {
namespace meta {

TEST(WorkerIdProcessorTest, WorkerIdTest) {
  fs::TempDir rootPath("/tmp/WorkerIdTest.XXXXXX");
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));

  for (auto i = 0; i < 10000; i++) {
    cpp2::GetWorkerIdReq req;
    req.host_ref() = std::to_string(i);
    auto* processor = GetWorkerIdProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(i, resp.get_workerid());
  }
}

TEST(SegmentIdProcessorTest, SegmentIdTest) {
  fs::TempDir rootPath("/tmp/SegmentIdTest.XXXXXX");
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));

  // get segment through GetSegmentIdProcessor
  for (auto i = 0; i < 10000; i++) {
    cpp2::GetSegmentIdReq req;
    req.length_ref() = 1;
    auto* processor = GetSegmentIdProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(i, resp.get_segment_id());
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
