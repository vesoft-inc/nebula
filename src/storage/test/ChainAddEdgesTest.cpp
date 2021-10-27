/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <folly/Benchmark.h>
#include <folly/Format.h>
#include <folly/String.h>
#include <folly/container/Enumerate.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <thrift/lib/cpp/util/EnumUtils.h>

#include "common/fs/TempDir.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "storage/CommonUtils.h"
#include "storage/test/ChainTestUtils.h"
#include "storage/test/TestUtils.h"
#include "storage/transaction/ChainAddEdgesGroupProcessor.h"
#include "storage/transaction/ChainAddEdgesProcessorLocal.h"
#include "storage/transaction/ConsistUtil.h"

namespace nebula {
namespace storage {

constexpr int32_t mockSpaceId = 1;
constexpr int32_t mockPartNum = 1;
constexpr int32_t fackTerm = 1;

// make sure test class works well
TEST(ChainAddEdgesTest, TestUtilsTest) {
  fs::TempDir rootPath("/tmp/AddEdgesTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();
  env->metaClient_ = mClient.get();
  MetaClientTestUpdater::addPartTerm(env->metaClient_, mockSpaceId, mockPartNum, fackTerm);

  auto* processor = new FakeChainAddEdgesProcessorLocal(env);

  processor->rcPrepareLocal = nebula::cpp2::ErrorCode::SUCCEEDED;
  processor->rcProcessRemote = nebula::cpp2::ErrorCode::SUCCEEDED;
  processor->rcProcessLocal = nebula::cpp2::ErrorCode::SUCCEEDED;

  LOG(INFO) << "Build AddEdgesRequest...";
  cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq(false, 1);

  LOG(INFO) << "Test AddEdgesProcessor...";
  auto fut = processor->getFuture();
  processor->process(req);
  auto resp = std::move(fut).get();
  EXPECT_EQ(0, resp.result.failed_parts.size());

  LOG(INFO) << "Check data in kv store...";
  // The number of data in serve is 334
  checkAddEdgesData(req, env, 0, 0);
}

TEST(ChainAddEdgesTest, prepareLocalSucceedTest) {
  fs::TempDir rootPath("/tmp/AddEdgesTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();
  env->metaClient_ = mClient.get();
  MetaClientTestUpdater::addPartTerm(env->metaClient_, mockSpaceId, mockPartNum, fackTerm);
  auto* proc = new FakeChainAddEdgesProcessorLocal(env);

  proc->rcProcessRemote = nebula::cpp2::ErrorCode::E_RPC_FAILURE;

  LOG(INFO) << "Build AddEdgesRequest...";
  cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq(false, 1);

  LOG(INFO) << "Test AddEdgesProcessor...";
  auto fut = proc->getFuture();
  proc->process(req);
  auto resp = std::move(fut).get();
  EXPECT_EQ(0, resp.result.failed_parts.size());

  ChainTestUtils util;
  // none of really edge key should be inserted
  EXPECT_EQ(334, numOfKey(req, util.genKey, env));
  EXPECT_EQ(0, numOfKey(req, util.genPrime, env));
  EXPECT_EQ(334, numOfKey(req, util.genDoublePrime, env));
}

TEST(ChainAddEdgesTest, processRemoteSucceededTest) {
  fs::TempDir rootPath("/tmp/AddEdgesTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();

  env->metaClient_ = mClient.get();
  auto* proc = new FakeChainAddEdgesProcessorLocal(env);
  MetaClientTestUpdater::addPartTerm(env->metaClient_, mockSpaceId, mockPartNum, fackTerm);

  proc->rcProcessRemote = nebula::cpp2::ErrorCode::SUCCEEDED;

  LOG(INFO) << "Build AddEdgesRequest...";
  cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq(false, 1);

  LOG(INFO) << "Test AddEdgesProcessor...";
  auto fut = proc->getFuture();
  proc->process(req);
  auto resp = std::move(fut).get();
  EXPECT_EQ(0, resp.result.failed_parts.size());

  ChainTestUtils util;
  // none of really edge key should be inserted
  EXPECT_EQ(334, numOfKey(req, util.genKey, env));
  // prime key should be deleted
  EXPECT_EQ(0, numOfKey(req, util.genPrime, env));
  EXPECT_EQ(0, numOfKey(req, util.genDoublePrime, env));
}

// check prepareLocal() will set prime key properly
TEST(ChainAddEdgesTest, processRemoteFailedTest) {
  fs::TempDir rootPath("/tmp/AddEdgesTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();
  env->metaClient_ = mClient.get();
  MetaClientTestUpdater::addPartTerm(env->metaClient_, mockSpaceId, mockPartNum, fackTerm);

  auto* proc = new FakeChainAddEdgesProcessorLocal(env);
  proc->rcProcessRemote = nebula::cpp2::ErrorCode::E_OUTDATED_TERM;

  LOG(INFO) << "Build AddEdgesRequest...";
  cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq(false, 1);

  LOG(INFO) << "Test AddEdgesProcessor...";
  auto fut = proc->getFuture();
  proc->process(req);
  auto resp = std::move(fut).get();
  EXPECT_EQ(1, resp.result.failed_parts.size());

  ChainTestUtils util;
  // none of really edge key should be inserted
  EXPECT_EQ(0, numOfKey(req, util.genKey, env));
  // prime key should be deleted
  EXPECT_EQ(0, numOfKey(req, util.genPrime, env));
  EXPECT_EQ(0, numOfKey(req, util.genDoublePrime, env));
}

TEST(ChainAddEdgesTest, processRemoteUnknownTest) {
  fs::TempDir rootPath("/tmp/AddEdgesTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();
  env->metaClient_ = mClient.get();
  MetaClientTestUpdater::addPartTerm(env->metaClient_, mockSpaceId, mockPartNum, fackTerm);

  auto* proc = new FakeChainAddEdgesProcessorLocal(env);

  proc->rcProcessRemote = nebula::cpp2::ErrorCode::E_RPC_FAILURE;

  LOG(INFO) << "Build AddEdgesRequest...";
  cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq(false, 1);

  LOG(INFO) << "Test AddEdgesProcessor...";
  auto fut = proc->getFuture();
  proc->process(req);
  auto resp = std::move(fut).get();
  EXPECT_EQ(0, resp.result.failed_parts.size());

  ChainTestUtils util;
  // none of really edge key should be inserted
  EXPECT_EQ(334, numOfKey(req, util.genKey, env));
  // prime key should be deleted
  EXPECT_EQ(0, numOfKey(req, util.genPrime, env));
  EXPECT_EQ(334, numOfKey(req, util.genDoublePrime, env));
}

// make a reversed request, make sure it can be added successfully
TEST(ChainAddEdgesTest, processRemoteTest) {
  fs::TempDir rootPath("/tmp/AddEdgesTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();

  env->metaClient_ = mClient.get();
  MetaClientTestUpdater::addPartTerm(env->metaClient_, mockSpaceId, mockPartNum, fackTerm);

  auto* proc = new FakeChainAddEdgesProcessorLocal(env);
  LOG(INFO) << "Build AddEdgesRequest...";
  cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq(false, 1);

  auto reversedRequest = proc->reverseRequestForward(req);
  delete proc;
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}
