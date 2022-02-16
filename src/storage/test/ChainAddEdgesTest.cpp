/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/Optional.h>        // for Optional
#include <folly/futures/Future.h>  // for Future::get
#include <folly/init/Init.h>
#include <glog/logging.h>  // for INFO
#include <gtest/gtest.h>   // for Message
#include <stdint.h>        // for int32_t

#include "common/base/Logging.h"               // for LOG, LogMessage, _LOG_...
#include "common/fs/TempDir.h"                 // for TempDir
#include "interface/gen-cpp2/common_types.h"   // for ErrorCode, ErrorCode::...
#include "interface/gen-cpp2/storage_types.h"  // for AddEdgesRequest, ExecR...
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "storage/StorageFlags.h"         // for FLAGS_trace_toss
#include "storage/test/ChainTestUtils.h"  // for numOfKey, FakeChainAdd...
#include "storage/test/TestUtils.h"       // for checkAddEdgesData

namespace nebula {
namespace storage {

constexpr int32_t mockSpaceId = 1;
constexpr int32_t mockPartNum = 1;
constexpr int32_t fackTerm = 1;
constexpr auto suc = nebula::cpp2::ErrorCode::SUCCEEDED;

// make sure test class works well
TEST(ChainAddEdgesTest, TestUtilsTest) {
  fs::TempDir rootPath("/tmp/AddEdgesTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto mClient = MetaClientTestUpdater::makeDefault();
  env->metaClient_ = mClient.get();
  MetaClientTestUpdater::addPartTerm(env->metaClient_, mockSpaceId, mockPartNum, fackTerm);

  auto* proc = new FakeChainAddEdgesLocalProcessor(env);

  proc->setPrepareCode(suc);
  proc->setRemoteCode(suc);
  proc->setCommitCode(suc);

  LOG(INFO) << "Build AddEdgesRequest...";
  cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq(false, 1);

  LOG(INFO) << "Test AddEdgesProcessor...";
  auto fut = proc->getFuture();
  proc->process(req);
  auto resp = std::move(fut).get();

  LOG(INFO) << "Check data in kv store...";
  // The number of data in serve is 334
  EXPECT_EQ(0, resp.result.failed_parts.size());
  checkAddEdgesData(req, env, 0, 0);
}

TEST(ChainAddEdgesTest, prepareLocalSucceedTest) {
  fs::TempDir rootPath("/tmp/AddEdgesTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto mClient = MetaClientTestUpdater::makeDefault();
  env->metaClient_ = mClient.get();
  MetaClientTestUpdater::addPartTerm(env->metaClient_, mockSpaceId, mockPartNum, fackTerm);
  auto* proc = new FakeChainAddEdgesLocalProcessor(env);

  proc->setRemoteCode(nebula::cpp2::ErrorCode::E_RPC_FAILURE);

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
  auto mClient = MetaClientTestUpdater::makeDefault();

  env->metaClient_ = mClient.get();
  auto* proc = new FakeChainAddEdgesLocalProcessor(env);
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
  auto mClient = MetaClientTestUpdater::makeDefault();
  env->metaClient_ = mClient.get();
  MetaClientTestUpdater::addPartTerm(env->metaClient_, mockSpaceId, mockPartNum, fackTerm);

  auto* proc = new FakeChainAddEdgesLocalProcessor(env);
  proc->setRemoteCode(nebula::cpp2::ErrorCode::E_OUTDATED_TERM);

  LOG(INFO) << "Build AddEdgesRequest...";
  cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq(false, 1);

  LOG(INFO) << "Test AddEdgesProcessor...";
  auto fut = proc->getFuture();
  proc->process(req);
  auto resp = std::move(fut).get();
  EXPECT_EQ(0, resp.result.failed_parts.size());

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
  auto mClient = MetaClientTestUpdater::makeDefault();
  env->metaClient_ = mClient.get();
  MetaClientTestUpdater::addPartTerm(env->metaClient_, mockSpaceId, mockPartNum, fackTerm);

  auto* proc = new FakeChainAddEdgesLocalProcessor(env);

  proc->setRemoteCode(nebula::cpp2::ErrorCode::E_RPC_FAILURE);

  LOG(INFO) << "Build AddEdgesRequest...";
  cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq(false, 1);

  LOG(INFO) << "Test AddEdgesProcessor...";
  auto fut = proc->getFuture();
  proc->process(req);
  auto resp = std::move(fut).get();

  ChainTestUtils util;
  // none of really edge key should be inserted
  EXPECT_EQ(0, resp.result.failed_parts.size());
  EXPECT_EQ(334, numOfKey(req, util.genKey, env));
  // prime key should be deleted
  EXPECT_EQ(0, numOfKey(req, util.genPrime, env));
  EXPECT_EQ(334, numOfKey(req, util.genDoublePrime, env));
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
  FLAGS_trace_toss = true;
  FLAGS_v = 1;
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}
