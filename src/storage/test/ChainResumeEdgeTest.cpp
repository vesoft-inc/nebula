/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
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
#include "storage/test/QueryTestUtils.h"
#include "storage/test/TestUtils.h"
#include "storage/transaction/ChainAddEdgesGroupProcessor.h"
#include "storage/transaction/ChainAddEdgesProcessorLocal.h"
#include "storage/transaction/ChainResumeProcessor.h"
#include "storage/transaction/ConsistUtil.h"

namespace nebula {
namespace storage {

constexpr int32_t mockSpaceId = 1;
constexpr int32_t mockPartNum = 6;
constexpr int32_t mockSpaceVidLen = 32;

ChainTestUtils gTestUtil;
ChainUpdateEdgeTestHelper helper;

/**
 * @brief resumeTest1 (resume insert prime)
 *                  insert      resume
 *  prepareLocal    succeed     succeed
 *  processRemote   skip        succeed
 *  processLocal    skip        succeed
 *  expect: edge/prime/double : 334/0/0
 *          keyOfRequest: false
 */
TEST(ChainResumeEdgesTest, resumeTest1) {
  fs::TempDir rootPath("/tmp/AddEdgesTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();

  env->metaClient_ = mClient.get();
  auto* proc = new FakeChainAddEdgesProcessorLocal(env);

  proc->rcProcessRemote = nebula::cpp2::ErrorCode::SUCCEEDED;
  proc->rcProcessLocal = nebula::cpp2::ErrorCode::SUCCEEDED;

  LOG(INFO) << "Build AddEdgesRequest...";
  cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq(false, 1);

  auto fut = proc->getFuture();
  proc->process(req);
  auto resp = std::move(fut).get();
  EXPECT_EQ(0, resp.result.failed_parts.size());
  EXPECT_EQ(0, numOfKey(req, gTestUtil.genKey, env));
  EXPECT_EQ(334, numOfKey(req, gTestUtil.genPrime, env));
  EXPECT_EQ(0, numOfKey(req, gTestUtil.genDoublePrime, env));

  env->txnMan_->scanPrimes(1, 1);

  auto* iClient = FakeInternalStorageClient::instance(env);
  FakeInternalStorageClient::hookInternalStorageClient(env, iClient);
  ChainResumeProcessor resumeProc(env);
  resumeProc.process();

  EXPECT_EQ(334, numOfKey(req, gTestUtil.genKey, env));
  EXPECT_EQ(0, numOfKey(req, gTestUtil.genPrime, env));
  EXPECT_EQ(0, numOfKey(req, gTestUtil.genDoublePrime, env));
}

/**
 * @brief resumeTest2 (resume insert prime, remote failed)
 *                  previous    resume
 *  prepareLocal    succeed     succeed
 *  processRemote   skip        failed
 *  processLocal    skip        failed
 *  expect: edge/prime/double : 0/0/0
 *          keyOfRequest: false
 */
TEST(ChainResumeEdgesTest, resumeTest2) {
  fs::TempDir rootPath("/tmp/AddEdgesTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();

  env->metaClient_ = mClient.get();
  auto* proc = new FakeChainAddEdgesProcessorLocal(env);

  proc->rcProcessRemote = nebula::cpp2::ErrorCode::SUCCEEDED;
  proc->rcProcessLocal = nebula::cpp2::ErrorCode::SUCCEEDED;

  LOG(INFO) << "Build AddEdgesRequest...";
  cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq(false, 1);

  LOG(INFO) << "Test AddEdgesProcessor...";
  auto fut = proc->getFuture();
  proc->process(req);
  auto resp = std::move(fut).get();
  EXPECT_EQ(0, resp.result.failed_parts.size());

  ChainTestUtils util;
  EXPECT_EQ(0, numOfKey(req, util.genKey, env));
  EXPECT_EQ(334, numOfKey(req, util.genPrime, env));
  EXPECT_EQ(0, numOfKey(req, util.genDoublePrime, env));

  auto* iClient = FakeInternalStorageClient::instance(env, nebula::cpp2::ErrorCode::E_UNKNOWN);
  FakeInternalStorageClient::hookInternalStorageClient(env, iClient);
  ChainResumeProcessor resumeProc(env);
  resumeProc.process();

  EXPECT_EQ(0, numOfKey(req, util.genKey, env));
  EXPECT_EQ(334, numOfKey(req, util.genPrime, env));
  EXPECT_EQ(0, numOfKey(req, util.genDoublePrime, env));
}

/**
 * @brief resumePrimeTest3 (resume insert prime outdated)
 */
TEST(ChainResumeEdgesTest, resumePrimeTest3) {
  fs::TempDir rootPath("/tmp/AddEdgesTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();

  env->metaClient_ = mClient.get();
  auto* proc = new FakeChainAddEdgesProcessorLocal(env);

  proc->rcProcessRemote = nebula::cpp2::ErrorCode::SUCCEEDED;
  proc->rcProcessLocal = nebula::cpp2::ErrorCode::SUCCEEDED;

  LOG(INFO) << "Build AddEdgesRequest...";
  cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq(false, 1);

  LOG(INFO) << "Test AddEdgesProcessor...";
  auto fut = proc->getFuture();
  proc->process(req);
  auto resp = std::move(fut).get();
  EXPECT_EQ(0, resp.result.failed_parts.size());

  ChainTestUtils util;
  EXPECT_EQ(0, numOfKey(req, util.genKey, env));
  EXPECT_EQ(334, numOfKey(req, util.genPrime, env));
  EXPECT_EQ(0, numOfKey(req, util.genDoublePrime, env));

  auto error = nebula::cpp2::ErrorCode::E_RPC_FAILURE;
  auto* iClient = FakeInternalStorageClient::instance(env, error);
  FakeInternalStorageClient::hookInternalStorageClient(env, iClient);
  env->txnMan_->scanPrimes(1, 1);
  ChainResumeProcessor resumeProc(env);
  resumeProc.process();

  // none of really edge key should be inserted
  EXPECT_EQ(334, numOfKey(req, util.genKey, env));
  EXPECT_EQ(0, numOfKey(req, util.genPrime, env));
  EXPECT_EQ(334, numOfKey(req, util.genDoublePrime, env));
}

/**
 * @brief resumeTest4 (resume double prime, resume failed)
 *                  insert      resume
 *  prepareLocal    succeed     succeed
 *  processRemote   outdate     failed
 *  processLocal    succeed     succeed
 *  expect: edge/prime/double : 334/0/334
 *          keyOfRequest: true
 */
TEST(ChainResumeEdgesTest, resumeTest4) {
  fs::TempDir rootPath("/tmp/AddEdgesTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();

  env->metaClient_ = mClient.get();
  auto* proc = new FakeChainAddEdgesProcessorLocal(env);

  proc->rcProcessRemote = nebula::cpp2::ErrorCode::E_RPC_FAILURE;

  LOG(INFO) << "Build AddEdgesRequest...";
  int partNum = 1;
  cpp2::AddEdgesRequest req = mock::MockData::mockAddEdgesReq(false, partNum);

  LOG(INFO) << "Test AddEdgesProcessor...";
  auto fut = proc->getFuture();
  proc->process(req);
  auto resp = std::move(fut).get();
  EXPECT_EQ(0, resp.result.failed_parts.size());

  // ChainTestUtils util;
  EXPECT_EQ(334, numOfKey(req, gTestUtil.genKey, env));
  EXPECT_EQ(0, numOfKey(req, gTestUtil.genPrime, env));
  EXPECT_EQ(334, numOfKey(req, gTestUtil.genDoublePrime, env));

  auto error = nebula::cpp2::ErrorCode::E_UNKNOWN;
  auto* iClient = FakeInternalStorageClient::instance(env, error);
  FakeInternalStorageClient::hookInternalStorageClient(env, iClient);
  ChainResumeProcessor resumeProc(env);
  resumeProc.process();

  EXPECT_EQ(334, numOfKey(req, gTestUtil.genKey, env));
  EXPECT_EQ(0, numOfKey(req, gTestUtil.genPrime, env));
  EXPECT_EQ(334, numOfKey(req, gTestUtil.genDoublePrime, env));
}

/**
 * @brief resumeTest5 (resume double prime, but outdated)
 */
TEST(ChainResumeEdgesTest, resumeTest5) {
  fs::TempDir rootPath("/tmp/AddEdgesTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();

  env->metaClient_ = mClient.get();
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
  EXPECT_EQ(334, numOfKey(req, util.genKey, env));
  EXPECT_EQ(0, numOfKey(req, util.genPrime, env));
  EXPECT_EQ(334, numOfKey(req, util.genDoublePrime, env));

  auto error = nebula::cpp2::ErrorCode::E_RPC_FAILURE;
  auto* iClient = FakeInternalStorageClient::instance(env, error);
  FakeInternalStorageClient::hookInternalStorageClient(env, iClient);
  ChainResumeProcessor resumeProc(env);
  resumeProc.process();

  EXPECT_EQ(334, numOfKey(req, util.genKey, env));
  EXPECT_EQ(0, numOfKey(req, util.genPrime, env));
  EXPECT_EQ(334, numOfKey(req, util.genDoublePrime, env));
}

/**
 * @brief resumeTest6 (resume add edge double prime, succeeded)
 */
TEST(ChainResumeEdgesTest, resumeTest6) {
  fs::TempDir rootPath("/tmp/AddEdgesTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();

  env->metaClient_ = mClient.get();
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
  EXPECT_EQ(334, numOfKey(req, util.genKey, env));
  EXPECT_EQ(0, numOfKey(req, util.genPrime, env));
  EXPECT_EQ(334, numOfKey(req, util.genDoublePrime, env));

  auto* iClient = FakeInternalStorageClient::instance(env);
  FakeInternalStorageClient::hookInternalStorageClient(env, iClient);
  env->txnMan_->scanPrimes(1, 1);
  ChainResumeProcessor resumeProc(env);
  resumeProc.process();

  EXPECT_EQ(334, numOfKey(req, util.genKey, env));
  EXPECT_EQ(0, numOfKey(req, util.genPrime, env));
  EXPECT_EQ(0, numOfKey(req, util.genDoublePrime, env));
}

// resume an update left prime, check resume succeeded
TEST(ChainUpdateEdgeTest, resumeTest7) {
  fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();
  env->metaClient_ = mClient.get();

  auto parts = cluster.getTotalParts();
  EXPECT_TRUE(QueryTestUtils::mockEdgeData(env, parts, mockSpaceVidLen));

  LOG(INFO) << "Test UpdateEdgeRequest...";
  auto req = helper.makeDefaultRequest();

  LOG(INFO) << "Fake Prime...";
  auto* proc = new FakeChainUpdateProcessor(env);
  auto f = proc->getFuture();
  proc->rcProcessRemote = Code::SUCCEEDED;
  proc->rcProcessLocal = Code::SUCCEEDED;
  proc->process(req);
  LOG(INFO) << "addUnfinishedEdge()";
  proc->wrapAddUnfinishedEdge(ResumeType::RESUME_CHAIN);
  auto resp = std::move(f).get();

  EXPECT_FALSE(helper.checkRequestUpdated(env, req));
  EXPECT_TRUE(helper.edgeExist(env, req));
  EXPECT_TRUE(helper.primeExist(env, req));
  EXPECT_FALSE(helper.doublePrimeExist(env, req));

  auto* iClient = FakeInternalStorageClient::instance(env);
  FakeInternalStorageClient::hookInternalStorageClient(env, iClient);
  env->txnMan_->scanPrimes(1, 1);
  ChainResumeProcessor resumeProc(env);
  resumeProc.process();

  EXPECT_TRUE(helper.edgeExist(env, req));
  EXPECT_FALSE(helper.primeExist(env, req));
  EXPECT_FALSE(helper.doublePrimeExist(env, req));
}

// resume an update left prime, resume failed
TEST(ChainUpdateEdgeTest, resumeTest8) {
  fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();
  env->metaClient_ = mClient.get();

  auto parts = cluster.getTotalParts();
  EXPECT_TRUE(QueryTestUtils::mockEdgeData(env, parts, mockSpaceVidLen));

  LOG(INFO) << "Test UpdateEdgeRequest...";
  auto req = helper.makeDefaultRequest();

  LOG(INFO) << "Fake Prime...";
  auto* proc = new FakeChainUpdateProcessor(env);
  auto f = proc->getFuture();
  proc->rcProcessRemote = Code::SUCCEEDED;
  proc->rcProcessLocal = Code::SUCCEEDED;
  proc->process(req);
  auto resp = std::move(f).get();

  // EXPECT_TRUE(helper.checkResp(req, resp));
  EXPECT_FALSE(helper.checkRequestUpdated(env, req));
  EXPECT_TRUE(helper.edgeExist(env, req));
  EXPECT_TRUE(helper.primeExist(env, req));
  EXPECT_FALSE(helper.doublePrimeExist(env, req));

  auto* iClient = FakeInternalStorageClient::instance(env);
  iClient->setErrorCode(Code::E_UNKNOWN);
  FakeInternalStorageClient::hookInternalStorageClient(env, iClient);
  ChainResumeProcessor resumeProc(env);
  resumeProc.process();

  EXPECT_TRUE(helper.edgeExist(env, req));
  EXPECT_TRUE(helper.primeExist(env, req));
  EXPECT_FALSE(helper.doublePrimeExist(env, req));
}

// resume an update left prime, resume outdated
TEST(ChainUpdateEdgeTest, resumeTest9) {
  fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();
  env->metaClient_ = mClient.get();

  auto parts = cluster.getTotalParts();
  EXPECT_TRUE(QueryTestUtils::mockEdgeData(env, parts, mockSpaceVidLen));

  LOG(INFO) << "Test UpdateEdgeRequest...";
  auto req = helper.makeDefaultRequest();

  LOG(INFO) << "Fake Prime...";
  auto* proc = new FakeChainUpdateProcessor(env);
  auto f = proc->getFuture();
  proc->rcProcessRemote = Code::SUCCEEDED;
  proc->rcProcessLocal = Code::SUCCEEDED;
  proc->process(req);
  proc->wrapAddUnfinishedEdge(ResumeType::RESUME_CHAIN);
  auto resp = std::move(f).get();

  // EXPECT_TRUE(helper.checkResp(req, resp));
  EXPECT_FALSE(helper.checkRequestUpdated(env, req));
  EXPECT_TRUE(helper.edgeExist(env, req));
  EXPECT_TRUE(helper.primeExist(env, req));
  EXPECT_FALSE(helper.doublePrimeExist(env, req));

  auto* iClient = FakeInternalStorageClient::instance(env);
  iClient->setErrorCode(Code::E_RPC_FAILURE);
  FakeInternalStorageClient::hookInternalStorageClient(env, iClient);
  ChainResumeProcessor resumeProc(env);
  resumeProc.process();

  EXPECT_TRUE(helper.edgeExist(env, req));
  EXPECT_FALSE(helper.primeExist(env, req));
  EXPECT_TRUE(helper.doublePrimeExist(env, req));
}

// resume an update left prime, check resume succeeded
TEST(ChainUpdateEdgeTest, resumeTest10) {
  fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();
  env->metaClient_ = mClient.get();

  auto parts = cluster.getTotalParts();
  EXPECT_TRUE(QueryTestUtils::mockEdgeData(env, parts, mockSpaceVidLen));

  LOG(INFO) << "Test UpdateEdgeRequest...";
  auto req = helper.makeDefaultRequest();

  LOG(INFO) << "Fake Prime...";
  auto* proc = new FakeChainUpdateProcessor(env);
  auto f = proc->getFuture();
  proc->rcProcessRemote = Code::E_RPC_FAILURE;
  // proc->rcProcessLocal = Code::SUCCEEDED;
  proc->process(req);
  auto resp = std::move(f).get();

  EXPECT_TRUE(helper.checkRequestUpdated(env, req));
  EXPECT_TRUE(helper.edgeExist(env, req));
  EXPECT_FALSE(helper.primeExist(env, req));
  EXPECT_TRUE(helper.doublePrimeExist(env, req));

  auto* iClient = FakeInternalStorageClient::instance(env);
  FakeInternalStorageClient::hookInternalStorageClient(env, iClient);
  ChainResumeProcessor resumeProc(env);
  resumeProc.process();

  EXPECT_TRUE(helper.edgeExist(env, req));
  EXPECT_FALSE(helper.primeExist(env, req));
  EXPECT_FALSE(helper.doublePrimeExist(env, req));
}

// resume an update left prime, resume failed
TEST(ChainUpdateEdgeTest, resumeTest11) {
  fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();
  env->metaClient_ = mClient.get();

  auto parts = cluster.getTotalParts();
  EXPECT_TRUE(QueryTestUtils::mockEdgeData(env, parts, mockSpaceVidLen));

  LOG(INFO) << "Test UpdateEdgeRequest...";
  auto req = helper.makeDefaultRequest();

  LOG(INFO) << "Fake Prime...";
  auto* proc = new FakeChainUpdateProcessor(env);
  auto f = proc->getFuture();
  proc->rcProcessRemote = Code::E_RPC_FAILURE;
  // proc->rcProcessLocal = Code::SUCCEEDED;
  proc->process(req);
  auto resp = std::move(f).get();

  EXPECT_TRUE(helper.checkRequestUpdated(env, req));
  EXPECT_TRUE(helper.edgeExist(env, req));
  EXPECT_FALSE(helper.primeExist(env, req));
  EXPECT_TRUE(helper.doublePrimeExist(env, req));

  auto* iClient = FakeInternalStorageClient::instance(env);
  iClient->setErrorCode(Code::E_UNKNOWN);
  FakeInternalStorageClient::hookInternalStorageClient(env, iClient);
  ChainResumeProcessor resumeProc(env);
  resumeProc.process();

  EXPECT_TRUE(helper.edgeExist(env, req));
  EXPECT_FALSE(helper.primeExist(env, req));
  EXPECT_TRUE(helper.doublePrimeExist(env, req));
}

// resume an update left prime, resume outdated
TEST(ChainUpdateEdgeTest, resumeTest12) {
  fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();
  env->metaClient_ = mClient.get();

  auto parts = cluster.getTotalParts();
  EXPECT_TRUE(QueryTestUtils::mockEdgeData(env, parts, mockSpaceVidLen));

  LOG(INFO) << "Test UpdateEdgeRequest...";
  auto req = helper.makeDefaultRequest();

  LOG(INFO) << "Fake Prime...";
  auto* proc = new FakeChainUpdateProcessor(env);
  auto f = proc->getFuture();
  proc->rcProcessRemote = Code::E_RPC_FAILURE;
  // proc->rcProcessLocal = Code::SUCCEEDED;
  proc->process(req);
  auto resp = std::move(f).get();

  EXPECT_TRUE(helper.checkRequestUpdated(env, req));
  EXPECT_TRUE(helper.edgeExist(env, req));
  EXPECT_FALSE(helper.primeExist(env, req));
  EXPECT_TRUE(helper.doublePrimeExist(env, req));

  auto* iClient = FakeInternalStorageClient::instance(env);
  iClient->setErrorCode(Code::E_RPC_FAILURE);
  FakeInternalStorageClient::hookInternalStorageClient(env, iClient);
  ChainResumeProcessor resumeProc(env);
  resumeProc.process();

  EXPECT_TRUE(helper.edgeExist(env, req));
  EXPECT_FALSE(helper.primeExist(env, req));
  EXPECT_TRUE(helper.doublePrimeExist(env, req));
}
}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}

/**
 * @brief resumeTest1 (prime add edge can be resumed)
 *                  insert      resume
 *  prepareLocal    succeed     succeed
 *  processRemote   skip        succeed
 *  processLocal    skip        succeed
 *  expect: edge/prime/double : 334/0/0
 *          keyOfRequest: false
 */

/**
 * @brief resumeTest2 (double prime add edge can be resumed)
 *                  insert      resume
 *  prepareLocal    succeed     succeed
 *  processRemote   skip        failed
 *  processLocal    skip        failed
 *  expect: edge/prime/double : 0/0/0
 *          keyOfRequest: false
 */

/**
 * @brief resumePrimeTest3
 *                  insert      resume
 *  prepareLocal    succeed     succeed
 *  processRemote   skip        outdate
 *  processLocal    skip        succeed
 *  expect: edge/prime/double : 334/0/334
 *          keyOfRequest: true
 */

/**
 * @brief resumeTest4
 *                  insert      resume
 *  prepareLocal    succeed     succeed
 *  processRemote   outdate     failed
 *  processLocal    succeed     succeed
 *  expect: edge/prime/double : 334/0/334
 *          keyOfRequest: true
 */

/**
 * @brief resumeTest5
 *                  insert      resume
 *  prepareLocal    succeed     succeed
 *  processRemote   outdate     outdate
 *  processLocal    succeed     succeed
 *  expect: edge/prime/double : 334/0/334
 *          keyOfRequest: true
 */

/**
 * @brief resumeTest6
 *                  insert      resume
 *  prepareLocal    succeed     succeed
 *  processRemote   outdate     succeed
 *  processLocal    succeed     succeed
 *  expect: edge/prime/double : 334/0/0
 *          keyOfRequest: false
 */
