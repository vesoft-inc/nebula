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
#include "storage/mutate/UpdateEdgeProcessor.h"
#include "storage/test/ChainTestUtils.h"
#include "storage/test/QueryTestUtils.h"
#include "storage/test/TestUtils.h"
#include "storage/transaction/ChainAddEdgesGroupProcessor.h"
#include "storage/transaction/ChainAddEdgesProcessorLocal.h"
#include "storage/transaction/ChainResumeProcessor.h"
#include "storage/transaction/ChainUpdateEdgeProcessorRemote.h"
#include "storage/transaction/ConsistUtil.h"

namespace nebula {
namespace storage {

// using Code = ::nebula::cpp2::ErrorCode;

constexpr int32_t mockSpaceId = 1;
constexpr int32_t mockPartNum = 6;
constexpr int32_t mockSpaceVidLen = 32;

ChainTestUtils gTestUtil;

ChainUpdateEdgeTestHelper helper;
TEST(ChainUpdateEdgeTest, updateTest1) {
  fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();
  env->metaClient_ = mClient.get();

  auto parts = cluster.getTotalParts();
  EXPECT_TRUE(QueryTestUtils::mockEdgeData(env, parts, mockSpaceVidLen));

  LOG(INFO) << "Test updateTest1...";
  auto req = helper.makeDefaultRequest();

  env->txnMan_->iClient_ = FakeInternalStorageClient::instance(env);
  auto reversedRequest = helper.reverseRequest(env, req);

  auto* proc = new FakeChainUpdateProcessor(env);
  LOG(INFO) << "proc: " << proc;
  auto f = proc->getFuture();
  proc->process(req);
  auto resp = std::move(f).get();

  EXPECT_TRUE(helper.checkResp2(resp));
  EXPECT_TRUE(helper.checkRequestUpdated(env, req));
  EXPECT_TRUE(helper.checkRequestUpdated(env, reversedRequest));
  EXPECT_TRUE(helper.edgeExist(env, req));
  EXPECT_FALSE(helper.primeExist(env, req));
  EXPECT_FALSE(helper.doublePrimeExist(env, req));
}

TEST(ChainUpdateEdgeTest, updateTest2) {
  fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();
  env->metaClient_ = mClient.get();

  auto parts = cluster.getTotalParts();
  EXPECT_TRUE(QueryTestUtils::mockEdgeData(env, parts, mockSpaceVidLen));

  LOG(INFO) << "Test UpdateEdgeRequest...";
  auto goodRequest = helper.makeDefaultRequest();
  EXPECT_TRUE(helper.edgeExist(env, goodRequest));
  EXPECT_FALSE(helper.primeExist(env, goodRequest));
  EXPECT_FALSE(helper.doublePrimeExist(env, goodRequest));

  auto badRequest = helper.makeInvalidRequest();

  auto* proc = new FakeChainUpdateProcessor(env);
  auto f = proc->getFuture();
  proc->rcProcessRemote = Code::E_KEY_NOT_FOUND;
  proc->process(badRequest);
  auto resp = std::move(f).get();

  EXPECT_EQ(1, (*resp.result_ref()).failed_parts.size());
  EXPECT_FALSE(helper.checkResp2(resp));
  EXPECT_FALSE(helper.edgeExist(env, badRequest));
  EXPECT_FALSE(helper.primeExist(env, badRequest));
  EXPECT_FALSE(helper.doublePrimeExist(env, badRequest));
}

TEST(ChainUpdateEdgeTest, updateTest3) {
  fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();
  env->metaClient_ = mClient.get();

  auto parts = cluster.getTotalParts();
  EXPECT_TRUE(QueryTestUtils::mockEdgeData(env, parts, mockSpaceVidLen));

  LOG(INFO) << "Test UpdateEdgeRequest...";
  auto goodRequest = helper.makeDefaultRequest();
  EXPECT_TRUE(helper.edgeExist(env, goodRequest));
  EXPECT_FALSE(helper.primeExist(env, goodRequest));
  EXPECT_FALSE(helper.doublePrimeExist(env, goodRequest));

  auto* proc = new FakeChainUpdateProcessor(env);
  auto f = proc->getFuture();
  proc->rcProcessRemote = Code::SUCCEEDED;
  proc->rcProcessLocal = Code::SUCCEEDED;
  proc->process(goodRequest);
  auto resp = std::move(f).get();

  EXPECT_TRUE(helper.edgeExist(env, goodRequest));
  EXPECT_TRUE(helper.primeExist(env, goodRequest));
  EXPECT_FALSE(helper.doublePrimeExist(env, goodRequest));
}

TEST(ChainUpdateEdgeTest, updateTest4) {
  fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto mClient = MetaClientTestUpdater::makeDefaultMetaClient();
  env->metaClient_ = mClient.get();

  auto parts = cluster.getTotalParts();
  EXPECT_TRUE(QueryTestUtils::mockEdgeData(env, parts, mockSpaceVidLen));

  LOG(INFO) << "Test UpdateEdgeRequest...";
  auto goodRequest = helper.makeDefaultRequest();
  EXPECT_TRUE(helper.edgeExist(env, goodRequest));
  EXPECT_FALSE(helper.primeExist(env, goodRequest));
  EXPECT_FALSE(helper.doublePrimeExist(env, goodRequest));

  auto* proc = new FakeChainUpdateProcessor(env);
  auto f = proc->getFuture();
  proc->rcProcessRemote = Code::E_RPC_FAILURE;
  proc->process(goodRequest);
  auto resp = std::move(f).get();

  EXPECT_TRUE(helper.edgeExist(env, goodRequest));
  EXPECT_FALSE(helper.primeExist(env, goodRequest));
  EXPECT_TRUE(helper.doublePrimeExist(env, goodRequest));
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}

//              ***** Test Plan *****
/**
 * @brief updateTest1 (update a normal edge will succeed)
 *                  previous      update
 *  prepareLocal    succeed         succeed
 *  processRemote   succeed         succeed
 *  processLocal    succeed         succeed
 *  expect: edge            true
 *          edge prime      false
 *          double prime    false
 *          prop changed    true
 */

/**
 * @brief updateTest2 (update non-exist edge will fail)
 *                  previous      update
 *  prepareLocal    failed      succeed
 *  processRemote   skip        succeed
 *  processLocal    failed      succeed
 *  expect: edge            false
 *          edge prime      false
 *          double prime    false
 *          prop changed    true
 */

/**
 * @brief updateTest3 (remote update failed will not change anything)
 *                  previous      update
 *  prepareLocal    succeed     succeed
 *  processRemote   skip        failed
 *  processLocal    skip        failed
 *  expect: edge            true
 *          edge prime      true
 *          double prime    false
 *          prop changed    false
 */

/**
 * @brief updateTest4 (remote update outdate will add double prime)
 *                  previous      update
 *  prepareLocal    succeed     succeed
 *  processRemote   skip        outdate
 *  processLocal    skip        succeed
 *  expect: edge            true
 *          edge prime      false
 *          double prime    true
 *          prop changed    false
 */

// /**
//  * @brief updateTest5 (update1 + resume)
//  *                  previous      update
//  *  prepareLocal    succeed     succeed
//  *  processRemote   skip        succeed
//  *  processLocal    succeed     succeed
//  *  expect: edge            true
//  *          edge prime      false
//  *          double prime    false
//  *          prop changed    true
//  */

// /**
//  * @brief updateTest6 (update2 + resume)
//  *                  previous      update
//  *  prepareLocal    failed     succeed
//  *  processRemote   skip       succeed
//  *  processLocal    failed     succeed
//  *  expect: edge            false
//  *          edge prime      false
//  *          double prime    false
//  *          prop changed    true
//  */

// /**
//  * @brief updateTest7 (updateTest3 + resume)
//  *                  previous      resume
//  *  prepareLocal    succeed     succeed
//  *  processRemote   skip        failed
//  *  processLocal    skip        failed
//  *  expect: edge            true
//  *          edge prime      true
//  *          double prime    false
//  *          prop changed    false
//  */

// /**
//  * @brief updateTest8
//  *                  previous      resume
//  *  prepareLocal    succeed     succeed
//  *  processRemote   skip        outdate
//  *  processLocal    skip        succeed
//  *  expect: edge            true
//  *          edge prime      false
//  *          double prime    true
//  *          prop changed    false
//  */

//              ***** End Test Plan *****
