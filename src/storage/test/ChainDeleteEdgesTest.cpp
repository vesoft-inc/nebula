/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/Format.h>
#include <folly/String.h>
#include <folly/container/Enumerate.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <thrift/lib/cpp/util/EnumUtils.h>

#include <optional>

#include "common/fs/TempDir.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "storage/CommonUtils.h"
#include "storage/StorageFlags.h"
#include "storage/test/ChainTestUtils.h"
#include "storage/test/TestUtils.h"
#include "storage/transaction/ChainDeleteEdgesGroupProcessor.h"
#include "storage/transaction/ChainDeleteEdgesLocalProcessor.h"
#include "storage/transaction/ConsistUtil.h"

namespace nebula {
namespace storage {

constexpr int32_t mockSpaceId = 1;
constexpr int32_t mockPartNum = 1;
constexpr int32_t gTerm = 1;

class FakeChainDeleteEdgesProcessor : public ChainDeleteEdgesLocalProcessor {
 public:
  explicit FakeChainDeleteEdgesProcessor(StorageEnv* env);
  folly::SemiFuture<Code> prepareLocal() override;
  folly::SemiFuture<Code> processRemote(Code code) override;
  folly::SemiFuture<Code> processLocal(Code code) override;

  cpp2::DeleteEdgesRequest makeDelRequest(cpp2::AddEdgesRequest,
                                          int32_t limit = std::numeric_limits<int>::max());

 public:
  std::optional<Code> rcPrepareLocal;
  std::optional<Code> rcProcessRemote;
  std::optional<Code> rcProcessLocal;
};

// make sure test utils works
TEST(ChainDeleteEdgesTest, TestUtilsTest) {
  fs::TempDir rootPath("/tmp/DeleteEdgesTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto mClient = MetaClientTestUpdater::makeDefault();
  env->metaClient_ = mClient.get();
  MetaClientTestUpdater::addPartTerm(env->metaClient_, mockSpaceId, mockPartNum, gTerm);

  auto* proc = new FakeChainDeleteEdgesProcessor(env);

  proc->rcPrepareLocal = nebula::cpp2::ErrorCode::SUCCEEDED;
  proc->rcProcessRemote = nebula::cpp2::ErrorCode::SUCCEEDED;
  proc->rcProcessLocal = nebula::cpp2::ErrorCode::SUCCEEDED;

  LOG(INFO) << "Build DeleteEdgesReq...";
  auto req = mock::MockData::mockDeleteEdgesReq(mockPartNum);

  LOG(INFO) << "Run DeleteEdgesReq...";
  auto fut = proc->getFuture();
  proc->process(req);
  auto resp = std::move(fut).get();

  LOG(INFO) << "Check data in kv store...";
}

// delete a not exist edge
TEST(ChainDeleteEdgesTest, Test2) {
  fs::TempDir rootPath("/tmp/DeleteEdgesTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto mClient = MetaClientTestUpdater::makeDefault();
  env->metaClient_ = mClient.get();
  MetaClientTestUpdater::addPartTerm(env->metaClient_, mockSpaceId, mockPartNum, gTerm);

  auto* proc = new FakeChainDeleteEdgesProcessor(env);

  proc->rcProcessRemote = nebula::cpp2::ErrorCode::SUCCEEDED;

  LOG(INFO) << "Build DeleteEdgesReq...";
  auto req = mock::MockData::mockDeleteEdgesReq(mockPartNum);

  LOG(INFO) << "Run DeleteEdgesReq...";
  LOG(INFO) << "spaceId = " << req.get_space_id();
  auto fut = proc->getFuture();
  proc->process(req);
  auto resp = std::move(fut).get();

  // we need this sleep to ensure processor deleted before transaction manager
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
}

// add some edges, then delete it, all phase succeed
TEST(ChainDeleteEdgesTest, Test3) {
  fs::TempDir rootPath("/tmp/DeleteEdgesTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto mClient = MetaClientTestUpdater::makeDefault();
  env->metaClient_ = mClient.get();
  MetaClientTestUpdater::addPartTerm(env->metaClient_, mockSpaceId, mockPartNum, gTerm);

  auto* addProc = new FakeChainAddEdgesLocalProcessor(env);
  addProc->rcProcessRemote = nebula::cpp2::ErrorCode::SUCCEEDED;

  bool upperPropVal = false;
  int32_t partNum = 1;
  bool hasInEdges = false;
  auto addReq = mock::MockData::mockAddEdgesReq(upperPropVal, partNum, hasInEdges);

  LOG(INFO) << "Run FakeChainAddEdgesLocalProcessor...";
  auto fut = addProc->getFuture();
  addProc->process(addReq);
  auto resp = std::move(fut).get();

  ChainTestUtils util;
  auto edgeKeys = util.genEdgeKeys(addReq, util.genKey);
  auto num = util.checkNumOfKey(env, mockSpaceId, edgeKeys);
  EXPECT_EQ(num, 167);
  LOG(INFO) << "after add(), edge num = " << num;

  auto* delProc = new FakeChainDeleteEdgesProcessor(env);
  auto delReq = delProc->makeDelRequest(addReq);
  delProc->rcProcessRemote = nebula::cpp2::ErrorCode::SUCCEEDED;

  LOG(INFO) << "Run DeleteEdgesReq...";
  auto futDel = delProc->getFuture();
  delProc->process(delReq);
  std::move(futDel).get();

  num = util.checkNumOfKey(env, mockSpaceId, edgeKeys);
  LOG(INFO) << "after del(), edge num = " << num;
  EXPECT_EQ(num, 0);
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
}

// add some edges, then delete one of them, all phase succeed
TEST(ChainDeleteEdgesTest, Test4) {
  fs::TempDir rootPath("/tmp/DeleteEdgesTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto mClient = MetaClientTestUpdater::makeDefault();
  env->metaClient_ = mClient.get();
  MetaClientTestUpdater::addPartTerm(env->metaClient_, mockSpaceId, mockPartNum, gTerm);

  auto* addProc = new FakeChainAddEdgesLocalProcessor(env);
  addProc->rcProcessRemote = nebula::cpp2::ErrorCode::SUCCEEDED;

  bool upperPropVal = false;
  int32_t partNum = 1;
  bool hasInEdges = false;
  auto addReq = mock::MockData::mockAddEdgesReq(upperPropVal, partNum, hasInEdges);

  LOG(INFO) << "Run FakeChainAddEdgesLocalProcessor...";
  auto fut = addProc->getFuture();
  addProc->process(addReq);
  auto resp = std::move(fut).get();

  ChainTestUtils util;
  auto edgeKeys = util.genEdgeKeys(addReq, util.genKey);
  auto num = util.checkNumOfKey(env, mockSpaceId, edgeKeys);
  EXPECT_EQ(num, 167);
  LOG(INFO) << "after add(), edge num = " << num;

  auto* delProc = new FakeChainDeleteEdgesProcessor(env);
  int32_t limit = 1;
  auto delReq = delProc->makeDelRequest(addReq, limit);
  delProc->rcProcessRemote = nebula::cpp2::ErrorCode::SUCCEEDED;

  LOG(INFO) << "Run DeleteEdgesReq...";
  auto futDel = delProc->getFuture();
  delProc->process(delReq);
  std::move(futDel).get();

  num = util.checkNumOfKey(env, mockSpaceId, edgeKeys);
  LOG(INFO) << "after del(), edge num = " << num;
  EXPECT_EQ(num, 166);
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
}

// add some edges, then delete one of them, not execute local commit
TEST(ChainDeleteEdgesTest, DISABLED_Test5) {
  fs::TempDir rootPath("/tmp/DeleteEdgesTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto mClient = MetaClientTestUpdater::makeDefault();
  env->metaClient_ = mClient.get();
  MetaClientTestUpdater::addPartTerm(env->metaClient_, mockSpaceId, mockPartNum, gTerm);

  auto* addProc = new FakeChainAddEdgesLocalProcessor(env);
  addProc->rcProcessRemote = nebula::cpp2::ErrorCode::SUCCEEDED;

  bool upperPropVal = false;
  int32_t partNum = 1;
  bool hasInEdges = false;
  auto addReq = mock::MockData::mockAddEdgesReq(upperPropVal, partNum, hasInEdges);

  LOG(INFO) << "Run FakeChainAddEdgesLocalProcessor...";
  auto fut = addProc->getFuture();
  addProc->process(addReq);
  auto resp = std::move(fut).get();

  ChainTestUtils util;
  auto edgeKeys = util.genEdgeKeys(addReq, util.genKey);
  auto num = util.checkNumOfKey(env, mockSpaceId, edgeKeys);
  EXPECT_EQ(num, 167);
  LOG(INFO) << "after add(), edge num = " << num;

  auto* delProc = new FakeChainDeleteEdgesProcessor(env);
  auto delReq = delProc->makeDelRequest(addReq);
  delProc->rcProcessRemote = nebula::cpp2::ErrorCode::SUCCEEDED;
  delProc->rcProcessLocal = nebula::cpp2::ErrorCode::SUCCEEDED;

  UPCLT iClient(FakeInternalStorageClient::instance(env, nebula::cpp2::ErrorCode::SUCCEEDED));
  FakeInternalStorageClient::hookInternalStorageClient(env, iClient.get());

  LOG(INFO) << "Run DeleteEdgesReq...";
  auto futDel = delProc->getFuture();
  delProc->process(delReq);
  std::move(futDel).get();

  num = util.checkNumOfKey(env, mockSpaceId, edgeKeys);
  LOG(INFO) << "after del(), edge num = " << num;
  EXPECT_EQ(num, 167);

  for (PartitionID i = 1; i <= partNum; ++i) {
    env->txnMan_->scanPrimes(mockSpaceId, i, 1);
  }
  env->txnMan_->stop();
  env->txnMan_->join();
  num = util.checkNumOfKey(env, mockSpaceId, edgeKeys);
  EXPECT_EQ(num, 0);
}

// add some edges, then delete all of them, not execute local commit
TEST(ChainDeleteEdgesTest, Test6) {
  fs::TempDir rootPath("/tmp/DeleteEdgesTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto mClient = MetaClientTestUpdater::makeDefault();
  env->metaClient_ = mClient.get();
  MetaClientTestUpdater::addPartTerm(env->metaClient_, mockSpaceId, mockPartNum, gTerm);

  auto* addProc = new FakeChainAddEdgesLocalProcessor(env);
  addProc->rcProcessRemote = nebula::cpp2::ErrorCode::SUCCEEDED;

  bool upperPropVal = false;
  int32_t partNum = 1;
  bool hasInEdges = false;
  auto addReq = mock::MockData::mockAddEdgesReq(upperPropVal, partNum, hasInEdges);

  LOG(INFO) << "Run FakeChainAddEdgesLocalProcessor...";
  auto fut = addProc->getFuture();
  addProc->process(addReq);
  auto resp = std::move(fut).get();

  ChainTestUtils util;
  auto edgeKeys = util.genEdgeKeys(addReq, util.genKey);
  auto num = util.checkNumOfKey(env, mockSpaceId, edgeKeys);
  EXPECT_EQ(num, 167);
  LOG(INFO) << "after add(), edge num = " << num;

  auto* delProc = new FakeChainDeleteEdgesProcessor(env);
  auto delReq = delProc->makeDelRequest(addReq);
  delProc->rcProcessRemote = nebula::cpp2::ErrorCode::SUCCEEDED;
  delProc->rcProcessLocal = nebula::cpp2::ErrorCode::SUCCEEDED;

  UPCLT iClient(FakeInternalStorageClient::instance(env, nebula::cpp2::ErrorCode::SUCCEEDED));
  FakeInternalStorageClient::hookInternalStorageClient(env, iClient.get());

  LOG(INFO) << "Run DeleteEdgesReq...";
  auto futDel = delProc->getFuture();
  delProc->process(delReq);
  std::move(futDel).get();

  num = util.checkNumOfKey(env, mockSpaceId, edgeKeys);
  LOG(INFO) << "after del(), edge num = " << num;
  EXPECT_EQ(num, 167);

  for (PartitionID i = 1; i <= partNum; ++i) {
    env->txnMan_->scanPrimes(mockSpaceId, i);
  }
  // ChainResumeProcessor resumeProc(env);
  // resumeProc.process();
  std::this_thread::sleep_for(std::chrono::seconds(2));
  sleep(1);
  env->txnMan_->stop();
  env->txnMan_->join();

  num = util.checkNumOfKey(env, mockSpaceId, edgeKeys);
  EXPECT_EQ(num, 0);
}

// add some edges, delete one of them, rpc failure
TEST(ChainDeleteEdgesTest, Test7) {
  fs::TempDir rootPath("/tmp/DeleteEdgesTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto mClient = MetaClientTestUpdater::makeDefault();
  env->metaClient_ = mClient.get();
  MetaClientTestUpdater::addPartTerm(env->metaClient_, mockSpaceId, mockPartNum, gTerm);

  auto* addProc = new FakeChainAddEdgesLocalProcessor(env);
  addProc->rcProcessRemote = nebula::cpp2::ErrorCode::SUCCEEDED;

  bool upperPropVal = false;
  int32_t partNum = 1;
  bool hasInEdges = false;
  auto addReq = mock::MockData::mockAddEdgesReq(upperPropVal, partNum, hasInEdges);

  LOG(INFO) << "Run FakeChainAddEdgesLocalProcessor...";
  auto fut = addProc->getFuture();
  addProc->process(addReq);
  auto resp = std::move(fut).get();

  ChainTestUtils util;
  auto edgeKeys = util.genEdgeKeys(addReq, util.genKey);
  auto num = util.checkNumOfKey(env, mockSpaceId, edgeKeys);
  EXPECT_EQ(num, 167);
  LOG(INFO) << "after add(), edge num = " << num;

  auto* delProc = new FakeChainDeleteEdgesProcessor(env);
  int32_t limit = 1;
  auto delReq = delProc->makeDelRequest(addReq, limit);
  delProc->rcProcessRemote = nebula::cpp2::ErrorCode::E_RPC_FAILURE;

  UPCLT iClient(FakeInternalStorageClient::instance(env, nebula::cpp2::ErrorCode::SUCCEEDED));
  FakeInternalStorageClient::hookInternalStorageClient(env, iClient.get());

  LOG(INFO) << "Run DeleteEdgesReq...";
  auto futDel = delProc->getFuture();
  delProc->process(delReq);
  std::move(futDel).get();

  num = util.checkNumOfKey(env, mockSpaceId, edgeKeys);
  LOG(INFO) << "after del(), edge num = " << num;
  EXPECT_EQ(num, 166);

  env->txnMan_->stop();
  env->txnMan_->join();

  LOG(INFO) << "after recover()";

  num = util.checkNumOfKey(env, mockSpaceId, edgeKeys);
  EXPECT_EQ(num, 166);
}

// add some edges, delete all, rpc failure
TEST(ChainDeleteEdgesTest, Test8) {
  fs::TempDir rootPath("/tmp/DeleteEdgesTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.initStorageKV(rootPath.path());
  auto* env = cluster.storageEnv_.get();
  auto mClient = MetaClientTestUpdater::makeDefault();
  env->metaClient_ = mClient.get();
  MetaClientTestUpdater::addPartTerm(env->metaClient_, mockSpaceId, mockPartNum, gTerm);

  auto* addProc = new FakeChainAddEdgesLocalProcessor(env);
  addProc->rcProcessRemote = nebula::cpp2::ErrorCode::SUCCEEDED;

  bool upperPropVal = false;
  int32_t partNum = 1;
  bool hasInEdges = false;
  auto addReq = mock::MockData::mockAddEdgesReq(upperPropVal, partNum, hasInEdges);

  LOG(INFO) << "Run FakeChainAddEdgesLocalProcessor...";
  auto fut = addProc->getFuture();
  addProc->process(addReq);
  auto resp = std::move(fut).get();

  ChainTestUtils util;
  auto edgeKeys = util.genEdgeKeys(addReq, util.genKey);
  auto num = util.checkNumOfKey(env, mockSpaceId, edgeKeys);
  EXPECT_EQ(num, 167);
  LOG(INFO) << "after add(), edge num = " << num;

  auto* delProc = new FakeChainDeleteEdgesProcessor(env);
  int32_t limit = num;
  auto delReq = delProc->makeDelRequest(addReq, limit);
  delProc->rcProcessRemote = nebula::cpp2::ErrorCode::E_RPC_FAILURE;

  LOG(INFO) << "Run DeleteEdgesReq...";
  auto futDel = delProc->getFuture();
  delProc->process(delReq);
  std::move(futDel).get();

  num = util.checkNumOfKey(env, mockSpaceId, edgeKeys);
  LOG(INFO) << "after del(), edge num = " << num;
  EXPECT_EQ(num, 0);

  // for (PartitionID i = 1; i <= partNum; ++i) {
  //   env->txnMan_->scanPrimes(mockSpaceId, i);
  // }
  UPCLT iClient(FakeInternalStorageClient::instance(env, nebula::cpp2::ErrorCode::SUCCEEDED));
  FakeInternalStorageClient::hookInternalStorageClient(env, iClient.get());
  // ChainResumeProcessor resumeProc(env);
  // resumeProc.process();

  env->txnMan_->stop();
  env->txnMan_->join();
  num = util.checkNumOfKey(env, mockSpaceId, edgeKeys);
  EXPECT_EQ(num, 0);
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

namespace nebula {
namespace storage {

FakeChainDeleteEdgesProcessor::FakeChainDeleteEdgesProcessor(StorageEnv* env)
    : ChainDeleteEdgesLocalProcessor(env) {
  spaceVidLen_ = 32;
}

folly::SemiFuture<Code> FakeChainDeleteEdgesProcessor::prepareLocal() {
  LOG(INFO) << "FakeChainDeleteEdgesProcessor::" << __func__ << "()";
  if (rcPrepareLocal) {
    LOG(INFO) << "Fake return " << apache::thrift::util::enumNameSafe(*rcPrepareLocal);
    return *rcPrepareLocal;
  }
  LOG(INFO) << "forward to ChainDeleteEdgesLocalProcessor::prepareLocal()";
  return ChainDeleteEdgesLocalProcessor::prepareLocal();
}

folly::SemiFuture<Code> FakeChainDeleteEdgesProcessor::processRemote(Code code) {
  LOG(INFO) << "FakeChainDeleteEdgesProcessor::" << __func__ << "()";
  if (rcProcessRemote) {
    LOG(INFO) << "processRemote() fake return "
              << apache::thrift::util::enumNameSafe(*rcProcessRemote);
    return *rcProcessRemote;
  }
  LOG(INFO) << "forward to ChainDeleteEdgesLocalProcessor::processRemote()";
  return ChainDeleteEdgesLocalProcessor::processRemote(code);
}

folly::SemiFuture<Code> FakeChainDeleteEdgesProcessor::processLocal(Code code) {
  LOG(INFO) << "FakeChainDeleteEdgesProcessor::" << __func__ << "()";
  if (rcProcessLocal) {
    LOG(INFO) << "Fake return " << apache::thrift::util::enumNameSafe(*rcProcessLocal);
    return *rcProcessLocal;
  }
  LOG(INFO) << "forward to ChainDeleteEdgesLocalProcessor::processLocal()";
  return ChainDeleteEdgesLocalProcessor::processLocal(code);
}

// make DeleteEdgesRequest according to an AddEdgesRequest
cpp2::DeleteEdgesRequest FakeChainDeleteEdgesProcessor::makeDelRequest(cpp2::AddEdgesRequest req,
                                                                       int32_t limit) {
  cpp2::DeleteEdgesRequest ret;
  int32_t num = 0;
  // ret.set_space_id(req.get_space_id());
  ret.space_id_ref() = req.get_space_id();
  for (auto& partAndEdges : req.get_parts()) {
    auto partId = partAndEdges.first;
    for (auto& newEdge : partAndEdges.second) {
      ret.parts_ref().value()[partId].emplace_back(newEdge.get_key());
      if (++num == limit) {
        break;
      }
    }
    if (num == limit) {
      break;
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace nebula
