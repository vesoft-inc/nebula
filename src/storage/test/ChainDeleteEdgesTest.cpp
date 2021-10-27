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

class GlobalCluster {
 public:
  static mock::MockCluster* get() {
    static mock::MockCluster cluster;
    static fs::TempDir rootPath("/tmp/DeleteEdgesTest.XXXXXX");
    static bool init = false;
    if (!init) {
      cluster.initStorageKV(rootPath.path());
      init = true;
    }
    return &cluster;
  }
};

// class FakeChainDeleteEdgesProcessor;
class FakeChainDeleteEdgesProcessor : public ChainDeleteEdgesLocalProcessor {
 public:
  explicit FakeChainDeleteEdgesProcessor(StorageEnv* env);
  folly::SemiFuture<Code> prepareLocal() override;
  folly::SemiFuture<Code> processRemote(Code code) override;
  folly::SemiFuture<Code> processLocal(Code code) override;

  cpp2::DeleteEdgesRequest makeDelRequest(cpp2::AddEdgesRequest,
                                          int32_t limit = std::numeric_limits<int>::max());

 public:
  folly::Optional<Code> rcPrepareLocal;
  folly::Optional<Code> rcProcessRemote;
  folly::Optional<Code> rcProcessLocal;
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
  // EXPECT_EQ(0, resp.result.failed_parts.size());

  LOG(INFO) << "Check data in kv store...";
  // sleep(1);
  // The number of data in serve is 334
  // checkAddEdgesData(req, env, 0, 0);
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

  // proc->rcPrepareLocal = nebula::cpp2::ErrorCode::SUCCEEDED;
  proc->rcProcessRemote = nebula::cpp2::ErrorCode::SUCCEEDED;
  // proc->rcProcessLocal = nebula::cpp2::ErrorCode::SUCCEEDED;

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
  // delProc->rcPrepareLocal = nebula::cpp2::ErrorCode::SUCCEEDED;
  delProc->rcProcessRemote = nebula::cpp2::ErrorCode::SUCCEEDED;
  // delProc->rcProcessLocal = nebula::cpp2::ErrorCode::SUCCEEDED;

  LOG(INFO) << "Build DeleteEdgesReq...";
  // auto req = mock::MockData::mockDeleteEdgesReq(mockPartNum);

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
  // delProc->rcPrepareLocal = nebula::cpp2::ErrorCode::SUCCEEDED;
  delProc->rcProcessRemote = nebula::cpp2::ErrorCode::SUCCEEDED;
  // delProc->rcProcessLocal = nebula::cpp2::ErrorCode::SUCCEEDED;

  LOG(INFO) << "Build DeleteEdgesReq...";
  // auto req = mock::MockData::mockDeleteEdgesReq(mockPartNum);

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
TEST(ChainDeleteEdgesTest, Test5) {
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
  // delProc->rcPrepareLocal = nebula::cpp2::ErrorCode::SUCCEEDED;
  delProc->rcProcessRemote = nebula::cpp2::ErrorCode::SUCCEEDED;
  delProc->rcProcessLocal = nebula::cpp2::ErrorCode::SUCCEEDED;

  LOG(INFO) << "Build DeleteEdgesReq...";
  // auto req = mock::MockData::mockDeleteEdgesReq(mockPartNum);

  LOG(INFO) << "Run DeleteEdgesReq...";
  auto futDel = delProc->getFuture();
  delProc->process(delReq);
  std::move(futDel).get();

  num = util.checkNumOfKey(env, mockSpaceId, edgeKeys);
  LOG(INFO) << "after del(), edge num = " << num;
  EXPECT_EQ(num, 167);

  env->txnMan_->scanAll();
  auto* iClient = FakeInternalStorageClient::instance(env, nebula::cpp2::ErrorCode::SUCCEEDED);
  FakeInternalStorageClient::hookInternalStorageClient(env, iClient);
  ChainResumeProcessor resumeProc(env);
  resumeProc.process();
  num = util.checkNumOfKey(env, mockSpaceId, edgeKeys);
  EXPECT_EQ(num, 0);
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
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
  // delProc->rcPrepareLocal = nebula::cpp2::ErrorCode::SUCCEEDED;
  delProc->rcProcessRemote = nebula::cpp2::ErrorCode::SUCCEEDED;
  delProc->rcProcessLocal = nebula::cpp2::ErrorCode::SUCCEEDED;

  LOG(INFO) << "Build DeleteEdgesReq...";
  // auto req = mock::MockData::mockDeleteEdgesReq(mockPartNum);

  LOG(INFO) << "Run DeleteEdgesReq...";
  auto futDel = delProc->getFuture();
  delProc->process(delReq);
  std::move(futDel).get();

  num = util.checkNumOfKey(env, mockSpaceId, edgeKeys);
  LOG(INFO) << "after del(), edge num = " << num;
  EXPECT_EQ(num, 167);

  env->txnMan_->scanAll();
  auto* iClient = FakeInternalStorageClient::instance(env, nebula::cpp2::ErrorCode::SUCCEEDED);
  FakeInternalStorageClient::hookInternalStorageClient(env, iClient);
  ChainResumeProcessor resumeProc(env);
  resumeProc.process();
  num = util.checkNumOfKey(env, mockSpaceId, edgeKeys);
  EXPECT_EQ(num, 0);
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
}

// add some edges, then one of them, rpc failure
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
  // delProc->rcPrepareLocal = nebula::cpp2::ErrorCode::SUCCEEDED;
  delProc->rcProcessRemote = nebula::cpp2::ErrorCode::E_RPC_FAILURE;
  // delProc->rcProcessLocal = nebula::cpp2::ErrorCode::SUCCEEDED;

  LOG(INFO) << "Build DeleteEdgesReq...";
  // auto req = mock::MockData::mockDeleteEdgesReq(mockPartNum);

  LOG(INFO) << "Run DeleteEdgesReq...";
  auto futDel = delProc->getFuture();
  delProc->process(delReq);
  std::move(futDel).get();

  num = util.checkNumOfKey(env, mockSpaceId, edgeKeys);
  LOG(INFO) << "after del(), edge num = " << num;
  EXPECT_EQ(num, 166);

  env->txnMan_->scanAll();
  auto* iClient = FakeInternalStorageClient::instance(env, nebula::cpp2::ErrorCode::SUCCEEDED);
  FakeInternalStorageClient::hookInternalStorageClient(env, iClient);
  ChainResumeProcessor resumeProc(env);
  resumeProc.process();
  num = util.checkNumOfKey(env, mockSpaceId, edgeKeys);
  EXPECT_EQ(num, 166);
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
}

// add some edges, then one all of them, rpc failure
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
  // delProc->rcPrepareLocal = nebula::cpp2::ErrorCode::SUCCEEDED;
  delProc->rcProcessRemote = nebula::cpp2::ErrorCode::E_RPC_FAILURE;
  // delProc->rcProcessLocal = nebula::cpp2::ErrorCode::SUCCEEDED;

  LOG(INFO) << "Build DeleteEdgesReq...";
  // auto req = mock::MockData::mockDeleteEdgesReq(mockPartNum);

  LOG(INFO) << "Run DeleteEdgesReq...";
  auto futDel = delProc->getFuture();
  delProc->process(delReq);
  std::move(futDel).get();

  num = util.checkNumOfKey(env, mockSpaceId, edgeKeys);
  LOG(INFO) << "after del(), edge num = " << num;
  EXPECT_EQ(num, 0);

  env->txnMan_->scanAll();
  auto* iClient = FakeInternalStorageClient::instance(env, nebula::cpp2::ErrorCode::SUCCEEDED);
  FakeInternalStorageClient::hookInternalStorageClient(env, iClient);
  ChainResumeProcessor resumeProc(env);
  resumeProc.process();
  num = util.checkNumOfKey(env, mockSpaceId, edgeKeys);
  EXPECT_EQ(num, 0);
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
  FLAGS_trace_toss = true;

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
  // if (limit <= 0) {
  //   limit = std::numeric_limit<int>::max();
  // }
  ret.set_space_id(req.get_space_id());
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
