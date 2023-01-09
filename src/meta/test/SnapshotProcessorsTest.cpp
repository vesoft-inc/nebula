/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/utils/Utils.h"
#include "meta/processors/admin/CreateSnapshotProcessor.h"
#include "meta/processors/admin/DropSnapshotProcessor.h"
#include "meta/processors/admin/ListSnapshotsProcessor.h"
#include "meta/test/TestUtils.h"

class JobManager;

namespace nebula {
namespace meta {

#define RETURN_OK(req)                                       \
  UNUSED(req);                                               \
  do {                                                       \
    folly::Promise<storage::cpp2::AdminExecResp> pro;        \
    auto f = pro.getFuture();                                \
    storage::cpp2::AdminExecResp resp;                       \
    storage::cpp2::ResponseCommon result;                    \
    std::vector<storage::cpp2::PartitionResult> partRetCode; \
    result.failed_parts_ref() = partRetCode;                 \
    resp.result_ref() = result;                              \
    pro.setValue(std::move(resp));                           \
    return f;                                                \
  } while (false)

static constexpr nebula::cpp2::LogID logId = 10;
static constexpr nebula::cpp2::TermID termId = 2;

class TestStorageService : public storage::cpp2::StorageAdminServiceSvIf {
 public:
  folly::Future<storage::cpp2::AdminExecResp> future_addPart(
      const storage::cpp2::AddPartReq& req) override {
    RETURN_OK(req);
  }

  folly::Future<storage::cpp2::CreateCPResp> future_createCheckpoint(
      const storage::cpp2::CreateCPRequest& req) override {
    folly::Promise<storage::cpp2::CreateCPResp> pro;
    auto f = pro.getFuture();
    storage::cpp2::CreateCPResp resp;

    if (isDisabled_) {
      resp.code_ref() = nebula::cpp2::ErrorCode::E_STORE_FAILURE;
    } else {
      for (auto spaceId : req.get_space_ids()) {
        std::unordered_map<nebula::cpp2::PartitionID, nebula::cpp2::LogInfo> info;
        nebula::cpp2::LogInfo logInfo;
        logInfo.log_id_ref() = logId;
        logInfo.term_id_ref() = termId;
        info.emplace(1, std::move(logInfo));
        nebula::cpp2::CheckpointInfo cpInfo;
        cpInfo.data_path_ref() = "snapshot_path";
        cpInfo.parts_ref() = std::move(info);
        cpInfo.space_id_ref() = spaceId;
        resp.info_ref()->emplace_back(std::move(cpInfo));
      }
      resp.code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
    }
    pro.setValue(std::move(resp));

    LOG(INFO) << folly::sformat("Create checkpint on 127.0.0.1:{}, disabled: {}, blocked:{}",
                                port_,
                                isDisabled_,
                                isBlocked_);
    return f;
  }

  folly::Future<storage::cpp2::DropCPResp> future_dropCheckpoint(
      const storage::cpp2::DropCPRequest& req) override {
    UNUSED(req);
    folly::Promise<storage::cpp2::DropCPResp> pro;
    auto f = pro.getFuture();
    storage::cpp2::DropCPResp resp;
    if (isDisabled_) {
      resp.code_ref() = nebula::cpp2::ErrorCode::E_STORE_FAILURE;
    } else {
      resp.code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
    }
    pro.setValue(std::move(resp));

    LOG(INFO) << folly::sformat(
        "Drop checkpint on 127.0.0.1:{}, disabled: {}, blocked:{}", port_, isDisabled_, isBlocked_);
    return f;
  }

  folly::Future<storage::cpp2::BlockingSignResp> future_blockingWrites(
      const storage::cpp2::BlockingSignRequest& req) override {
    if (req.get_sign() == storage::cpp2::EngineSignType::BLOCK_ON) {
      isBlocked_ = true;
    } else {
      isBlocked_ = false;
    }

    folly::Promise<storage::cpp2::BlockingSignResp> pro;
    auto f = pro.getFuture();
    storage::cpp2::BlockingSignResp resp;
    resp.code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
    pro.setValue(std::move(resp));

    LOG(INFO) << folly::sformat("Blocking writes on 127.0.0.1:{}, disabled: {}, blocked:{}",
                                port_,
                                isDisabled_,
                                isBlocked_);
    return f;
  }

  // for test
  bool isBlocked() {
    return isBlocked_;
  }
  void disableSnapshot() {
    isDisabled_ = true;
  }
  void enableSnapshot() {
    isDisabled_ = false;
  }
  void setPort(Port port) {
    port_ = port;
  }

 private:
  bool isBlocked_{false};
  bool isDisabled_{false};
  Port port_;
};

class SnapshotProcessorsTest : public ::testing::Test {
 protected:
  static void initStorage() {
    for (size_t i = 0; i < storageNum; ++i) {
      auto& storaged = storageds_[i];
      auto& handler = handlers_[i];

      storaged = std::make_unique<mock::RpcServer>();
      handler = std::make_shared<TestStorageService>();

      storaged->start("storage-admin", 0, handler);
      LOG(INFO) << "Start storage server on " << storaged->port_;

      auto storeAddr = Utils::getStoreAddrFromAdminAddr(HostAddr(localIp_, storaged->port_));
      handler->setPort(storeAddr.port);
      hosts_.emplace_back(storeAddr);
    }
  }

  static void registerStorage(const std::vector<HostAddr>& addrs) {
    std::vector<kvstore::KV> machines;
    for (const auto& addr : addrs) {
      machines.emplace_back(nebula::MetaKeyUtils::machineKey(addr.host, addr.port), "");
    }
    TestUtils::doPut(metaKv_.get(), machines);
  }

  static void unregisterStorage(const std::vector<HostAddr>& addrs) {
    std::vector<std::string> machines;
    for (const auto& addr : addrs) {
      machines.emplace_back(nebula::MetaKeyUtils::machineKey(addr.host, addr.port));
    }
    TestUtils::doRemove(metaKv_.get(), std::move(machines));
  }

  static void activeStorage(const std::vector<HostAddr>& addrs) {
    std::vector<kvstore::KV> time;
    for (auto addr : addrs) {
      auto now = time::WallClock::fastNowInMilliSec();
      ActiveHostsMan::updateHostInfo(
          metaKv_.get(), addr, HostInfo(now, meta::cpp2::HostRole::STORAGE, ""), time);
    }
    TestUtils::doPut(metaKv_.get(), time);
  }

  static void offlineStorage(const std::vector<HostAddr>& addrs) {
    std::vector<std::string> data;
    for (auto addr : addrs) {
      data.emplace_back(MetaKeyUtils::hostKey(addr.host, addr.port));
    }
    TestUtils::doRemove(metaKv_.get(), std::move(data));
  }

  static void mockSpace(GraphSpaceID id, const std::string& name) {
    cpp2::SpaceDesc properties;
    properties.space_name_ref() = name;
    properties.partition_num_ref() = 1;
    properties.replica_factor_ref() = storageNum;
    auto spaceVal = MetaKeyUtils::spaceVal(properties);
    std::vector<nebula::kvstore::KV> data;
    data.emplace_back(MetaKeyUtils::indexSpaceKey(name),
                      std::string(reinterpret_cast<const char*>(&id), sizeof(GraphSpaceID)));
    data.emplace_back(MetaKeyUtils::spaceKey(id), MetaKeyUtils::spaceVal(properties));
    TestUtils::doPut(metaKv_.get(), data);
  }

  static void mockPartition(std::vector<GraphSpaceID> graphIds) {
    std::vector<nebula::kvstore::KV> data;
    for (auto partId = 1; partId <= 1; partId++) {
      size_t idx = partId;
      std::vector<HostAddr> hosts;
      for (size_t i = 0; i < storageNum; i++, idx++) {  // 3 replications
        hosts.emplace_back(hosts_[idx % storageNum]);
      }

      for (auto graphId : graphIds) {
        data.emplace_back(MetaKeyUtils::partKey(graphId, partId), MetaKeyUtils::partVal(hosts));
      }
    }
    TestUtils::doPut(metaKv_.get(), data);
  }

  static void mockIndex(GraphSpaceID spaceId,
                        TagID tagId,
                        IndexID indexId,
                        const std::string& tagName,
                        const std::string& indexName) {
    cpp2::IndexItem item;
    item.index_id_ref() = indexId;
    item.index_name_ref() = indexName;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = tagId;
    item.schema_id_ref() = schemaId;
    item.schema_name_ref() = tagName;

    std::vector<nebula::kvstore::KV> data;
    data.emplace_back(MetaKeyUtils::indexIndexKey(spaceId, indexName),
                      std::string(reinterpret_cast<const char*>(&indexId), sizeof(IndexID)));
    data.emplace_back(MetaKeyUtils::indexKey(spaceId, indexId), MetaKeyUtils::indexVal(item));
    TestUtils::doPut(metaKv_.get(), data);
  }

  static void initMeta() {
    metaPath_ = std::make_unique<fs::TempDir>("/tmp/create_snpashot_test.XXXXXX");
    metaKv_ = MockCluster::initMetaKV(metaPath_->path());
    client_ = std::make_unique<AdminClient>(metaKv_.get());
    jobMgr_ = JobManager::getInstance();
    ASSERT_TRUE(jobMgr_->init(metaKv_.get(), client_.get()));
    // TODO(spw): prevent the mock job really been scheduled. Mock an JobManager is a better way.
    jobMgr_->status_.store(JobManager::JbmgrStatus::STOPPED, std::memory_order_release);

    // register storageds
    registerStorage(hosts_);
    activeStorage(hosts_);

    // mock two spaces and 3 partition
    GraphSpaceID spaceId1 = 1;
    GraphSpaceID spaceId2 = 2;
    mockSpace(spaceId1, "test_space1");
    mockSpace(spaceId2, "test_space2");
    spaceIds_.emplace_back(spaceId1);
    spaceIds_.emplace_back(spaceId2);
    mockPartition(spaceIds_);
  }

  void dropAllSnapshots() {
    const auto& prefix = MetaKeyUtils::snapshotPrefix();
    auto iter = TestUtils::doPrefix(metaKv_.get(), prefix);

    std::vector<std::string> snapshots;
    while (iter->valid()) {
      snapshots.emplace_back(iter->key());
      iter->next();
    }

    if (!snapshots.empty()) {
      TestUtils::doRemove(metaKv_.get(), std::move(snapshots));
    }
  }

  std::vector<std::string> getAllSnapshots() {
    const auto& prefix = MetaKeyUtils::snapshotPrefix();
    auto iter = TestUtils::doPrefix(metaKv_.get(), prefix);

    std::vector<std::string> snapshots;
    while (iter->valid()) {
      snapshots.emplace_back(MetaKeyUtils::parseSnapshotName(iter->key()));
      iter->next();
    }
    return snapshots;
  }

 protected:
  static void SetUpTestCase() {
    localIp_ = "127.0.0.1";
    jobMgr_ = nullptr;
    initStorage();
    initMeta();
  }

  static void TearDownTestCase() {
    jobMgr_->shutDown();
    jobMgr_->bgThread_.join();

    client_.reset(nullptr);
    metaKv_.reset(nullptr);
    metaPath_.reset(nullptr);
    for (auto& storaged : storageds_) {
      storaged.reset(nullptr);
    }
  }

 protected:
  inline static std::string localIp_;
  inline static std::vector<GraphSpaceID> spaceIds_;

  inline static const size_t storageNum = 3;
  inline static std::vector<std::unique_ptr<mock::RpcServer>> storageds_{storageNum};
  inline static std::vector<std::shared_ptr<TestStorageService>> handlers_{storageNum};
  inline static std::vector<HostAddr> hosts_;
  inline static std::unique_ptr<fs::TempDir> metaPath_;
  inline static std::unique_ptr<kvstore::KVStore> metaKv_;
  inline static std::unique_ptr<AdminClient> client_;
  inline static JobManager* jobMgr_;
};

TEST_F(SnapshotProcessorsTest, Base) {
  cpp2::CreateSnapshotReq req;
  auto processor = CreateSnapshotProcessor::instance(metaKv_.get(), client_.get());
  auto f = processor->getFuture();
  processor->process(req);  // will delete the processor pointer
  auto resp = std::move(f).get();
  ASSERT_EQ(resp.code(), nebula::cpp2::ErrorCode::SUCCEEDED);
  dropAllSnapshots();
}

TEST_F(SnapshotProcessorsTest, RunningJobs) {
  std::vector<cpp2::JobType> jobTypes{cpp2::JobType::REBUILD_TAG_INDEX,
                                      cpp2::JobType::REBUILD_EDGE_INDEX,
                                      cpp2::JobType::COMPACT,
                                      cpp2::JobType::INGEST,
                                      cpp2::JobType::DATA_BALANCE,
                                      cpp2::JobType::LEADER_BALANCE};
  JobID jobId = 1;
  for (auto jobType : jobTypes) {
    auto currJobId = jobId++;
    JobDescription job(spaceIds_.front(), currJobId, jobType);
    jobMgr_->addJob(job);

    cpp2::CreateSnapshotReq req;
    auto processor = CreateSnapshotProcessor::instance(metaKv_.get(), client_.get());
    auto f = processor->getFuture();
    processor->process(req);  // will delete the processor pointer
    auto resp = std::move(f).get();
    ASSERT_EQ(resp.code(), nebula::cpp2::ErrorCode::E_SNAPSHOT_RUNNING_JOBS);

    jobMgr_->stopJob(spaceIds_.front(), currJobId);
  }
}

TEST_F(SnapshotProcessorsTest, BadHosts) {
  // no registered host
  {
    unregisterStorage(hosts_);
    cpp2::CreateSnapshotReq req;
    auto processor = CreateSnapshotProcessor::instance(metaKv_.get(), client_.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(resp.code(), nebula::cpp2::ErrorCode::E_NO_HOSTS);
    registerStorage(hosts_);
  }

  // some registered host but inactive
  {
    HostAddr noUse{localIp_, storageds_[0]->port_ + 10};
    registerStorage({noUse});
    cpp2::CreateSnapshotReq req;
    auto processor = CreateSnapshotProcessor::instance(metaKv_.get(), client_.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(resp.code(), nebula::cpp2::ErrorCode::E_SNAPSHOT_FAILURE);
    unregisterStorage({noUse});
  }
}

TEST_F(SnapshotProcessorsTest, BadStoraged) {
  handlers_[0]->disableSnapshot();
  cpp2::CreateSnapshotReq req;
  auto processor = CreateSnapshotProcessor::instance(metaKv_.get(), client_.get());
  auto f = processor->getFuture();
  processor->process(req);
  auto resp = std::move(f).get();
  ASSERT_EQ(resp.code(), nebula::cpp2::ErrorCode::E_RPC_FAILURE);
  handlers_[0]->enableSnapshot();

  for (auto& handler : handlers_) {
    ASSERT(!handler->isBlocked());
    UNUSED(handler);
  }
}

TEST_F(SnapshotProcessorsTest, ShowSnapshot) {
  size_t count = 2;
  for (size_t i = 0; i < count; ++i) {
    cpp2::CreateSnapshotReq req;
    auto processor = CreateSnapshotProcessor::instance(metaKv_.get(), client_.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(resp.code(), nebula::cpp2::ErrorCode::SUCCEEDED);
    usleep(1200000);
  }

  cpp2::ListSnapshotsReq req;
  auto processor = ListSnapshotsProcessor::instance(metaKv_.get());
  auto f = processor->getFuture();
  processor->process(req);
  auto resp = std::move(f).get();
  ASSERT_EQ(resp.code(), nebula::cpp2::ErrorCode::SUCCEEDED);
  ASSERT_EQ(resp.get_snapshots().size(), count);
  dropAllSnapshots();
}

TEST_F(SnapshotProcessorsTest, DropSnapshot) {
  // create 2 snapshots
  size_t count = 2;
  for (size_t i = 0; i < count; ++i) {
    cpp2::CreateSnapshotReq req;
    auto processor = CreateSnapshotProcessor::instance(metaKv_.get(), client_.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(resp.code(), nebula::cpp2::ErrorCode::SUCCEEDED);

    if (i != count - 1) {
      usleep(1200000);
    }
  }

  std::vector<std::string> snapshots = getAllSnapshots();
  // only the first snapshot will be dropped
  {
    cpp2::DropSnapshotReq req;
    req.names_ref() = snapshots;
    auto processor = DropSnapshotProcessor::instance(metaKv_.get(), client_.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(resp.code(), nebula::cpp2::ErrorCode::SUCCEEDED);

    snapshots = getAllSnapshots();
    EXPECT_EQ(snapshots.size(), count - 1);
  }

  // drop successfully though some storaged failed
  {
    handlers_[0]->disableSnapshot();
    cpp2::DropSnapshotReq req;
    req.names_ref() = snapshots;
    auto processor = DropSnapshotProcessor::instance(metaKv_.get(), client_.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(resp.code(), nebula::cpp2::ErrorCode::SUCCEEDED);
    handlers_[0]->enableSnapshot();

    snapshots = getAllSnapshots();
    EXPECT_EQ(snapshots.size(), count - 2);
  }
}

TEST_F(SnapshotProcessorsTest, DropNotExist) {
  {
    cpp2::CreateSnapshotReq req;
    auto processor = CreateSnapshotProcessor::instance(metaKv_.get(), client_.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(resp.code(), nebula::cpp2::ErrorCode::SUCCEEDED);
  }

  // Drop nothing with space not exist given
  {
    cpp2::DropSnapshotReq req;
    req.names_ref() = {"spaceNotExist"};
    auto processor = DropSnapshotProcessor::instance(metaKv_.get(), client_.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(resp.code(), nebula::cpp2::ErrorCode::E_SNAPSHOT_NOT_FOUND);

    ASSERT_EQ(getAllSnapshots().size(), 1);
  }
  dropAllSnapshots();
}

TEST_F(SnapshotProcessorsTest, DropAfterAddSpace) {
  {
    cpp2::CreateSnapshotReq req;
    auto processor = CreateSnapshotProcessor::instance(metaKv_.get(), client_.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(resp.code(), nebula::cpp2::ErrorCode::SUCCEEDED);
  }

  // add a new space
  mockPartition({spaceIds_.back() + 1});

  // drop successfully with:
  // 1. new spaces added
  // 2. some storaged drop failed
  {
    handlers_[0]->disableSnapshot();
    cpp2::DropSnapshotReq req;
    req.names_ref() = getAllSnapshots();
    auto processor = DropSnapshotProcessor::instance(metaKv_.get(), client_.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(resp.code(), nebula::cpp2::ErrorCode::SUCCEEDED);
    handlers_[0]->enableSnapshot();

    ASSERT_EQ(getAllSnapshots().size(), 0);
  }
}

TEST_F(SnapshotProcessorsTest, DropAfterAddHost) {
  {
    cpp2::CreateSnapshotReq req;
    auto processor = CreateSnapshotProcessor::instance(metaKv_.get(), client_.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(resp.code(), nebula::cpp2::ErrorCode::SUCCEEDED);
  }

  // add a host
  HostAddr host{localIp_, storageds_[0]->port_ + 5};
  registerStorage({host});
  activeStorage({host});

  // drop successfully with new host added
  {
    cpp2::DropSnapshotReq req;
    req.names_ref() = getAllSnapshots();
    auto processor = DropSnapshotProcessor::instance(metaKv_.get(), client_.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(resp.code(), nebula::cpp2::ErrorCode::SUCCEEDED);

    ASSERT_EQ(getAllSnapshots().size(), 0);
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
