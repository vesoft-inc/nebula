/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/utils/Utils.h"
#include "meta/processors/admin/CreateBackupProcessor.h"
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
    pro.setValue(std::move(resp));
    return f;
  }

  folly::Future<storage::cpp2::DropCPResp> future_dropCheckpoint(
      const storage::cpp2::DropCPRequest& req) override {
    UNUSED(req);
    folly::Promise<storage::cpp2::DropCPResp> pro;
    auto f = pro.getFuture();
    storage::cpp2::DropCPResp resp;
    resp.code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
    pro.setValue(std::move(resp));
    return f;
  }

  folly::Future<storage::cpp2::BlockingSignResp> future_blockingWrites(
      const storage::cpp2::BlockingSignRequest& req) override {
    UNUSED(req);
    folly::Promise<storage::cpp2::BlockingSignResp> pro;
    auto f = pro.getFuture();
    storage::cpp2::BlockingSignResp resp;
    resp.code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
    pro.setValue(std::move(resp));
    return f;
  }
};

class CreateBackupProcessorTest : public ::testing::Test {
 protected:
  static void initStorage() {
    storaged_ = std::make_unique<mock::RpcServer>();
    auto adminHandler = std::make_shared<TestStorageService>();
    storaged_->start("storage-admin", 0, adminHandler);
    LOG(INFO) << "Start storage server on " << storaged_->port_;
  }

  static void registerStorage() {
    std::vector<kvstore::KV> machines;
    machines.emplace_back(nebula::MetaKeyUtils::machineKey(localIp_, storaged_->port_), "");
    TestUtils::doPut(metaKv_.get(), machines);
  }

  static void activeStorage() {
    HostAddr host(localIp_, storaged_->port_);
    std::vector<kvstore::KV> time;
    auto now = time::WallClock::fastNowInMilliSec();
    ActiveHostsMan::updateHostInfo(
        metaKv_.get(), host, HostInfo(now, meta::cpp2::HostRole::STORAGE, ""), time);
    TestUtils::doPut(metaKv_.get(), time);
  }

  static void mockSpace(GraphSpaceID id, const std::string& name) {
    cpp2::SpaceDesc properties;
    properties.space_name_ref() = name;
    properties.partition_num_ref() = 1;
    properties.replica_factor_ref() = 1;
    auto spaceVal = MetaKeyUtils::spaceVal(properties);
    std::vector<nebula::kvstore::KV> data;
    data.emplace_back(MetaKeyUtils::indexSpaceKey(name),
                      std::string(reinterpret_cast<const char*>(&id), sizeof(GraphSpaceID)));
    data.emplace_back(MetaKeyUtils::spaceKey(id), MetaKeyUtils::spaceVal(properties));
    TestUtils::doPut(metaKv_.get(), data);
  }

  static void mockPartition(std::vector<GraphSpaceID> graphIds) {
    HostAddr host(localIp_, storaged_->port_);
    HostAddr storageHost = Utils::getStoreAddrFromAdminAddr(host);
    std::vector<HostAddr> allHosts{storageHost};
    std::vector<nebula::kvstore::KV> data;
    for (auto partId = 1; partId <= 1; partId++) {
      std::vector<HostAddr> hosts;
      size_t idx = partId;
      for (int32_t i = 0; i < 1; i++, idx++) {
        hosts.emplace_back(allHosts[idx % 1]);
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
    metaPath_ = std::make_unique<fs::TempDir>("/tmp/create_backup_test.XXXXXX");
    metaKv_ = MockCluster::initMetaKV(metaPath_->path());
    client_ = std::make_unique<AdminClient>(metaKv_.get());
    jobMgr_ = JobManager::getInstance();
    ASSERT_TRUE(jobMgr_->init(metaKv_.get(), client_.get()));
    // TODO(spw): prevent the mock job really been scheduled. Mock an JobManager is a better way.
    jobMgr_->status_.store(JobManager::JbmgrStatus::STOPPED, std::memory_order_release);

    // register storageds
    registerStorage();
    activeStorage();

    // mock two spaces and partition
    GraphSpaceID spaceId1 = 1;
    GraphSpaceID spaceId2 = 2;
    mockSpace(spaceId1, "test_space1");
    mockSpace(spaceId2, "test_space2");
    spacesIds_.emplace_back(spaceId1);
    spacesIds_.emplace_back(spaceId2);
    mockPartition(spacesIds_);

    // mock index data
    mockIndex(spaceId1, 10, 11, "tag_space_tag1", "test_space_index1");
    mockIndex(spaceId2, 20, 21, "tag_space_tag2", "test_space_index2");
  }

  static void verify(meta::cpp2::CreateBackupResp& resp) {
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    auto meta = resp.get_meta();

    auto metaFiles = meta.get_meta_files();
    for (auto m : metaFiles) {
      LOG(INFO) << "meta files name:" << m;
    }

    auto it = std::find_if(metaFiles.cbegin(), metaFiles.cend(), [](auto const& m) {
      auto name = m.substr(m.size() - sizeof("__indexes__.sst") + 1);
      return name == "__indexes__.sst";
    });

    ASSERT_NE(it, metaFiles.cend());

    it = std::find_if(metaFiles.cbegin(), metaFiles.cend(), [](auto const& m) {
      auto name = m.substr(m.size() - sizeof("__users__.sst") + 1);
      return name == "__users__.sst";
    });
    ASSERT_EQ(it, metaFiles.cend());

    ASSERT_EQ(2, meta.get_space_backups().size());
    for (auto s : meta.get_space_backups()) {
      auto spaceBackup = s.second;
      ASSERT_EQ(1, spaceBackup.get_host_backups().size());
      ASSERT_EQ(1, spaceBackup.get_host_backups()[0].get_checkpoints().size());

      auto checkInfo = spaceBackup.get_host_backups()[0].get_checkpoints()[0];
      ASSERT_EQ("snapshot_path", checkInfo.get_data_path());
      ASSERT_TRUE(meta.get_full());
      ASSERT_TRUE(meta.get_all_spaces());
      auto parts = checkInfo.get_parts();
      ASSERT_EQ(parts.size(), 1);
      for (auto p : parts) {
        ASSERT_EQ(p.first, 1);
        auto logInfo = p.second;
        ASSERT_EQ(logInfo.get_log_id(), logId);
        ASSERT_EQ(logInfo.get_term_id(), termId);
      }
    }
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
    storaged_.reset(nullptr);
  }

 protected:
  inline static std::string localIp_;
  inline static std::vector<GraphSpaceID> spacesIds_;

  inline static std::unique_ptr<mock::RpcServer> storaged_;
  inline static std::unique_ptr<fs::TempDir> metaPath_;
  inline static std::unique_ptr<kvstore::KVStore> metaKv_;
  inline static std::unique_ptr<AdminClient> client_;
  inline static JobManager* jobMgr_;
};

TEST_F(CreateBackupProcessorTest, Basic) {
  cpp2::CreateBackupReq req;
  auto processor = CreateBackupProcessor::instance(metaKv_.get(), client_.get());
  auto f = processor->getFuture();
  processor->process(req);
  auto resp = std::move(f).get();
  verify(resp);
}

TEST_F(CreateBackupProcessorTest, RunningJobs) {
  std::vector<cpp2::JobType> jobTypes{cpp2::JobType::REBUILD_TAG_INDEX,
                                      cpp2::JobType::REBUILD_EDGE_INDEX,
                                      cpp2::JobType::REBUILD_FULLTEXT_INDEX,
                                      cpp2::JobType::COMPACT,
                                      cpp2::JobType::INGEST,
                                      cpp2::JobType::DATA_BALANCE,
                                      cpp2::JobType::ZONE_BALANCE,
                                      cpp2::JobType::LEADER_BALANCE};
  JobID jobId = 1;
  for (auto jobType : jobTypes) {
    auto currJobId = jobId++;
    JobDescription job(spacesIds_.front(), currJobId, jobType);
    jobMgr_->addJob(job);

    cpp2::CreateBackupReq req;
    auto processor = CreateBackupProcessor::instance(metaKv_.get(), client_.get());
    auto f = processor->getFuture();
    processor->process(req);  // will delete the processor pointer
    auto resp = std::move(f).get();
    ASSERT_EQ(resp.code(), nebula::cpp2::ErrorCode::E_BACKUP_RUNNING_JOBS);

    jobMgr_->stopJob(spacesIds_.front(), currJobId);
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
