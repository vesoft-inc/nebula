/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/utils/Utils.h"
#include "meta/processors/admin/CreateBackupProcessor.h"
#include "meta/processors/job/JobManager.h"
#include "meta/test/TestUtils.h"

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
    storage::cpp2::ResponseCommon result;
    std::unordered_map<nebula::cpp2::PartitionID, nebula::cpp2::LogInfo> info;
    nebula::cpp2::LogInfo logInfo;
    logInfo.log_id_ref() = logId;
    logInfo.term_id_ref() = termId;
    info.emplace(1, std::move(logInfo));
    nebula::cpp2::CheckpointInfo cpInfo;
    cpInfo.data_path_ref() = "snapshot_path";
    cpInfo.parts_ref() = std::move(info);
    cpInfo.space_id_ref() = req.get_space_ids()[0];
    resp.info_ref() = {cpInfo};
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

TEST(ProcessorTest, CreateBackupTest) {
  auto rpcServer = std::make_unique<mock::RpcServer>();
  auto handler = std::make_shared<TestStorageService>();
  rpcServer->start("storage-admin", 0, handler);
  LOG(INFO) << "Start storage server on " << rpcServer->port_;

  LOG(INFO) << "Now test interfaces with retry to leader!";
  fs::TempDir rootPath("/tmp/create_backup_test.XXXXXX");
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));

  // register machines
  std::vector<kvstore::KV> machines;
  std::string localIp("127.0.0.1");
  machines.emplace_back(nebula::MetaKeyUtils::machineKey(localIp, rpcServer->port_), "");
  folly::Baton<true, std::atomic> b;
  kv->asyncMultiPut(kDefaultSpaceId, kDefaultPartId, std::move(machines), [&](auto) { b.post(); });
  b.wait();

  // resgister active hosts, same with heartbeat
  auto now = time::WallClock::fastNowInMilliSec();
  HostAddr host(localIp, rpcServer->port_);
  std::vector<kvstore::KV> time;
  ActiveHostsMan::updateHostInfo(
      kv.get(), host, HostInfo(now, meta::cpp2::HostRole::STORAGE, ""), time);
  TestUtils::doPut(kv.get(), time);

  // mock space 1: test_space
  bool ret = false;
  cpp2::SpaceDesc properties;
  GraphSpaceID id = 1;
  properties.space_name_ref() = "test_space";
  properties.partition_num_ref() = 1;
  properties.replica_factor_ref() = 1;
  auto spaceVal = MetaKeyUtils::spaceVal(properties);
  std::vector<nebula::kvstore::KV> data;
  data.emplace_back(MetaKeyUtils::indexSpaceKey("test_space"),
                    std::string(reinterpret_cast<const char*>(&id), sizeof(GraphSpaceID)));
  data.emplace_back(MetaKeyUtils::spaceKey(id), MetaKeyUtils::spaceVal(properties));

  // mock space 2: test_space2
  cpp2::SpaceDesc properties2;
  GraphSpaceID id2 = 2;
  properties2.space_name_ref() = "test_space2";
  properties2.partition_num_ref() = 1;
  properties2.replica_factor_ref() = 1;
  spaceVal = MetaKeyUtils::spaceVal(properties2);
  data.emplace_back(MetaKeyUtils::indexSpaceKey("test_space2"),
                    std::string(reinterpret_cast<const char*>(&id2), sizeof(GraphSpaceID)));
  data.emplace_back(MetaKeyUtils::spaceKey(id2), MetaKeyUtils::spaceVal(properties2));

  // mock index data
  std::string indexName = "test_space_index";
  int32_t tagIndex = 2;
  cpp2::IndexItem item;
  item.index_id_ref() = tagIndex;
  item.index_name_ref() = indexName;
  nebula::cpp2::SchemaID schemaID;
  TagID tagID = 3;
  std::string tagName = "test_space_tag1";
  schemaID.tag_id_ref() = tagID;
  item.schema_id_ref() = schemaID;
  item.schema_name_ref() = tagName;
  data.emplace_back(MetaKeyUtils::indexIndexKey(id, indexName),
                    std::string(reinterpret_cast<const char*>(&tagIndex), sizeof(IndexID)));
  data.emplace_back(MetaKeyUtils::indexKey(id, tagIndex), MetaKeyUtils::indexVal(item));

  // mock partition data
  std::vector<HostAddr> allHosts;
  HostAddr storageHost = Utils::getStoreAddrFromAdminAddr(host);
  allHosts.emplace_back(storageHost);
  for (auto partId = 1; partId <= 1; partId++) {
    std::vector<HostAddr> hosts2;
    size_t idx = partId;
    for (int32_t i = 0; i < 1; i++, idx++) {
      hosts2.emplace_back(allHosts[idx % 1]);
    }
    data.emplace_back(MetaKeyUtils::partKey(id, partId), MetaKeyUtils::partVal(hosts2));
    data.emplace_back(MetaKeyUtils::partKey(id2, partId), MetaKeyUtils::partVal(hosts2));
  }

  folly::Baton<true, std::atomic> baton;
  kv->asyncMultiPut(
      kDefaultSpaceId, kDefaultPartId, std::move(data), [&](nebula::cpp2::ErrorCode code) {
        ret = (code == nebula::cpp2::ErrorCode::SUCCEEDED);
        baton.post();
      });
  baton.wait();

  auto client = std::make_unique<AdminClient>(kv.get());
  {
    cpp2::CreateBackupReq req;
    std::vector<std::string> spaces = {"test_space"};
    req.spaces_ref() = std::move(spaces);
    JobManager* jobMgr = JobManager::getInstance();
    ASSERT_TRUE(jobMgr->init(kv.get()));
    auto* processor = CreateBackupProcessor::instance(kv.get(), client.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    LOG(INFO) << folly::to<int>(resp.get_code());
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

    ASSERT_EQ(1, meta.get_space_backups().size());
    for (auto s : meta.get_space_backups()) {
      auto spaceBackup = s.second;
      ASSERT_EQ(1, spaceBackup.get_host_backups().size());
      ASSERT_EQ(1, spaceBackup.get_host_backups()[0].get_checkpoints().size());

      auto checkInfo = spaceBackup.get_host_backups()[0].get_checkpoints()[0];
      ASSERT_EQ("snapshot_path", checkInfo.get_data_path());
      ASSERT_TRUE(meta.get_full());
      ASSERT_FALSE(meta.get_all_spaces());
      auto parts = checkInfo.get_parts();
      ASSERT_EQ(parts.size(), 1);
      for (auto p : parts) {
        ASSERT_EQ(p.first, 1);
        auto logInfo = p.second;
        ASSERT_EQ(logInfo.get_log_id(), logId);
        ASSERT_EQ(logInfo.get_term_id(), termId);
      }
    }
    jobMgr->shutDown();
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
