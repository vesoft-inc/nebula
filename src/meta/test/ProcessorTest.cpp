/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "meta/processors/admin/CreateBackupProcessor.h"
#include "meta/processors/kv/GetProcessor.h"
#include "meta/processors/kv/MultiGetProcessor.h"
#include "meta/processors/kv/MultiPutProcessor.h"
#include "meta/processors/kv/RemoveProcessor.h"
#include "meta/processors/kv/RemoveRangeProcessor.h"
#include "meta/processors/kv/ScanProcessor.h"
#include "meta/processors/parts/CreateSpaceProcessor.h"
#include "meta/processors/parts/DropSpaceProcessor.h"
#include "meta/processors/parts/GetPartsAllocProcessor.h"
#include "meta/processors/parts/GetSpaceProcessor.h"
#include "meta/processors/parts/ListPartsProcessor.h"
#include "meta/processors/parts/ListSpacesProcessor.h"
#include "meta/processors/schema/AlterEdgeProcessor.h"
#include "meta/processors/schema/AlterTagProcessor.h"
#include "meta/processors/schema/CreateEdgeProcessor.h"
#include "meta/processors/schema/CreateTagProcessor.h"
#include "meta/processors/schema/DropEdgeProcessor.h"
#include "meta/processors/schema/DropTagProcessor.h"
#include "meta/processors/schema/GetEdgeProcessor.h"
#include "meta/processors/schema/GetTagProcessor.h"
#include "meta/processors/schema/ListEdgesProcessor.h"
#include "meta/processors/schema/ListTagsProcessor.h"
#include "meta/processors/session/SessionManagerProcessor.h"
#include "meta/processors/zone/AddZoneProcessor.h"
#include "meta/processors/zone/ListZonesProcessor.h"
#include "meta/processors/zone/UpdateZoneProcessor.h"
#include "meta/test/TestUtils.h"

DECLARE_int32(expired_threshold_sec);
DECLARE_int32(heartbeat_interval_secs);
DECLARE_uint32(expired_time_factor);

namespace nebula {
namespace meta {

using nebula::cpp2::PropertyType;

TEST(ProcessorTest, ListHostsTest) {
  fs::TempDir rootPath("/tmp/ListHostsTest.XXXXXX");
  FLAGS_heartbeat_interval_secs = 1;
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
  std::vector<HostAddr> hosts;
  for (auto i = 0; i < 10; i++) {
    hosts.emplace_back(std::to_string(i), i);
  }
  {
    // after received heartbeat, host status will become online
    TestUtils::registerHB(kv.get(), hosts);
    cpp2::ListHostsReq req;
    req.type_ref() = cpp2::ListHostType::STORAGE;
    auto* processor = ListHostsProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(10, (*resp.hosts_ref()).size());
    for (auto i = 0; i < 10; i++) {
      ASSERT_EQ(std::to_string(i), (*(*resp.hosts_ref())[i].hostAddr_ref()).host);
      ASSERT_EQ(i, (*(*resp.hosts_ref())[i].hostAddr_ref()).port);
      ASSERT_EQ(cpp2::HostStatus::ALIVE, *(*resp.hosts_ref())[i].status_ref());
    }
  }
  {
    // host info expired
    sleep(FLAGS_heartbeat_interval_secs * FLAGS_expired_time_factor + 1);
    cpp2::ListHostsReq req;
    req.type_ref() = cpp2::ListHostType::STORAGE;
    auto* processor = ListHostsProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(10, (*resp.hosts_ref()).size());
    for (auto i = 0; i < 10; i++) {
      ASSERT_EQ(std::to_string(i), (*(*resp.hosts_ref())[i].hostAddr_ref()).host);
      ASSERT_EQ(i, (*(*resp.hosts_ref())[i].hostAddr_ref()).port);
      ASSERT_EQ(cpp2::HostStatus::ALIVE, *(*resp.hosts_ref())[i].status_ref());
    }
  }
}

TEST(ProcessorTest, ListSpecficHostsTest) {
  fs::TempDir rootPath("/tmp/ListMultiRoleHostsTest.XXXXXX");
  std::vector<cpp2::HostRole> roleVec{
      cpp2::HostRole::GRAPH, cpp2::HostRole::META, cpp2::HostRole::STORAGE};
  std::vector<std::string> gitInfoShaVec{
      "fakeGraphInfoSHA", "fakeMetaInfoSHA", "fakeStorageInfoSHA"};
  FLAGS_heartbeat_interval_secs = 1;
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
  std::vector<HostAddr> graphHosts;
  for (auto i = 0; i < 3; ++i) {
    graphHosts.emplace_back(std::to_string(i), i);
  }
  std::vector<HostAddr> metaHosts;
  for (auto i = 3; i < 4; ++i) {
    metaHosts.emplace_back(std::to_string(i), i);
  }
  std::vector<HostAddr> storageHosts;
  for (auto i = 4; i < 10; ++i) {
    storageHosts.emplace_back(std::to_string(i), i);
  }
  meta::TestUtils::setupHB(kv.get(), graphHosts, roleVec[0], gitInfoShaVec[0]);
  meta::TestUtils::setupHB(kv.get(), storageHosts, roleVec[2], gitInfoShaVec[2]);
  {
    cpp2::ListHostsReq req;
    req.type_ref() = cpp2::ListHostType::GRAPH;
    auto* processor = ListHostsProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    f.wait();
    auto resp = std::move(f).get();
    ASSERT_EQ(graphHosts.size(), (*resp.hosts_ref()).size());
    for (auto i = 0U; i < (*resp.hosts_ref()).size(); ++i) {
      ASSERT_EQ(std::to_string(i), (*(*resp.hosts_ref())[i].hostAddr_ref()).host);
      ASSERT_EQ(i, (*(*resp.hosts_ref())[i].hostAddr_ref()).port);
      ASSERT_EQ(cpp2::HostStatus::ALIVE, *(*resp.hosts_ref())[i].status_ref());
      ASSERT_EQ(gitInfoShaVec[0], *(*resp.hosts_ref())[i].git_info_sha_ref());
    }
  }

  {
    cpp2::ListHostsReq req;
    req.type_ref() = cpp2::ListHostType::STORAGE;
    auto* processor = ListHostsProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    f.wait();
    auto resp = std::move(f).get();
    ASSERT_EQ(storageHosts.size(), (*resp.hosts_ref()).size());
    auto b = graphHosts.size() + metaHosts.size();
    for (auto i = 0U; i < (*resp.hosts_ref()).size(); ++i) {
      ASSERT_EQ(std::to_string(i + b), (*(*resp.hosts_ref())[i].hostAddr_ref()).host);
      ASSERT_EQ(i + b, (*(*resp.hosts_ref())[i].hostAddr_ref()).port);
      ASSERT_EQ(cpp2::HostStatus::ALIVE, *(*resp.hosts_ref())[i].status_ref());
      ASSERT_EQ(gitInfoShaVec[2], *(*resp.hosts_ref())[i].git_info_sha_ref());
    }
  }
}

TEST(ProcessorTest, ListPartsTest) {
  fs::TempDir rootPath("/tmp/ListPartsTest.XXXXXX");
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
  std::vector<HostAddr> hosts = {{"0", 0}, {"1", 1}, {"2", 2}};
  TestUtils::createSomeHosts(kv.get(), hosts);
  // 9 partition in space 1, 3 replica, 3 hosts
  TestUtils::assembleSpace(kv.get(), 1, 9, 3, 3);
  {
    cpp2::ListPartsReq req;
    req.space_id_ref() = 1;
    auto* processor = ListPartsProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(9, (*resp.parts_ref()).size());

    auto parts = std::move((*resp.parts_ref()));
    std::sort(parts.begin(), parts.end(), [](const auto& a, const auto& b) {
      return a.get_part_id() < b.get_part_id();
    });
    PartitionID partId = 0;
    for (auto& part : parts) {
      partId++;
      EXPECT_EQ(partId, part.get_part_id());
      EXPECT_FALSE(part.leader_ref().has_value());
      EXPECT_EQ(3, (*part.peers_ref()).size());
      EXPECT_EQ(0, (*part.losts_ref()).size());
    }
  }

  // List specified part, and part exists
  {
    cpp2::ListPartsReq req;
    req.space_id_ref() = 1;
    req.part_ids_ref() = {9};
    auto* processor = ListPartsProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    auto parts = std::move((*resp.parts_ref()));
    ASSERT_EQ(1, parts.size());

    PartitionID partId = 9;
    EXPECT_EQ(partId, parts[0].get_part_id());
    EXPECT_FALSE(parts[0].leader_ref().has_value());
    EXPECT_EQ(3, (*parts[0].peers_ref()).size());
    EXPECT_EQ(0, (*parts[0].losts_ref()).size());
  }

  // List specified part, and part not exist
  {
    cpp2::ListPartsReq req;
    req.space_id_ref() = 1;
    req.part_ids_ref() = {11};
    auto* processor = ListPartsProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND, resp.get_code());
  }

  // register HB with leader distribution
  {
    auto now = time::WallClock::fastNowInMilliSec();
    HostInfo info(now, cpp2::HostRole::STORAGE, gitInfoSha());

    int64_t term = 999;
    auto makeLeaderInfo = [&](PartitionID partId) {
      cpp2::LeaderInfo leaderInfo;
      leaderInfo.part_id_ref() = partId;
      leaderInfo.term_ref() = term;
      return leaderInfo;
    };

    GraphSpaceID spaceId = 1;
    ActiveHostsMan::AllLeaders allLeaders;
    for (int i = 1; i < 6; ++i) {
      allLeaders[spaceId].emplace_back(makeLeaderInfo(i));
    }
    auto ret = ActiveHostsMan::updateHostInfo(kv.get(), {"0", 0}, info, &allLeaders);
    ASSERT_EQ(ret, nebula::cpp2::ErrorCode::SUCCEEDED);

    allLeaders.clear();
    for (int i = 6; i < 9; ++i) {
      allLeaders[spaceId].emplace_back(makeLeaderInfo(i));
    }
    ret = ActiveHostsMan::updateHostInfo(kv.get(), {"1", 1}, info, &allLeaders);
    ASSERT_EQ(ret, nebula::cpp2::ErrorCode::SUCCEEDED);

    allLeaders.clear();
    allLeaders[spaceId].emplace_back(makeLeaderInfo(9));
    ret = ActiveHostsMan::updateHostInfo(kv.get(), {"2", 2}, info, &allLeaders);
    ASSERT_EQ(ret, nebula::cpp2::ErrorCode::SUCCEEDED);
  }

  {
    cpp2::ListPartsReq req;
    req.space_id_ref() = 1;
    auto* processor = ListPartsProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(9, (*resp.parts_ref()).size());

    auto parts = std::move((*resp.parts_ref()));
    std::sort(parts.begin(), parts.end(), [](const auto& a, const auto& b) {
      return a.get_part_id() < b.get_part_id();
    });
    PartitionID partId = 0;
    for (auto& part : parts) {
      partId++;
      EXPECT_EQ(partId, part.get_part_id());

      EXPECT_TRUE(part.leader_ref().has_value());
      if (partId <= 5) {
        EXPECT_EQ("0", (*part.leader_ref()).host);
        EXPECT_EQ(0, (*part.leader_ref()).port);
      } else if (partId > 5 && partId <= 8) {
        EXPECT_EQ("1", (*part.leader_ref()).host);
        EXPECT_EQ(1, (*part.leader_ref()).port);
      } else {
        EXPECT_EQ("2", (*part.leader_ref()).host);
        EXPECT_EQ(2, (*part.leader_ref()).port);
      }

      EXPECT_EQ(3, (*part.peers_ref()).size());
      for (auto& peer : *part.peers_ref()) {
        auto it = std::find_if(hosts.begin(), hosts.end(), [&](const auto& host) {
          return host.host == peer.host && host.port == peer.port;
        });
        EXPECT_TRUE(it != hosts.end());
      }
      EXPECT_EQ(0, (*part.losts_ref()).size());
    }
  }
}

TEST(ProcessorTest, HashTest) {
  HostAddr addr1("123.45.67.89", 0xFFEEDDCC);
  HostAddr addr2("123.45.67.89", 0xFFEEDDCC);
  EXPECT_EQ(std::hash<HostAddr>()(addr1), std::hash<HostAddr>()(addr2));

  HostAddr addr3("123.45.67.89", 0x00000001);
  HostAddr addr4("123.45.67.89", 0x00000002);
  EXPECT_NE(std::hash<HostAddr>()(addr3), std::hash<HostAddr>()(addr4));

  HostAddr addr5("123.45.67.89", 0x00000001);
  HostAddr addr6("123.45.67.89", 0x00000001);
  EXPECT_EQ(std::hash<HostAddr>()(addr5), std::hash<HostAddr>()(addr6));

  HostAddr addr7("123.45.67.89", 0x10000000);
  HostAddr addr8("123.45.67.89", 0x20000000);
  EXPECT_NE(std::hash<HostAddr>()(addr7), std::hash<HostAddr>()(addr8));

  HostAddr addr9("123.45.67.89", 0x10000000);
  HostAddr addr10("123.45.67.89", 0x10000000);
  EXPECT_EQ(std::hash<HostAddr>()(addr9), std::hash<HostAddr>()(addr10));
}

TEST(ProcessorTest, SpaceTest) {
  fs::TempDir rootPath("/tmp/SpaceTest.XXXXXX");
  auto kv = MockCluster::initMetaKV(rootPath.path());
  auto hostsNum = TestUtils::createSomeHosts(kv.get());

  {
    cpp2::SpaceDesc properties;
    properties.space_name_ref() = "default_space";
    properties.partition_num_ref() = 8;
    properties.replica_factor_ref() = 3;
    properties.charset_name_ref() = "utf8";
    properties.collate_name_ref() = "utf8_bin";
    cpp2::CreateSpaceReq req;
    req.properties_ref() = std::move(properties);
    auto* processor = CreateSpaceProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(1, resp.get_id().get_space_id());
  }
  {
    cpp2::GetSpaceReq req;
    req.space_name_ref() = "default_space";
    auto* processor = GetSpaceProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ("default_space", resp.get_item().get_properties().get_space_name());
    ASSERT_EQ(8, resp.get_item().get_properties().get_partition_num());
    ASSERT_EQ(3, resp.get_item().get_properties().get_replica_factor());
    ASSERT_EQ(8, *resp.get_item().get_properties().get_vid_type().get_type_length());
    ASSERT_EQ(PropertyType::FIXED_STRING,
              resp.get_item().get_properties().get_vid_type().get_type());
    ASSERT_EQ("utf8", resp.get_item().get_properties().get_charset_name());
    ASSERT_EQ("utf8_bin", resp.get_item().get_properties().get_collate_name());
  }

  {
    cpp2::ListSpacesReq req;
    auto* processor = ListSpacesProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(1, resp.get_spaces().size());
    ASSERT_EQ(1, resp.get_spaces()[0].get_id().get_space_id());
    ASSERT_EQ("default_space", resp.get_spaces()[0].get_name());
  }
  // Check the result. The dispatch way from part to hosts is in a round robin
  // fashion.
  {
    cpp2::GetPartsAllocReq req;
    req.space_id_ref() = 1;
    auto* processor = GetPartsAllocProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    std::unordered_map<HostAddr, std::set<PartitionID>> hostsParts;
    for (auto& p : resp.get_parts()) {
      for (auto& h : p.second) {
        hostsParts[HostAddr(h.host, h.port)].insert(p.first);
        // ASSERT_EQ(h.host, std::to_string(h.port));
      }
    }
    ASSERT_EQ(hostsNum, hostsParts.size());  // 4 == 17
    for (auto it = hostsParts.begin(); it != hostsParts.end(); it++) {
      ASSERT_EQ(6, it->second.size());
    }
  }
  {
    cpp2::DropSpaceReq req;
    req.space_name_ref() = "default_space";
    auto* processor = DropSpaceProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    cpp2::ListSpacesReq req;
    auto* processor = ListSpacesProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(0, resp.get_spaces().size());
  }
  // With IF EXISTS
  {
    cpp2::DropSpaceReq req;
    req.space_name_ref() = "not_exist_space";
    req.if_exists_ref() = true;
    auto* processor = DropSpaceProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    constexpr char spaceName[] = "exist_space";
    cpp2::CreateSpaceReq req;
    cpp2::SpaceDesc properties;
    properties.space_name_ref() = spaceName;
    req.properties_ref() = std::move(properties);
    auto* processor = CreateSpaceProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

    cpp2::DropSpaceReq req1;
    req1.space_name_ref() = spaceName;
    req1.if_exists_ref() = true;
    auto* processor1 = DropSpaceProcessor::instance(kv.get());
    auto f1 = processor1->getFuture();
    processor1->process(req1);
    auto resp1 = std::move(f1).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp1.get_code());
  }
  // Test default value
  {
    cpp2::SpaceDesc properties;
    properties.space_name_ref() = "space_with_no_option";
    cpp2::CreateSpaceReq creq;
    creq.properties_ref() = std::move(properties);
    auto* cprocessor = CreateSpaceProcessor::instance(kv.get());
    auto cf = cprocessor->getFuture();
    cprocessor->process(creq);
    auto cresp = std::move(cf).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, cresp.get_code());

    cpp2::GetSpaceReq greq;
    greq.space_name_ref() = "space_with_no_option";
    auto* gprocessor = GetSpaceProcessor::instance(kv.get());
    auto gf = gprocessor->getFuture();
    gprocessor->process(greq);
    auto gresp = std::move(gf).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, gresp.get_code());
    ASSERT_EQ("space_with_no_option", gresp.get_item().get_properties().get_space_name());
    ASSERT_EQ(100, gresp.get_item().get_properties().get_partition_num());
    ASSERT_EQ(1, gresp.get_item().get_properties().get_replica_factor());
    // Because setting default value in graph
    ASSERT_EQ("", gresp.get_item().get_properties().get_charset_name());
    ASSERT_EQ("", gresp.get_item().get_properties().get_collate_name());

    cpp2::DropSpaceReq dreq;
    dreq.space_name_ref() = "space_with_no_option";
    auto* dprocessor = DropSpaceProcessor::instance(kv.get());
    auto df = dprocessor->getFuture();
    dprocessor->process(dreq);
    auto dresp = std::move(df).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, dresp.get_code());
  }
}

TEST(ProcessorTest, CreateTagTest) {
  fs::TempDir rootPath("/tmp/CreateTagTest.XXXXXX");
  auto kv = MockCluster::initMetaKV(rootPath.path());
  TestUtils::createSomeHosts(kv.get());

  {
    cpp2::SpaceDesc properties;
    properties.space_name_ref() = "first_space";
    properties.partition_num_ref() = 9;
    properties.replica_factor_ref() = 1;
    cpp2::CreateSpaceReq req;
    req.properties_ref() = std::move(properties);

    auto* processor = CreateSpaceProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(1, resp.get_id().get_space_id());
  }
  {
    cpp2::SpaceDesc properties;
    properties.space_name_ref() = "second_space";
    properties.partition_num_ref() = 9;
    properties.replica_factor_ref() = 1;
    cpp2::CreateSpaceReq req;
    req.properties_ref() = std::move(properties);

    auto* processor = CreateSpaceProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(2, resp.get_id().get_space_id());
  }
  cpp2::Schema schema;
  std::vector<cpp2::ColumnDef> cols;
  cols.emplace_back(TestUtils::columnDef(0, PropertyType::INT64));
  cols.emplace_back(TestUtils::columnDef(1, PropertyType::FLOAT));
  cols.emplace_back(TestUtils::columnDef(2, PropertyType::STRING));
  schema.columns_ref() = std::move(cols);
  {
    // Space not exist
    cpp2::CreateTagReq req;
    req.space_id_ref() = 0;
    req.tag_name_ref() = "default_tag";
    req.schema_ref() = schema;
    auto* processor = CreateTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND, resp.get_code());
  }
  {
    // Succeeded
    cpp2::CreateTagReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "default_tag";
    req.schema_ref() = schema;
    auto* processor = CreateTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(3, resp.get_id().get_tag_id());
  }
  {
    // Tag have existed
    cpp2::CreateTagReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "default_tag";
    req.schema_ref() = schema;
    auto* processor = CreateTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_EXISTED, resp.get_code());
  }
  {
    // Create same name tag in diff spaces
    cpp2::CreateTagReq req;
    req.space_id_ref() = 2;
    req.tag_name_ref() = "default_tag";
    req.schema_ref() = schema;
    auto* processor = CreateTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(3, resp.get_id().get_tag_id());
  }
  {
    // Create same name edge in same spaces
    cpp2::CreateEdgeReq req;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "default_tag";
    req.schema_ref() = schema;
    auto* processor = CreateEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_CONFLICT, resp.get_code());
  }
  {
    // Set schema ttl property
    cpp2::SchemaProp schemaProp;
    schemaProp.ttl_duration_ref() = 100;
    schemaProp.ttl_col_ref() = "col_0";
    schema.schema_prop_ref() = std::move(schemaProp);

    // Tag with TTL
    cpp2::CreateTagReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_ttl";
    req.schema_ref() = schema;
    auto* processor = CreateTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(4, resp.get_id().get_tag_id());
  }
  // Wrong default value type
  {
    cpp2::Schema schemaWithDefault;
    std::vector<cpp2::ColumnDef> colsWithDefault;

    cpp2::ColumnDef columnWithDefault;
    columnWithDefault.name_ref() = folly::stringPrintf("col_type_mismatch");
    columnWithDefault.type.type_ref() = PropertyType::BOOL;
    const auto& strValue = *ConstantExpression::make(metaPool, "default value");
    columnWithDefault.default_value_ref() = Expression::encode(strValue);

    colsWithDefault.push_back(std::move(columnWithDefault));
    schemaWithDefault.columns_ref() = std::move(colsWithDefault);

    cpp2::CreateTagReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_type_mismatche";
    req.schema_ref() = std::move(schemaWithDefault);
    auto* processor = CreateTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_INVALID_PARM, resp.get_code());
  }
  // Wrong default value
  {
    cpp2::Schema schemaWithDefault;
    std::vector<cpp2::ColumnDef> colsWithDefault;

    cpp2::ColumnDef columnWithDefault;
    columnWithDefault.name_ref() = folly::stringPrintf("col_value_mismatch");
    columnWithDefault.type.type_ref() = PropertyType::INT8;
    const auto& intValue = *ConstantExpression::make(metaPool, 256);
    columnWithDefault.default_value_ref() = Expression::encode(intValue);

    colsWithDefault.push_back(std::move(columnWithDefault));
    schemaWithDefault.columns_ref() = std::move(colsWithDefault);

    cpp2::CreateTagReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_value_mismatche";
    req.schema_ref() = std::move(schemaWithDefault);
    auto* processor = CreateTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_INVALID_PARM, resp.get_code());
  }
  {
    cpp2::CreateTagReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "all_tag_type";
    auto allSchema = TestUtils::mockSchemaWithAllType();
    req.schema_ref() = std::move(allSchema);
    auto* processor = CreateTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

    cpp2::GetTagReq getReq;
    getReq.space_id_ref() = 1;
    getReq.tag_name_ref() = "all_tag_type";
    getReq.version_ref() = 0;
    auto* getProcessor = GetTagProcessor::instance(kv.get());
    auto getFut = getProcessor->getFuture();
    getProcessor->process(getReq);
    auto getResp = std::move(getFut).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, getResp.get_code());
    TestUtils::checkSchemaWithAllType(getResp.get_schema());
  }
}

TEST(ProcessorTest, CreateEdgeTest) {
  fs::TempDir rootPath("/tmp/CreateEdgeTest.XXXXXX");
  auto kv = MockCluster::initMetaKV(rootPath.path());
  TestUtils::createSomeHosts(kv.get());

  {
    cpp2::SpaceDesc properties;
    properties.space_name_ref() = "default_space";
    properties.partition_num_ref() = 9;
    properties.replica_factor_ref() = 3;
    cpp2::CreateSpaceReq req;
    req.properties_ref() = std::move(properties);

    auto* processor = CreateSpaceProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(1, resp.get_id().get_space_id());
  }
  {
    // Create another space
    cpp2::SpaceDesc properties;
    properties.space_name_ref() = "another_space";
    properties.partition_num_ref() = 9;
    properties.replica_factor_ref() = 3;
    cpp2::CreateSpaceReq req;
    req.properties_ref() = std::move(properties);

    auto* processor = CreateSpaceProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(2, resp.get_id().get_space_id());
  }

  cpp2::Schema schema;
  std::vector<cpp2::ColumnDef> cols;
  cols.emplace_back(TestUtils::columnDef(0, PropertyType::INT64));
  cols.emplace_back(TestUtils::columnDef(1, PropertyType::FLOAT));
  cols.emplace_back(TestUtils::columnDef(2, PropertyType::STRING));
  schema.columns_ref() = std::move(cols);
  {
    cpp2::CreateEdgeReq req;
    req.space_id_ref() = 0;
    req.edge_name_ref() = "default_edge";
    req.schema_ref() = schema;
    auto* processor = CreateEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND, resp.get_code());
  }
  {
    // Succeeded
    cpp2::CreateEdgeReq req;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "default_edge";
    req.schema_ref() = schema;
    auto* processor = CreateEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(3, resp.get_id().get_edge_type());
  }
  {
    // Existed
    cpp2::CreateEdgeReq req;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "default_edge";
    req.schema_ref() = schema;
    auto* processor = CreateEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_EXISTED, resp.get_code());
  }
  {
    // Create same name edge in diff spaces
    cpp2::CreateEdgeReq req;
    req.space_id_ref() = 2;
    req.edge_name_ref() = "default_edge";
    req.schema_ref() = schema;
    auto* processor = CreateEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(3, resp.get_id().get_edge_type());
  }
  {
    // Create same name tag in same spaces
    cpp2::CreateTagReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "default_edge";
    req.schema_ref() = schema;
    auto* processor = CreateTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_CONFLICT, resp.get_code());
  }

  // Set schema ttl property
  cpp2::SchemaProp schemaProp;
  schemaProp.ttl_duration_ref() = 100;
  schemaProp.ttl_col_ref() = "col_0";
  schema.schema_prop_ref() = std::move(schemaProp);
  {
    // Edge with TTL
    cpp2::CreateEdgeReq req;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_ttl";
    req.schema_ref() = schema;
    auto* processor = CreateEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(4, resp.get_id().get_edge_type());
  }
  {
    cpp2::Schema schemaWithDefault;
    std::vector<cpp2::ColumnDef> colsWithDefault;
    colsWithDefault.emplace_back(TestUtils::columnDef(0, PropertyType::BOOL, Value(false)));
    colsWithDefault.emplace_back(TestUtils::columnDef(1, PropertyType::INT64, Value(11)));
    colsWithDefault.emplace_back(TestUtils::columnDef(2, PropertyType::DOUBLE, Value(11.0)));
    colsWithDefault.emplace_back(TestUtils::columnDef(3, PropertyType::STRING, Value("test")));
    schemaWithDefault.columns_ref() = std::move(colsWithDefault);

    cpp2::CreateEdgeReq req;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_with_defaule";
    req.schema_ref() = std::move(schemaWithDefault);
    auto* processor = CreateEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(5, resp.get_id().get_edge_type());
  }
  {
    cpp2::Schema schemaWithDefault;
    std::vector<cpp2::ColumnDef> colsWithDefault;
    colsWithDefault.push_back(TestUtils::columnDef(0, PropertyType::BOOL, Value("default value")));
    schemaWithDefault.columns_ref() = std::move(colsWithDefault);

    cpp2::CreateEdgeReq req;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_type_mismatche";
    req.schema_ref() = std::move(schemaWithDefault);
    auto* processor = CreateEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_INVALID_PARM, resp.get_code());
  }
  {
    cpp2::CreateTagReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "all_edge_type";
    auto allSchema = TestUtils::mockSchemaWithAllType();
    req.schema_ref() = std::move(allSchema);
    auto* processor = CreateTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

    cpp2::GetTagReq getReq;
    getReq.space_id_ref() = 1;
    getReq.tag_name_ref() = "all_edge_type";
    getReq.version_ref() = 0;
    auto* getProcessor = GetTagProcessor::instance(kv.get());
    auto getFut = getProcessor->getFuture();
    getProcessor->process(getReq);
    auto getResp = std::move(getFut).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, getResp.get_code());
    TestUtils::checkSchemaWithAllType(getResp.get_schema());
  }
}

TEST(ProcessorTest, KVOperationTest) {
  fs::TempDir rootPath("/tmp/KVOperationTest.XXXXXX");
  auto kv = MockCluster::initMetaKV(rootPath.path());
  TestUtils::createSomeHosts(kv.get());

  {
    cpp2::SpaceDesc properties;
    properties.space_name_ref() = "default_space";
    properties.partition_num_ref() = 9;
    properties.replica_factor_ref() = 3;
    cpp2::CreateSpaceReq req;
    req.properties_ref() = std::move(properties);

    auto* processor = CreateSpaceProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(1, resp.get_id().get_space_id());
  }
  {
    // Multi Put Test
    std::vector<nebula::KeyValue> pairs;
    for (auto i = 0; i < 10; i++) {
      pairs.emplace_back(
          std::make_pair(folly::stringPrintf("key_%d", i), folly::stringPrintf("value_%d", i)));
    }

    cpp2::MultiPutReq req;
    req.segment_ref() = "test";
    req.pairs_ref() = std::move(pairs);

    auto* processor = MultiPutProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    // Get Test
    cpp2::GetReq req;
    req.segment_ref() = "test";
    req.key_ref() = "key_0";

    auto* processor = GetProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ("value_0", resp.get_value());

    cpp2::GetReq missedReq;
    missedReq.segment_ref() = "test";
    missedReq.key_ref() = "missed_key";

    auto* missedProcessor = GetProcessor::instance(kv.get());
    auto missedFuture = missedProcessor->getFuture();
    missedProcessor->process(missedReq);
    auto missedResp = std::move(missedFuture).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND, missedResp.get_code());
  }
  {
    // Multi Get Test
    std::vector<std::string> keys;
    for (auto i = 0; i < 2; i++) {
      keys.emplace_back(folly::stringPrintf("key_%d", i));
    }

    cpp2::MultiGetReq req;
    req.segment_ref() = "test";
    req.keys_ref() = std::move(keys);

    auto* processor = MultiGetProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(2, resp.get_values().size());
    ASSERT_EQ("value_0", resp.get_values()[0]);
    ASSERT_EQ("value_1", resp.get_values()[1]);
  }
  {
    // Scan Test
    cpp2::ScanReq req;
    req.segment_ref() = "test";
    req.start_ref() = "key_1";
    req.end_ref() = "key_4";

    auto* processor = ScanProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(3, resp.get_values().size());
    ASSERT_EQ("value_1", resp.get_values()[0]);
    ASSERT_EQ("value_2", resp.get_values()[1]);
    ASSERT_EQ("value_3", resp.get_values()[2]);
  }
  {
    // Remove Test
    cpp2::RemoveReq req;
    req.segment_ref() = "test";
    req.key_ref() = "key_9";

    auto* processor = RemoveProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    // Remove Range Test
    cpp2::RemoveRangeReq req;
    req.segment_ref() = "test";
    req.start_ref() = "key_0";
    req.end_ref() = "key_4";

    auto* processor = RemoveRangeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
}

TEST(ProcessorTest, ListOrGetTagsTest) {
  fs::TempDir rootPath("/tmp/ListOrGetTagsTest.XXXXXX");
  auto kv = MockCluster::initMetaKV(rootPath.path());
  TestUtils::assembleSpace(kv.get(), 1, 1);
  TestUtils::mockTag(kv.get(), 10);

  // Test ListTagsProcessor
  {
    cpp2::ListTagsReq req;
    req.space_id_ref() = 1;
    auto* processor = ListTagsProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    std::vector<nebula::meta::cpp2::TagItem> tags;
    tags = resp.get_tags();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(10, tags.size());

    for (auto t = 0; t < 10; t++) {
      auto tag = tags[t];
      ASSERT_EQ(t, tag.get_tag_id());
      ASSERT_EQ(t, tag.get_version());
      ASSERT_EQ(folly::stringPrintf("tag_%d", t), tag.get_tag_name());
      ASSERT_EQ(2, (*tag.get_schema().columns_ref()).size());
    }
  }

  // Test GetTagProcessor with version
  {
    cpp2::GetTagReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    req.version_ref() = 0;

    auto* processor = GetTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    auto schema = resp.get_schema();

    std::vector<cpp2::ColumnDef> cols = schema.get_columns();
    ASSERT_EQ(cols.size(), 2);
    for (auto i = 0; i < 2; i++) {
      ASSERT_EQ(folly::stringPrintf("tag_%d_col_%d", 0, i), cols[i].get_name());
      ASSERT_EQ((i < 1 ? PropertyType::INT64 : PropertyType::FIXED_STRING),
                cols[i].get_type().get_type());
    }
  }

  // Test GetTagProcessor without version
  {
    cpp2::GetTagReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    req.version_ref() = -1;

    auto* processor = GetTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    auto schema = resp.get_schema();

    std::vector<cpp2::ColumnDef> cols = schema.get_columns();
    ASSERT_EQ(cols.size(), 2);
    for (auto i = 0; i < 2; i++) {
      ASSERT_EQ(folly::stringPrintf("tag_%d_col_%d", 0, i), cols[i].get_name());
      ASSERT_EQ((i < 1 ? PropertyType::INT64 : PropertyType::FIXED_STRING),
                cols[i].get_type().get_type());
    }
  }

  // Test GetTagProcessor with not exist tagName
  {
    cpp2::GetTagReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_1";

    auto* processor = GetTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND, resp.get_code());
  }
}

TEST(ProcessorTest, ListOrGetEdgesTest) {
  fs::TempDir rootPath("/tmp/ListOrGetEdgesTest.XXXXXX");
  auto kv = MockCluster::initMetaKV(rootPath.path());
  TestUtils::assembleSpace(kv.get(), 1, 1);
  TestUtils::mockEdge(kv.get(), 10);

  // Test ListEdgesProcessor
  {
    cpp2::ListEdgesReq req;
    req.space_id_ref() = 1;
    auto* processor = ListEdgesProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    std::vector<nebula::meta::cpp2::EdgeItem> edges;
    edges = resp.get_edges();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(10, edges.size());

    for (auto t = 0; t < 10; t++) {
      auto edge = edges[t];
      ASSERT_EQ(t, edge.get_edge_type());
      ASSERT_EQ(t, edge.get_version());
      ASSERT_EQ(folly::stringPrintf("edge_%d", t), edge.get_edge_name());
      ASSERT_EQ(2, (*edge.get_schema().columns_ref()).size());
    }
  }

  // Test GetEdgeProcessor
  {
    for (auto t = 0; t < 10; t++) {
      cpp2::GetEdgeReq req;
      req.space_id_ref() = 1;
      req.edge_name_ref() = folly::stringPrintf("edge_%d", t);
      req.version_ref() = t;

      auto* processor = GetEdgeProcessor::instance(kv.get());
      auto f = processor->getFuture();
      processor->process(req);
      auto resp = std::move(f).get();
      auto schema = resp.get_schema();

      std::vector<cpp2::ColumnDef> cols = schema.get_columns();
      ASSERT_EQ(cols.size(), 2);
      for (auto i = 0; i < 2; i++) {
        ASSERT_EQ(folly::stringPrintf("edge_%d_col_%d", t, i), cols[i].get_name());
        ASSERT_EQ((i < 1 ? PropertyType::INT64 : PropertyType::FIXED_STRING),
                  cols[i].get_type().get_type());
      }
    }
  }

  // Test GetEdgeProcessor without version
  {
    cpp2::GetEdgeReq req;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    req.version_ref() = -1;

    auto* processor = GetEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    auto schema = resp.get_schema();

    std::vector<cpp2::ColumnDef> cols = schema.get_columns();
    ASSERT_EQ(cols.size(), 2);
    for (auto i = 0; i < 2; i++) {
      ASSERT_EQ(folly::stringPrintf("edge_%d_col_%d", 0, i), cols[i].get_name());
      ASSERT_EQ((i < 1 ? PropertyType::INT64 : PropertyType::FIXED_STRING),
                cols[i].get_type().get_type());
    }
  }

  // Test GetEdgeProcessor with not exist edgeName
  {
    cpp2::GetEdgeReq req;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_10";

    auto* processor = GetEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND, resp.get_code());
  }
}

TEST(ProcessorTest, DropTagTest) {
  fs::TempDir rootPath("/tmp/DropTagTest.XXXXXX");
  auto kv = MockCluster::initMetaKV(rootPath.path());
  TestUtils::assembleSpace(kv.get(), 1, 1);
  TestUtils::mockTag(kv.get(), 1);

  // Space not exist
  {
    cpp2::DropTagReq req;
    req.space_id_ref() = 0;
    req.tag_name_ref() = "tag_0";
    auto* processor = DropTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND, resp.get_code());
  }
  // Tag not exist
  {
    cpp2::DropTagReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_no";
    auto* processor = DropTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND, resp.get_code());
  }
  // Succeeded
  {
    cpp2::DropTagReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    auto* processor = DropTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }

  // Check tag data has been deleted.
  {
    std::string tagVal;
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kv.get()->get(
        kDefaultSpaceId, kDefaultPartId, std::move(MetaKeyUtils::indexTagKey(1, "tag_0")), &tagVal);
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND, ret);
    std::string tagPrefix = "__tags__";
    ret = kv.get()->prefix(kDefaultSpaceId, kDefaultPartId, tagPrefix, &iter);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
    ASSERT_FALSE(iter->valid());
  }
  // With IF EXISTS
  {
    cpp2::DropTagReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "not_exist_tag";
    req.if_exists_ref() = true;
    auto* processor = DropTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    constexpr GraphSpaceID spaceId = 1;
    constexpr char tagName[] = "exist_tag";
    cpp2::CreateTagReq req;
    req.space_id_ref() = spaceId;
    req.tag_name_ref() = tagName;
    auto* processor = CreateTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

    cpp2::DropTagReq req1;
    req1.space_id_ref() = spaceId;
    req1.tag_name_ref() = tagName;
    req1.if_exists_ref() = true;
    auto* processor1 = DropTagProcessor::instance(kv.get());
    auto f1 = processor1->getFuture();
    processor1->process(req1);
    auto resp1 = std::move(f1).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp1.get_code());
  }
}

TEST(ProcessorTest, DropEdgeTest) {
  fs::TempDir rootPath("/tmp/DropEdgeTest.XXXXXX");
  auto kv = MockCluster::initMetaKV(rootPath.path());
  TestUtils::assembleSpace(kv.get(), 1, 1);
  TestUtils::mockEdge(kv.get(), 1);

  // Space not exist
  {
    cpp2::DropEdgeReq req;
    req.space_id_ref() = 0;
    req.edge_name_ref() = "edge_0";
    auto* processor = DropEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND, resp.get_code());
  }
  // Edge not exist
  {
    cpp2::DropEdgeReq req;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_no";
    auto* processor = DropEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND, resp.get_code());
  }
  // Succeeded
  {
    cpp2::DropEdgeReq req;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    auto* processor = DropEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // Check edge data has been deleted.
  {
    std::string edgeVal;
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kv.get()->get(kDefaultSpaceId,
                             kDefaultPartId,
                             std::move(MetaKeyUtils::indexEdgeKey(1, "edge_0")),
                             &edgeVal);
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND, ret);
    std::string edgePrefix = "__edges__";
    ret = kv.get()->prefix(kDefaultSpaceId, kDefaultPartId, edgePrefix, &iter);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
    ASSERT_FALSE(iter->valid());
  }
  // With IF EXISTS
  {
    cpp2::DropEdgeReq req;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "not_exist_edge";
    req.if_exists_ref() = true;
    auto* processor = DropEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    constexpr GraphSpaceID spaceId = 1;
    constexpr char edgeName[] = "exist_edge";
    cpp2::CreateTagReq req;
    req.space_id_ref() = spaceId;
    req.tag_name_ref() = edgeName;
    auto* processor = CreateTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

    cpp2::DropEdgeReq req1;
    req1.space_id_ref() = spaceId;
    req1.edge_name_ref() = edgeName;
    req1.if_exists_ref() = true;
    auto* processor1 = DropEdgeProcessor::instance(kv.get());
    auto f1 = processor1->getFuture();
    processor1->process(req1);
    auto resp1 = std::move(f1).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp1.get_code());
  }
}

TEST(ProcessorTest, AlterTagTest) {
  fs::TempDir rootPath("/tmp/AlterTagTest.XXXXXX");
  auto kv = MockCluster::initMetaKV(rootPath.path());
  TestUtils::assembleSpace(kv.get(), 1, 1);
  TestUtils::mockTag(kv.get(), 1);
  // Alter tag options test
  {
    cpp2::AlterTagReq req;
    std::vector<cpp2::AlterSchemaItem> items;
    cpp2::Schema addSch;
    for (auto i = 0; i < 2; i++) {
      cpp2::ColumnDef column;
      column.name = folly::stringPrintf("tag_0_col_%d", i + 10);
      column.type.type_ref() = i < 1 ? PropertyType::INT64 : PropertyType::STRING;
      (*addSch.columns_ref()).emplace_back(std::move(column));
    }
    cpp2::Schema changeSch;
    for (auto i = 0; i < 2; i++) {
      cpp2::ColumnDef column;
      column.name = folly::stringPrintf("tag_0_col_%d", i);
      column.type.type_ref() = i < 1 ? PropertyType::BOOL : PropertyType::DOUBLE;
      (*changeSch.columns_ref()).emplace_back(std::move(column));
    }
    cpp2::Schema dropSch;
    cpp2::ColumnDef column;
    column.name = "tag_0_col_0";
    (*dropSch.columns_ref()).emplace_back(std::move(column));
    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::ADD;
    items.back().schema_ref() = std::move(addSch);

    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::CHANGE;
    items.back().schema_ref() = std::move(changeSch);

    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::DROP;
    items.back().schema_ref() = std::move(dropSch);
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    req.tag_items_ref() = items;
    auto* processor = AlterTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // Verify alter result.
  {
    cpp2::ListTagsReq req;
    req.space_id_ref() = 1;
    auto* processor = ListTagsProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    auto tags = resp.get_tags();
    ASSERT_EQ(2, tags.size());
    // TagItems in vector are unordered.So need to get the latest one by
    // comparing the versions.
    auto tag = tags[0].get_version() > 0 ? tags[0] : tags[1];
    EXPECT_EQ(0, tag.get_tag_id());
    EXPECT_EQ(folly::stringPrintf("tag_%d", 0), tag.get_tag_name());
    EXPECT_EQ(1, tag.get_version());

    cpp2::Schema schema;
    std::vector<cpp2::ColumnDef> cols;

    cpp2::ColumnDef column;
    column.name = "tag_0_col_1";
    column.type.type_ref() = PropertyType::DOUBLE;
    cols.emplace_back(std::move(column));

    column.name = "tag_0_col_10";
    column.type.type_ref() = PropertyType::INT64;
    cols.emplace_back(std::move(column));

    column.name = "tag_0_col_11";
    column.type.type_ref() = PropertyType::STRING;
    cols.emplace_back(std::move(column));

    schema.columns_ref() = std::move(cols);
    EXPECT_EQ(schema, tag.get_schema());
  }
  // Alter tag with ttl
  {
    // Only set ttl_duration
    cpp2::AlterTagReq req;
    cpp2::SchemaProp schemaProp;
    schemaProp.ttl_duration_ref() = 100;

    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    req.schema_prop_ref() = std::move(schemaProp);
    auto* processor = AlterTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_NE(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // Succeeded
  {
    cpp2::AlterTagReq req;
    cpp2::SchemaProp schemaProp;
    schemaProp.ttl_duration_ref() = 100;
    schemaProp.ttl_col_ref() = "tag_0_col_10";

    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    req.schema_prop_ref() = std::move(schemaProp);
    auto* processor = AlterTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // Verify alter result.
  {
    cpp2::ListTagsReq req;
    req.space_id_ref() = 1;
    auto* processor = ListTagsProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    auto tags = resp.get_tags();
    ASSERT_EQ(3, tags.size());
    // TagItems in vector are unordered.So need to get the latest one by
    // comparing the versions.
    auto tag = tags[0].get_version() > tags[1].get_version() ? tags[0] : tags[1];
    tag = tag.get_version() > tags[2].get_version() ? tag : tags[2];

    EXPECT_EQ(0, tag.get_tag_id());
    EXPECT_EQ(folly::stringPrintf("tag_%d", 0), tag.get_tag_name());
    EXPECT_EQ(2, tag.get_version());

    cpp2::Schema schema;
    std::vector<cpp2::ColumnDef> cols;

    cpp2::ColumnDef column;
    column.name = "tag_0_col_1";
    column.type.type_ref() = PropertyType::DOUBLE;
    cols.emplace_back(std::move(column));

    column.name = "tag_0_col_10";
    column.type.type_ref() = PropertyType::INT64;
    cols.emplace_back(std::move(column));

    column.name = "tag_0_col_11";
    column.type.type_ref() = PropertyType::STRING;
    cols.emplace_back(std::move(column));

    schema.columns_ref() = std::move(cols);

    cpp2::SchemaProp schemaProp;
    schemaProp.ttl_duration_ref() = 100;
    schemaProp.ttl_col_ref() = "tag_0_col_10";
    schema.schema_prop_ref() = std::move(schemaProp);
    EXPECT_EQ(schema.get_columns(), tag.get_schema().get_columns());
    EXPECT_EQ(*schema.get_schema_prop().get_ttl_duration(),
              *tag.get_schema().get_schema_prop().get_ttl_duration());
    EXPECT_EQ(*schema.get_schema_prop().get_ttl_col(),
              *tag.get_schema().get_schema_prop().get_ttl_col());
  }
  // Change col with ttl, failed
  {
    cpp2::AlterTagReq req;
    std::vector<cpp2::AlterSchemaItem> items;
    cpp2::Schema changeSch;
    cpp2::ColumnDef column;
    column.name = "tag_0_col_10";
    column.type.type_ref() = PropertyType::DOUBLE;
    (*changeSch.columns_ref()).emplace_back(std::move(column));

    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::CHANGE;
    items.back().schema_ref() = std::move(changeSch);

    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    req.tag_items_ref() = items;
    auto* processor = AlterTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_NE(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // Set ttl col on illegal column type, failed
  {
    cpp2::AlterTagReq req;
    cpp2::SchemaProp schemaProp;
    schemaProp.ttl_duration_ref() = 100;
    schemaProp.ttl_col_ref() = "tag_0_col_11";

    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    req.schema_prop_ref() = std::move(schemaProp);
    auto* processor = AlterTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_NE(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // Drop ttl_col column
  {
    cpp2::AlterTagReq req;
    std::vector<cpp2::AlterSchemaItem> items;
    cpp2::Schema dropSch;
    cpp2::ColumnDef column;
    column.name = "tag_0_col_10";
    (*dropSch.columns_ref()).emplace_back(std::move(column));

    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::DROP;
    items.back().schema_ref() = std::move(dropSch);
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    req.tag_items_ref() = items;
    auto* processor = AlterTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // Verify alter drop ttl col result.
  {
    cpp2::ListTagsReq req;
    req.space_id_ref() = 1;
    auto* processor = ListTagsProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    auto tags = resp.get_tags();
    ASSERT_EQ(4, tags.size());
    // TagItems in vector are unordered.So need to get the latest one by
    // comparing the versions.
    int version = 0;
    int max_index = 0;
    for (uint32_t i = 0; i < tags.size(); i++) {
      if (tags[i].get_version() > version) {
        max_index = i;
        version = tags[i].get_version();
      }
    }
    auto tag = tags[max_index];
    EXPECT_EQ(0, tag.get_tag_id());
    EXPECT_EQ(folly::stringPrintf("tag_%d", 0), tag.get_tag_name());
    EXPECT_EQ(3, tag.get_version());

    cpp2::Schema schema;
    std::vector<cpp2::ColumnDef> cols;

    cpp2::ColumnDef column;
    column.name = "tag_0_col_1";
    column.type.type_ref() = PropertyType::DOUBLE;
    cols.emplace_back(std::move(column));

    column.name = "tag_0_col_11";
    column.type.type_ref() = PropertyType::STRING;
    cols.emplace_back(std::move(column));

    schema.columns_ref() = std::move(cols);

    cpp2::SchemaProp schemaProp;
    schemaProp.ttl_duration_ref() = 0;
    schemaProp.ttl_col_ref() = "";
    schema.schema_prop_ref() = std::move(schemaProp);
    EXPECT_EQ(schema.get_columns(), tag.get_schema().get_columns());
    EXPECT_EQ(*schema.get_schema_prop().get_ttl_duration(),
              *tag.get_schema().get_schema_prop().get_ttl_duration());
    EXPECT_EQ(*schema.get_schema_prop().get_ttl_col(),
              *tag.get_schema().get_schema_prop().get_ttl_col());
  }

  // Verify ErrorCode of add
  {
    cpp2::AlterTagReq req;
    std::vector<cpp2::AlterSchemaItem> items;
    cpp2::Schema addSch;
    cpp2::ColumnDef column;
    column.name = "tag_0_col_1";
    column.type.type_ref() = PropertyType::INT64;
    (*addSch.columns_ref()).emplace_back(std::move(column));

    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::ADD;
    items.back().schema_ref() = std::move(addSch);
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    req.tag_items_ref() = items;
    auto* processor = AlterTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_EXISTED, resp.get_code());
  }
  // Verify ErrorCode of change
  {
    cpp2::AlterTagReq req;
    std::vector<cpp2::AlterSchemaItem> items;
    cpp2::Schema changeSch;
    cpp2::ColumnDef column;
    column.name = "tag_0_col_2";
    column.type.type_ref() = PropertyType::INT64;
    (*changeSch.columns_ref()).emplace_back(std::move(column));

    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::CHANGE;
    items.back().schema_ref() = std::move(changeSch);
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    req.tag_items_ref() = items;
    auto* processor = AlterTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND, resp.get_code());
  }
  // Verify ErrorCode of drop
  {
    cpp2::AlterTagReq req;
    std::vector<cpp2::AlterSchemaItem> items;
    cpp2::Schema dropSch;
    cpp2::ColumnDef column;
    column.name = "tag_0_col_0";
    column.type.type_ref() = PropertyType::INT64;
    (*dropSch.columns_ref()).emplace_back(std::move(column));

    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::DROP;
    items.back().schema_ref() = std::move(dropSch);
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    req.tag_items_ref() = items;
    auto* processor = AlterTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND, resp.get_code());
  }
  // Add col with wrong default value type
  {
    cpp2::AlterTagReq req;
    std::vector<cpp2::AlterSchemaItem> items;
    cpp2::Schema schema;
    cpp2::ColumnDef column;
    column.name = "add_col_mismatch_type";
    column.type.type_ref() = PropertyType::INT64;
    const auto& strValue = *ConstantExpression::make(metaPool, "default value");
    column.default_value_ref() = Expression::encode(strValue);
    (*schema.columns_ref()).emplace_back(std::move(column));

    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::ADD;
    items.back().schema_ref() = std::move(schema);
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    req.tag_items_ref() = items;
    auto* processor = AlterTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_INVALID_PARM, resp.get_code());
  }
  // Add col with out of range of fixed string
  {
    cpp2::AlterTagReq req;
    std::vector<cpp2::AlterSchemaItem> items;
    cpp2::Schema schema;
    cpp2::ColumnDef column;
    column.name = "add_col_fixed_string_type";
    column.type.type_ref() = PropertyType::FIXED_STRING;
    column.type.type_length_ref() = 5;
    const auto& strValue = *ConstantExpression::make(metaPool, "Hello world!");
    column.default_value_ref() = Expression::encode(strValue);
    (*schema.columns_ref()).emplace_back(std::move(column));

    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::ADD;
    items.back().schema_ref() = std::move(schema);
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    req.tag_items_ref() = items;
    auto* processor = AlterTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

    // check result
    cpp2::GetTagReq getReq;
    getReq.space_id_ref() = 1;
    getReq.tag_name_ref() = "tag_0";
    getReq.version_ref() = -1;

    auto* getProcessor = GetTagProcessor::instance(kv.get());
    auto getFut = getProcessor->getFuture();
    getProcessor->process(getReq);
    auto resp1 = std::move(getFut).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp1.get_code());
    std::vector<cpp2::ColumnDef> cols = resp1.get_schema().get_columns();
    bool expected = false;
    for (const auto& col : cols) {
      if (col.get_name() == "add_col_fixed_string_type") {
        ASSERT_EQ(PropertyType::FIXED_STRING, col.get_type().get_type());
        auto defaultValueExpr = Expression::decode(metaPool, *col.get_default_value());
        DefaultValueContext mContext;
        auto value = Expression::eval(defaultValueExpr, mContext);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("Hello", value.getStr());
        expected = true;
      }
    }
    ASSERT_TRUE(expected);
  }
}

TEST(ProcessorTest, AlterEdgeTest) {
  fs::TempDir rootPath("/tmp/AlterEdgeTest.XXXXXX");
  auto kv = MockCluster::initMetaKV(rootPath.path());
  TestUtils::assembleSpace(kv.get(), 1, 1);
  TestUtils::mockEdge(kv.get(), 1);

  // Drop all, then add
  {
    cpp2::AlterEdgeReq req;
    cpp2::Schema dropSch;
    cpp2::ColumnDef column;
    std::vector<cpp2::AlterSchemaItem> items;
    for (int32_t i = 0; i < 2; i++) {
      column.name = folly::stringPrintf("edge_0_col_%d", i);
      (*dropSch.columns_ref()).emplace_back(std::move(column));
    }

    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::DROP;
    items.back().schema_ref() = std::move(dropSch);
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    req.edge_items_ref() = items;
    auto* processor = AlterEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    cpp2::ListEdgesReq req;
    req.space_id_ref() = 1;
    auto* processor = ListEdgesProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    auto edges = resp.get_edges();
    ASSERT_EQ(2, edges.size());
    auto edge = edges[0].get_version() > 0 ? edges[0] : edges[1];
    EXPECT_EQ(0, edge.get_edge_type());
    EXPECT_EQ("edge_0", edge.get_edge_name());
    EXPECT_EQ(1, edge.get_version());
    EXPECT_EQ(0, (*edge.get_schema().columns_ref()).size());
  }
  {
    cpp2::AlterEdgeReq req;
    cpp2::Schema addSch;
    std::vector<cpp2::AlterSchemaItem> items;
    for (int32_t i = 0; i < 2; i++) {
      cpp2::ColumnDef column;
      column.name = folly::stringPrintf("edge_0_col_%d", i);
      column.type.type_ref() = i < 1 ? PropertyType::INT64 : PropertyType::STRING;
      (*addSch.columns_ref()).emplace_back(std::move(column));
    }

    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::ADD;
    items.back().schema_ref() = std::move(addSch);
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    req.edge_items_ref() = items;
    auto* processor = AlterEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    cpp2::AlterEdgeReq req;
    std::vector<cpp2::AlterSchemaItem> items;
    cpp2::Schema addSch;
    for (auto i = 0; i < 2; i++) {
      cpp2::ColumnDef column;
      column.name = folly::stringPrintf("edge_%d_col_%d", 0, i + 10);
      column.type.type_ref() = i < 1 ? PropertyType::INT64 : PropertyType::STRING;
      (*addSch.columns_ref()).emplace_back(std::move(column));
    }
    cpp2::Schema changeSch;
    for (auto i = 0; i < 2; i++) {
      cpp2::ColumnDef column;
      column.name = folly::stringPrintf("edge_%d_col_%d", 0, i);
      column.type.type_ref() = i < 1 ? PropertyType::BOOL : PropertyType::DOUBLE;
      (*changeSch.columns_ref()).emplace_back(std::move(column));
    }
    cpp2::Schema dropSch;
    cpp2::ColumnDef column;
    column.name = "edge_0_col_0";
    (*dropSch.columns_ref()).emplace_back(std::move(column));

    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::ADD;
    items.back().schema_ref() = std::move(addSch);

    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::CHANGE;
    items.back().schema_ref() = std::move(changeSch);

    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::DROP;
    items.back().schema_ref() = std::move(dropSch);

    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    req.edge_items_ref() = items;
    auto* processor = AlterEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // Verify alter result.
  {
    cpp2::ListEdgesReq req;
    req.space_id_ref() = 1;
    auto* processor = ListEdgesProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    auto edges = resp.get_edges();
    ASSERT_EQ(4, edges.size());
    // Get the latest one by comparing the versions.
    int version = 0;
    int max_index = 0;
    for (uint32_t i = 0; i < edges.size(); i++) {
      if (edges[i].get_version() > version) {
        max_index = i;
        version = edges[i].get_version();
      }
    }
    auto edge = edges[max_index];
    EXPECT_EQ(0, edge.get_edge_type());
    EXPECT_EQ("edge_0", edge.get_edge_name());
    EXPECT_EQ(3, edge.get_version());

    cpp2::Schema schema;
    std::vector<cpp2::ColumnDef> cols;

    cpp2::ColumnDef column;
    column.name = "edge_0_col_1";
    column.type.type_ref() = PropertyType::DOUBLE;
    cols.emplace_back(std::move(column));

    column.name = "edge_0_col_10";
    column.type.type_ref() = PropertyType::INT64;
    cols.emplace_back(std::move(column));

    column.name = "edge_0_col_11";
    column.type.type_ref() = PropertyType::STRING;
    cols.emplace_back(std::move(column));
    schema.columns_ref() = std::move(cols);
    EXPECT_EQ(schema, edge.get_schema());
  }

  // Only set ttl_duration, failed
  {
    cpp2::AlterEdgeReq req;
    cpp2::SchemaProp schemaProp;
    schemaProp.ttl_duration_ref() = 100;

    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    req.schema_prop_ref() = std::move(schemaProp);
    auto* processor = AlterEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_NE(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // Succeed
  {
    cpp2::AlterEdgeReq req;
    cpp2::SchemaProp schemaProp;
    schemaProp.ttl_duration_ref() = 100;
    schemaProp.ttl_col_ref() = "edge_0_col_10";

    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    req.schema_prop_ref() = std::move(schemaProp);
    auto* processor = AlterEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // Verify alter result.
  {
    cpp2::ListEdgesReq req;
    req.space_id_ref() = 1;
    auto* processor = ListEdgesProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    auto edges = resp.get_edges();
    ASSERT_EQ(5, edges.size());
    // EdgeItems in vector are unordered.So get the latest one by comparing the
    // versions.
    int version = 0;
    int max_index = 0;
    for (uint32_t i = 0; i < edges.size(); i++) {
      if (edges[i].get_version() > version) {
        max_index = i;
        version = edges[i].get_version();
      }
    }
    auto edge = edges[max_index];

    EXPECT_EQ(0, edge.get_edge_type());
    EXPECT_EQ("edge_0", edge.get_edge_name());
    EXPECT_EQ(4, edge.get_version());

    cpp2::Schema schema;
    std::vector<cpp2::ColumnDef> cols;

    cpp2::ColumnDef column;
    column.name = "edge_0_col_1";
    column.type.type_ref() = PropertyType::DOUBLE;
    cols.emplace_back(std::move(column));

    column.name = "edge_0_col_10";
    column.type.type_ref() = PropertyType::INT64;
    cols.emplace_back(std::move(column));

    column.name = "edge_0_col_11";
    column.type.type_ref() = PropertyType::STRING;
    cols.emplace_back(std::move(column));

    schema.columns_ref() = std::move(cols);

    cpp2::SchemaProp schemaProp;
    schemaProp.ttl_duration_ref() = 100;
    schemaProp.ttl_col_ref() = "edge_0_col_10";
    schema.schema_prop_ref() = std::move(schemaProp);
    EXPECT_EQ(schema.get_columns(), edge.get_schema().get_columns());
    EXPECT_EQ(*schema.get_schema_prop().get_ttl_duration(),
              *edge.get_schema().get_schema_prop().get_ttl_duration());
    EXPECT_EQ(*schema.get_schema_prop().get_ttl_col(),
              *edge.get_schema().get_schema_prop().get_ttl_col());
  }
  // Change col on ttl, failed
  {
    cpp2::AlterEdgeReq req;
    std::vector<cpp2::AlterSchemaItem> items;
    cpp2::Schema changeSch;
    cpp2::ColumnDef column;
    column.name = "edge_0_col_10";
    column.type.type_ref() = PropertyType::DOUBLE;
    (*changeSch.columns_ref()).emplace_back(std::move(column));

    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::CHANGE;
    items.back().schema_ref() = std::move(changeSch);

    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    req.edge_items_ref() = items;
    auto* processor = AlterEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_NE(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // Set ttl col on illegal column type, failed
  {
    cpp2::AlterEdgeReq req;
    cpp2::SchemaProp schemaProp;
    schemaProp.ttl_duration_ref() = 100;
    schemaProp.ttl_col_ref() = "edge_0_col_11";

    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    req.schema_prop_ref() = std::move(schemaProp);
    auto* processor = AlterEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_NE(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // Drop ttl_col column
  {
    cpp2::AlterEdgeReq req;
    std::vector<cpp2::AlterSchemaItem> items;
    cpp2::Schema dropSch;
    cpp2::ColumnDef column;
    column.name = "edge_0_col_10";
    (*dropSch.columns_ref()).emplace_back(std::move(column));

    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::DROP;
    items.back().schema_ref() = std::move(dropSch);
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    req.edge_items_ref() = items;
    auto* processor = AlterEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // Verify alter drop ttl col result
  {
    cpp2::ListEdgesReq req;
    req.space_id_ref() = 1;
    auto* processor = ListEdgesProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    auto edges = resp.get_edges();
    ASSERT_EQ(6, edges.size());
    // EdgeItems in vector are unordered.So get the latest one by comparing the
    // versions.
    int version = 0;
    int max_index = 0;
    for (uint32_t i = 0; i < edges.size(); i++) {
      if (edges[i].get_version() > version) {
        max_index = i;
        version = edges[i].get_version();
      }
    }
    auto edge = edges[max_index];

    EXPECT_EQ(0, edge.get_edge_type());
    EXPECT_EQ("edge_0", edge.get_edge_name());
    EXPECT_EQ(5, edge.get_version());

    cpp2::Schema schema;
    std::vector<cpp2::ColumnDef> cols;

    cpp2::ColumnDef column;
    column.name = "edge_0_col_1";
    column.type.type_ref() = PropertyType::DOUBLE;
    cols.emplace_back(std::move(column));

    column.name = "edge_0_col_11";
    column.type.type_ref() = PropertyType::STRING;
    cols.emplace_back(std::move(column));

    schema.columns_ref() = std::move(cols);

    cpp2::SchemaProp schemaProp;
    schemaProp.ttl_duration_ref() = 0;
    schemaProp.ttl_col_ref() = "";
    schema.schema_prop_ref() = std::move(schemaProp);
    EXPECT_EQ(schema.get_columns(), edge.get_schema().get_columns());
    EXPECT_EQ(*schema.get_schema_prop().get_ttl_duration(),
              *edge.get_schema().get_schema_prop().get_ttl_duration());
    EXPECT_EQ(*schema.get_schema_prop().get_ttl_col(),
              *edge.get_schema().get_schema_prop().get_ttl_col());
  }

  // Verify ErrorCode of add
  {
    cpp2::AlterEdgeReq req;
    std::vector<cpp2::AlterSchemaItem> items;
    cpp2::Schema addSch;
    cpp2::ColumnDef column;
    column.name = "edge_0_col_1";
    column.type.type_ref() = PropertyType::INT64;
    (*addSch.columns_ref()).emplace_back(std::move(column));

    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::ADD;
    items.back().schema_ref() = std::move(addSch);

    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    req.edge_items_ref() = items;
    auto* processor = AlterEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_EXISTED, resp.get_code());
  }
  // Verify ErrorCode of change
  {
    cpp2::AlterEdgeReq req;
    std::vector<cpp2::AlterSchemaItem> items;
    cpp2::Schema changeSch;
    cpp2::ColumnDef column;
    column.name = "edge_0_col_2";
    column.type.type_ref() = PropertyType::INT64;
    (*changeSch.columns_ref()).emplace_back(std::move(column));

    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::CHANGE;
    items.back().schema_ref() = std::move(changeSch);

    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    req.edge_items_ref() = items;
    auto* processor = AlterEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND, resp.get_code());
  }
  // Verify ErrorCode of drop
  {
    cpp2::AlterEdgeReq req;
    std::vector<cpp2::AlterSchemaItem> items;
    cpp2::Schema dropSch;
    cpp2::ColumnDef column;
    column.name = "edge_0_col_2";
    column.type.type_ref() = PropertyType::INT64;
    (*dropSch.columns_ref()).emplace_back(std::move(column));

    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::DROP;
    items.back().schema_ref() = std::move(dropSch);

    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    req.edge_items_ref() = items;
    auto* processor = AlterEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND, resp.get_code());
  }
  // Add col with wrong default value type
  {
    cpp2::AlterEdgeReq req;
    std::vector<cpp2::AlterSchemaItem> items;
    cpp2::Schema schema;
    cpp2::ColumnDef column;
    column.name = "add_col_mismatch_type";
    column.type.type_ref() = PropertyType::INT64;
    const auto& strValue = *ConstantExpression::make(metaPool, "default value");
    column.default_value_ref() = Expression::encode(strValue);
    (*schema.columns_ref()).emplace_back(std::move(column));

    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::ADD;
    items.back().schema_ref() = std::move(schema);
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    req.edge_items_ref() = items;
    auto* processor = AlterEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_INVALID_PARM, resp.get_code());
  }
  // Add col with out of range of fixed string
  {
    cpp2::AlterEdgeReq req;
    std::vector<cpp2::AlterSchemaItem> items;
    cpp2::Schema schema;
    cpp2::ColumnDef column;
    column.name = "add_col_fixed_string_type";
    column.type.type_ref() = PropertyType::FIXED_STRING;
    column.type.type_length_ref() = 5;
    const auto& strValue = *ConstantExpression::make(metaPool, "Hello world!");
    column.default_value_ref() = Expression::encode(strValue);
    (*schema.columns_ref()).emplace_back(std::move(column));

    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::ADD;
    items.back().schema_ref() = std::move(schema);
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    req.edge_items_ref() = items;
    auto* processor = AlterEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());

    // check result
    cpp2::GetEdgeReq getReq;
    getReq.space_id_ref() = 1;
    getReq.edge_name_ref() = "edge_0";
    getReq.version_ref() = -1;

    auto* getProcessor = GetEdgeProcessor::instance(kv.get());
    auto getFut = getProcessor->getFuture();
    getProcessor->process(getReq);
    auto resp1 = std::move(getFut).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp1.get_code());
    std::vector<cpp2::ColumnDef> cols = resp1.get_schema().get_columns();
    bool expected = false;
    for (const auto& col : cols) {
      if (col.get_name() == "add_col_fixed_string_type") {
        ASSERT_EQ(PropertyType::FIXED_STRING, col.get_type().get_type());
        auto defaultValueExpr = Expression::decode(metaPool, *col.get_default_value());
        DefaultValueContext mContext;
        auto value = Expression::eval(defaultValueExpr, mContext);
        ASSERT_TRUE(value.isStr());
        ASSERT_EQ("Hello", value.getStr());
        expected = true;
      }
    }
    ASSERT_TRUE(expected);
  }
}

TEST(ProcessorTest, SameNameTagsTest) {
  fs::TempDir rootPath("/tmp/SameNameTagsTest.XXXXXX");
  auto kv = MockCluster::initMetaKV(rootPath.path());
  TestUtils::createSomeHosts(kv.get());

  {
    cpp2::SpaceDesc properties;
    properties.space_name_ref() = "default_space";
    properties.partition_num_ref() = 9;
    properties.replica_factor_ref() = 3;
    cpp2::CreateSpaceReq req;
    req.properties_ref() = std::move(properties);
    auto* processor = CreateSpaceProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(1, resp.get_id().get_space_id());
  }
  {
    cpp2::SpaceDesc properties;
    properties.space_name_ref() = "second_space";
    properties.partition_num_ref() = 9;
    properties.replica_factor_ref() = 1;
    cpp2::CreateSpaceReq req;
    req.properties_ref() = std::move(properties);
    auto* processor = CreateSpaceProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(2, resp.get_id().get_space_id());
  }

  // Add same tag name in different space
  cpp2::Schema schema;
  std::vector<cpp2::ColumnDef> cols;
  cols.emplace_back(TestUtils::columnDef(0, PropertyType::INT64));
  cols.emplace_back(TestUtils::columnDef(1, PropertyType::FLOAT));
  cols.emplace_back(TestUtils::columnDef(2, PropertyType::STRING));
  schema.columns_ref() = std::move(cols);
  {
    cpp2::CreateTagReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "default_tag";
    req.schema_ref() = schema;
    auto* processor = CreateTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(3, resp.get_id().get_tag_id());
  }
  {
    cpp2::CreateTagReq req;
    req.space_id_ref() = 2;
    req.tag_name_ref() = "default_tag";
    req.schema_ref() = schema;
    auto* processor = CreateTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(3, resp.get_id().get_tag_id());
  }

  // Remove Test
  {
    cpp2::DropTagReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "default_tag";
    auto* processor = DropTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }

  // List Test
  {
    cpp2::ListTagsReq req;
    req.space_id_ref() = 1;
    auto* processor = ListTagsProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    std::vector<nebula::meta::cpp2::TagItem> tags;
    tags = resp.get_tags();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(0, tags.size());
  }
  {
    cpp2::ListTagsReq req;
    req.space_id_ref() = 2;
    auto* processor = ListTagsProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    std::vector<nebula::meta::cpp2::TagItem> tags;
    tags = resp.get_tags();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(1, tags.size());

    ASSERT_EQ(3, tags[0].get_tag_id());
    ASSERT_EQ("default_tag", tags[0].get_tag_name());
  }
}

TEST(ProcessorTest, SessionManagerTest) {
  fs::TempDir rootPath("/tmp/SessionTest.XXXXXX");
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
  TestUtils::createSomeHosts(kv.get());
  TestUtils::assembleSpace(kv.get(), 1, 1);
  SessionID sessionId = 0;
  ExecutionPlanID epId = 1;
  {
    cpp2::CreateUserReq req;
    req.if_not_exists_ref() = false;
    req.account_ref() = "test_user";
    req.encoded_pwd_ref() = "password";
    auto* processor = CreateUserProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // create session
  {
    cpp2::CreateSessionReq req;
    req.user_ref() = "test_user";
    req.graph_addr_ref() = HostAddr("127.0.0.1", 3699);

    auto* processor = CreateSessionProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    sessionId = resp.get_session().get_session_id();
  }
  // update session
  {
    cpp2::UpdateSessionsReq req;
    meta::cpp2::Session session;
    session.session_id_ref() = sessionId;
    session.space_name_ref() = "test";
    session.update_time_ref() = time::WallClock::fastNowInMicroSec();
    session.queries_ref()->emplace(epId, cpp2::QueryDesc());
    req.sessions_ref() = {session};

    auto* processor = UpdateSessionsProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_TRUE(resp.get_killed_queries().empty());
  }
  // list session
  {
    cpp2::ListSessionsReq req;

    auto* processor = ListSessionsProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(resp.get_sessions().size(), 1);
    ASSERT_EQ("test", resp.get_sessions()[0].get_space_name());
  }
  // get session
  {
    cpp2::GetSessionReq req;
    req.session_id_ref() = sessionId;

    auto* processor = GetSessionProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ("test", resp.get_session().get_space_name());
  }
  // Kill query
  {
    cpp2::KillQueryReq killReq;
    std::unordered_map<SessionID, std::unordered_set<ExecutionPlanID>> killedQueries;
    std::unordered_set<ExecutionPlanID> eps = {epId};
    killedQueries.emplace(sessionId, std::move(eps));
    killReq.kill_queries_ref() = std::move(killedQueries);

    auto* processor = KillQueryProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(killReq);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // update session and get all killed queries
  {
    cpp2::UpdateSessionsReq req;
    meta::cpp2::Session session;
    session.session_id_ref() = sessionId;
    session.space_name_ref() = "test";
    session.queries_ref()->emplace(epId, cpp2::QueryDesc());
    req.sessions_ref() = {session};

    auto* processor = UpdateSessionsProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    auto& killedQueries = resp.get_killed_queries();
    EXPECT_EQ(killedQueries.size(), 1);
    for (auto& s : killedQueries) {
      EXPECT_EQ(s.first, sessionId);
      for (auto& q : s.second) {
        EXPECT_EQ(q.first, epId);
      }
    }
  }
  // delete session
  {
    cpp2::RemoveSessionReq delReq;
    delReq.session_id_ref() = sessionId;

    auto* dProcessor = RemoveSessionProcessor::instance(kv.get());
    auto delFut = dProcessor->getFuture();
    dProcessor->process(delReq);
    auto dResp = std::move(delFut).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, dResp.get_code());

    // check result
    cpp2::GetSessionReq getReq;
    getReq.session_id_ref() = sessionId;

    auto* gProcessor = GetSessionProcessor::instance(kv.get());
    auto getFut = gProcessor->getFuture();
    gProcessor->process(getReq);
    auto gResp = std::move(getFut).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_SESSION_NOT_FOUND, gResp.get_code());
  }
}

TEST(ProcessorTest, TagIdAndEdgeTypeInSpaceRangeTest) {
  fs::TempDir rootPath("/tmp/TagIdAndEdgeTypeInSpaceRangeTest.XXXXXX");
  auto kv = MockCluster::initMetaKV(rootPath.path());
  TestUtils::createSomeHosts(kv.get());

  // mock one space and ten tag, ten edge
  {
    // space Id is 1
    TestUtils::assembleSpace(kv.get(), 1, 1);
    // tagId is from 2 to 11 in space 1
    TestUtils::mockTag(kv.get(), 10, 0, false, 2, 1);
    // edgeType is form 12 to 21 in space 1
    TestUtils::mockEdge(kv.get(), 10, 0, false, 12, 1);

    // check tag and edge count
    int count = 0;

    auto tagprefix = MetaKeyUtils::schemaTagsPrefix(1);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto retCode = kv->prefix(kDefaultSpaceId, kDefaultPartId, tagprefix, &iter);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, retCode);
    while (iter->valid()) {
      count++;
      iter->next();
    }
    ASSERT_EQ(10, count);

    auto edgeprefix = MetaKeyUtils::schemaEdgesPrefix(1);
    retCode = kv->prefix(kDefaultSpaceId, kDefaultPartId, edgeprefix, &iter);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, retCode);
    while (iter->valid()) {
      count++;
      iter->next();
    }
    ASSERT_EQ(20, count);

    // modify id to 21 for mock some schema
    folly::SharedMutex::WriteHolder holder(LockUtils::idLock());
    std::string kId = "__id__";
    int32_t id = 21;
    std::vector<kvstore::KV> data;
    data.emplace_back(kId, std::string(reinterpret_cast<const char*>(&id), sizeof(id)));
    folly::Baton<true, std::atomic> baton;
    auto ret = nebula::cpp2::ErrorCode::SUCCEEDED;
    kv->asyncMultiPut(
        kDefaultSpaceId, kDefaultPartId, std::move(data), [&](nebula::cpp2::ErrorCode code) {
          ret = code;
          baton.post();
        });
    baton.wait();
    ASSERT_EQ(ret, nebula::cpp2::ErrorCode::SUCCEEDED);
  }

  cpp2::Schema schema;
  std::vector<cpp2::ColumnDef> cols;
  cols.emplace_back(TestUtils::columnDef(0, PropertyType::INT64));
  cols.emplace_back(TestUtils::columnDef(1, PropertyType::FLOAT));
  cols.emplace_back(TestUtils::columnDef(2, PropertyType::STRING));
  schema.columns_ref() = std::move(cols);

  // create space, use global id
  {
    cpp2::SpaceDesc properties;
    properties.space_name_ref() = "my_space";
    properties.partition_num_ref() = 9;
    properties.replica_factor_ref() = 1;
    cpp2::CreateSpaceReq req;
    req.properties_ref() = std::move(properties);

    auto* processor = CreateSpaceProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(22, resp.get_id().get_space_id());
  }
  // create tag in my_space, because there is no local_id key, use global id + 1
  {
    // Succeeded
    cpp2::CreateTagReq req;
    req.space_id_ref() = 22;
    req.tag_name_ref() = "default_tag";
    req.schema_ref() = schema;
    auto* processor = CreateTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(23, resp.get_id().get_tag_id());
  }
  // create edge in my_space, because there is local_id key, use local_id + 1
  {
    // Succeeded
    cpp2::CreateEdgeReq req;
    req.space_id_ref() = 22;
    req.edge_name_ref() = "default_edge";
    req.schema_ref() = schema;
    auto* processor = CreateEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(24, resp.get_id().get_edge_type());
  }

  // create space, space Id is global id + 1
  {
    cpp2::SpaceDesc properties;
    properties.space_name_ref() = "last_space";
    properties.partition_num_ref() = 9;
    properties.replica_factor_ref() = 1;
    cpp2::CreateSpaceReq req;
    req.properties_ref() = std::move(properties);

    auto* processor = CreateSpaceProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(23, resp.get_id().get_space_id());
  }
  // create tag in my_space, because there is local_id key, use local id + 1
  {
    // Succeeded
    cpp2::CreateTagReq req;
    req.space_id_ref() = 22;
    req.tag_name_ref() = "my_tag";
    req.schema_ref() = schema;
    auto* processor = CreateTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(25, resp.get_id().get_tag_id());
  }
  // create edge in my_space, because there is local_id key use local_id + 1
  {
    // Succeeded
    cpp2::CreateEdgeReq req;
    req.space_id_ref() = 22;
    req.edge_name_ref() = "my_edge";
    req.schema_ref() = schema;
    auto* processor = CreateEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(26, resp.get_id().get_edge_type());
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
