/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include <gtest/gtest.h>
#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "meta/test/TestUtils.h"
#include "meta/processors/partsMan/CreateSpaceProcessor.h"
#include "meta/processors/partsMan/ListSpacesProcessor.h"
#include "meta/processors/partsMan/ListSpacesProcessor.h"
#include "meta/processors/partsMan/ListPartsProcessor.h"
#include "meta/processors/partsMan/DropSpaceProcessor.h"
#include "meta/processors/partsMan/GetSpaceProcessor.h"
#include "meta/processors/partsMan/GetPartsAllocProcessor.h"
#include "meta/processors/schemaMan/CreateTagProcessor.h"
#include "meta/processors/schemaMan/CreateEdgeProcessor.h"
#include "meta/processors/schemaMan/DropTagProcessor.h"
#include "meta/processors/schemaMan/DropEdgeProcessor.h"
#include "meta/processors/schemaMan/GetTagProcessor.h"
#include "meta/processors/schemaMan/GetEdgeProcessor.h"
#include "meta/processors/schemaMan/ListTagsProcessor.h"
#include "meta/processors/schemaMan/ListEdgesProcessor.h"
#include "meta/processors/schemaMan/AlterTagProcessor.h"
#include "meta/processors/schemaMan/AlterEdgeProcessor.h"
#include "meta/processors/indexMan/CreateTagIndexProcessor.h"
#include "meta/processors/indexMan/DropTagIndexProcessor.h"
#include "meta/processors/indexMan/GetTagIndexProcessor.h"
#include "meta/processors/indexMan/ListTagIndexesProcessor.h"
#include "meta/processors/indexMan/CreateEdgeIndexProcessor.h"
#include "meta/processors/indexMan/DropEdgeIndexProcessor.h"
#include "meta/processors/indexMan/GetEdgeIndexProcessor.h"
#include "meta/processors/indexMan/ListEdgeIndexesProcessor.h"
#include "meta/processors/customKV/MultiPutProcessor.h"
#include "meta/processors/customKV/GetProcessor.h"
#include "meta/processors/customKV/MultiGetProcessor.h"
#include "meta/processors/customKV/RemoveProcessor.h"
#include "meta/processors/customKV/RemoveRangeProcessor.h"
#include "meta/processors/customKV/ScanProcessor.h"
#include "meta/processors/zoneMan/AddGroupProcessor.h"
#include "meta/processors/zoneMan/DropGroupProcessor.h"
#include "meta/processors/zoneMan/ListGroupsProcessor.h"
#include "meta/processors/zoneMan/UpdateGroupProcessor.h"
#include "meta/processors/zoneMan/AddZoneProcessor.h"
#include "meta/processors/zoneMan/ListZonesProcessor.h"
#include "meta/processors/admin/CreateBackupProcessor.h"

DECLARE_int32(heartbeat_interval_secs);
DECLARE_uint32(expired_time_factor);
namespace nebula {
namespace meta {

using cpp2::PropertyType;

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
        req.set_type(cpp2::ListHostType::STORAGE);
        auto* processor = ListHostsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(10, (*resp.hosts_ref()).size());
        for (auto i = 0; i < 10; i++) {
            ASSERT_EQ(std::to_string(i), (*(*resp.hosts_ref())[i].hostAddr_ref()).host);
            ASSERT_EQ(i, (*(*resp.hosts_ref())[i].hostAddr_ref()).port);
            ASSERT_EQ(cpp2::HostStatus::ONLINE, *(*resp.hosts_ref())[i].status_ref());
        }
    }
    {
        // host info expired
        sleep(FLAGS_heartbeat_interval_secs * FLAGS_expired_time_factor + 1);
        cpp2::ListHostsReq req;
        req.set_type(cpp2::ListHostType::STORAGE);
        auto* processor = ListHostsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(10, (*resp.hosts_ref()).size());
        for (auto i = 0; i < 10; i++) {
            ASSERT_EQ(std::to_string(i), (*(*resp.hosts_ref())[i].hostAddr_ref()).host);
            ASSERT_EQ(i, (*(*resp.hosts_ref())[i].hostAddr_ref()).port);
            ASSERT_EQ(cpp2::HostStatus::OFFLINE, *(*resp.hosts_ref())[i].status_ref());
        }
    }
}

TEST(ProcessorTest, ListSpecficHostsTest) {
    fs::TempDir rootPath("/tmp/ListMultiRoleHostsTest.XXXXXX");
    std::vector<cpp2::HostRole> roleVec{cpp2::HostRole::GRAPH,
                                        cpp2::HostRole::META,
                                        cpp2::HostRole::STORAGE};
    std::vector<std::string> gitInfoShaVec{"fakeGraphInfoSHA",
                                           "fakeMetaInfoSHA",
                                           "fakeStorageInfoSHA"};
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
        req.set_type(cpp2::ListHostType::GRAPH);
        auto* processor = ListHostsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        f.wait();
        auto resp = std::move(f).get();
        ASSERT_EQ(graphHosts.size(), (*resp.hosts_ref()).size());
        for (auto i = 0U; i < (*resp.hosts_ref()).size(); ++i) {
            ASSERT_EQ(std::to_string(i), (*(*resp.hosts_ref())[i].hostAddr_ref()).host);
            ASSERT_EQ(i, (*(*resp.hosts_ref())[i].hostAddr_ref()).port);
            ASSERT_EQ(cpp2::HostStatus::ONLINE, *(*resp.hosts_ref())[i].status_ref());
            ASSERT_EQ(gitInfoShaVec[0], *(*resp.hosts_ref())[i].git_info_sha_ref());
        }
    }

    {
        cpp2::ListHostsReq req;
        req.set_type(cpp2::ListHostType::STORAGE);
        auto* processor = ListHostsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        f.wait();
        auto resp = std::move(f).get();
        ASSERT_EQ(storageHosts.size(), (*resp.hosts_ref()).size());
        auto b = graphHosts.size() + metaHosts.size();
        for (auto i = 0U; i < (*resp.hosts_ref()).size(); ++i) {
            ASSERT_EQ(std::to_string(i+b), (*(*resp.hosts_ref())[i].hostAddr_ref()).host);
            ASSERT_EQ(i+b, (*(*resp.hosts_ref())[i].hostAddr_ref()).port);
            ASSERT_EQ(cpp2::HostStatus::ONLINE, *(*resp.hosts_ref())[i].status_ref());
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
        req.set_space_id(1);
        auto* processor = ListPartsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(9, (*resp.parts_ref()).size());

        auto parts = std::move((*resp.parts_ref()));
        std::sort(parts.begin(), parts.end(), [] (const auto& a, const auto& b) {
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

    // register HB with leader distribution
    {
        auto now = time::WallClock::fastNowInMilliSec();
        HostInfo info(now, cpp2::HostRole::STORAGE, gitInfoSha());

        int64_t term = 999;
        auto makeLeaderInfo = [&](PartitionID partId) {
            cpp2::LeaderInfo leaderInfo;
            leaderInfo.set_part_id(partId);
            leaderInfo.set_term(term);
            return leaderInfo;
        };

        GraphSpaceID spaceId = 1;
        ActiveHostsMan::AllLeaders allLeaders;
        for (int i = 1; i < 6; ++i) {
            allLeaders[spaceId].emplace_back(makeLeaderInfo(i));
        }
        auto ret = ActiveHostsMan::updateHostInfo(kv.get(), {"0", 0}, info, &allLeaders);
        CHECK_EQ(ret, kvstore::ResultCode::SUCCEEDED);

        allLeaders.clear();
        for (int i = 6; i < 9; ++i) {
            allLeaders[spaceId].emplace_back(makeLeaderInfo(i));
        }
        ret = ActiveHostsMan::updateHostInfo(kv.get(), {"1", 1}, info, &allLeaders);
        CHECK_EQ(ret, kvstore::ResultCode::SUCCEEDED);


        // for (int i = 9; i <= 9; ++i) {
        //     allLeaders[spaceId].emplace_back(makeLeaderInfo(i));
        // }
        allLeaders.clear();
        allLeaders[spaceId].emplace_back(makeLeaderInfo(9));
        ret = ActiveHostsMan::updateHostInfo(kv.get(), {"2", 2}, info, &allLeaders);
        CHECK_EQ(ret, kvstore::ResultCode::SUCCEEDED);
    }

    {
        cpp2::ListPartsReq req;
        req.set_space_id(1);
        auto* processor = ListPartsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(9, (*resp.parts_ref()).size());

        auto parts = std::move((*resp.parts_ref()));
        std::sort(parts.begin(), parts.end(), [] (const auto& a, const auto& b) {
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
                auto it = std::find_if(hosts.begin(), hosts.end(),
                        [&] (const auto& host) {
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
        properties.set_space_name("default_space");
        properties.set_partition_num(8);
        properties.set_replica_factor(3);
        properties.set_charset_name("utf8");
        properties.set_collate_name("utf8_bin");
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));
        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(1, resp.get_id().get_space_id());
    }
    {
        cpp2::GetSpaceReq req;
        req.set_space_name("default_space");
        auto* processor = GetSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ("default_space", resp.get_item().get_properties().get_space_name());
        ASSERT_EQ(8, resp.get_item().get_properties().get_partition_num());
        ASSERT_EQ(3, resp.get_item().get_properties().get_replica_factor());
        ASSERT_EQ(8, *resp.get_item().get_properties().get_vid_type().get_type_length());
        ASSERT_EQ(cpp2::PropertyType::FIXED_STRING,
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
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(1, resp.get_spaces().size());
        ASSERT_EQ(1, resp.get_spaces()[0].get_id().get_space_id());
        ASSERT_EQ("default_space", resp.get_spaces()[0].get_name());
    }
    // Check the result. The dispatch way from part to hosts is in a round robin fashion.
    {
        cpp2::GetPartsAllocReq req;
        req.set_space_id(1);
        auto* processor = GetPartsAllocProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
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
        req.set_space_name("default_space");
        auto* processor = DropSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::ListSpacesReq req;
        auto* processor = ListSpacesProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(0, resp.get_spaces().size());
    }
    // With IF EXISTS
    {
        cpp2::DropSpaceReq req;
        req.set_space_name("not_exist_space");
        req.set_if_exists(true);
        auto* processor = DropSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        constexpr char spaceName[] = "exist_space";
        cpp2::CreateSpaceReq req;
        cpp2::SpaceDesc properties;
        properties.set_space_name(spaceName);
        req.set_properties(std::move(properties));
        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());

        cpp2::DropSpaceReq req1;
        req1.set_space_name(spaceName);
        req1.set_if_exists(true);
        auto* processor1 = DropSpaceProcessor::instance(kv.get());
        auto f1 = processor1->getFuture();
        processor1->process(req1);
        auto resp1 = std::move(f1).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp1.get_code());
    }
    // Test default value
    {
        cpp2::SpaceDesc properties;
        properties.set_space_name("space_with_no_option");
        cpp2::CreateSpaceReq creq;
        creq.set_properties(std::move(properties));
        auto* cprocessor = CreateSpaceProcessor::instance(kv.get());
        auto cf = cprocessor->getFuture();
        cprocessor->process(creq);
        auto cresp = std::move(cf).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, cresp.get_code());

        cpp2::GetSpaceReq greq;
        greq.set_space_name("space_with_no_option");
        auto* gprocessor = GetSpaceProcessor::instance(kv.get());
        auto gf = gprocessor->getFuture();
        gprocessor->process(greq);
        auto gresp = std::move(gf).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, gresp.get_code());
        ASSERT_EQ("space_with_no_option", gresp.get_item().get_properties().get_space_name());
        ASSERT_EQ(100, gresp.get_item().get_properties().get_partition_num());
        ASSERT_EQ(1, gresp.get_item().get_properties().get_replica_factor());
        // Because setting default value in graph
        ASSERT_EQ("", gresp.get_item().get_properties().get_charset_name());
        ASSERT_EQ("", gresp.get_item().get_properties().get_collate_name());

        cpp2::DropSpaceReq dreq;
        dreq.set_space_name("space_with_no_option");
        auto* dprocessor = DropSpaceProcessor::instance(kv.get());
        auto df = dprocessor->getFuture();
        dprocessor->process(dreq);
        auto dresp = std::move(df).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, dresp.get_code());
    }
}

TEST(ProcessorTest, SpaceWithGroupTest) {
    fs::TempDir rootPath("/tmp/SpaceWithGroupTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    std::vector<HostAddr> addresses;
    for (int32_t i = 0; i < 10; i++) {
        addresses.emplace_back(std::to_string(i), i);
    }
    TestUtils::createSomeHosts(kv.get(), std::move(addresses));

    // Add Zones
    {
        {
            std::vector<HostAddr> nodes;
            for (int32_t i = 0; i < 2; i++) {
                nodes.emplace_back(std::to_string(i), i);
            }
            cpp2::AddZoneReq req;
            req.set_zone_name("zone_0");
            req.set_nodes(std::move(nodes));
            auto* processor = AddZoneProcessor::instance(kv.get());
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        }
        {
            std::vector<HostAddr> nodes;
            for (int32_t i = 2; i < 4; i++) {
                nodes.emplace_back(std::to_string(i), i);
            }
            cpp2::AddZoneReq req;
            req.set_zone_name("zone_1");
            req.set_nodes(std::move(nodes));
            auto* processor = AddZoneProcessor::instance(kv.get());
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        }
        {
            std::vector<HostAddr> nodes;
            for (int32_t i = 4; i < 6; i++) {
                nodes.emplace_back(std::to_string(i), i);
            }
            cpp2::AddZoneReq req;
            req.set_zone_name("zone_2");
            req.set_nodes(std::move(nodes));
            auto* processor = AddZoneProcessor::instance(kv.get());
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        }
        {
            std::vector<HostAddr> nodes;
            for (int32_t i = 6; i < 8; i++) {
                nodes.emplace_back(std::to_string(i), i);
            }
            cpp2::AddZoneReq req;
            req.set_zone_name("zone_3");
            req.set_nodes(std::move(nodes));
            auto* processor = AddZoneProcessor::instance(kv.get());
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        }
        {
            std::vector<HostAddr> nodes;
            for (int32_t i = 8; i < 10; i++) {
                nodes.emplace_back(std::to_string(i), i);
            }
            cpp2::AddZoneReq req;
            req.set_zone_name("zone_4");
            req.set_nodes(std::move(nodes));
            auto* processor = AddZoneProcessor::instance(kv.get());
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        }
    }
    // List Zones
    {
        cpp2::ListZonesReq req;
        auto* processor = ListZonesProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(5, resp.get_zones().size());
        ASSERT_EQ("zone_0", resp.get_zones()[0].get_zone_name());
        ASSERT_EQ("zone_1", resp.get_zones()[1].get_zone_name());
        ASSERT_EQ("zone_2", resp.get_zones()[2].get_zone_name());
        ASSERT_EQ("zone_3", resp.get_zones()[3].get_zone_name());
        ASSERT_EQ("zone_4", resp.get_zones()[4].get_zone_name());
    }

    // Add Group
    {
        cpp2::AddGroupReq req;
        req.set_group_name("group_0");
        std::vector<std::string> zones = {"zone_0", "zone_1", "zone_2"};
        req.set_zone_names(std::move(zones));
        auto* processor = AddGroupProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::AddGroupReq req;
        req.set_group_name("group_1");
        std::vector<std::string> zones = {"zone_0", "zone_1", "zone_2", "zone_3", "zone_4"};
        req.set_zone_names(std::move(zones));
        auto* processor = AddGroupProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // List Groups
    {
        cpp2::ListGroupsReq req;
        auto* processor = ListGroupsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(2, resp.get_groups().size());
        ASSERT_EQ("group_0", resp.get_groups()[0].get_group_name());
        ASSERT_EQ("group_1", resp.get_groups()[1].get_group_name());
    }

    // Create Space without Group
    {
        cpp2::SpaceDesc properties;
        properties.set_space_name("default_space");
        properties.set_partition_num(9);
        properties.set_replica_factor(3);
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));
        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Create Space on group_0, replica factor is equal with zone size
    {
        cpp2::SpaceDesc properties;
        properties.set_space_name("space_on_group_0_3");
        properties.set_partition_num(9);
        properties.set_replica_factor(3);
        properties.set_group_name("group_0");
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));
        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Drop Group should failed
    {
        cpp2::DropGroupReq req;
        req.set_group_name("group_0");
        auto* processor = DropGroupProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_DROP, resp.get_code());
    }
    // Create Space on group_0, replica factor is less than zone size
    {
        cpp2::SpaceDesc properties;
        properties.set_space_name("space_on_group_0_1");
        properties.set_partition_num(9);
        properties.set_replica_factor(1);
        properties.set_group_name("group_0");
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));
        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Create Space on group_0, replica factor is larger than zone size
    {
        cpp2::SpaceDesc properties;
        properties.set_space_name("space_on_group_0_4");
        properties.set_partition_num(9);
        properties.set_replica_factor(4);
        properties.set_group_name("group_0");
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));
        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_INVALID_PARM, resp.get_code());
    }
    {
        cpp2::AddZoneIntoGroupReq req;
        req.set_group_name("group_0");
        req.set_zone_name("zone_3");
        auto* processor = AddZoneIntoGroupProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::SpaceDesc properties;
        properties.set_space_name("space_on_group_0_4");
        properties.set_partition_num(9);
        properties.set_replica_factor(4);
        properties.set_group_name("group_0");
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));
        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Create Space on a group which is not exist
    {
        cpp2::SpaceDesc properties;
        properties.set_space_name("space_on_group_not_exist");
        properties.set_partition_num(9);
        properties.set_replica_factor(4);
        properties.set_group_name("group_not_exist");
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));
        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.get_code());
    }
}

TEST(ProcessorTest, CreateTagTest) {
    fs::TempDir rootPath("/tmp/CreateTagTest.XXXXXX");
    auto kv = MockCluster::initMetaKV(rootPath.path());
    TestUtils::createSomeHosts(kv.get());

    {
        cpp2::SpaceDesc properties;
        properties.set_space_name("first_space");
        properties.set_partition_num(9);
        properties.set_replica_factor(1);
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));

        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(1, resp.get_id().get_space_id());
    }
    {
        cpp2::SpaceDesc properties;
        properties.set_space_name("second_space");
        properties.set_partition_num(9);
        properties.set_replica_factor(1);
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));

        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(2, resp.get_id().get_space_id());
    }
    cpp2::Schema schema;
    std::vector<cpp2::ColumnDef> cols;
    cols.emplace_back(TestUtils::columnDef(0, PropertyType::INT64));
    cols.emplace_back(TestUtils::columnDef(1, PropertyType::FLOAT));
    cols.emplace_back(TestUtils::columnDef(2, PropertyType::STRING));
    schema.set_columns(std::move(cols));
    {
        // Space not exist
        cpp2::CreateTagReq req;
        req.set_space_id(0);
        req.set_tag_name("default_tag");
        req.set_schema(schema);
        auto* processor = CreateTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.get_code());
    }
    {
        // Succeeded
        cpp2::CreateTagReq req;
        req.set_space_id(1);
        req.set_tag_name("default_tag");
        req.set_schema(schema);
        auto* processor = CreateTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(3, resp.get_id().get_tag_id());
    }
    {
        // Tag have existed
        cpp2::CreateTagReq req;
        req.set_space_id(1);
        req.set_tag_name("default_tag");
        req.set_schema(schema);
        auto* processor = CreateTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_EXISTED, resp.get_code());
    }
    {
        // Create same name tag in diff spaces
        cpp2::CreateTagReq req;
        req.set_space_id(2);
        req.set_tag_name("default_tag");
        req.set_schema(schema);
        auto* processor = CreateTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(4, resp.get_id().get_tag_id());
    }
    {
        // Create same name edge in same spaces
        cpp2::CreateEdgeReq req;
        req.set_space_id(1);
        req.set_edge_name("default_tag");
        req.set_schema(schema);
        auto* processor = CreateEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_CONFLICT, resp.get_code());
    }
    {
        // Set schema ttl property
        cpp2::SchemaProp schemaProp;
        schemaProp.set_ttl_duration(100);
        schemaProp.set_ttl_col("col_0");
        schema.set_schema_prop(std::move(schemaProp));

        // Tag with TTL
        cpp2::CreateTagReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_ttl");
        req.set_schema(schema);
        auto* processor = CreateTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(5, resp.get_id().get_tag_id());
    }
    // Wrong default value type
    {
        cpp2::Schema schemaWithDefault;
        std::vector<cpp2::ColumnDef> colsWithDefault;

        cpp2::ColumnDef columnWithDefault;
        columnWithDefault.set_name(folly::stringPrintf("col_type_mismatch"));
        columnWithDefault.type.set_type(PropertyType::BOOL);
        ConstantExpression strValue("default value");;
        columnWithDefault.set_default_value(Expression::encode(strValue));

        colsWithDefault.push_back(std::move(columnWithDefault));
        schemaWithDefault.set_columns(std::move(colsWithDefault));

        cpp2::CreateTagReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_type_mismatche");
        req.set_schema(std::move(schemaWithDefault));
        auto* processor = CreateTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_INVALID_PARM, resp.get_code());
    }
    // Wrong default value
    {
        cpp2::Schema schemaWithDefault;
        std::vector<cpp2::ColumnDef> colsWithDefault;

        cpp2::ColumnDef columnWithDefault;
        columnWithDefault.set_name(folly::stringPrintf("col_value_mismatch"));
        columnWithDefault.type.set_type(PropertyType::INT8);
        ConstantExpression intValue(256);;
        columnWithDefault.set_default_value(Expression::encode(intValue));

        colsWithDefault.push_back(std::move(columnWithDefault));
        schemaWithDefault.set_columns(std::move(colsWithDefault));

        cpp2::CreateTagReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_value_mismatche");
        req.set_schema(std::move(schemaWithDefault));
        auto* processor = CreateTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_INVALID_PARM, resp.get_code());
    }
    {
        cpp2::CreateTagReq req;
        req.set_space_id(1);
        req.set_tag_name("all_tag_type");
        auto allSchema = TestUtils::mockSchemaWithAllType();
        req.set_schema(std::move(allSchema));
        auto* processor = CreateTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());

        cpp2::GetTagReq getReq;
        getReq.set_space_id(1);
        getReq.set_tag_name("all_tag_type");
        getReq.set_version(0);
        auto* getProcessor = GetTagProcessor::instance(kv.get());
        auto getFut = getProcessor->getFuture();
        getProcessor->process(getReq);
        auto getResp = std::move(getFut).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, getResp.get_code());
        TestUtils::checkSchemaWithAllType(getResp.get_schema());
    }
}


TEST(ProcessorTest, CreateEdgeTest) {
    fs::TempDir rootPath("/tmp/CreateEdgeTest.XXXXXX");
    auto kv = MockCluster::initMetaKV(rootPath.path());
    TestUtils::createSomeHosts(kv.get());

    {
        cpp2::SpaceDesc properties;
        properties.set_space_name("default_space");
        properties.set_partition_num(9);
        properties.set_replica_factor(3);
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));

        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(1, resp.get_id().get_space_id());
   }
   {
        // Create another space
        cpp2::SpaceDesc properties;
        properties.set_space_name("another_space");
        properties.set_partition_num(9);
        properties.set_replica_factor(3);
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));

        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(2, resp.get_id().get_space_id());
    }

    cpp2::Schema schema;
    std::vector<cpp2::ColumnDef> cols;
    cols.emplace_back(TestUtils::columnDef(0, PropertyType::INT64));
    cols.emplace_back(TestUtils::columnDef(1, PropertyType::FLOAT));
    cols.emplace_back(TestUtils::columnDef(2, PropertyType::STRING));
    schema.set_columns(std::move(cols));
    {
        cpp2::CreateEdgeReq req;
        req.set_space_id(0);
        req.set_edge_name("default_edge");
        req.set_schema(schema);
        auto* processor = CreateEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.get_code());
    }
    {
        // Succeeded
        cpp2::CreateEdgeReq req;
        req.set_space_id(1);
        req.set_edge_name("default_edge");
        req.set_schema(schema);
        auto* processor = CreateEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(3, resp.get_id().get_edge_type());
    }
    {
        // Existed
        cpp2::CreateEdgeReq req;
        req.set_space_id(1);
        req.set_edge_name("default_edge");
        req.set_schema(schema);
        auto* processor = CreateEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_EXISTED, resp.get_code());
    }
    {
        // Create same name edge in diff spaces
        cpp2::CreateEdgeReq req;
        req.set_space_id(2);
        req.set_edge_name("default_edge");
        req.set_schema(schema);
        auto* processor = CreateEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(4, resp.get_id().get_edge_type());
    }
    {
        // Create same name tag in same spaces
        cpp2::CreateTagReq req;
        req.set_space_id(1);
        req.set_tag_name("default_edge");
        req.set_schema(schema);
        auto* processor = CreateTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_CONFLICT, resp.get_code());
    }

    // Set schema ttl property
    cpp2::SchemaProp schemaProp;
    schemaProp.set_ttl_duration(100);
    schemaProp.set_ttl_col("col_0");
    schema.set_schema_prop(std::move(schemaProp));
    {
        // Edge with TTL
        cpp2::CreateEdgeReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_ttl");
        req.set_schema(schema);
        auto* processor = CreateEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(5, resp.get_id().get_edge_type());
    }
    {
        cpp2::Schema schemaWithDefault;
        std::vector<cpp2::ColumnDef> colsWithDefault;
        colsWithDefault.emplace_back(TestUtils::columnDef(0, PropertyType::BOOL, Value(false)));
        colsWithDefault.emplace_back(TestUtils::columnDef(1, PropertyType::INT64, Value(11)));
        colsWithDefault.emplace_back(TestUtils::columnDef(2, PropertyType::DOUBLE, Value(11.0)));
        colsWithDefault.emplace_back(TestUtils::columnDef(3, PropertyType::STRING, Value("test")));
        schemaWithDefault.set_columns(std::move(colsWithDefault));

        cpp2::CreateEdgeReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_with_defaule");
        req.set_schema(std::move(schemaWithDefault));
        auto* processor = CreateEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(6, resp.get_id().get_edge_type());
    }
    {
        cpp2::Schema schemaWithDefault;
        std::vector<cpp2::ColumnDef> colsWithDefault;
        colsWithDefault.push_back(TestUtils::columnDef(0,
                    PropertyType::BOOL, Value("default value")));
        schemaWithDefault.set_columns(std::move(colsWithDefault));

        cpp2::CreateEdgeReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_type_mismatche");
        req.set_schema(std::move(schemaWithDefault));
        auto* processor = CreateEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_INVALID_PARM, resp.get_code());
    }
    {
        cpp2::CreateTagReq req;
        req.set_space_id(1);
        req.set_tag_name("all_edge_type");
        auto allSchema = TestUtils::mockSchemaWithAllType();
        req.set_schema(std::move(allSchema));
        auto* processor = CreateTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());

        cpp2::GetTagReq getReq;
        getReq.set_space_id(1);
        getReq.set_tag_name("all_edge_type");
        getReq.set_version(0);
        auto* getProcessor = GetTagProcessor::instance(kv.get());
        auto getFut = getProcessor->getFuture();
        getProcessor->process(getReq);
        auto getResp = std::move(getFut).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, getResp.get_code());
        TestUtils::checkSchemaWithAllType(getResp.get_schema());
    }
}

TEST(ProcessorTest, KVOperationTest) {
    fs::TempDir rootPath("/tmp/KVOperationTest.XXXXXX");
    auto kv = MockCluster::initMetaKV(rootPath.path());
    TestUtils::createSomeHosts(kv.get());

    {
        cpp2::SpaceDesc properties;
        properties.set_space_name("default_space");
        properties.set_partition_num(9);
        properties.set_replica_factor(3);
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));

        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(1, resp.get_id().get_space_id());
    }
    {
        // Multi Put Test
        std::vector<nebula::KeyValue> pairs;
        for (auto i = 0; i < 10; i++) {
            pairs.emplace_back(std::make_pair(folly::stringPrintf("key_%d", i),
                                              folly::stringPrintf("value_%d", i)));
        }

        cpp2::MultiPutReq req;
        req.set_segment("test");
        req.set_pairs(std::move(pairs));

        auto* processor = MultiPutProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        // Get Test
        cpp2::GetReq req;
        req.set_segment("test");
        req.set_key("key_0");

        auto* processor = GetProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ("value_0", resp.get_value());

        cpp2::GetReq missedReq;
        missedReq.set_segment("test");
        missedReq.set_key("missed_key");

        auto* missedProcessor = GetProcessor::instance(kv.get());
        auto missedFuture = missedProcessor->getFuture();
        missedProcessor->process(missedReq);
        auto missedResp = std::move(missedFuture).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, missedResp.get_code());
    }
    {
        // Multi Get Test
        std::vector<std::string> keys;
        for (auto i = 0; i < 2; i++) {
            keys.emplace_back(folly::stringPrintf("key_%d", i));
        }

        cpp2::MultiGetReq req;
        req.set_segment("test");
        req.set_keys(std::move(keys));

        auto* processor = MultiGetProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(2, resp.get_values().size());
        ASSERT_EQ("value_0", resp.get_values()[0]);
        ASSERT_EQ("value_1", resp.get_values()[1]);
    }
    {
        // Scan Test
        cpp2::ScanReq req;
        req.set_segment("test");
        req.set_start("key_1");
        req.set_end("key_4");

        auto* processor = ScanProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(3, resp.get_values().size());
        ASSERT_EQ("value_1", resp.get_values()[0]);
        ASSERT_EQ("value_2", resp.get_values()[1]);
        ASSERT_EQ("value_3", resp.get_values()[2]);
    }
    {
        // Remove Test
        cpp2::RemoveReq req;
        req.set_segment("test");
        req.set_key("key_9");

        auto* processor = RemoveProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        // Remove Range Test
        cpp2::RemoveRangeReq req;
        req.set_segment("test");
        req.set_start("key_0");
        req.set_end("key_4");

        auto* processor = RemoveRangeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
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
        req.set_space_id(1);
        auto* processor = ListTagsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        std::vector<nebula::meta::cpp2::TagItem> tags;
        tags = resp.get_tags();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
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
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_version(0);

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
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_version(-1);

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
}


TEST(ProcessorTest, ListOrGetEdgesTest) {
    fs::TempDir rootPath("/tmp/ListOrGetEdgesTest.XXXXXX");
    auto kv = MockCluster::initMetaKV(rootPath.path());
    TestUtils::assembleSpace(kv.get(), 1, 1);
    TestUtils::mockEdge(kv.get(), 10);

    // Test ListEdgesProcessor
    {
        cpp2::ListEdgesReq req;
        req.set_space_id(1);
        auto* processor = ListEdgesProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        std::vector<nebula::meta::cpp2::EdgeItem> edges;
        edges = resp.get_edges();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
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
            req.set_space_id(1);
            req.set_edge_name(folly::stringPrintf("edge_%d", t));
            req.set_version(t);

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
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_version(-1);

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
}

TEST(ProcessorTest, DropTagTest) {
    fs::TempDir rootPath("/tmp/DropTagTest.XXXXXX");
    auto kv = MockCluster::initMetaKV(rootPath.path());
    TestUtils::assembleSpace(kv.get(), 1, 1);
    TestUtils::mockTag(kv.get(), 1);

    // Space not exist
    {
        cpp2::DropTagReq req;
        req.set_space_id(0);
        req.set_tag_name("tag_0");
        auto* processor = DropTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.get_code());
    }
    // Tag not exist
    {
        cpp2::DropTagReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_no");
        auto* processor = DropTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.get_code());
    }
    // Succeeded
    {
        cpp2::DropTagReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        auto* processor = DropTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }

    // Check tag data has been deleted.
    {
        std::string tagVal;
        kvstore::ResultCode ret;
        std::unique_ptr<kvstore::KVIterator> iter;
        ret = kv.get()->get(kDefaultSpaceId, kDefaultPartId,
                            std::move(MetaServiceUtils::indexTagKey(1, "tag_0")),
                            &tagVal);
        ASSERT_EQ(kvstore::ResultCode::ERR_KEY_NOT_FOUND, ret);
        std::string tagPrefix = "__tags__";
        ret = kv.get()->prefix(kDefaultSpaceId, kDefaultPartId, tagPrefix, &iter);
        ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
        ASSERT_FALSE(iter->valid());
    }
    // With IF EXISTS
    {
        cpp2::DropTagReq req;
        req.set_space_id(1);
        req.set_tag_name("not_exist_tag");
        req.set_if_exists(true);
        auto* processor = DropTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        constexpr GraphSpaceID spaceId = 1;
        constexpr char tagName[] = "exist_tag";
        cpp2::CreateTagReq req;
        req.set_space_id(spaceId);
        req.set_tag_name(tagName);
        auto* processor = CreateTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());

        cpp2::DropTagReq req1;
        req1.set_space_id(spaceId);
        req1.set_tag_name(tagName);
        req1.set_if_exists(true);
        auto* processor1 = DropTagProcessor::instance(kv.get());
        auto f1 = processor1->getFuture();
        processor1->process(req1);
        auto resp1 = std::move(f1).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp1.get_code());
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
        req.set_space_id(0);
        req.set_edge_name("edge_0");
        auto* processor = DropEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.get_code());
    }
    // Edge not exist
    {
        cpp2::DropEdgeReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_no");
        auto* processor = DropEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.get_code());
    }
    // Succeeded
    {
        cpp2::DropEdgeReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        auto* processor = DropEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Check edge data has been deleted.
    {
        std::string edgeVal;
        kvstore::ResultCode ret;
        std::unique_ptr<kvstore::KVIterator> iter;
        ret = kv.get()->get(kDefaultSpaceId, kDefaultPartId,
                            std::move(MetaServiceUtils::indexEdgeKey(1, "edge_0")),
                            &edgeVal);
        ASSERT_EQ(kvstore::ResultCode::ERR_KEY_NOT_FOUND, ret);
        std::string edgePrefix = "__edges__";
        ret = kv.get()->prefix(kDefaultSpaceId, kDefaultPartId, edgePrefix, &iter);
        ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
        ASSERT_FALSE(iter->valid());
    }
    // With IF EXISTS
    {
        cpp2::DropEdgeReq req;
        req.set_space_id(1);
        req.set_edge_name("not_exist_edge");
        req.set_if_exists(true);
        auto* processor = DropEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        constexpr GraphSpaceID spaceId = 1;
        constexpr char edgeName[] = "exist_edge";
        cpp2::CreateTagReq req;
        req.set_space_id(spaceId);
        req.set_tag_name(edgeName);
        auto* processor = CreateTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());

        cpp2::DropEdgeReq req1;
        req1.set_space_id(spaceId);
        req1.set_edge_name(edgeName);
        req1.set_if_exists(true);
        auto* processor1 = DropEdgeProcessor::instance(kv.get());
        auto f1 = processor1->getFuture();
        processor1->process(req1);
        auto resp1 = std::move(f1).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp1.get_code());
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
            column.type.set_type(i < 1 ? PropertyType::INT64 : PropertyType::STRING);
            (*addSch.columns_ref()).emplace_back(std::move(column));
        }
        cpp2::Schema changeSch;
        for (auto i = 0; i < 2; i++) {
            cpp2::ColumnDef column;
            column.name = folly::stringPrintf("tag_0_col_%d", i);
            column.type.set_type(i < 1 ? PropertyType::BOOL : PropertyType::DOUBLE);
            (*changeSch.columns_ref()).emplace_back(std::move(column));
        }
        cpp2::Schema dropSch;
        cpp2::ColumnDef column;
        column.name = "tag_0_col_0";
        (*dropSch.columns_ref()).emplace_back(std::move(column));
        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::ADD);
        items.back().set_schema(std::move(addSch));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::CHANGE);
        items.back().set_schema(std::move(changeSch));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::DROP);
        items.back().set_schema(std::move(dropSch));
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_tag_items(items);
        auto* processor = AlterTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Verify alter result.
    {
        cpp2::ListTagsReq req;
        req.set_space_id(1);
        auto* processor = ListTagsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        auto tags = resp.get_tags();
        ASSERT_EQ(2, tags.size());
        // TagItems in vector are unordered.So need to get the latest one by comparing the versions.
        auto tag = tags[0].get_version() > 0 ? tags[0] : tags[1];
        EXPECT_EQ(0, tag.get_tag_id());
        EXPECT_EQ(folly::stringPrintf("tag_%d", 0), tag.get_tag_name());
        EXPECT_EQ(1, tag.get_version());

        cpp2::Schema schema;
        std::vector<cpp2::ColumnDef> cols;

        cpp2::ColumnDef column;
        column.name = "tag_0_col_1";
        column.type.set_type(PropertyType::DOUBLE);
        cols.emplace_back(std::move(column));

        column.name = "tag_0_col_10";
        column.type.set_type(PropertyType::INT64);
        cols.emplace_back(std::move(column));

        column.name = "tag_0_col_11";
        column.type.set_type(PropertyType::STRING);
        cols.emplace_back(std::move(column));

        schema.set_columns(std::move(cols));
        EXPECT_EQ(schema, tag.get_schema());
    }
    // Alter tag with ttl
    {
        // Only set ttl_duration
        cpp2::AlterTagReq req;
        cpp2::SchemaProp schemaProp;
        schemaProp.set_ttl_duration(100);

        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_schema_prop(std::move(schemaProp));
        auto* processor = AlterTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Succeeded
    {
        cpp2::AlterTagReq req;
        cpp2::SchemaProp schemaProp;
        schemaProp.set_ttl_duration(100);
        schemaProp.set_ttl_col("tag_0_col_10");

        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_schema_prop(std::move(schemaProp));
        auto* processor = AlterTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Verify alter result.
    {
        cpp2::ListTagsReq req;
        req.set_space_id(1);
        auto* processor = ListTagsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        auto tags = resp.get_tags();
        ASSERT_EQ(3, tags.size());
        // TagItems in vector are unordered.So need to get the latest one by comparing the versions.
        auto tag = tags[0].get_version() > tags[1].get_version() ? tags[0] : tags[1];
        tag =  tag.get_version() > tags[2].get_version() ? tag : tags[2];

        EXPECT_EQ(0, tag.get_tag_id());
        EXPECT_EQ(folly::stringPrintf("tag_%d", 0), tag.get_tag_name());
        EXPECT_EQ(2, tag.get_version());

        cpp2::Schema schema;
        std::vector<cpp2::ColumnDef> cols;

        cpp2::ColumnDef column;
        column.name = "tag_0_col_1";
        column.type.set_type(PropertyType::DOUBLE);
        cols.emplace_back(std::move(column));

        column.name = "tag_0_col_10";
        column.type.set_type(PropertyType::INT64);
        cols.emplace_back(std::move(column));

        column.name = "tag_0_col_11";
        column.type.set_type(PropertyType::STRING);
        cols.emplace_back(std::move(column));

        schema.set_columns(std::move(cols));

        cpp2::SchemaProp schemaProp;
        schemaProp.set_ttl_duration(100);
        schemaProp.set_ttl_col("tag_0_col_10");
        schema.set_schema_prop(std::move(schemaProp));
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
        column.type.set_type(PropertyType::DOUBLE);
        (*changeSch.columns_ref()).emplace_back(std::move(column));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::CHANGE);
        items.back().set_schema(std::move(changeSch));

        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_tag_items(items);
        auto* processor = AlterTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Set ttl col on illegal column type, failed
    {
        cpp2::AlterTagReq req;
        cpp2::SchemaProp schemaProp;
        schemaProp.set_ttl_duration(100);
        schemaProp.set_ttl_col("tag_0_col_11");

        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_schema_prop(std::move(schemaProp));
        auto* processor = AlterTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
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
        items.back().set_op(cpp2::AlterSchemaOp::DROP);
        items.back().set_schema(std::move(dropSch));
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_tag_items(items);
        auto* processor = AlterTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Verify alter drop ttl col result.
    {
        cpp2::ListTagsReq req;
        req.set_space_id(1);
        auto* processor = ListTagsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        auto tags = resp.get_tags();
        ASSERT_EQ(4, tags.size());
        // TagItems in vector are unordered.So need to get the latest one by comparing the versions.
        int version = 0;
        int max_index = 0;
        for (uint32_t i = 0; i < tags.size(); i++) {
            if (tags[i].get_version() > version) {
                max_index = i;
                version  = tags[i].get_version();
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
        column.type.set_type(PropertyType::DOUBLE);
        cols.emplace_back(std::move(column));

        column.name = "tag_0_col_11";
        column.type.set_type(PropertyType::STRING);
        cols.emplace_back(std::move(column));

        schema.set_columns(std::move(cols));

        cpp2::SchemaProp schemaProp;
        schemaProp.set_ttl_duration(0);
        schemaProp.set_ttl_col("");
        schema.set_schema_prop(std::move(schemaProp));
        EXPECT_EQ(schema.get_columns(), tag.get_schema().get_columns());
        EXPECT_EQ(*schema.get_schema_prop().get_ttl_duration(),
                  *tag.get_schema().get_schema_prop().get_ttl_duration());
        EXPECT_EQ(*schema.get_schema_prop().get_ttl_col(),
                  *tag.get_schema().get_schema_prop().get_ttl_col());
    }
    // verify alter ttl prop with index exists
    {
        {
            cpp2::Schema schema;
            cpp2::SchemaProp schemaProp;
            std::vector<cpp2::ColumnDef> cols;
            cols.emplace_back(TestUtils::columnDef(0, PropertyType::INT64));
            schemaProp.set_ttl_duration(100);
            schemaProp.set_ttl_col("col_0");
            schema.set_schema_prop(std::move(schemaProp));
            schema.set_columns(std::move(cols));

            cpp2::CreateTagReq req;
            req.set_space_id(1);
            req.set_tag_name("ttl");
            req.set_schema(schema);
            auto* processor = CreateTagProcessor::instance(kv.get());
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        }
        {
            cpp2::CreateTagIndexReq req;
            req.set_space_id(1);
            req.set_tag_name("ttl");
            cpp2::IndexFieldDef field;
            field.set_name("col_0");
            req.set_fields({field});
            req.set_index_name("ttl_index");
            auto* processor = CreateTagIndexProcessor::instance(kv.get());
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        }
        {
            cpp2::AlterTagReq req;
            cpp2::SchemaProp schemaProp;
            schemaProp.set_ttl_duration(1000);
            schemaProp.set_ttl_col("col_0");
            req.set_space_id(1);
            req.set_tag_name("ttl");
            req.set_schema_prop(std::move(schemaProp));
            auto* processor = AlterTagProcessor::instance(kv.get());
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ASSERT_EQ(cpp2::ErrorCode::E_UNSUPPORTED, resp.get_code());
        }
    }
    // Verify ErrorCode of add
    {
        cpp2::AlterTagReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        cpp2::Schema addSch;
        cpp2::ColumnDef column;
        column.name = "tag_0_col_1";
        column.type.set_type(PropertyType::INT64);
        (*addSch.columns_ref()).emplace_back(std::move(column));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::ADD);
        items.back().set_schema(std::move(addSch));
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_tag_items(items);
        auto* processor = AlterTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_EXISTED, resp.get_code());
    }
    // Verify ErrorCode of change
    {
        cpp2::AlterTagReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        cpp2::Schema changeSch;
        cpp2::ColumnDef column;
        column.name = "tag_0_col_2";
        column.type.set_type(PropertyType::INT64);
        (*changeSch.columns_ref()).emplace_back(std::move(column));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::CHANGE);
        items.back().set_schema(std::move(changeSch));
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_tag_items(items);
        auto* processor = AlterTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.get_code());
    }
    // Verify ErrorCode of drop
    {
        cpp2::AlterTagReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        cpp2::Schema dropSch;
        cpp2::ColumnDef column;
        column.name = "tag_0_col_0";
        column.type.set_type(PropertyType::INT64);
        (*dropSch.columns_ref()).emplace_back(std::move(column));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::DROP);
        items.back().set_schema(std::move(dropSch));
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_tag_items(items);
        auto* processor = AlterTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.get_code());
    }
    // Add col with wrong default value type
    {
        cpp2::AlterTagReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        cpp2::Schema schema;
        cpp2::ColumnDef column;
        column.name = "add_col_mismatch_type";
        column.type.set_type(PropertyType::INT64);
        ConstantExpression strValue("default value");
        column.set_default_value(Expression::encode(strValue));
        (*schema.columns_ref()).emplace_back(std::move(column));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::ADD);
        items.back().set_schema(std::move(schema));
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_tag_items(items);
        auto* processor = AlterTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_INVALID_PARM, resp.get_code());
    }
    // Add col with out of range of fixed string
    {
        cpp2::AlterTagReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        cpp2::Schema schema;
        cpp2::ColumnDef column;
        column.name = "add_col_fixed_string_type";
        column.type.set_type(PropertyType::FIXED_STRING);
        column.type.set_type_length(5);;
        ConstantExpression strValue("Hello world!");
        column.set_default_value(Expression::encode(strValue));
        (*schema.columns_ref()).emplace_back(std::move(column));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::ADD);
        items.back().set_schema(std::move(schema));
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_tag_items(items);
        auto* processor = AlterTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());

        // check result
        cpp2::GetTagReq getReq;
        getReq.set_space_id(1);
        getReq.set_tag_name("tag_0");
        getReq.set_version(-1);

        auto* getProcessor = GetTagProcessor::instance(kv.get());
        auto getFut = getProcessor->getFuture();
        getProcessor->process(getReq);
        auto resp1 = std::move(getFut).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp1.get_code());
        std::vector<cpp2::ColumnDef> cols = resp1.get_schema().get_columns();
        bool expected = false;
        for (const auto &col : cols) {
            if (col.get_name() == "add_col_fixed_string_type") {
                ASSERT_EQ(PropertyType::FIXED_STRING, col.get_type().get_type());
                auto defaultValueExpr = Expression::decode(*col.get_default_value());
                DefaultValueContext mContext;
                auto value = Expression::eval(defaultValueExpr.get(), mContext);
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
        items.back().set_op(cpp2::AlterSchemaOp::DROP);
        items.back().set_schema(std::move(dropSch));
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_edge_items(items);
        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::ListEdgesReq req;
        req.set_space_id(1);
        auto* processor = ListEdgesProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
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
            column.type.set_type(i < 1 ? PropertyType::INT64 : PropertyType::STRING);
            (*addSch.columns_ref()).emplace_back(std::move(column));
        }

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::ADD);
        items.back().set_schema(std::move(addSch));
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_edge_items(items);
        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::AlterEdgeReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        cpp2::Schema addSch;
        for (auto i = 0; i < 2; i++) {
            cpp2::ColumnDef column;
            column.name = folly::stringPrintf("edge_%d_col_%d", 0, i + 10);
            column.type.set_type(i < 1 ? PropertyType::INT64 : PropertyType::STRING);
            (*addSch.columns_ref()).emplace_back(std::move(column));
        }
        cpp2::Schema changeSch;
        for (auto i = 0; i < 2; i++) {
            cpp2::ColumnDef column;
            column.name = folly::stringPrintf("edge_%d_col_%d", 0, i);
            column.type.set_type(i < 1 ? PropertyType::BOOL : PropertyType::DOUBLE);
            (*changeSch.columns_ref()).emplace_back(std::move(column));
        }
        cpp2::Schema dropSch;
        cpp2::ColumnDef column;
        column.name = "edge_0_col_0";
        (*dropSch.columns_ref()).emplace_back(std::move(column));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::ADD);
        items.back().set_schema(std::move(addSch));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::CHANGE);
        items.back().set_schema(std::move(changeSch));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::DROP);
        items.back().set_schema(std::move(dropSch));

        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_edge_items(items);
        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Verify alter result.
    {
        cpp2::ListEdgesReq req;
        req.set_space_id(1);
        auto* processor = ListEdgesProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        auto edges = resp.get_edges();
        ASSERT_EQ(4, edges.size());
        // Get the latest one by comparing the versions.
        int version = 0;
        int max_index = 0;
        for (uint32_t i = 0; i < edges.size(); i++) {
            if (edges[i].get_version() > version) {
                max_index = i;
                version  = edges[i].get_version();
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
        column.type.set_type(PropertyType::DOUBLE);
        cols.emplace_back(std::move(column));

        column.name = "edge_0_col_10";
        column.type.set_type(PropertyType::INT64);
        cols.emplace_back(std::move(column));

        column.name = "edge_0_col_11";
        column.type.set_type(PropertyType::STRING);
        cols.emplace_back(std::move(column));
        schema.set_columns(std::move(cols));
        EXPECT_EQ(schema, edge.get_schema());
    }

    // Only set ttl_duration, failed
    {
        cpp2::AlterEdgeReq req;
        cpp2::SchemaProp schemaProp;
        schemaProp.set_ttl_duration(100);

        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_schema_prop(std::move(schemaProp));
        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Succeed
    {
        cpp2::AlterEdgeReq req;
        cpp2::SchemaProp schemaProp;
        schemaProp.set_ttl_duration(100);
        schemaProp.set_ttl_col("edge_0_col_10");

        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_schema_prop(std::move(schemaProp));
        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Verify alter result.
    {
        cpp2::ListEdgesReq req;
        req.set_space_id(1);
        auto* processor = ListEdgesProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        auto edges = resp.get_edges();
        ASSERT_EQ(5, edges.size());
        // EdgeItems in vector are unordered.So get the latest one by comparing the versions.
        int version = 0;
        int max_index = 0;
        for (uint32_t i = 0; i < edges.size(); i++) {
            if (edges[i].get_version() > version) {
                max_index = i;
                version  = edges[i].get_version();
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
        column.type.set_type(PropertyType::DOUBLE);
        cols.emplace_back(std::move(column));

        column.name = "edge_0_col_10";
        column.type.set_type(PropertyType::INT64);
        cols.emplace_back(std::move(column));

        column.name = "edge_0_col_11";
        column.type.set_type(PropertyType::STRING);
        cols.emplace_back(std::move(column));

        schema.set_columns(std::move(cols));

        cpp2::SchemaProp schemaProp;
        schemaProp.set_ttl_duration(100);
        schemaProp.set_ttl_col("edge_0_col_10");
        schema.set_schema_prop(std::move(schemaProp));
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
        column.type.set_type(PropertyType::DOUBLE);
        (*changeSch.columns_ref()).emplace_back(std::move(column));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::CHANGE);
        items.back().set_schema(std::move(changeSch));

        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_edge_items(items);
        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Set ttl col on illegal column type, failed
    {
        cpp2::AlterEdgeReq req;
        cpp2::SchemaProp schemaProp;
        schemaProp.set_ttl_duration(100);
        schemaProp.set_ttl_col("edge_0_col_11");

        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_schema_prop(std::move(schemaProp));
        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
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
        items.back().set_op(cpp2::AlterSchemaOp::DROP);
        items.back().set_schema(std::move(dropSch));
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_edge_items(items);
        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Verify alter drop ttl col result
    {
        cpp2::ListEdgesReq req;
        req.set_space_id(1);
        auto* processor = ListEdgesProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        auto edges = resp.get_edges();
        ASSERT_EQ(6, edges.size());
        // EdgeItems in vector are unordered.So get the latest one by comparing the versions.
        int version = 0;
        int max_index = 0;
        for (uint32_t i = 0; i < edges.size(); i++) {
            if (edges[i].get_version() > version) {
                max_index = i;
                version  = edges[i].get_version();
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
        column.type.set_type(PropertyType::DOUBLE);
        cols.emplace_back(std::move(column));

        column.name = "edge_0_col_11";
        column.type.set_type(PropertyType::STRING);
        cols.emplace_back(std::move(column));

        schema.set_columns(std::move(cols));

        cpp2::SchemaProp schemaProp;
        schemaProp.set_ttl_duration(0);
        schemaProp.set_ttl_col("");
        schema.set_schema_prop(std::move(schemaProp));
        EXPECT_EQ(schema.get_columns(), edge.get_schema().get_columns());
        EXPECT_EQ(*schema.get_schema_prop().get_ttl_duration(),
                  *edge.get_schema().get_schema_prop().get_ttl_duration());
        EXPECT_EQ(*schema.get_schema_prop().get_ttl_col(),
                  *edge.get_schema().get_schema_prop().get_ttl_col());
    }
    // verify alter ttl prop with index exists
    {
        {
            cpp2::Schema schema;
            cpp2::SchemaProp schemaProp;
            std::vector<cpp2::ColumnDef> cols;
            cols.emplace_back(TestUtils::columnDef(0, PropertyType::INT64));
            schemaProp.set_ttl_duration(100);
            schemaProp.set_ttl_col("col_0");
            schema.set_schema_prop(std::move(schemaProp));
            schema.set_columns(std::move(cols));

            cpp2::CreateEdgeReq req;
            req.set_space_id(1);
            req.set_edge_name("ttl");
            req.set_schema(schema);
            auto* processor = CreateEdgeProcessor::instance(kv.get());
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        }
        {
            cpp2::CreateEdgeIndexReq req;
            req.set_space_id(1);
            req.set_edge_name("ttl");
            cpp2::IndexFieldDef field;
            field.set_name("col_0");
            req.set_fields({field});
            req.set_index_name("ttl_index");
            auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        }
        {
            cpp2::AlterEdgeReq req;
            cpp2::SchemaProp schemaProp;
            schemaProp.set_ttl_duration(1000);
            schemaProp.set_ttl_col("col_0");
            req.set_space_id(1);
            req.set_edge_name("ttl");
            req.set_schema_prop(std::move(schemaProp));
            auto* processor = AlterEdgeProcessor::instance(kv.get());
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ASSERT_EQ(cpp2::ErrorCode::E_UNSUPPORTED, resp.get_code());
        }
    }
    // Verify ErrorCode of add
    {
        cpp2::AlterEdgeReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        cpp2::Schema addSch;
        cpp2::ColumnDef column;
        column.name = "edge_0_col_1";
        column.type.set_type(PropertyType::INT64);
        (*addSch.columns_ref()).emplace_back(std::move(column));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::ADD);
        items.back().set_schema(std::move(addSch));

        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_edge_items(items);
        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_EXISTED, resp.get_code());
    }
    // Verify ErrorCode of change
    {
        cpp2::AlterEdgeReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        cpp2::Schema changeSch;
        cpp2::ColumnDef column;
        column.name = "edge_0_col_2";
        column.type.set_type(PropertyType::INT64);
        (*changeSch.columns_ref()).emplace_back(std::move(column));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::CHANGE);
        items.back().set_schema(std::move(changeSch));

        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_edge_items(items);
        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.get_code());
    }
    // Verify ErrorCode of drop
    {
        cpp2::AlterEdgeReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        cpp2::Schema dropSch;
        cpp2::ColumnDef column;
        column.name = "edge_0_col_2";
        column.type.set_type(PropertyType::INT64);
        (*dropSch.columns_ref()).emplace_back(std::move(column));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::DROP);
        items.back().set_schema(std::move(dropSch));

        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_edge_items(items);
        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.get_code());
    }
    // Add col with wrong default value type
    {
        cpp2::AlterEdgeReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        cpp2::Schema schema;
        cpp2::ColumnDef column;
        column.name = "add_col_mismatch_type";
        column.type.set_type(PropertyType::INT64);
        ConstantExpression strValue("default value");
        column.set_default_value(Expression::encode(strValue));
        (*schema.columns_ref()).emplace_back(std::move(column));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::ADD);
        items.back().set_schema(std::move(schema));
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_edge_items(items);
        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_INVALID_PARM, resp.get_code());
    }
    // Add col with out of range of fixed string
    {
        cpp2::AlterEdgeReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        cpp2::Schema schema;
        cpp2::ColumnDef column;
        column.name = "add_col_fixed_string_type";
        column.type.set_type(PropertyType::FIXED_STRING);
        column.type.set_type_length(5);;
        ConstantExpression strValue("Hello world!");
        column.set_default_value(Expression::encode(strValue));
        (*schema.columns_ref()).emplace_back(std::move(column));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::ADD);
        items.back().set_schema(std::move(schema));
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_edge_items(items);
        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());

        // check result
        cpp2::GetEdgeReq getReq;
        getReq.set_space_id(1);
        getReq.set_edge_name("edge_0");
        getReq.set_version(-1);

        auto* getProcessor = GetEdgeProcessor::instance(kv.get());
        auto getFut = getProcessor->getFuture();
        getProcessor->process(getReq);
        auto resp1 = std::move(getFut).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp1.get_code());
        std::vector<cpp2::ColumnDef> cols = resp1.get_schema().get_columns();
        bool expected = false;
        for (const auto &col : cols) {
            if (col.get_name() == "add_col_fixed_string_type") {
                ASSERT_EQ(PropertyType::FIXED_STRING, col.get_type().get_type());
                auto defaultValueExpr = Expression::decode(*col.get_default_value());
                DefaultValueContext mContext;
                auto value = Expression::eval(defaultValueExpr.get(), mContext);
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
        properties.set_space_name("default_space");
        properties.set_partition_num(9);
        properties.set_replica_factor(3);
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));
        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(1, resp.get_id().get_space_id());
    }
    {
        cpp2::SpaceDesc properties;
        properties.set_space_name("second_space");
        properties.set_partition_num(9);
        properties.set_replica_factor(1);
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));
        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(2, resp.get_id().get_space_id());
    }

    // Add same tag name in different space
    cpp2::Schema schema;
    std::vector<cpp2::ColumnDef> cols;
    cols.emplace_back(TestUtils::columnDef(0, PropertyType::INT64));
    cols.emplace_back(TestUtils::columnDef(1, PropertyType::FLOAT));
    cols.emplace_back(TestUtils::columnDef(2, PropertyType::STRING));
    schema.set_columns(std::move(cols));
    {
        cpp2::CreateTagReq req;
        req.set_space_id(1);
        req.set_tag_name("default_tag");
        req.set_schema(schema);
        auto* processor = CreateTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(3, resp.get_id().get_tag_id());
    }
    {
        cpp2::CreateTagReq req;
        req.set_space_id(2);
        req.set_tag_name("default_tag");
        req.set_schema(schema);
        auto* processor = CreateTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(4, resp.get_id().get_tag_id());
    }

    // Remove Test
    {
        cpp2::DropTagReq req;
        req.set_space_id(1);
        req.set_tag_name("default_tag");
        auto* processor = DropTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }

    // List Test
    {
        cpp2::ListTagsReq req;
        req.set_space_id(1);
        auto* processor = ListTagsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        std::vector<nebula::meta::cpp2::TagItem> tags;
        tags = resp.get_tags();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(0, tags.size());
    }
    {
        cpp2::ListTagsReq req;
        req.set_space_id(2);
        auto* processor = ListTagsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        std::vector<nebula::meta::cpp2::TagItem> tags;
        tags = resp.get_tags();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(1, tags.size());

        ASSERT_EQ(4, tags[0].get_tag_id());
        ASSERT_EQ("default_tag", tags[0].get_tag_name());
    }
}

TEST(ProcessorTest, TagIndexTest) {
    fs::TempDir rootPath("/tmp/TagIndexTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    TestUtils::createSomeHosts(kv.get());
    TestUtils::assembleSpace(kv.get(), 1, 1);
    TestUtils::mockTag(kv.get(), 2);
    {
        // Allow to create tag index on no fields
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        std::vector<cpp2::IndexFieldDef> fields{};
        req.set_fields(std::move(fields));
        req.set_index_name("no_field_index");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        // Duplicate tag index on no fields
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        std::vector<cpp2::IndexFieldDef> fields{};
        req.set_fields(std::move(fields));
        req.set_index_name("no_field_index_1");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        cpp2::IndexFieldDef field;
        field.set_name("tag_0_col_0");
        req.set_fields({field});
        req.set_index_name("single_field_index");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        cpp2::IndexFieldDef field;
        field.set_name("tag_0_col_0");
        req.set_fields({field});
        req.set_index_name("duplicate_field_index");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_EXISTED, resp.get_code());
    }
    {
        cpp2::DropTagIndexReq req;
        req.set_space_id(1);
        req.set_index_name("no_field_index");
        auto* processor = DropTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        // Allow to create tag index on no fields
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        std::vector<cpp2::IndexFieldDef> fields{};
        req.set_fields(std::move(fields));
        req.set_index_name("no_field_index");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        std::vector<cpp2::IndexFieldDef> fields;
        cpp2::IndexFieldDef field1, field2;
        field1.set_name("tag_0_col_0");
        field2.set_name("tag_0_col_1");
        fields.emplace_back(std::move(field1));
        fields.emplace_back(std::move(field2));
        req.set_fields(std::move(fields));
        req.set_index_name("multi_field_index");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        std::vector<cpp2::IndexFieldDef> fields;
        cpp2::IndexFieldDef field1, field2;
        field1.set_name("tag_0_col_0");
        field2.set_name("tag_0_col_1");
        fields.emplace_back(std::move(field1));
        fields.emplace_back(std::move(field2));
        req.set_fields(std::move(fields));
        req.set_index_name("duplicate_field_index");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_EXISTED, resp.get_code());
    }
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        cpp2::IndexFieldDef field;
        field.set_name("tag_0_col_0");
        req.set_fields({field});
        req.set_index_name("duplicate_field_index");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_EXISTED, resp.get_code());
    }
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        std::vector<cpp2::IndexFieldDef> fields;
        cpp2::IndexFieldDef field1, field2;
        field1.set_name("tag_0_col_1");
        field2.set_name("tag_0_col_0");
        fields.emplace_back(std::move(field1));
        fields.emplace_back(std::move(field2));
        req.set_fields(std::move(fields));
        req.set_index_name("disorder_field_index");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        std::vector<cpp2::IndexFieldDef> fields;
        cpp2::IndexFieldDef field1, field2;
        field1.set_name("tag_0_col_0");
        field2.set_name("tag_0_col_0");
        fields.emplace_back(std::move(field1));
        fields.emplace_back(std::move(field2));
        req.set_fields(std::move(fields));
        req.set_index_name("conflict_index");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_CONFLICT, resp.get_code());
    }
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_not_exist");
        cpp2::IndexFieldDef field;
        field.set_name("tag_0_col_0");
        req.set_fields({field});
        req.set_index_name("tag_not_exist_index");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.get_code());
    }
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        cpp2::IndexFieldDef field;
        field.set_name("field_not_exist");
        req.set_fields({field});
        req.set_index_name("field_not_exist_index");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.get_code());
    }
    {
        // Test index have exist
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        cpp2::IndexFieldDef field;
        field.set_name("tag_0_col_0");
        req.set_fields({field});
        req.set_index_name("single_field_index");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_EXISTED, resp.get_code());
    }
    {
        cpp2::ListTagIndexesReq req;
        req.set_space_id(1);
        auto* processor = ListTagIndexesProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        auto items = resp.get_items();

        ASSERT_EQ(3, items.size());
        {
            cpp2::ColumnDef column;
            column.set_name("tag_0_col_0");
            column.type.set_type(PropertyType::INT64);
            std::vector<cpp2::ColumnDef> columns;
            columns.emplace_back(std::move(column));

            auto singleItem = items[0];
            ASSERT_EQ(2, singleItem.get_index_id());
            ASSERT_EQ("single_field_index", singleItem.get_index_name());
            auto singleFieldResult = singleItem.get_fields();
            ASSERT_TRUE(TestUtils::verifyResult(columns, singleFieldResult));
        }
        {
            std::vector<cpp2::ColumnDef> columns;
            cpp2::ColumnDef intColumn;
            intColumn.set_name("tag_0_col_0");
            intColumn.type.set_type(PropertyType::INT64);
            columns.emplace_back(std::move(intColumn));

            cpp2::ColumnDef stringColumn;
            stringColumn.set_name("tag_0_col_1");
            stringColumn.type.set_type(PropertyType::FIXED_STRING);
            columns.emplace_back(std::move(stringColumn));

            auto multiItem = items[1];
            ASSERT_EQ(3, multiItem.get_index_id());
            auto multiFieldResult = multiItem.get_fields();
            ASSERT_TRUE(TestUtils::verifyResult(columns, multiFieldResult));
        }
        {
            std::vector<cpp2::ColumnDef> columns;
            cpp2::ColumnDef stringColumn;
            stringColumn.set_name("tag_0_col_1");
            stringColumn.type.set_type(PropertyType::FIXED_STRING);
            columns.emplace_back(std::move(stringColumn));
            cpp2::ColumnDef intColumn;
            intColumn.set_name("tag_0_col_0");
            intColumn.type.set_type(PropertyType::INT64);
            columns.emplace_back(std::move(intColumn));

            auto disorderItem = items[2];
            ASSERT_EQ(4, disorderItem.get_index_id());
            auto disorderFieldResult = disorderItem.get_fields();
            ASSERT_TRUE(TestUtils::verifyResult(columns, disorderFieldResult));
        }
    }
    {
        cpp2::GetTagIndexReq req;
        req.set_space_id(1);
        req.set_index_name("single_field_index");

        auto* processor = GetTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        auto item = resp.get_item();
        auto fields = item.get_fields();
        ASSERT_EQ(2, item.get_index_id());

        cpp2::ColumnDef column;
        column.set_name("tag_0_col_0");
        column.type.set_type(PropertyType::INT64);
        std::vector<cpp2::ColumnDef> columns;
        columns.emplace_back(std::move(column));

        ASSERT_TRUE(TestUtils::verifyResult(columns, fields));
    }
    {
        cpp2::DropTagIndexReq req;
        req.set_space_id(1);
        req.set_index_name("single_field_index");

        auto* processor = DropTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::GetTagIndexReq req;
        req.set_space_id(1);
        req.set_index_name("single_field_index");

        auto* processor = GetTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.get_code());
    }
    {
        cpp2::DropTagIndexReq req;
        req.set_space_id(1);
        req.set_index_name("single_field_index");

        auto* processor = DropTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.get_code());
    }
}

TEST(ProcessorTest, TagIndexTestV2) {
    fs::TempDir rootPath("/tmp/TagIndexTestV2.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    TestUtils::createSomeHosts(kv.get());
    TestUtils::assembleSpace(kv.get(), 1, 1);
    TestUtils::mockTag(kv.get(), 2, 0, true);
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        cpp2::IndexFieldDef field;
        field.set_name("tag_0_col_0");
        req.set_fields({field});
        req.set_index_name("single_field_index");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::GetTagIndexReq req;
        req.set_space_id(1);
        req.set_index_name("single_field_index");

        auto* processor = GetTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        auto item = resp.get_item();
        auto fields = item.get_fields();
        ASSERT_EQ(1, item.get_index_id());

        cpp2::ColumnDef column;
        column.set_name("tag_0_col_0");
        column.type.set_type(PropertyType::INT64);
        column.set_nullable(true);
        std::vector<cpp2::ColumnDef> columns;
        columns.emplace_back(std::move(column));

        ASSERT_TRUE(TestUtils::verifyResult(columns, fields));
    }
}

TEST(ProcessorTest, EdgeIndexTest) {
    fs::TempDir rootPath("/tmp/EdgeIndexTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    TestUtils::createSomeHosts(kv.get());
    TestUtils::assembleSpace(kv.get(), 1, 1);
    TestUtils::mockEdge(kv.get(), 2);
    {
        // Allow to create edge index on no fields
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        std::vector<cpp2::IndexFieldDef> fields{};
        req.set_fields(std::move(fields));
        req.set_index_name("no_field_index");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        // Duplicate edge index on no fields
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        std::vector<cpp2::IndexFieldDef> fields{};
        req.set_fields(std::move(fields));
        req.set_index_name("no_field_index_1");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        cpp2::IndexFieldDef field;
        field.set_name("edge_0_col_0");
        req.set_fields({field});
        req.set_index_name("single_field_index");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        cpp2::IndexFieldDef field;
        field.set_name("edge_0_col_0");
        req.set_fields({field});
        req.set_index_name("duplicate_field_index");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_EXISTED, resp.get_code());
    }
    {
        cpp2::DropEdgeIndexReq req;
        req.set_space_id(1);
        req.set_index_name("no_field_index");
        auto* processor = DropEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        // Allow to create edge index on no fields
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        std::vector<cpp2::IndexFieldDef> fields{};
        req.set_fields(std::move(fields));
        req.set_index_name("no_field_index");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        std::vector<cpp2::IndexFieldDef> fields;
        cpp2::IndexFieldDef field1, field2;
        field1.set_name("edge_0_col_0");
        field2.set_name("edge_0_col_1");
        fields.emplace_back(std::move(field1));
        fields.emplace_back(std::move(field2));
        req.set_fields(std::move(fields));
        req.set_index_name("multi_field_index");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        std::vector<cpp2::IndexFieldDef> fields;
        cpp2::IndexFieldDef field1, field2;
        field1.set_name("edge_0_col_0");
        field2.set_name("edge_0_col_1");
        fields.emplace_back(std::move(field1));
        fields.emplace_back(std::move(field2));
        req.set_fields(std::move(fields));
        req.set_index_name("duplicate_field_index");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_EXISTED, resp.get_code());
    }
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        cpp2::IndexFieldDef field;
        field.set_name("edge_0_col_0");
        req.set_fields({field});
        req.set_index_name("duplicate_field_index");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_EXISTED, resp.get_code());
    }
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        std::vector<cpp2::IndexFieldDef> fields;
        cpp2::IndexFieldDef field1, field2;
        field1.set_name("edge_0_col_1");
        field2.set_name("edge_0_col_0");
        fields.emplace_back(std::move(field1));
        fields.emplace_back(std::move(field2));
        req.set_fields(std::move(fields));
        req.set_index_name("disorder_field_index");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        std::vector<cpp2::IndexFieldDef> fields;
        cpp2::IndexFieldDef field1, field2;
        field1.set_name("edge_0_col_0");
        field2.set_name("edge_0_col_0");
        fields.emplace_back(std::move(field1));
        fields.emplace_back(std::move(field2));
        req.set_fields(std::move(fields));
        req.set_index_name("conflict_index");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_CONFLICT, resp.get_code());
    }
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_not_exist");
        cpp2::IndexFieldDef field;
        field.set_name("edge_0_col_0");
        req.set_fields({field});
        req.set_index_name("edge_not_exist_index");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.get_code());
    }
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        cpp2::IndexFieldDef field;
        field.set_name("edge_field_not_exist");
        req.set_fields({field});
        req.set_index_name("field_not_exist_index");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.get_code());
    }
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        cpp2::IndexFieldDef field;
        field.set_name("edge_0_col_0");
        req.set_fields({field});
        req.set_index_name("single_field_index");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_EXISTED, resp.get_code());
    }
    {
        cpp2::ListEdgeIndexesReq req;
        req.set_space_id(1);
        auto* processor = ListEdgeIndexesProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        auto items = resp.get_items();
        ASSERT_EQ(3, items.size());
        {
            cpp2::ColumnDef column;
            column.set_name("edge_0_col_0");
            column.type.set_type(PropertyType::INT64);
            std::vector<cpp2::ColumnDef> columns;
            columns.emplace_back(std::move(column));

            auto singleItem = items[0];
            ASSERT_EQ(2, singleItem.get_index_id());
            auto singleFieldResult = singleItem.get_fields();
            ASSERT_TRUE(TestUtils::verifyResult(columns, singleFieldResult));
        }
        {
            std::vector<cpp2::ColumnDef> columns;
            cpp2::ColumnDef intColumn;
            intColumn.set_name("edge_0_col_0");
            intColumn.type.set_type(PropertyType::INT64);
            columns.emplace_back(std::move(intColumn));

            cpp2::ColumnDef stringColumn;
            stringColumn.set_name("edge_0_col_1");
            stringColumn.type.set_type(PropertyType::FIXED_STRING);
            columns.emplace_back(std::move(stringColumn));

            auto multiItem = items[1];
            ASSERT_EQ(3, multiItem.get_index_id());
            auto multiFieldResult = multiItem.get_fields();
            ASSERT_TRUE(TestUtils::verifyResult(columns, multiFieldResult));
        }
        {
            std::vector<cpp2::ColumnDef> columns;
            cpp2::ColumnDef stringColumn;
            stringColumn.set_name("edge_0_col_1");
            stringColumn.type.set_type(PropertyType::FIXED_STRING);
            columns.emplace_back(std::move(stringColumn));
            cpp2::ColumnDef intColumn;
            intColumn.set_name("edge_0_col_0");
            intColumn.type.set_type(PropertyType::INT64);
            columns.emplace_back(std::move(intColumn));

            auto disorderItem = items[2];
            ASSERT_EQ(4, disorderItem.get_index_id());
            auto disorderFieldResult = disorderItem.get_fields();
            ASSERT_TRUE(TestUtils::verifyResult(columns, disorderFieldResult));
        }
    }
    {
        cpp2::GetEdgeIndexReq req;
        req.set_space_id(1);
        req.set_index_name("single_field_index");

        auto* processor = GetEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        auto item = resp.get_item();
        auto properties = item.get_fields();
        ASSERT_EQ(2, item.get_index_id());
    }
    {
        cpp2::DropEdgeIndexReq req;
        req.set_space_id(1);
        req.set_index_name("single_field_index");

        auto* processor = DropEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::GetEdgeIndexReq req;
        req.set_space_id(1);
        req.set_index_name("single_field_index");

        auto* processor = GetEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.get_code());
    }
    {
        cpp2::DropEdgeIndexReq req;
        req.set_space_id(1);
        req.set_index_name("single_field_index");

        auto* processor = DropEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.get_code());
    }
    // Test the maximum limit for index columns
    std::vector<cpp2::IndexFieldDef> bigFields;
    {
        for (auto i = 1; i < 18; i++) {
            cpp2::IndexFieldDef field;
            field.set_name(folly::stringPrintf("col-%d", i));
            bigFields.emplace_back(std::move(field));
        }
    }
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_fields(bigFields);
        req.set_index_name("index_0");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_CONFLICT, resp.get_code());
    }
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_fields(std::move(bigFields));
        req.set_index_name("index_0");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_CONFLICT, resp.get_code());
    }
}

TEST(ProcessorTest, EdgeIndexTestV2) {
    fs::TempDir rootPath("/tmp/EdgeIndexTestV2.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    TestUtils::createSomeHosts(kv.get());
    TestUtils::assembleSpace(kv.get(), 1, 1);
    TestUtils::mockEdge(kv.get(), 2, 0, true);
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        cpp2::IndexFieldDef field;
        field.set_name("edge_0_col_0");
        req.set_fields({field});
        req.set_index_name("single_field_index");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::GetEdgeIndexReq req;
        req.set_space_id(1);
        req.set_index_name("single_field_index");
        auto* processor = GetEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        auto item = resp.get_item();
        auto fields = item.get_fields();
        ASSERT_EQ(1, item.get_index_id());

        cpp2::ColumnDef column;
        column.set_name("edge_0_col_0");
        column.type.set_type(PropertyType::INT64);
        column.set_nullable(true);
        std::vector<cpp2::ColumnDef> columns;
        columns.emplace_back(std::move(column));
        ASSERT_TRUE(TestUtils::verifyResult(columns, fields));
    }
}
TEST(ProcessorTest, IndexCheckAlterEdgeTest) {
    fs::TempDir rootPath("/tmp/IndexCheckAlterEdgeTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    TestUtils::createSomeHosts(kv.get());
    TestUtils::assembleSpace(kv.get(), 1, 1);
    TestUtils::mockEdge(kv.get(), 2);
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        cpp2::IndexFieldDef field;
        field.set_name("edge_0_col_0");
        req.set_fields({field});
        req.set_index_name("single_field_index");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::AlterEdgeReq req;
        cpp2::Schema addSch;
        cpp2::ColumnDef column;
        std::vector<cpp2::AlterSchemaItem> items;
        for (int32_t i = 2; i < 4; i++) {
            column.name = folly::stringPrintf("edge_0_col_%d", i);
            (*addSch.columns_ref()).emplace_back(std::move(column));
        }

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::ADD);
        items.back().set_schema(std::move(addSch));
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_edge_items(items);
        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::AlterEdgeReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        cpp2::Schema changeSch;
        cpp2::ColumnDef column;
        column.name = "edge_0_col_2";
        column.type.set_type(PropertyType::INT64);
        (*changeSch.columns_ref()).emplace_back(std::move(column));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::CHANGE);
        items.back().set_schema(std::move(changeSch));

        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_edge_items(items);
        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Verify ErrorCode of drop
    {
        cpp2::AlterEdgeReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        cpp2::Schema dropSch;
        cpp2::ColumnDef column;
        column.name = "edge_0_col_3";
        column.type.set_type(PropertyType::INT64);
        (*dropSch.columns_ref()).emplace_back(std::move(column));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::DROP);
        items.back().set_schema(std::move(dropSch));

        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_edge_items(items);
        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Verify ErrorCode of change
    {
        cpp2::AlterEdgeReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        cpp2::Schema changeSch;
        cpp2::ColumnDef column;
        column.name = "edge_0_col_0";
        column.type.set_type(PropertyType::INT64);
        (*changeSch.columns_ref()).emplace_back(std::move(column));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::CHANGE);
        items.back().set_schema(std::move(changeSch));

        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_edge_items(items);
        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_CONFLICT, resp.get_code());
    }
    // Verify ErrorCode of drop
    {
        cpp2::AlterEdgeReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        cpp2::Schema dropSch;
        cpp2::ColumnDef column;
        column.name = "edge_0_col_0";
        column.type.set_type(PropertyType::INT64);
        (*dropSch.columns_ref()).emplace_back(std::move(column));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::DROP);
        items.back().set_schema(std::move(dropSch));

        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_edge_items(items);
        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_CONFLICT, resp.get_code());
    }
}

TEST(ProcessorTest, IndexCheckAlterTagTest) {
    fs::TempDir rootPath("/tmp/IndexCheckAlterTagTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    TestUtils::createSomeHosts(kv.get());
    TestUtils::assembleSpace(kv.get(), 1, 1);
    TestUtils::mockTag(kv.get(), 2);
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        cpp2::IndexFieldDef field;
        field.set_name("tag_0_col_0");
        req.set_fields({field});
        req.set_index_name("single_field_index");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::AlterTagReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        cpp2::Schema addSch;
        cpp2::ColumnDef column;
        column.name = "tag_0_col_2";
        column.type.set_type(PropertyType::INT64);
        (*addSch.columns_ref()).emplace_back(std::move(column));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::ADD);
        items.back().set_schema(std::move(addSch));
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_tag_items(items);
        auto* processor = AlterTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::AlterTagReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        cpp2::Schema changeSch;
        cpp2::ColumnDef column;
        column.name = "tag_0_col_2";
        column.type.set_type(PropertyType::INT64);
        (*changeSch.columns_ref()).emplace_back(std::move(column));
        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::CHANGE);
        items.back().set_schema(std::move(changeSch));
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_tag_items(items);
        auto* processor = AlterTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::AlterTagReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        cpp2::Schema dropSch;
        cpp2::ColumnDef column;
        column.name = "tag_0_col_2";
        column.type.set_type(PropertyType::INT64);
        (*dropSch.columns_ref()).emplace_back(std::move(column));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::DROP);
        items.back().set_schema(std::move(dropSch));
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_tag_items(items);
        auto* processor = AlterTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::AlterTagReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        cpp2::Schema changeSch;
        cpp2::ColumnDef column;
        column.name = "tag_0_col_0";
        column.type.set_type(PropertyType::INT64);
        (*changeSch.columns_ref()).emplace_back(std::move(column));
        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::CHANGE);
        items.back().set_schema(std::move(changeSch));
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_tag_items(items);
        auto* processor = AlterTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_CONFLICT, resp.get_code());
    }
    {
        cpp2::AlterTagReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        cpp2::Schema dropSch;
        cpp2::ColumnDef column;
        column.name = "tag_0_col_0";
        column.type.set_type(PropertyType::INT64);
        (*dropSch.columns_ref()).emplace_back(std::move(column));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::DROP);
        items.back().set_schema(std::move(dropSch));
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_tag_items(items);
        auto* processor = AlterTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_CONFLICT, resp.get_code());
    }
}

TEST(ProcessorTest, IndexCheckDropEdgeTest) {
    fs::TempDir rootPath("/tmp/IndexCheckDropEdgeTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    TestUtils::createSomeHosts(kv.get());
    TestUtils::assembleSpace(kv.get(), 1, 1);
    TestUtils::mockEdge(kv.get(), 2);
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        cpp2::IndexFieldDef field;
        field.set_name("edge_0_col_0");
        req.set_fields({field});
        req.set_index_name("single_field_index");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::DropEdgeReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        auto* processor = DropEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_CONFLICT, resp.get_code());
    }
}


TEST(ProcessorTest, IndexCheckDropTagTest) {
    fs::TempDir rootPath("/tmp/IndexCheckDropTagTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    TestUtils::createSomeHosts(kv.get());
    TestUtils::assembleSpace(kv.get(), 1, 1);
    TestUtils::mockTag(kv.get(), 2);
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        cpp2::IndexFieldDef field;
        field.set_name("tag_0_col_0");
        req.set_fields({field});
        req.set_index_name("single_field_index");
        auto *processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::DropTagReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        auto* processor = DropTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_CONFLICT, resp.get_code());
    }
}

TEST(ProcessorTest, IndexTTLTagTest) {
    fs::TempDir rootPath("/tmp/IndexTTLTagTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    TestUtils::createSomeHosts(kv.get());
    TestUtils::assembleSpace(kv.get(), 1, 1);
    TestUtils::mockTag(kv.get(), 1);
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        cpp2::IndexFieldDef field;
        field.set_name("tag_0_col_0");
        req.set_fields({field});
        req.set_index_name("single_field_index");

        auto *processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::AlterTagReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        cpp2::Schema addSch;
        for (auto i = 0; i < 2; i++) {
            cpp2::ColumnDef column;
            column.name = folly::stringPrintf("tag_0_col_%d", i + 10);
            column.type.set_type(i < 1 ? PropertyType::INT64 : PropertyType::STRING);
            (*addSch.columns_ref()).emplace_back(std::move(column));
        }
        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::ADD);
        items.back().set_schema(std::move(addSch));
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_tag_items(items);

        auto* processor = AlterTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Tag with index add ttl property on index col, failed
    {
        cpp2::AlterTagReq req;
        cpp2::SchemaProp schemaProp;
        schemaProp.set_ttl_col("tag_0_col_0");
        schemaProp.set_ttl_duration(100);
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_schema_prop(std::move(schemaProp));

        auto* processor = AlterTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_UNSUPPORTED, resp.get_code());
    }
    // Tag with index add ttl property on no index col, failed
    {
        cpp2::AlterTagReq req;
        cpp2::SchemaProp schemaProp;
        schemaProp.set_ttl_col("tag_0_col_10");
        schemaProp.set_ttl_duration(100);
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_schema_prop(std::move(schemaProp));

        auto* processor = AlterTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_UNSUPPORTED, resp.get_code());
    }
    // Drop index
    {
        cpp2::DropTagIndexReq req;
        req.set_space_id(1);
        req.set_index_name("single_field_index");

        auto* processor = DropTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Add ttl property, succeed
    {
        cpp2::AlterTagReq req;
        cpp2::SchemaProp schemaProp;
        schemaProp.set_ttl_col("tag_0_col_0");
        schemaProp.set_ttl_duration(100);
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_schema_prop(std::move(schemaProp));

        auto* processor = AlterTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Tag with ttl to creat index on ttl col
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        cpp2::IndexFieldDef field;
        field.set_name("tag_0_col_0");
        req.set_fields({field});
        req.set_index_name("ttl_with_index");

        auto *processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Drop ttl property
    {
        cpp2::AlterTagReq req;
        cpp2::SchemaProp schemaProp;
        schemaProp.set_ttl_duration(0);
        schemaProp.set_ttl_col("");
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_schema_prop(std::move(schemaProp));

        auto* processor = AlterTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_UNSUPPORTED, resp.get_code());
    }
    {
        cpp2::DropTagIndexReq req;
        req.set_space_id(1);
        req.set_index_name("ttl_with_index");

        auto* processor = DropTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
}


TEST(ProcessorTest, IndexTTLEdgeTest) {
    fs::TempDir rootPath("/tmp/IndexTTLEdgeTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
    TestUtils::createSomeHosts(kv.get());
    TestUtils::assembleSpace(kv.get(), 1, 1);
    TestUtils::mockEdge(kv.get(), 1);
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        cpp2::IndexFieldDef field;
        field.set_name("edge_0_col_0");
        req.set_fields({field});
        req.set_index_name("single_field_index");

        auto *processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::AlterEdgeReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        cpp2::Schema addSch;
        for (auto i = 0; i < 2; i++) {
            cpp2::ColumnDef column;
            column.name = folly::stringPrintf("edge_0_col_%d", i + 10);
            column.type.set_type(i < 1 ? PropertyType::INT64 : PropertyType::STRING);
            (*addSch.columns_ref()).emplace_back(std::move(column));
        }
        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::ADD);
        items.back().set_schema(std::move(addSch));

        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_edge_items(items);
        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Edge with index add ttl property on index col
    {
        cpp2::AlterEdgeReq req;
        cpp2::SchemaProp schemaProp;
        schemaProp.set_ttl_col("edge_0_col_0");
        schemaProp.set_ttl_duration(100);
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_schema_prop(std::move(schemaProp));

        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_UNSUPPORTED, resp.get_code());
    }
    // Edge with index add ttl property on no index col
    {
        cpp2::AlterEdgeReq req;
        cpp2::SchemaProp schemaProp;
        schemaProp.set_ttl_col("edge_0_col_10");
        schemaProp.set_ttl_duration(100);
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_schema_prop(std::move(schemaProp));

        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_UNSUPPORTED, resp.get_code());
    }
    // Drop index
    {
        cpp2::DropEdgeIndexReq req;
        req.set_space_id(1);
        req.set_index_name("single_field_index");

        auto* processor = DropEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Drop index, then add ttl property, succeed
    {
        cpp2::AlterEdgeReq req;
        cpp2::SchemaProp schemaProp;
        schemaProp.set_ttl_col("edge_0_col_0");
        schemaProp.set_ttl_duration(100);
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_schema_prop(std::move(schemaProp));

        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Edge with ttl to create index on ttl col
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        cpp2::IndexFieldDef field;
        field.set_name("edge_0_col_0");
        req.set_fields({field});
        req.set_index_name("ttl_with_index");

        auto *processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Drop ttl property
    {
        cpp2::AlterEdgeReq req;
        cpp2::SchemaProp schemaProp;
        schemaProp.set_ttl_duration(0);
        schemaProp.set_ttl_col("");
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_schema_prop(std::move(schemaProp));

        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_UNSUPPORTED, resp.get_code());
    }
    {
        cpp2::DropEdgeIndexReq req;
        req.set_space_id(1);
        req.set_index_name("ttl_with_index");

        auto* processor = DropEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
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
