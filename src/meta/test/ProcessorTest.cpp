/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <folly/String.h>
#include <fstream>
#include "fs/TempDir.h"
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

DECLARE_int32(expired_threshold_sec);

namespace nebula {
namespace meta {

using nebula::cpp2::SupportedType;

TEST(ProcessorTest, ListHostsTest) {
    fs::TempDir rootPath("/tmp/ListHostsTest.XXXXXX");
    FLAGS_expired_threshold_sec = 1;
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    std::vector<HostAddr> hosts;
    for (auto i = 0; i < 10; i++) {
        hosts.emplace_back(i, i);
    }
    {
        // after received heartbeat, host status will become online
        meta::TestUtils::registerHB(kv.get(), hosts);
        cpp2::ListHostsReq req;
        auto* processor = ListHostsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(10, resp.hosts.size());
        for (auto i = 0; i < 10; i++) {
            ASSERT_EQ(i, resp.hosts[i].hostAddr.ip);
            ASSERT_EQ(i, resp.hosts[i].hostAddr.port);
            ASSERT_EQ(cpp2::HostStatus::ONLINE, resp.hosts[i].status);
        }
    }
    {
        // host info expired
        sleep(FLAGS_expired_threshold_sec + 1);
        cpp2::ListHostsReq req;
        auto* processor = ListHostsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(10, resp.hosts.size());
        for (auto i = 0; i < 10; i++) {
            ASSERT_EQ(i, resp.hosts[i].hostAddr.ip);
            ASSERT_EQ(i, resp.hosts[i].hostAddr.port);
            ASSERT_EQ(cpp2::HostStatus::OFFLINE, resp.hosts[i].status);
        }
    }
}

TEST(ProcessorTest, ListPartsTest) {
    fs::TempDir rootPath("/tmp/ListPartsTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    std::vector<HostAddr> hosts = {{0, 0}, {1, 1}, {2, 2}};
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
        ASSERT_EQ(9, resp.parts.size());

        auto parts = std::move(resp.parts);
        std::sort(parts.begin(), parts.end(), [] (const auto& a, const auto& b) {
            return a.get_part_id() < b.get_part_id();
        });
        PartitionID partId = 0;
        for (auto& part : parts) {
            partId++;
            EXPECT_EQ(partId, part.get_part_id());
            EXPECT_FALSE(part.__isset.leader);
            EXPECT_EQ(3, part.peers.size());
            EXPECT_EQ(0, part.losts.size());
        }
    }

    // register HB with leader distribution
    {
        auto now = time::WallClock::fastNowInMilliSec();
        HostInfo info(now);

        LeaderParts leaderParts;
        leaderParts[1] = {1, 2, 3, 4, 5};
        auto ret = ActiveHostsMan::updateHostInfo(kv.get(), {0, 0}, info, &leaderParts);
        CHECK_EQ(ret, kvstore::ResultCode::SUCCEEDED);

        leaderParts[1] = {6, 7, 8};
        ret = ActiveHostsMan::updateHostInfo(kv.get(), {1, 1}, info, &leaderParts);
        CHECK_EQ(ret, kvstore::ResultCode::SUCCEEDED);

        leaderParts[1] = {9};
        ret = ActiveHostsMan::updateHostInfo(kv.get(), {2, 2}, info, &leaderParts);
        CHECK_EQ(ret, kvstore::ResultCode::SUCCEEDED);
    }

    {
        cpp2::ListPartsReq req;
        req.set_space_id(1);
        auto* processor = ListPartsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(9, resp.parts.size());

        auto parts = std::move(resp.parts);
        std::sort(parts.begin(), parts.end(), [] (const auto& a, const auto& b) {
            return a.get_part_id() < b.get_part_id();
        });
        PartitionID partId = 0;
        for (auto& part : parts) {
            partId++;
            EXPECT_EQ(partId, part.get_part_id());

            EXPECT_TRUE(part.__isset.leader);
            if (partId <= 5) {
                EXPECT_EQ(0, part.leader.ip);
                EXPECT_EQ(0, part.leader.port);
            } else if (partId > 5 && partId <= 8) {
                EXPECT_EQ(1, part.leader.ip);
                EXPECT_EQ(1, part.leader.port);
            } else {
                EXPECT_EQ(2, part.leader.ip);
                EXPECT_EQ(2, part.leader.port);
            }

            EXPECT_EQ(3, part.peers.size());
            for (auto& peer : part.peers) {
                auto it = std::find_if(hosts.begin(), hosts.end(),
                        [&] (const auto& host) {
                            return host.first == peer.ip && host.second == peer.port;
                    });
                EXPECT_TRUE(it != hosts.end());
            }
            EXPECT_EQ(0, part.losts.size());
        }
    }
}

TEST(ProcessorTest, SpaceTest) {
    fs::TempDir rootPath("/tmp/CreateSpaceTest.XXXXXX");
    auto kv = TestUtils::initKV(rootPath.path());
    auto hostsNum = TestUtils::createSomeHosts(kv.get());

    {
        cpp2::SpaceProperties properties;
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
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_EQ(1, resp.get_id().get_space_id());
    }
    {
        cpp2::GetSpaceReq req;
        req.set_space_name("default_space");
        auto* processor = GetSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_EQ("default_space", resp.item.properties.space_name);
        ASSERT_EQ(8, resp.item.properties.partition_num);
        ASSERT_EQ(3, resp.item.properties.replica_factor);
        ASSERT_EQ("utf8", resp.item.properties.charset_name);
        ASSERT_EQ("utf8_bin", resp.item.properties.collate_name);
    }

    {
        cpp2::ListSpacesReq req;
        auto* processor = ListSpacesProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_EQ(1, resp.spaces.size());
        ASSERT_EQ(1, resp.spaces[0].id.get_space_id());
        ASSERT_EQ("default_space", resp.spaces[0].name);
    }
    // Check the result. The dispatch way from part to hosts is in a round robin fashion.
    {
        cpp2::GetPartsAllocReq req;
        req.set_space_id(1);
        auto* processor = GetPartsAllocProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        std::unordered_map<HostAddr, std::set<PartitionID>> hostsParts;
        for (auto& p : resp.get_parts()) {
            for (auto& h : p.second) {
                hostsParts[std::make_pair(h.get_ip(), h.get_port())].insert(p.first);
                ASSERT_EQ(h.get_ip(), h.get_port());
            }
        }
        ASSERT_EQ(hostsNum, hostsParts.size());
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
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
    }
    {
        cpp2::ListSpacesReq req;
        auto* processor = ListSpacesProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_EQ(0, resp.spaces.size());
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
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
    }
    {
        constexpr char spaceName[] = "exist_space";
        cpp2::CreateSpaceReq req;
        cpp2::SpaceProperties properties;
        properties.set_space_name(spaceName);
        req.set_properties(std::move(properties));
        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);

        cpp2::DropSpaceReq req1;
        req1.set_space_name(spaceName);
        req1.set_if_exists(true);
        auto* processor1 = DropSpaceProcessor::instance(kv.get());
        auto f1 = processor1->getFuture();
        processor1->process(req1);
        auto resp1 = std::move(f1).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp1.code);
    }
    // Test default value
    {
        cpp2::SpaceProperties properties;
        properties.set_space_name("space_with_no_option");
        cpp2::CreateSpaceReq creq;
        creq.set_properties(std::move(properties));
        auto* cprocessor = CreateSpaceProcessor::instance(kv.get());
        auto cf = cprocessor->getFuture();
        cprocessor->process(creq);
        auto cresp = std::move(cf).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, cresp.code);

        cpp2::GetSpaceReq greq;
        greq.set_space_name("space_with_no_option");
        auto* gprocessor = GetSpaceProcessor::instance(kv.get());
        auto gf = gprocessor->getFuture();
        gprocessor->process(greq);
        auto gresp = std::move(gf).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, gresp.code);
        ASSERT_EQ("space_with_no_option", gresp.item.properties.space_name);
        ASSERT_EQ(100, gresp.item.properties.partition_num);
        ASSERT_EQ(1, gresp.item.properties.replica_factor);
        // Because setting default value in graph
        ASSERT_EQ("", gresp.item.properties.charset_name);
        ASSERT_EQ("", gresp.item.properties.collate_name);

        cpp2::DropSpaceReq dreq;
        dreq.set_space_name("space_with_no_option");
        auto* dprocessor = DropSpaceProcessor::instance(kv.get());
        auto df = dprocessor->getFuture();
        dprocessor->process(dreq);
        auto dresp = std::move(df).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, dresp.code);
    }
}

TEST(ProcessorTest, CreateTagTest) {
    fs::TempDir rootPath("/tmp/CreateTagTest.XXXXXX");
    auto kv = TestUtils::initKV(rootPath.path());
    TestUtils::createSomeHosts(kv.get());

    {
        cpp2::SpaceProperties properties;
        properties.set_space_name("first_space");
        properties.set_partition_num(9);
        properties.set_replica_factor(1);
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));

        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_EQ(1, resp.get_id().get_space_id());
    }
    {
        cpp2::SpaceProperties properties;
        properties.set_space_name("second_space");
        properties.set_partition_num(9);
        properties.set_replica_factor(1);
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));

        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_EQ(2, resp.get_id().get_space_id());
    }
    nebula::cpp2::Schema schema;
    decltype(schema.columns) cols;
    cols.emplace_back(TestUtils::columnDef(0, SupportedType::INT));
    cols.emplace_back(TestUtils::columnDef(1, SupportedType::FLOAT));
    cols.emplace_back(TestUtils::columnDef(2, SupportedType::STRING));
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
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.code);
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
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
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
        ASSERT_EQ(cpp2::ErrorCode::E_EXISTED, resp.code);
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
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
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
        ASSERT_EQ(cpp2::ErrorCode::E_CONFLICT, resp.code);
    }
    {
        // Set schema ttl property
        nebula::cpp2::SchemaProp schemaProp;
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
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_EQ(5, resp.get_id().get_tag_id());
    }
    {
        nebula::cpp2::Schema schemaWithDefault;
        decltype(schema.columns) colsWithDefault;
        colsWithDefault.emplace_back(TestUtils::columnDefWithDefault(0, SupportedType::BOOL));
        colsWithDefault.emplace_back(TestUtils::columnDefWithDefault(1, SupportedType::INT));
        colsWithDefault.emplace_back(TestUtils::columnDefWithDefault(2, SupportedType::DOUBLE));
        colsWithDefault.emplace_back(TestUtils::columnDefWithDefault(3, SupportedType::STRING));
        schemaWithDefault.set_columns(std::move(colsWithDefault));

        cpp2::CreateTagReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_with_default");
        req.set_schema(std::move(schemaWithDefault));
        auto* processor = CreateTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_EQ(6, resp.get_id().get_tag_id());
    }
    {
        nebula::cpp2::Schema schemaWithDefault;
        decltype(schema.columns) colsWithDefault;

        nebula::cpp2::ColumnDef columnWithDefault;
        columnWithDefault.set_name(folly::stringPrintf("col_type_mismatch"));
        nebula::cpp2::ValueType vType;
        vType.set_type(SupportedType::BOOL);
        columnWithDefault.set_type(std::move(vType));
        nebula::cpp2::Value defaultValue;
        defaultValue.set_string_value("default value");
        columnWithDefault.set_default_value(std::move(defaultValue));

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
        ASSERT_EQ(cpp2::ErrorCode::E_CONFLICT, resp.code);
    }
}


TEST(ProcessorTest, CreateEdgeTest) {
    fs::TempDir rootPath("/tmp/CreateEdgeTest.XXXXXX");
    auto kv = TestUtils::initKV(rootPath.path());
    TestUtils::createSomeHosts(kv.get());

    {
        cpp2::SpaceProperties properties;
        properties.set_space_name("default_space");
        properties.set_partition_num(9);
        properties.set_replica_factor(3);
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));

        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_EQ(1, resp.get_id().get_space_id());
   }
   {
        // Create another space
        cpp2::SpaceProperties properties;
        properties.set_space_name("another_space");
        properties.set_partition_num(9);
        properties.set_replica_factor(3);
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));

        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_EQ(2, resp.get_id().get_space_id());
    }

    nebula::cpp2::Schema schema;
    decltype(schema.columns) cols;
    cols.emplace_back(TestUtils::columnDef(0, SupportedType::INT));
    cols.emplace_back(TestUtils::columnDef(1, SupportedType::FLOAT));
    cols.emplace_back(TestUtils::columnDef(2, SupportedType::STRING));
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
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.code);
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
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
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
        ASSERT_EQ(cpp2::ErrorCode::E_EXISTED, resp.code);
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
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
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
        ASSERT_EQ(cpp2::ErrorCode::E_CONFLICT, resp.code);
    }

    // Set schema ttl property
    nebula::cpp2::SchemaProp schemaProp;
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
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_EQ(5, resp.get_id().get_edge_type());
    }
    {
        nebula::cpp2::Schema schemaWithDefault;
        decltype(schema.columns) colsWithDefault;
        colsWithDefault.emplace_back(TestUtils::columnDefWithDefault(0, SupportedType::BOOL));
        colsWithDefault.emplace_back(TestUtils::columnDefWithDefault(1, SupportedType::INT));
        colsWithDefault.emplace_back(TestUtils::columnDefWithDefault(2, SupportedType::DOUBLE));
        colsWithDefault.emplace_back(TestUtils::columnDefWithDefault(3, SupportedType::STRING));
        schemaWithDefault.set_columns(std::move(colsWithDefault));

        cpp2::CreateEdgeReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_with_defaule");
        req.set_schema(std::move(schemaWithDefault));
        auto* processor = CreateEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_EQ(6, resp.get_id().get_edge_type());
    }
    {
        nebula::cpp2::Schema schemaWithDefault;
        decltype(schema.columns) colsWithDefault;

        nebula::cpp2::ColumnDef columnWithDefault;
        columnWithDefault.set_name(folly::stringPrintf("col_type_mismatch"));
        nebula::cpp2::ValueType vType;
        vType.set_type(SupportedType::BOOL);
        columnWithDefault.set_type(std::move(vType));
        nebula::cpp2::Value defaultValue;
        defaultValue.set_string_value("default value");
        columnWithDefault.set_default_value(std::move(defaultValue));

        colsWithDefault.push_back(std::move(columnWithDefault));
        schemaWithDefault.set_columns(std::move(colsWithDefault));

        cpp2::CreateEdgeReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_type_mismatche");
        req.set_schema(std::move(schemaWithDefault));
        auto* processor = CreateEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_CONFLICT, resp.code);
    }
}

TEST(ProcessorTest, KVOperationTest) {
    fs::TempDir rootPath("/tmp/KVOperationTest.XXXXXX");
    auto kv = TestUtils::initKV(rootPath.path());
    TestUtils::createSomeHosts(kv.get());

    {
        cpp2::SpaceProperties properties;
        properties.set_space_name("default_space");
        properties.set_partition_num(9);
        properties.set_replica_factor(3);
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));

        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_EQ(1, resp.get_id().get_space_id());
    }
    {
        // Multi Put Test
        std::vector<nebula::cpp2::Pair> pairs;
        for (auto i = 0; i < 10; i++) {
            nebula::cpp2::Pair pair;
            pair.set_key(folly::stringPrintf("key_%d", i));
            pair.set_value(folly::stringPrintf("value_%d", i));
            pairs.emplace_back(std::move(pair));
        }

        cpp2::MultiPutReq req;
        req.set_segment("test");
        req.set_pairs(std::move(pairs));

        auto* processor = MultiPutProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
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
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_EQ("value_0", resp.value);

        cpp2::GetReq missedReq;
        missedReq.set_segment("test");
        missedReq.set_key("missed_key");

        auto* missedProcessor = GetProcessor::instance(kv.get());
        auto missedFuture = missedProcessor->getFuture();
        missedProcessor->process(missedReq);
        auto missedResp = std::move(missedFuture).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, missedResp.code);
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
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_EQ(2, resp.values.size());
        ASSERT_EQ("value_0", resp.values[0]);
        ASSERT_EQ("value_1", resp.values[1]);
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
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_EQ(3, resp.values.size());
        ASSERT_EQ("value_1", resp.values[0]);
        ASSERT_EQ("value_2", resp.values[1]);
        ASSERT_EQ("value_3", resp.values[2]);
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
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
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
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
    }
}


TEST(ProcessorTest, ListOrGetTagsTest) {
    fs::TempDir rootPath("/tmp/ListOrGetTagsTest.XXXXXX");
    auto kv = TestUtils::initKV(rootPath.path());
    ASSERT_TRUE(TestUtils::assembleSpace(kv.get(), 1, 1));
    TestUtils::mockTag(kv.get(), 10);

    // Test ListTagsProcessor
    {
        cpp2::ListTagsReq req;
        req.set_space_id(1);
        auto* processor = ListTagsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        decltype(resp.tags) tags;
        tags = resp.get_tags();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(10, tags.size());

        for (auto t = 0; t < 10; t++) {
            auto tag = tags[t];
            ASSERT_EQ(t, tag.get_tag_id());
            ASSERT_EQ(t, tag.get_version());
            ASSERT_EQ(folly::stringPrintf("tag_%d", t), tag.get_tag_name());
            ASSERT_EQ(2, tag.get_schema().columns.size());
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

        std::vector<nebula::cpp2::ColumnDef> cols = schema.get_columns();
        ASSERT_EQ(cols.size(), 2);
        for (auto i = 0; i < 2; i++) {
            ASSERT_EQ(folly::stringPrintf("tag_%d_col_%d", 0, i), cols[i].get_name());
            ASSERT_EQ((i < 1 ? SupportedType::INT : SupportedType::STRING),
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

        std::vector<nebula::cpp2::ColumnDef> cols = schema.get_columns();
        ASSERT_EQ(cols.size(), 2);
        for (auto i = 0; i < 2; i++) {
            ASSERT_EQ(folly::stringPrintf("tag_%d_col_%d", 0, i), cols[i].get_name());
            ASSERT_EQ((i < 1 ? SupportedType::INT : SupportedType::STRING),
                      cols[i].get_type().get_type());
        }
    }
}


TEST(ProcessorTest, ListOrGetEdgesTest) {
    fs::TempDir rootPath("/tmp/ListOrGetEdgesTest.XXXXXX");
    auto kv = TestUtils::initKV(rootPath.path());
    ASSERT_TRUE(TestUtils::assembleSpace(kv.get(), 1, 1));
    TestUtils::mockEdge(kv.get(), 10);

    // Test ListEdgesProcessor
    {
        cpp2::ListEdgesReq req;
        req.set_space_id(1);
        auto* processor = ListEdgesProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        decltype(resp.edges) edges;
        edges = resp.get_edges();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(10, edges.size());

        for (auto t = 0; t < 10; t++) {
            auto edge = edges[t];
            ASSERT_EQ(t, edge.get_edge_type());
            ASSERT_EQ(t, edge.get_version());
            ASSERT_EQ(folly::stringPrintf("edge_%d", t), edge.get_edge_name());
            ASSERT_EQ(2, edge.get_schema().columns.size());
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

            std::vector<nebula::cpp2::ColumnDef> cols = schema.get_columns();
            ASSERT_EQ(cols.size(), 2);
            for (auto i = 0; i < 2; i++) {
                ASSERT_EQ(folly::stringPrintf("edge_%d_col_%d", t, i), cols[i].get_name());
                ASSERT_EQ((i < 1 ? SupportedType::INT : SupportedType::STRING),
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

        std::vector<nebula::cpp2::ColumnDef> cols = schema.get_columns();
        ASSERT_EQ(cols.size(), 2);
        for (auto i = 0; i < 2; i++) {
            ASSERT_EQ(folly::stringPrintf("edge_%d_col_%d", 0, i), cols[i].get_name());
            ASSERT_EQ((i < 1 ? SupportedType::INT : SupportedType::STRING),
                      cols[i].get_type().get_type());
        }
    }
}


TEST(ProcessorTest, DropTagTest) {
    fs::TempDir rootPath("/tmp/DropTagTest.XXXXXX");
    auto kv = TestUtils::initKV(rootPath.path());
    ASSERT_TRUE(TestUtils::assembleSpace(kv.get(), 1, 1));
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
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);

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
    auto kv = TestUtils::initKV(rootPath.path());
    ASSERT_TRUE(TestUtils::assembleSpace(kv.get(), 1, 1));
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
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);

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
    auto kv = TestUtils::initKV(rootPath.path());
    ASSERT_TRUE(TestUtils::assembleSpace(kv.get(), 1, 1));
    TestUtils::mockTag(kv.get(), 1);
    // Alter tag options test
    {
        cpp2::AlterTagReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        nebula::cpp2::Schema addSch;
        for (auto i = 0; i < 2; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("tag_0_col_%d", i + 10);
            column.type.type = i < 1 ? SupportedType::INT : SupportedType::STRING;
            addSch.columns.emplace_back(std::move(column));
        }
        nebula::cpp2::Schema changeSch;
        for (auto i = 0; i < 2; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("tag_0_col_%d", i);
            column.type.type = i < 1 ? SupportedType::BOOL : SupportedType::DOUBLE;
            changeSch.columns.emplace_back(std::move(column));
        }
        nebula::cpp2::Schema dropSch;
        nebula::cpp2::ColumnDef column;
        column.name = "tag_0_col_0";
        dropSch.columns.emplace_back(std::move(column));
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
        auto tag = tags[0].version > 0 ? tags[0] : tags[1];
        EXPECT_EQ(0, tag.get_tag_id());
        EXPECT_EQ(folly::stringPrintf("tag_%d", 0), tag.get_tag_name());
        EXPECT_EQ(1, tag.version);

        nebula::cpp2::Schema schema;
        decltype(schema.columns) cols;

        nebula::cpp2::ColumnDef column;
        column.name = "tag_0_col_1";
        column.type.type = SupportedType::DOUBLE;
        cols.emplace_back(std::move(column));

        column.name = "tag_0_col_10";
        column.type.type = SupportedType::INT;
        cols.emplace_back(std::move(column));

        column.name = "tag_0_col_11";
        column.type.type = SupportedType::STRING;
        cols.emplace_back(std::move(column));

        schema.set_columns(std::move(cols));
        EXPECT_EQ(schema, tag.get_schema());
    }
    // Alter tag with ttl
    {
        // Only set ttl_duration
        cpp2::AlterTagReq req;
        nebula::cpp2::SchemaProp schemaProp;
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
        nebula::cpp2::SchemaProp schemaProp;
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
        auto tag = tags[0].version > tags[1].version ? tags[0] : tags[1];
        tag =  tag.version > tags[2].version ? tag : tags[2];

        EXPECT_EQ(0, tag.get_tag_id());
        EXPECT_EQ(folly::stringPrintf("tag_%d", 0), tag.get_tag_name());
        EXPECT_EQ(2, tag.version);

        nebula::cpp2::Schema schema;
        decltype(schema.columns) cols;

        nebula::cpp2::ColumnDef column;
        column.name = "tag_0_col_1";
        column.type.type = SupportedType::DOUBLE;
        cols.emplace_back(std::move(column));

        column.name = "tag_0_col_10";
        column.type.type = SupportedType::INT;
        cols.emplace_back(std::move(column));

        column.name = "tag_0_col_11";
        column.type.type = SupportedType::STRING;
        cols.emplace_back(std::move(column));

        schema.set_columns(std::move(cols));

        nebula::cpp2::SchemaProp schemaProp;
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
        nebula::cpp2::Schema changeSch;
        nebula::cpp2::ColumnDef column;
        column.name = "tag_0_col_10";
        column.type.type = SupportedType::DOUBLE;
        changeSch.columns.emplace_back(std::move(column));

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
        nebula::cpp2::SchemaProp schemaProp;
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
        nebula::cpp2::Schema dropSch;
        nebula::cpp2::ColumnDef column;
        column.name = "tag_0_col_10";
        dropSch.columns.emplace_back(std::move(column));

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
            if (tags[i].version > version) {
                max_index = i;
                version  = tags[i].version;
            }
        }
        auto tag = tags[max_index];
        EXPECT_EQ(0, tag.get_tag_id());
        EXPECT_EQ(folly::stringPrintf("tag_%d", 0), tag.get_tag_name());
        EXPECT_EQ(3, tag.version);

        nebula::cpp2::Schema schema;
        decltype(schema.columns) cols;

        nebula::cpp2::ColumnDef column;
        column.name = "tag_0_col_1";
        column.type.type = SupportedType::DOUBLE;
        cols.emplace_back(std::move(column));

        column.name = "tag_0_col_11";
        column.type.type = SupportedType::STRING;
        cols.emplace_back(std::move(column));

        schema.set_columns(std::move(cols));

        nebula::cpp2::SchemaProp schemaProp;
        schemaProp.set_ttl_duration(0);
        schemaProp.set_ttl_col("");
        schema.set_schema_prop(std::move(schemaProp));
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
        nebula::cpp2::Schema addSch;
        nebula::cpp2::ColumnDef column;
        column.name = "tag_0_col_1";
        column.type.type = SupportedType::INT;
        addSch.columns.emplace_back(std::move(column));

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
        nebula::cpp2::Schema changeSch;
        nebula::cpp2::ColumnDef column;
        column.name = "tag_0_col_2";
        column.type.type = SupportedType::INT;
        changeSch.columns.emplace_back(std::move(column));

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
        nebula::cpp2::Schema dropSch;
        nebula::cpp2::ColumnDef column;
        column.name = "tag_0_col_0";
        column.type.type = SupportedType::INT;
        dropSch.columns.emplace_back(std::move(column));

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
}


TEST(ProcessorTest, AlterEdgeTest) {
    fs::TempDir rootPath("/tmp/AlterEdgeTest.XXXXXX");
    auto kv = TestUtils::initKV(rootPath.path());
    ASSERT_TRUE(TestUtils::assembleSpace(kv.get(), 1, 1));
    TestUtils::mockEdge(kv.get(), 1);

    // Drop all, then add
    {
        cpp2::AlterEdgeReq req;
        nebula::cpp2::Schema dropSch;
        nebula::cpp2::ColumnDef column;
        std::vector<cpp2::AlterSchemaItem> items;
        for (int32_t i = 0; i < 2; i++) {
            column.name = folly::stringPrintf("edge_0_col_%d", i);
            dropSch.columns.emplace_back(std::move(column));
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
        auto edge = edges[0].version > 0 ? edges[0] : edges[1];
        EXPECT_EQ(0, edge.get_edge_type());
        EXPECT_EQ("edge_0", edge.get_edge_name());
        EXPECT_EQ(1, edge.version);
        EXPECT_EQ(0, edge.get_schema().columns.size());
    }
    {
        cpp2::AlterEdgeReq req;
        nebula::cpp2::Schema addSch;
        std::vector<cpp2::AlterSchemaItem> items;
        for (int32_t i = 0; i < 2; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("edge_0_col_%d", i);
            column.type.type = i < 1 ? SupportedType::INT : SupportedType::STRING;
            addSch.columns.emplace_back(std::move(column));
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
        nebula::cpp2::Schema addSch;
        for (auto i = 0; i < 2; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("edge_%d_col_%d", 0, i + 10);
            column.type.type = i < 1 ? SupportedType::INT : SupportedType::STRING;
            addSch.columns.emplace_back(std::move(column));
        }
        nebula::cpp2::Schema changeSch;
        for (auto i = 0; i < 2; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("edge_%d_col_%d", 0, i);
            column.type.type = i < 1 ? SupportedType::BOOL : SupportedType::DOUBLE;
            changeSch.columns.emplace_back(std::move(column));
        }
        nebula::cpp2::Schema dropSch;
        nebula::cpp2::ColumnDef column;
        column.name = "edge_0_col_0";
        dropSch.columns.emplace_back(std::move(column));

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
            if (edges[i].version > version) {
                max_index = i;
                version  = edges[i].version;
            }
        }
        auto edge = edges[max_index];
        EXPECT_EQ(0, edge.get_edge_type());
        EXPECT_EQ("edge_0", edge.get_edge_name());
        EXPECT_EQ(3, edge.version);

        nebula::cpp2::Schema schema;
        decltype(schema.columns) cols;

        nebula::cpp2::ColumnDef column;
        column.name = "edge_0_col_1";
        column.type.type = SupportedType::DOUBLE;
        cols.emplace_back(std::move(column));

        column.name = "edge_0_col_10";
        column.type.type = SupportedType::INT;
        cols.emplace_back(std::move(column));

        column.name = "edge_0_col_11";
        column.type.type = SupportedType::STRING;
        cols.emplace_back(std::move(column));
        schema.set_columns(std::move(cols));
        EXPECT_EQ(schema, edge.get_schema());
    }

    // Only set ttl_duration, failed
    {
        cpp2::AlterEdgeReq req;
        nebula::cpp2::SchemaProp schemaProp;
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
        nebula::cpp2::SchemaProp schemaProp;
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
            if (edges[i].version > version) {
                max_index = i;
                version  = edges[i].version;
            }
        }
        auto edge = edges[max_index];

        EXPECT_EQ(0, edge.get_edge_type());
        EXPECT_EQ("edge_0", edge.get_edge_name());
        EXPECT_EQ(4, edge.version);

        nebula::cpp2::Schema schema;
        decltype(schema.columns) cols;

        nebula::cpp2::ColumnDef column;
        column.name = "edge_0_col_1";
        column.type.type = SupportedType::DOUBLE;
        cols.emplace_back(std::move(column));

        column.name = "edge_0_col_10";
        column.type.type = SupportedType::INT;
        cols.emplace_back(std::move(column));

        column.name = "edge_0_col_11";
        column.type.type = SupportedType::STRING;
        cols.emplace_back(std::move(column));

        schema.set_columns(std::move(cols));

        nebula::cpp2::SchemaProp schemaProp;
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
        nebula::cpp2::Schema changeSch;
        nebula::cpp2::ColumnDef column;
        column.name = "edge_0_col_10";
        column.type.type = SupportedType::DOUBLE;
        changeSch.columns.emplace_back(std::move(column));

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
        nebula::cpp2::SchemaProp schemaProp;
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
        nebula::cpp2::Schema dropSch;
        nebula::cpp2::ColumnDef column;
        column.name = "edge_0_col_10";
        dropSch.columns.emplace_back(std::move(column));

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
            if (edges[i].version > version) {
                max_index = i;
                version  = edges[i].version;
            }
        }
        auto edge = edges[max_index];

        EXPECT_EQ(0, edge.get_edge_type());
        EXPECT_EQ("edge_0", edge.get_edge_name());
        EXPECT_EQ(5, edge.version);

        nebula::cpp2::Schema schema;
        decltype(schema.columns) cols;

        nebula::cpp2::ColumnDef column;
        column.name = "edge_0_col_1";
        column.type.type = SupportedType::DOUBLE;
        cols.emplace_back(std::move(column));

        column.name = "edge_0_col_11";
        column.type.type = SupportedType::STRING;
        cols.emplace_back(std::move(column));

        schema.set_columns(std::move(cols));

        nebula::cpp2::SchemaProp schemaProp;
        schemaProp.set_ttl_duration(0);
        schemaProp.set_ttl_col("");
        schema.set_schema_prop(std::move(schemaProp));
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
        nebula::cpp2::Schema addSch;
        nebula::cpp2::ColumnDef column;
        column.name = "edge_0_col_1";
        column.type.type = SupportedType::INT;
        addSch.columns.emplace_back(std::move(column));

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
        nebula::cpp2::Schema changeSch;
        nebula::cpp2::ColumnDef column;
        column.name = "edge_0_col_2";
        column.type.type = SupportedType::INT;
        changeSch.columns.emplace_back(std::move(column));

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
        nebula::cpp2::Schema dropSch;
        nebula::cpp2::ColumnDef column;
        column.name = "edge_0_col_2";
        column.type.type = SupportedType::INT;
        dropSch.columns.emplace_back(std::move(column));

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
}

TEST(ProcessorTest, SameNameTagsTest) {
    fs::TempDir rootPath("/tmp/SameNameTagsTest.XXXXXX");
    auto kv = TestUtils::initKV(rootPath.path());
    TestUtils::createSomeHosts(kv.get());

    {
        cpp2::SpaceProperties properties;
        properties.set_space_name("default_space");
        properties.set_partition_num(9);
        properties.set_replica_factor(3);
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));
        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_EQ(1, resp.get_id().get_space_id());
    }
    {
        cpp2::SpaceProperties properties;
        properties.set_space_name("second_space");
        properties.set_partition_num(9);
        properties.set_replica_factor(1);
        cpp2::CreateSpaceReq req;
        req.set_properties(std::move(properties));
        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_EQ(2, resp.get_id().get_space_id());
    }

    // Add same tag name in different space
    nebula::cpp2::Schema schema;
    decltype(schema.columns) cols;
    cols.emplace_back(TestUtils::columnDef(0, SupportedType::INT));
    cols.emplace_back(TestUtils::columnDef(1, SupportedType::FLOAT));
    cols.emplace_back(TestUtils::columnDef(2, SupportedType::STRING));
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
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
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
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
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
        decltype(resp.tags) tags;
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
        decltype(resp.tags) tags;
        tags = resp.get_tags();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(1, tags.size());

        ASSERT_EQ(4, tags[0].get_tag_id());
        ASSERT_EQ("default_tag", tags[0].get_tag_name());
    }
}

TEST(ProcessorTest, TagIndexTest) {
    fs::TempDir rootPath("/tmp/TagIndexTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    TestUtils::createSomeHosts(kv.get());
    ASSERT_TRUE(TestUtils::assembleSpace(kv.get(), 1, 1));
    TestUtils::mockTag(kv.get(), 2);
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        std::vector<std::string> fields{"tag_0_col_0"};
        req.set_fields(std::move(fields));
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
        std::vector<std::string> fields{"tag_0_col_0"};
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
        std::vector<std::string> fields{"tag_0_col_0", "tag_0_col_1"};
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
        std::vector<std::string> fields{"tag_0_col_0", "tag_0_col_0"};
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
        req.set_tag_name("tag_0");
        std::vector<std::string> fields{"tag_0_col_0", "tag_0_col_1"};
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
        std::vector<std::string> fields{"tag_0_col_0"};
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
        std::vector<std::string> fields{"tag_0_col_1", "tag_0_col_0"};
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
        req.set_tag_name("tag_not_exist");
        std::vector<std::string> fields{"tag_0_col_0"};
        req.set_fields(std::move(fields));
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
        std::vector<std::string> fields{"field_not_exist"};
        req.set_fields(std::move(fields));
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
        std::vector<std::string> fields{"tag_0_col_0"};
        req.set_fields(std::move(fields));
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
            nebula::cpp2::ColumnDef column;
            column.set_name("tag_0_col_0");
            nebula::cpp2::ValueType type;
            type.set_type(SupportedType::INT);
            column.set_type(std::move(type));
            std::vector<nebula::cpp2::ColumnDef> columns;
            columns.emplace_back(std::move(column));

            auto singleItem = items[0];
            ASSERT_EQ(1, singleItem.get_index_id());
            ASSERT_EQ("single_field_index", singleItem.get_index_name());
            auto singleFieldResult = singleItem.get_fields();
            ASSERT_TRUE(TestUtils::verifyResult(columns, singleFieldResult));
        }
        {
            std::vector<nebula::cpp2::ColumnDef> columns;
            nebula::cpp2::ColumnDef intColumn;
            intColumn.set_name("tag_0_col_0");
            nebula::cpp2::ValueType intType;
            intType.set_type(SupportedType::INT);
            intColumn.set_type(std::move(intType));
            columns.emplace_back(std::move(intColumn));
            nebula::cpp2::ColumnDef stringColumn;
            stringColumn.set_name("tag_0_col_1");
            nebula::cpp2::ValueType stringType;
            stringType.set_type(SupportedType::STRING);
            stringColumn.set_type(std::move(stringType));
            columns.emplace_back(std::move(stringColumn));

            auto multiItem = items[1];
            ASSERT_EQ(2, multiItem.get_index_id());
            auto multiFieldResult = multiItem.get_fields();
            ASSERT_TRUE(TestUtils::verifyResult(columns, multiFieldResult));
        }
        {
            std::vector<nebula::cpp2::ColumnDef> columns;
            nebula::cpp2::ColumnDef stringColumn;
            stringColumn.set_name("tag_0_col_1");
            nebula::cpp2::ValueType stringType;
            stringType.set_type(SupportedType::STRING);
            stringColumn.set_type(std::move(stringType));
            columns.emplace_back(std::move(stringColumn));
            nebula::cpp2::ColumnDef intColumn;
            intColumn.set_name("tag_0_col_0");
            nebula::cpp2::ValueType intType;
            intType.set_type(SupportedType::INT);
            intColumn.set_type(std::move(intType));
            columns.emplace_back(std::move(intColumn));

            auto disorderItem = items[2];
            ASSERT_EQ(3, disorderItem.get_index_id());
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
        ASSERT_EQ(1, item.get_index_id());

        nebula::cpp2::ColumnDef column;
        column.set_name("tag_0_col_0");
        nebula::cpp2::ValueType type;
        type.set_type(SupportedType::INT);
        column.set_type(std::move(type));
        std::vector<nebula::cpp2::ColumnDef> columns;
        columns.emplace_back(std::move(column));

        TestUtils::verifyResult(columns, fields);
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

TEST(ProcessorTest, EdgeIndexTest) {
    fs::TempDir rootPath("/tmp/EdgeIndexTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    TestUtils::createSomeHosts(kv.get());
    ASSERT_TRUE(TestUtils::assembleSpace(kv.get(), 1, 1));
    TestUtils::mockEdge(kv.get(), 2);
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        std::vector<std::string> fields{"edge_0_col_0"};
        req.set_fields(std::move(fields));
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
        std::vector<std::string> fields{"edge_0_col_0"};
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
        std::vector<std::string> fields{"edge_0_col_0", "edge_0_col_1"};
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
        std::vector<std::string> fields{"edge_0_col_0", "edge_0_col_0"};
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
        req.set_edge_name("edge_0");
        std::vector<std::string> fields{"edge_0_col_0", "edge_0_col_1"};
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
        std::vector<std::string> fields{"edge_0_col_0"};
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
        std::vector<std::string> fields{"edge_0_col_1", "edge_0_col_0"};
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
        req.set_edge_name("edge_not_exist");
        std::vector<std::string> fields{"edge_0_col_0"};
        req.set_fields(std::move(fields));
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
        std::vector<std::string> fields{"edge_field_not_exist"};
        req.set_fields(std::move(fields));
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
        std::vector<std::string> fields{"edge_0_col_0"};
        req.set_fields(std::move(fields));
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
            nebula::cpp2::ColumnDef column;
            column.set_name("edge_0_col_0");
            nebula::cpp2::ValueType type;
            type.set_type(SupportedType::INT);
            column.set_type(std::move(type));
            std::vector<nebula::cpp2::ColumnDef> columns;
            columns.emplace_back(std::move(column));

            auto singleItem = items[0];
            ASSERT_EQ(1, singleItem.get_index_id());
            auto singleFieldResult = singleItem.get_fields();
            ASSERT_TRUE(TestUtils::verifyResult(columns, singleFieldResult));
        }

        {
            std::vector<nebula::cpp2::ColumnDef> columns;
            nebula::cpp2::ColumnDef intColumn;
            intColumn.set_name("edge_0_col_0");
            nebula::cpp2::ValueType intType;
            intType.set_type(SupportedType::INT);
            intColumn.set_type(std::move(intType));
            columns.emplace_back(std::move(intColumn));
            nebula::cpp2::ColumnDef stringColumn;
            stringColumn.set_name("edge_0_col_1");
            nebula::cpp2::ValueType stringType;
            stringType.set_type(SupportedType::STRING);
            stringColumn.set_type(std::move(stringType));
            columns.emplace_back(std::move(stringColumn));

            auto multiItem = items[1];
            ASSERT_EQ(2, multiItem.get_index_id());
            auto multiFieldResult = multiItem.get_fields();
            ASSERT_TRUE(TestUtils::verifyResult(columns, multiFieldResult));
        }
        {
            std::vector<nebula::cpp2::ColumnDef> columns;
            nebula::cpp2::ColumnDef stringColumn;
            stringColumn.set_name("edge_0_col_1");
            nebula::cpp2::ValueType stringType;
            stringType.set_type(SupportedType::STRING);
            stringColumn.set_type(std::move(stringType));
            columns.emplace_back(std::move(stringColumn));
            nebula::cpp2::ColumnDef intColumn;
            intColumn.set_name("edge_0_col_0");
            nebula::cpp2::ValueType intType;
            intType.set_type(SupportedType::INT);
            intColumn.set_type(std::move(intType));
            columns.emplace_back(std::move(intColumn));

            auto disorderItem = items[2];
            ASSERT_EQ(3, disorderItem.get_index_id());
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
        ASSERT_EQ(1, item.get_index_id());
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
}

TEST(ProcessorTest, IndexCheckAlterEdgeTest) {
    fs::TempDir rootPath("/tmp/IndexCheckAlterEdgeTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    TestUtils::createSomeHosts(kv.get());
    ASSERT_TRUE(TestUtils::assembleSpace(kv.get(), 1, 1));
    TestUtils::mockEdge(kv.get(), 2);
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        std::vector<std::string> fields{"edge_0_col_0"};
        req.set_fields(std::move(fields));
        req.set_index_name("single_field_index");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::AlterEdgeReq req;
        nebula::cpp2::Schema addSch;
        nebula::cpp2::ColumnDef column;
        std::vector<cpp2::AlterSchemaItem> items;
        for (int32_t i = 2; i < 4; i++) {
            column.name = folly::stringPrintf("edge_0_col_%d", i);
            addSch.columns.emplace_back(std::move(column));
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
        nebula::cpp2::Schema changeSch;
        nebula::cpp2::ColumnDef column;
        column.name = "edge_0_col_2";
        column.type.type = SupportedType::INT;
        changeSch.columns.emplace_back(std::move(column));

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
        nebula::cpp2::Schema dropSch;
        nebula::cpp2::ColumnDef column;
        column.name = "edge_0_col_3";
        column.type.type = SupportedType::INT;
        dropSch.columns.emplace_back(std::move(column));

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
        nebula::cpp2::Schema changeSch;
        nebula::cpp2::ColumnDef column;
        column.name = "edge_0_col_0";
        column.type.type = SupportedType::INT;
        changeSch.columns.emplace_back(std::move(column));

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
        nebula::cpp2::Schema dropSch;
        nebula::cpp2::ColumnDef column;
        column.name = "edge_0_col_0";
        column.type.type = SupportedType::INT;
        dropSch.columns.emplace_back(std::move(column));

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
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    TestUtils::createSomeHosts(kv.get());
    ASSERT_TRUE(TestUtils::assembleSpace(kv.get(), 1, 1));
    TestUtils::mockTag(kv.get(), 2);
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        std::vector<std::string> fields{"tag_0_col_0"};
        req.set_fields(std::move(fields));
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
        nebula::cpp2::Schema addSch;
        nebula::cpp2::ColumnDef column;
        column.name = "tag_0_col_2";
        column.type.type = SupportedType::INT;
        addSch.columns.emplace_back(std::move(column));

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
        nebula::cpp2::Schema changeSch;
        nebula::cpp2::ColumnDef column;
        column.name = "tag_0_col_2";
        column.type.type = SupportedType::INT;
        changeSch.columns.emplace_back(std::move(column));
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
        nebula::cpp2::Schema dropSch;
        nebula::cpp2::ColumnDef column;
        column.name = "tag_0_col_2";
        column.type.type = SupportedType::INT;
        dropSch.columns.emplace_back(std::move(column));

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
        nebula::cpp2::Schema changeSch;
        nebula::cpp2::ColumnDef column;
        column.name = "tag_0_col_0";
        column.type.type = SupportedType::INT;
        changeSch.columns.emplace_back(std::move(column));
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
        nebula::cpp2::Schema dropSch;
        nebula::cpp2::ColumnDef column;
        column.name = "tag_0_col_0";
        column.type.type = SupportedType::INT;
        dropSch.columns.emplace_back(std::move(column));

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
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    TestUtils::createSomeHosts(kv.get());
    ASSERT_TRUE(TestUtils::assembleSpace(kv.get(), 1, 1));
    TestUtils::mockEdge(kv.get(), 2);
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        std::vector<std::string> fields{"edge_0_col_0"};
        req.set_fields(std::move(fields));
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
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    TestUtils::createSomeHosts(kv.get());
    ASSERT_TRUE(TestUtils::assembleSpace(kv.get(), 1, 1));
    TestUtils::mockTag(kv.get(), 2);
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        std::vector<std::string> fields{"tag_0_col_0"};
        req.set_fields(std::move(fields));
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
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    TestUtils::createSomeHosts(kv.get());
    ASSERT_TRUE(TestUtils::assembleSpace(kv.get(), 1, 1));
    TestUtils::mockTag(kv.get(), 1);
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        std::vector<std::string> fields{"tag_0_col_0"};
        req.set_fields(std::move(fields));
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
        nebula::cpp2::Schema addSch;
        for (auto i = 0; i < 2; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("tag_0_col_%d", i + 10);
            column.type.type = i < 1 ? SupportedType::INT : SupportedType::STRING;
            addSch.columns.emplace_back(std::move(column));
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
        nebula::cpp2::SchemaProp schemaProp;
        schemaProp.set_ttl_col("tag_0_col_0");
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
    // Tag with index add ttl property on no index col, failed
    {
        cpp2::AlterTagReq req;
        nebula::cpp2::SchemaProp schemaProp;
        schemaProp.set_ttl_col("tag_0_col_10");
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
        nebula::cpp2::SchemaProp schemaProp;
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
    // Tag with ttl to creat index on ttl col, failed
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        std::vector<std::string> fields{"tag_0_col_0"};
        req.set_fields(std::move(fields));
        req.set_index_name("single_field_index");

        auto *processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Tag with ttl to creat index on no ttl col, failed
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        std::vector<std::string> fields{"tag_0_col_10"};
        req.set_fields(std::move(fields));
        req.set_index_name("single_field_index");

        auto *processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Drop ttl property
    {
        cpp2::AlterTagReq req;
        nebula::cpp2::SchemaProp schemaProp;
        schemaProp.set_ttl_duration(0);
        schemaProp.set_ttl_col("");
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_schema_prop(std::move(schemaProp));

        auto* processor = AlterTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Tag without ttl to creat index, succeed
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        std::vector<std::string> fields{"tag_0_col_0"};
        req.set_fields(std::move(fields));
        req.set_index_name("single_field_index_col_0");
        auto *processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        std::vector<std::string> fields{"tag_0_col_10"};
        req.set_fields(std::move(fields));
        req.set_index_name("single_field_index_col_10");
        auto *processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::DropTagIndexReq req;
        req.set_space_id(1);
        req.set_index_name("single_field_index_col_0");

        auto* processor = DropTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::DropTagIndexReq req;
        req.set_space_id(1);
        req.set_index_name("single_field_index_col_10");

        auto* processor = DropTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
}


TEST(ProcessorTest, IndexTTLEdgeTest) {
    fs::TempDir rootPath("/tmp/IndexTTLEdgeTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    TestUtils::createSomeHosts(kv.get());
    ASSERT_TRUE(TestUtils::assembleSpace(kv.get(), 1, 1));
    TestUtils::mockEdge(kv.get(), 1);
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        std::vector<std::string> fields{"edge_0_col_0"};
        req.set_fields(std::move(fields));
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
        nebula::cpp2::Schema addSch;
        for (auto i = 0; i < 2; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("edge_0_col_%d", i + 10);
            column.type.type = i < 1 ? SupportedType::INT : SupportedType::STRING;
            addSch.columns.emplace_back(std::move(column));
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
    // Edge with index add ttl property on index col, failed
    {
        cpp2::AlterEdgeReq req;
        nebula::cpp2::SchemaProp schemaProp;
        schemaProp.set_ttl_col("edge_0_col_0");
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
    // Edge with index add ttl property on no index col, failed
    {
        cpp2::AlterEdgeReq req;
        nebula::cpp2::SchemaProp schemaProp;
        schemaProp.set_ttl_col("edge_0_col_10");
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
        nebula::cpp2::SchemaProp schemaProp;
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
    // Edge with ttl to creat index on ttl col, failed
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        std::vector<std::string> fields{"edge_0_col_0"};
        req.set_fields(std::move(fields));
        req.set_index_name("single_field_index");

        auto *processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Edge with ttl to creat index on no ttl col, failed
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        std::vector<std::string> fields{"edge_0_col_10"};
        req.set_fields(std::move(fields));
        req.set_index_name("single_field_index");

        auto *processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Drop ttl property
    {
        cpp2::AlterEdgeReq req;
        nebula::cpp2::SchemaProp schemaProp;
        schemaProp.set_ttl_duration(0);
        schemaProp.set_ttl_col("");
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_schema_prop(std::move(schemaProp));

        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Edge without ttl to create index, succeed
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        std::vector<std::string> fields{"edge_0_col_0"};
        req.set_fields(std::move(fields));
        req.set_index_name("single_field_index_col_0");

        auto *processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        std::vector<std::string> fields{"edge_0_col_10"};
        req.set_fields(std::move(fields));
        req.set_index_name("single_field_index_col_10");

        auto *processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::DropEdgeIndexReq req;
        req.set_space_id(1);
        req.set_index_name("single_field_index_col_0");

        auto* processor = DropEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::DropEdgeIndexReq req;
        req.set_space_id(1);
        req.set_index_name("single_field_index_col_10");

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


