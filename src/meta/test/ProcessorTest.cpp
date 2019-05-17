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
#include <common/time/TimeUtils.h>
#include "meta/processors/CreateSpaceProcessor.h"
#include "meta/processors/ListSpacesProcessor.h"
#include "meta/processors/ListSpacesProcessor.h"
#include "meta/processors/DropSpaceProcessor.h"
#include "meta/processors/RemoveHostsProcessor.h"
#include "meta/processors/GetPartsAllocProcessor.h"
#include "meta/processors/CreateTagProcessor.h"
#include "meta/processors/CreateEdgeProcessor.h"
#include "meta/processors/RemoveTagProcessor.h"
#include "meta/processors/RemoveEdgeProcessor.h"
#include "meta/processors/GetTagProcessor.h"
#include "meta/processors/GetEdgeProcessor.h"
#include "meta/processors/ListTagsProcessor.h"
#include "meta/processors/ListEdgesProcessor.h"
#include "meta/processors/MultiPutProcessor.h"
#include "meta/processors/GetProcessor.h"
#include "meta/processors/MultiGetProcessor.h"
#include "meta/processors/RemoveProcessor.h"
#include "meta/processors/RemoveRangeProcessor.h"
#include "meta/processors/ScanProcessor.h"
#include "meta/processors/AlterTagProcessor.h"
#include "meta/processors/AlterEdgeProcessor.h"

namespace nebula {
namespace meta {

using nebula::cpp2::SupportedType;
using apache::thrift::FragileConstructor::FRAGILE;

TEST(ProcessorTest, AddHostsTest) {
    fs::TempDir rootPath("/tmp/AddHostsTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    {
        std::vector<nebula::cpp2::HostAddr> thriftHosts;
        for (auto i = 0; i < 10; i++) {
            thriftHosts.emplace_back(FRAGILE, i, i);
        }
        cpp2::AddHostsReq req;
        req.set_hosts(std::move(thriftHosts));
        auto* processor = AddHostsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
    }
    {
        cpp2::ListHostsReq req;
        auto* processor = ListHostsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(10, resp.hosts.size());
        for (auto i = 0; i < 10; i++) {
            ASSERT_EQ(i, resp.hosts[i].ip);
            ASSERT_EQ(i, resp.hosts[i].port);
        }
    }
    {
        std::vector<nebula::cpp2::HostAddr> thriftHosts;
        for (auto i = 10; i < 20; i++) {
            thriftHosts.emplace_back(FRAGILE, i, i);
        }
        cpp2::AddHostsReq req;
        req.set_hosts(std::move(thriftHosts));
        auto* processor = AddHostsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
    }
    {
        cpp2::ListHostsReq req;
        auto* processor = ListHostsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(20, resp.hosts.size());
        for (auto i = 0; i < 20; i++) {
            ASSERT_EQ(i, resp.hosts[i].ip);
            ASSERT_EQ(i, resp.hosts[i].port);
        }
    }
    {
        std::vector<nebula::cpp2::HostAddr> thriftHosts;
        for (auto i = 0; i < 20; i++) {
            thriftHosts.emplace_back(apache::thrift::FragileConstructor::FRAGILE, i, i);
        }
        cpp2::RemoveHostsReq req;
        req.set_hosts(std::move(thriftHosts));
        auto* processor = RemoveHostsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
    }
    {
        cpp2::ListHostsReq req;
        auto* processor = ListHostsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(0, resp.hosts.size());
    }
}

TEST(ProcessorTest, CreateSpaceTest) {
    fs::TempDir rootPath("/tmp/CreateSpaceTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    auto hostsNum = TestUtils::createSomeHosts(kv.get());
    {
        cpp2::SpaceProperties properties;
        properties.set_space_name("default_space");
        properties.set_partition_num(9);
        properties.set_replica_factor(3);
        cpp2::CreateSpaceReq req;
        req.set_properties(properties);

        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_EQ(1, resp.get_id().get_space_id());
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
        for (auto& p : resp.get_parts()) {
            auto startIndex = p.first;
            for (auto& h : p.second) {
                ASSERT_EQ(startIndex++ % hostsNum, h.get_ip());
                ASSERT_EQ(h.get_ip(), h.get_port());
            }
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
}

TEST(ProcessorTest, CreateTagTest) {
    fs::TempDir rootPath("/tmp/CreateTagTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    TestUtils::createSomeHosts(kv.get());
    {
        cpp2::SpaceProperties properties;
        properties.set_space_name("default_space");
        properties.set_partition_num(9);
        properties.set_replica_factor(3);
        cpp2::CreateSpaceReq req;
        req.set_properties(properties);

        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_EQ(1, resp.get_id().get_space_id());
    }

    nebula::cpp2::Schema schema;
    decltype(schema.columns) cols;
    cols.emplace_back(TestUtils::columnDef(0, SupportedType::INT));
    cols.emplace_back(TestUtils::columnDef(1, SupportedType::FLOAT));
    cols.emplace_back(TestUtils::columnDef(2, SupportedType::STRING));
    schema.set_columns(std::move(cols));
    {
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
        cpp2::CreateTagReq req;
        req.set_space_id(1);
        req.set_tag_name("default_tag");
        req.set_schema(schema);
        auto* processor = CreateTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_EQ(2, resp.get_id().get_tag_id());
    }
}

TEST(ProcessorTest, CreateEdgeTest) {
    fs::TempDir rootPath("/tmp/CreateEdgeTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    TestUtils::createSomeHosts(kv.get());
    {
        cpp2::SpaceProperties properties;
        properties.set_space_name("default_space");
        properties.set_partition_num(9);
        properties.set_replica_factor(3);
        cpp2::CreateSpaceReq req;
        req.set_properties(properties);

        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_EQ(1, resp.get_id().get_space_id());
   }
   {
        // create another space
        cpp2::SpaceProperties properties;
        properties.set_space_name("another_space");
        properties.set_partition_num(9);
        properties.set_replica_factor(3);
        cpp2::CreateSpaceReq req;
        req.set_properties(properties);

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
        // succeeded
        cpp2::CreateEdgeReq req;
        req.set_space_id(1);
        req.set_edge_name("default_edge");
        req.set_schema(schema);
        auto* processor = CreateEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_NE(0, resp.get_id().get_edge_type());
    }
    {
        // existed
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
        // create same name edge in diff spaces
        cpp2::CreateEdgeReq req;
        req.set_space_id(2);
        req.set_edge_name("default_edge");
        req.set_schema(schema);
        auto* processor = CreateEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_NE(0, resp.get_id().get_edge_type());
    }
}

TEST(ProcessorTest, KVOperationTest) {
    fs::TempDir rootPath("/tmp/KVOperationTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    auto hostsNum = TestUtils::createSomeHosts(kv.get());
    UNUSED(hostsNum);

    {
        cpp2::SpaceProperties properties;
        properties.set_space_name("default_space");
        properties.set_partition_num(9);
        properties.set_replica_factor(3);
        cpp2::CreateSpaceReq req;
        req.set_properties(properties);

        auto* processor = CreateSpaceProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_EQ(1, resp.get_id().get_space_id());
    }
    {
        // Multi Put Test
        std::vector<cpp2::Pair> pairs;
        for (auto i = 0; i < 10; i++) {
            pairs.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                               folly::stringPrintf("key_%d", i),
                               folly::stringPrintf("value_%d", i));
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
    {
        // Illegal Segment Test
        cpp2::GetReq req;
        req.set_segment("_test0_");
        req.set_key("key_8");

        auto* processor = GetProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_STORE_SEGMENT_ILLEGAL, resp.code);
    }
}

TEST(ProcessorTest, ListOrGetTagsTest) {
    fs::TempDir rootPath("/tmp/ListTagsTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    ASSERT_TRUE(TestUtils::assembleSpace(kv.get(), 1));
    TestUtils::mockTag(kv.get(), 10);

    // test ListTagsProcessor
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

    // test GetTagProcessor
    {
        cpp2::GetTagReq req;
        req.set_space_id(1);
        req.set_tag_id(0);
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
}

TEST(ProcessorTest, ListOrGetEdgesTest) {
    fs::TempDir rootPath("/tmp/ListEdgesTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    ASSERT_TRUE(TestUtils::assembleSpace(kv.get(), 1));
    TestUtils::mockEdge(kv.get(), 10);

    // test ListEdgesProcessor
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

    // test GetEdgeProcessor
    {
        for (auto t = 0; t < 10; t++) {
            cpp2::GetEdgeReq req;
            req.set_space_id(1);
            req.set_edge_type(t);
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
}

TEST(ProcessorTest, RemoveTagTest) {
     fs::TempDir rootPath("/tmp/RemoveTagTest.XXXXXX");
     std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
     ASSERT_TRUE(TestUtils::assembleSpace(kv.get(), 1));
     TestUtils::mockTag(kv.get(), 1);
      // remove tag processor test
     {
         cpp2::RemoveTagReq req;
         req.set_space_id(1);
         req.set_tag_name("tag_0");
         auto* processor = RemoveTagProcessor::instance(kv.get());
         auto f = processor->getFuture();
         processor->process(req);
         auto resp = std::move(f).get();
         ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
     }

     // check tag data has been deleted.
     {
         std::string tagVal;
         kvstore::ResultCode ret;
         std::unique_ptr<kvstore::KVIterator> iter;
         ret = kv.get()->get(0, 0, std::move(MetaServiceUtils::indexTagKey(1, "tag_1")),
                             &tagVal);
         ASSERT_EQ(kvstore::ResultCode::ERR_KEY_NOT_FOUND, ret);
         std::string tagPrefix = "__tags__";
         ret = kv.get()->prefix(0, 0, tagPrefix, &iter);
         ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
         ASSERT_FALSE(iter->valid());
     }
}

TEST(ProcessorTest, RemoveEdgeTest) {
    fs::TempDir rootPath("/tmp/RemoveEdgeTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    ASSERT_TRUE(TestUtils::assembleSpace(kv.get(), 1));
    TestUtils::mockEdge(kv.get(), 1);
    // Space not exist
    {
        cpp2::RemoveEdgeReq req;
        req.set_space_id(0);
        req.set_edge_name("edge_0");
        auto* processor = RemoveEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.get_code());
    }
    // Edge not exist
    {
        cpp2::RemoveEdgeReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_no");
        auto* processor = RemoveEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.get_code());
    }
    // Succeeded
    {
        cpp2::RemoveEdgeReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        auto* processor = RemoveEdgeProcessor::instance(kv.get());
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
        ret = kv.get()->get(0, 0, std::move(MetaServiceUtils::indexEdgeKey(1, "edge_1")),
                            &edgeVal);
        ASSERT_EQ(kvstore::ResultCode::ERR_KEY_NOT_FOUND, ret);
        std::string edgePrefix = "__edges__";
        ret = kv.get()->prefix(0, 0, edgePrefix, &iter);
        ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
        ASSERT_FALSE(iter->valid());
    }
}

TEST(ProcessorTest, AlterTagTest) {
    fs::TempDir rootPath("/tmp/AlterTagTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    ASSERT_TRUE(TestUtils::assembleSpace(kv.get(), 1));
    TestUtils::mockTag(kv.get(), 1);
    // Alter tag processor test
    {
        cpp2::AlterTagReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        nebula::cpp2::Schema addSch;
        for (auto i = 0; i < 2; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("tag_%d_col_%d", 0, i + 10);
            column.type.type = i < 1 ? SupportedType::INT : SupportedType::STRING;
            addSch.columns.emplace_back(std::move(column));
        }
        nebula::cpp2::Schema setSch;
        for (auto i = 0; i < 2; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("tag_%d_col_%d", 0, i);
            column.type.type = i < 1 ? SupportedType::BOOL : SupportedType::DOUBLE;
            setSch.columns.emplace_back(std::move(column));
        }
        nebula::cpp2::Schema dropSch;
        nebula::cpp2::ColumnDef column;
        column.name = folly::stringPrintf("tag_%d_col_%d", 0, 0);
        dropSch.columns.emplace_back(std::move(column));

        items.emplace_back(FRAGILE, cpp2::AlterSchemaOp::ADD, std::move(addSch));
        items.emplace_back(FRAGILE, cpp2::AlterSchemaOp::CHANGE, std::move(setSch));
        items.emplace_back(FRAGILE, cpp2::AlterSchemaOp::DROP, std::move(dropSch));
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_tag_items(items);
        auto* processor = AlterTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // verify alter result.
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
    // verify ErrorCode of add
    {
        cpp2::AlterTagReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        nebula::cpp2::Schema addSch;
        nebula::cpp2::ColumnDef column;
        column.name = "tag_0_col_1";
        column.type.type = SupportedType::INT;
        addSch.columns.emplace_back(std::move(column));
        auto addItem = cpp2::AlterSchemaItem(FRAGILE,
                                             cpp2::AlterSchemaOp::ADD,
                                             std::move(addSch));
        items.push_back(std::move(addItem));
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_tag_items(items);
        auto* processor = AlterTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_EXISTED, resp.get_code());
    }
    // verify ErrorCode of set
    {
        cpp2::AlterTagReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        nebula::cpp2::Schema addSch;
        nebula::cpp2::ColumnDef column;
        column.name = "tag_0_col_2";
        column.type.type = SupportedType::INT;
        addSch.columns.emplace_back(std::move(column));
        auto addItem = cpp2::AlterSchemaItem(FRAGILE,
                                             cpp2::AlterSchemaOp::CHANGE,
                                             std::move(addSch));
        items.push_back(std::move(addItem));
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_tag_items(items);
        auto* processor = AlterTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.get_code());
    }
    // verify ErrorCode of drop
    {
        cpp2::AlterTagReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        nebula::cpp2::Schema addSch;
        nebula::cpp2::ColumnDef column;
        column.name = "tag_0_col_2";
        column.type.type = SupportedType::INT;
        addSch.columns.emplace_back(std::move(column));
        items.emplace_back(FRAGILE, cpp2::AlterSchemaOp::DROP, std::move(addSch));
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
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    ASSERT_TRUE(TestUtils::assembleSpace(kv.get(), 1));
    TestUtils::mockEdge(kv.get(), 1);
    // Drop all, then add
    {
        cpp2::AlterEdgeReq req;
        nebula::cpp2::Schema dropSch;
        nebula::cpp2::ColumnDef column;
        std::vector<cpp2::AlterSchemaItem> items;
        column.name = folly::stringPrintf("edge_%d_col_%d", 0, 0);
        dropSch.columns.emplace_back(std::move(column));
        column.name = folly::stringPrintf("edge_%d_col_%d", 0, 1);
        dropSch.columns.emplace_back(std::move(column));

        auto dropItem = cpp2::AlterSchemaItem(FRAGILE,
                                              cpp2::AlterSchemaOp::DROP,
                                              std::move(dropSch));
        items.emplace_back(std::move(dropItem));
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
        EXPECT_EQ(folly::stringPrintf("edge_%d", 0), edge.get_edge_name());
        EXPECT_EQ(1, edge.version);
        EXPECT_EQ(0, edge.get_schema().columns.size());
    }
    {
        cpp2::AlterEdgeReq req;
        nebula::cpp2::Schema addSch;
        nebula::cpp2::ColumnDef column;
        std::vector<cpp2::AlterSchemaItem> items;
        column.name = folly::stringPrintf("edge_%d_col_%d", 0, 0);
        addSch.columns.emplace_back(std::move(column));
        column.name = folly::stringPrintf("edge_%d_col_%d", 0, 1);
        addSch.columns.emplace_back(std::move(column));

        auto dropItem = cpp2::AlterSchemaItem(FRAGILE,
                                              cpp2::AlterSchemaOp::ADD,
                                              std::move(addSch));
        items.emplace_back(std::move(dropItem));
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
        nebula::cpp2::Schema setSch;
        for (auto i = 0; i < 2; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("edge_%d_col_%d", 0, i);
            column.type.type = i < 1 ? SupportedType::BOOL : SupportedType::DOUBLE;
            setSch.columns.emplace_back(std::move(column));
        }
        nebula::cpp2::Schema dropSch;
        nebula::cpp2::ColumnDef column;
        column.name = folly::stringPrintf("edge_%d_col_%d", 0, 0);
        dropSch.columns.emplace_back(std::move(column));

        auto addItem = cpp2::AlterSchemaItem(FRAGILE,
                                             cpp2::AlterSchemaOp::ADD,
                                             std::move(addSch));
        auto setItem = cpp2::AlterSchemaItem(FRAGILE,
                                             cpp2::AlterSchemaOp::CHANGE,
                                             std::move(setSch));
        auto dropItem = cpp2::AlterSchemaItem(FRAGILE,
                                              cpp2::AlterSchemaOp::DROP,
                                              std::move(dropSch));
        items.emplace_back(std::move(addItem));
        items.emplace_back(std::move(setItem));
        items.emplace_back(std::move(dropItem));
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
        auto edge = edges[0].version > 0 ? edges[0] : edges[1];
        EXPECT_EQ(0, edge.get_edge_type());
        EXPECT_EQ(folly::stringPrintf("edge_%d", 0), edge.get_edge_name());
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
    // verify ErrorCode of add
    {
        cpp2::AlterEdgeReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        nebula::cpp2::Schema addSch;
        nebula::cpp2::ColumnDef column;
        column.name = "edge_0_col_1";
        column.type.type = SupportedType::INT;
        addSch.columns.emplace_back(std::move(column));
        auto addItem = cpp2::AlterSchemaItem(FRAGILE,
                                             cpp2::AlterSchemaOp::ADD,
                                             std::move(addSch));
        items.emplace_back(std::move(addItem));
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_edge_items(items);
        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_EXISTED, resp.get_code());
    }
    // verify ErrorCode of set
    {
        cpp2::AlterEdgeReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        nebula::cpp2::Schema addSch;
        nebula::cpp2::ColumnDef column;
        column.name = "edge_0_col_2";
        column.type.type = SupportedType::INT;
        addSch.columns.emplace_back(std::move(column));
        auto addItem = cpp2::AlterSchemaItem(FRAGILE,
                                             cpp2::AlterSchemaOp::CHANGE,
                                             std::move(addSch));
        items.emplace_back(std::move(addItem));
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_edge_items(items);
        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.get_code());
    }
    // verify ErrorCode of drop
    {
        cpp2::AlterEdgeReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        nebula::cpp2::Schema addSch;
        nebula::cpp2::ColumnDef column;
        column.name = "edge_0_col_2";
        column.type.type = SupportedType::INT;
        addSch.columns.emplace_back(std::move(column));
        auto addItem = cpp2::AlterSchemaItem(FRAGILE, cpp2::AlterSchemaOp::DROP, std::move(addSch));
        items.emplace_back(addItem);
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
    fs::TempDir rootPath("/tmp/CreateSpaceTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    TestUtils::createSomeHosts(kv.get());
    {
        cpp2::SpaceProperties properties;
        properties.set_space_name("default_space");
        properties.set_partition_num(9);
        properties.set_replica_factor(3);
        cpp2::CreateSpaceReq req;
        req.set_properties(properties);
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
        req.set_properties(properties);
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
        cpp2::RemoveTagReq req;
        req.set_space_id(1);
        req.set_tag_name("default_tag");
        auto* processor = RemoveTagProcessor::instance(kv.get());
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
}  // namespace meta
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}


