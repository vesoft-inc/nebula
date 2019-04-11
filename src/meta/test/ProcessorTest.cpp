/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <folly/String.h>
#include <fstream>
#include "fs/TempDir.h"
#include "meta/test/TestUtils.h"
#include <common/time/TimeUtils.h>
#include "meta/processors/CreateSpaceProcessor.h"
#include "meta/processors/GetPartsAllocProcessor.h"
#include "meta/processors/ListSpacesProcessor.h"
#include "meta/processors/AddTagProcessor.h"
#include "meta/processors/GetTagProcessor.h"
#include "meta/processors/ListTagsProcessor.h"
#include "meta/processors/MultiPutProcessor.h"
#include "meta/processors/GetProcessor.h"
#include "meta/processors/MultiGetProcessor.h"
#include "meta/processors/RemoveProcessor.h"
#include "meta/processors/RemoveRangeProcessor.h"
#include "meta/processors/ScanProcessor.h"

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
        EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
    }
    {
        cpp2::ListHostsReq req;
        auto* processor = ListHostsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        EXPECT_EQ(10, resp.hosts.size());
        for (auto i = 0; i < 10; i++) {
            EXPECT_EQ(i, resp.hosts[i].ip);
            EXPECT_EQ(i, resp.hosts[i].port);
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
        EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
    }
    {
        cpp2::ListHostsReq req;
        auto* processor = ListHostsProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        EXPECT_EQ(20, resp.hosts.size());
        for (auto i = 0; i < 20; i++) {
            EXPECT_EQ(i, resp.hosts[i].ip);
            EXPECT_EQ(i, resp.hosts[i].port);
        }
    }
}

TEST(ProcessorTest, CreateSpaceTest) {
    fs::TempDir rootPath("/tmp/CreateSpaceTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    auto hostsNum = TestUtils::createSomeHosts(kv.get());
    {
        cpp2::CreateSpaceReq req;
        req.set_space_name("default_space");
        req.set_parts_num(9);
        req.set_replica_factor(3);

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
}

TEST(ProcessorTest, AddTagsTest) {
    fs::TempDir rootPath("/tmp/CreateSpaceTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    TestUtils::createSomeHosts(kv.get());
    {
        cpp2::CreateSpaceReq req;
        req.set_space_name("default_space");
        req.set_parts_num(9);
        req.set_replica_factor(1);
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
        cpp2::AddTagReq req;
        req.set_space_id(0);
        req.set_tag_name("default_tag");
        req.set_schema(schema);
        auto* processor = AddTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::E_NOT_FOUND, resp.code);
    }
    {
        cpp2::AddTagReq req;
        req.set_space_id(1);
        req.set_tag_name("default_tag");
        req.set_schema(schema);
        auto* processor = AddTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        ASSERT_EQ(2, resp.get_id().get_tag_id());
    }
}

TEST(ProcessorTest, KVOperationTest) {
    fs::TempDir rootPath("/tmp/KVOperationTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));
    auto hostsNum = TestUtils::createSomeHosts(kv.get());
    UNUSED(hostsNum);

    {
        cpp2::CreateSpaceReq req;
        req.set_space_name("default_space");
        req.set_parts_num(9);
        req.set_replica_factor(3);

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
        ASSERT_EQ(cpp2::ErrorCode::E_KEY_NOT_FOUND, missedResp.code);
    }
    {
        // Multi Get Test
        std::vector<std::string> keys;
        for (auto i = 0; i < 2; i++) {
            keys.emplace_back(std::move(folly::stringPrintf("key_%d", i)));
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
    auto version = time::TimeUtils::nowInMSeconds();
    // setup
    {
        std::vector<nebula::kvstore::KV> tags;
        for (auto t = 1; t < 10; t++) {
            nebula::cpp2::Schema srcsch;
            for (auto i = 0; i < 2; i++) {
                nebula::cpp2::ColumnDef column;
                column.name = folly::stringPrintf("tag_%d_col_%d", t, i);
                column.type.type = i < 1 ? SupportedType::INT : SupportedType::STRING;
                srcsch.columns.emplace_back(std::move(column));
            }

            tags.emplace_back(MetaUtils::schemaTagKey(1, t, version++),
                    MetaUtils::schemaTagVal(folly::stringPrintf("tag_%d", t), srcsch));
        }

        kv.get()->asyncMultiPut(0, 0, std::move(tags),
                                [] (kvstore::ResultCode code, HostAddr leader) {
            ASSERT_EQ(kvstore::ResultCode::SUCCEEDED, code);
            UNUSED(leader);
        });
    }

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
        ASSERT_EQ(9, tags.size());

        for (auto t = 1; t < 10; t++) {
            auto tag = tags[t-1];
            EXPECT_EQ(t, tag.get_tag_id());
            EXPECT_EQ(folly::stringPrintf("tag_%d", t), tag.get_tag_name());
        }
    }

    // test GetTagProcessor
    {
        cpp2::GetTagReq req;
        req.set_space_id(1);
        req.set_tag_id(9);
        req.set_version(version - 1);

        auto* processor = GetTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        auto schema = resp.get_schema();

        std::vector<nebula::cpp2::ColumnDef> cols = schema.get_columns();
        ASSERT_EQ(cols.size(), 2);
        for (auto i = 0; i < 2; i++) {
            EXPECT_EQ(folly::stringPrintf("tag_%d_col_%d", 9, i), cols[i].get_name());
            EXPECT_EQ((i < 1 ? SupportedType::INT : SupportedType::STRING),
                      cols[i].get_type().get_type());
        }
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


