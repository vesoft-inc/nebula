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
#include "meta/processors/RemoveTagProcessor.h"
#include "meta/processors/GetTagProcessor.h"
#include "meta/processors/ListTagsProcessor.h"

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
        ASSERT_EQ(resp.code, cpp2::ErrorCode::SUCCEEDED);
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

TEST(ProcessorTest, RemoveTagTest) {
     fs::TempDir rootPath("/tmp/RemoveTagTest.XXXXXX");
     std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));

      // Setup tag index and tag data
     {
         TagID tagId = 1;
         std::vector<nebula::kvstore::KV> tags;
         auto version = time::TimeUtils::nowInMSeconds();
         tags.emplace_back(MetaUtils::spaceKey(0), "default_space");
         tags.emplace_back(MetaUtils::indexKey(EntryType::TAG, "tag_1"),
                 std::string(reinterpret_cast<const char*>(&tagId), sizeof(tagId)));
         tags.emplace_back(MetaUtils::schemaTagKey(0, tagId, version), "tag_schema1");
         tags.emplace_back(MetaUtils::schemaTagKey(0, tagId, version + 1000), "tag_schema2");
         kv.get()->asyncMultiPut(0, 0, std::move(tags),
                 [] (kvstore::ResultCode code, HostAddr leader) {
                 ASSERT_TRUE(code == kvstore::ResultCode::SUCCEEDED);
                 UNUSED(leader);
         });
     }

      // remove tag processor test
     {
         cpp2::RemoveTagReq req;
         req.set_space_id(0);
         req.set_tag_name("tag_1");
         auto* processor = RemoveTagProcessor::instance(kv.get());
         auto f = processor->getFuture();
         processor->process(req);
         auto resp = std::move(f).get();
         ASSERT_EQ(resp.get_code(), cpp2::ErrorCode::SUCCEEDED);
     }

 
      // check tag data has been deleted.
     {
         std::string tagVal, tagKey;
         kvstore::ResultCode ret;
         const std::string kTagsTable   = "__tags__";
         std::unique_ptr<kvstore::KVIterator> iter;

          ret = kv.get()->get(0, 0, std::move(MetaUtils::indexKey(EntryType::TAG, "tag_1")), &tagVal);
         ASSERT_TRUE(ret == kvstore::ResultCode::ERR_KEY_NOT_FOUND);
         tagKey.reserve(kTagsTable.size());
         tagKey.append(kTagsTable.data(), kTagsTable.size());
         ret = kv.get()->prefix(0, 0, tagKey, &iter);
         ASSERT_TRUE(ret == kvstore::ResultCode::SUCCEEDED);
         ASSERT_FALSE(iter->valid());
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


