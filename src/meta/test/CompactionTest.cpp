/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <folly/synchronization/Baton.h>
#include "fs/TempDir.h"
#include "meta/CompactionFilter.h"
#include "meta/processors/partsMan/CreateSpaceProcessor.h"
#include "meta/processors/partsMan/DropSpaceProcessor.h"
#include "meta/processors/schemaMan/CreateTagProcessor.h"
#include "meta/processors/schemaMan/CreateEdgeProcessor.h"
#include "meta/processors/schemaMan/ListTagsProcessor.h"
#include "meta/processors/schemaMan/ListEdgesProcessor.h"
#include "meta/processors/indexMan/CreateTagIndexProcessor.h"
#include "meta/processors/indexMan/CreateEdgeIndexProcessor.h"
#include "meta/processors/indexMan/ListTagIndexesProcessor.h"
#include "meta/processors/indexMan/ListEdgeIndexesProcessor.h"
#include "meta/test/TestUtils.h"

namespace nebula {
namespace meta {

std::vector<GraphSpaceID> mockSchema(kvstore::KVStore* kv) {
    std::vector<GraphSpaceID> spaces;
    // create 10 space
    for (int i = 0; i < 10; i++) {
        // In each space we create:
        // 11 tags (10 normal tags, 1 with default value)
        // 11 edges (10 normal edges, 1 with default value)
        // 1 tag index of tag_0
        // 1 edge index of edge_0
        {
            cpp2::SpaceProperties properties;
            properties.set_space_name(folly::stringPrintf("test_space_%d", i));
            properties.set_partition_num(10);
            properties.set_replica_factor(3);
            cpp2::CreateSpaceReq req;
            req.set_properties(std::move(properties));
            auto* processor = CreateSpaceProcessor::instance(kv);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
            spaces.emplace_back(resp.get_id().get_space_id());
        }
        for (int j = 0; j < 10; j++) {
            nebula::cpp2::Schema schema;
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("tag_%d_col_0", j);
            column.type.type = SupportedType::INT;
            schema.columns.emplace_back(std::move(column));

            cpp2::CreateTagReq req;
            req.set_space_id(spaces.back());
            req.set_tag_name(folly::stringPrintf("tag_%d", j));
            req.set_schema(std::move(schema));

            auto* processor = CreateTagProcessor::instance(kv);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        }
        for (int j = 0; j < 10; j++) {
            nebula::cpp2::Schema schema;
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("edge_%d_col_0", j);
            column.type.type = SupportedType::INT;
            schema.columns.emplace_back(std::move(column));

            cpp2::CreateEdgeReq req;
            req.set_space_id(spaces.back());
            req.set_edge_name(folly::stringPrintf("edge_%d", j));
            req.set_schema(std::move(schema));

            auto* processor = CreateEdgeProcessor::instance(kv);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        }
        {
            nebula::cpp2::Schema schema;
            nebula::cpp2::ColumnDef column;
            column.name = "tag_default_col_0";
            column.type.type = SupportedType::INT;
            nebula::cpp2::Value defaultValue;
            defaultValue.set_int_value(123);
            nebula::cpp2::ValueType valueType;
            valueType.set_type(SupportedType::INT);
            column.set_default_value(defaultValue);
            column.set_type(valueType);
            schema.columns.emplace_back(std::move(column));

            cpp2::CreateTagReq req;
            req.set_space_id(spaces.back());
            req.set_tag_name("default_tag");
            req.set_schema(std::move(schema));

            auto* processor = CreateTagProcessor::instance(kv);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        }
        {
            nebula::cpp2::Schema schema;
            nebula::cpp2::ColumnDef column;
            column.name = "edge_default_col_0";
            column.type.type = SupportedType::INT;
            nebula::cpp2::Value defaultValue;
            defaultValue.set_int_value(321);
            nebula::cpp2::ValueType valueType;
            valueType.set_type(SupportedType::INT);
            column.set_default_value(defaultValue);
            column.set_type(valueType);
            schema.columns.emplace_back(std::move(column));

            cpp2::CreateEdgeReq req;
            req.set_space_id(spaces.back());
            req.set_edge_name("default_edge");
            req.set_schema(std::move(schema));

            auto* processor = CreateEdgeProcessor::instance(kv);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
        }
        {
            cpp2::CreateTagIndexReq req;
            req.set_space_id(spaces.back());
            req.set_tag_name("tag_0");
            std::vector<std::string> fields{"tag_0_col_0"};
            req.set_fields(std::move(fields));
            req.set_index_name("tag_0_col_0_index");
            auto* processor = CreateTagIndexProcessor::instance(kv);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        }
        {
            cpp2::CreateEdgeIndexReq req;
            req.set_space_id(spaces.back());
            req.set_edge_name("edge_0");
            std::vector<std::string> fields{"edge_0_col_0"};
            req.set_fields(std::move(fields));
            req.set_index_name("edge_0_col_0_index");
            auto* processor = CreateEdgeIndexProcessor::instance(kv);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        }
    }
    return spaces;
}

size_t checkTags(kvstore::KVStore* kv, GraphSpaceID spaceId) {
    auto prefix = MetaServiceUtils::schemaTagsPrefix(spaceId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        return 0;
    }
    std::vector<TagID> tagIds;
    while (iter->valid()) {
        auto key = iter->key();
        auto tagId = *reinterpret_cast<const TagID *>(key.data() + prefix.size());
        tagIds.emplace_back(tagId);
        iter->next();
    }
    return tagIds.size();
}

size_t checkEdges(kvstore::KVStore* kv, GraphSpaceID spaceId) {
    auto prefix = MetaServiceUtils::schemaEdgesPrefix(spaceId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        return 0;
    }
    std::vector<EdgeType> edgeTypes;
    while (iter->valid()) {
        auto key = iter->key();
        auto edgeType = *reinterpret_cast<const EdgeType*>(key.data() + prefix.size());
        edgeTypes.emplace_back(edgeType);
        iter->next();
    }
    return edgeTypes.size();
}

std::pair<int, int> checkIndexes(kvstore::KVStore* kv, GraphSpaceID spaceId) {
    auto prefix = MetaServiceUtils::indexPrefix(spaceId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kv->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        return {0, 0};
    }

    int tagIndexCount = 0, edgeIndexCount = 0;
    while (iter->valid()) {
        auto val = iter->val();
        auto item = MetaServiceUtils::parseIndex(val);
        if (item.get_schema_id().getType() == nebula::cpp2::SchemaID::Type::tag_id) {
            tagIndexCount++;
        } else if (item.get_schema_id().getType() == nebula::cpp2::SchemaID::Type::edge_type) {
            edgeIndexCount++;
        }
        iter->next();
    }
    return {tagIndexCount, edgeIndexCount};
}

void dropSpace(kvstore::KVStore* kv, const std::string& spaceName) {
    cpp2::DropSpaceReq req;
    req.set_space_name(spaceName);
    auto* processor = DropSpaceProcessor::instance(kv);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.code);
}


TEST(MetaCompactionFilterTest, InvalidSpaceFilterTest) {
    fs::TempDir rootPath("/tmp/MetaCompactionFilterTest.XXXXXX");
    uint32_t localMetaPort = network::NetworkUtils::getAvailablePort();
    MetaCFHelper cfHelper;
    auto kv = TestUtils::initKV(rootPath.path(), localMetaPort, &cfHelper);
    TestUtils::createSomeHosts(kv.get());

    // create 10 spaces
    auto spaces = mockSchema(kv.get());

    for (size_t i = 0; i < spaces.size(); i++) {
        ASSERT_EQ(11, checkTags(kv.get(), spaces[i]));
        ASSERT_EQ(11, checkEdges(kv.get(), spaces[i]));
        ASSERT_EQ(std::make_pair(1, 1), checkIndexes(kv.get(), spaces[i]));
    }

    // drop even space of even index
    for (size_t i = 0; i < spaces.size(); i++) {
        if (i % 2 == 0) {
            dropSpace(kv.get(), folly::stringPrintf("test_space_%lu", i));
        }
    }

    auto* store = static_cast<kvstore::NebulaStore*>(kv.get());
    store->compact(0);

    for (size_t i = 0; i < spaces.size(); i++) {
        if (i % 2 == 0) {
            ASSERT_EQ(0, checkTags(kv.get(), spaces[i]));
            ASSERT_EQ(0, checkEdges(kv.get(), spaces[i]));
            ASSERT_EQ(std::make_pair(0, 0), checkIndexes(kv.get(), spaces[i]));
        } else {
            ASSERT_EQ(11, checkTags(kv.get(), spaces[i]));
            ASSERT_EQ(11, checkEdges(kv.get(), spaces[i]));
            ASSERT_EQ(std::make_pair(1, 1), checkIndexes(kv.get(), spaces[i]));
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
