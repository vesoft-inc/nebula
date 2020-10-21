/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Status.h"
#include "common/network/NetworkUtils.h"
#include "common/interface/gen-cpp2/common_types.h"
#include "mock/test/TestEnv.h"
#include "mock/test/TestBase.h"
#include "context/QueryExpressionContext.h"
#include <gtest/gtest.h>

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace graph {
class MockServerTest : public TestBase {
public:
    void SetUp() override {
        TestBase::SetUp();
    };

    void TearDown() override {
        TestBase::TearDown();
    };
};

TEST_F(MockServerTest, TestMeta) {
    GraphSpaceID spaceId1 = 0;
    GraphSpaceID spaceId2 = 0;
    auto metaClient = gEnv->getMetaClient();
    // Test space
    {
        // Create space
        std::string spaceName1 = "TEST_SPACE1";
        meta::cpp2::SpaceDesc spaceDesc1;
        spaceDesc1.set_space_name(spaceName1);
        spaceDesc1.set_partition_num(10);
        spaceDesc1.set_replica_factor(1);
        bool ifNotExists = true;
        auto status = metaClient->createSpace(spaceDesc1, ifNotExists).get();
        spaceId1 = status.value();

        std::string spaceName2 = "TEST_SPACE2";
        meta::cpp2::SpaceDesc spaceDesc2;
        spaceDesc2.set_space_name(spaceName2);
        spaceDesc2.set_partition_num(100);
        spaceDesc2.set_replica_factor(3);
        status = metaClient->createSpace(spaceDesc2, ifNotExists).get();
        spaceId2 = status.value();

        // Get space
        auto getStatus = metaClient->getSpace(spaceName1).get();
        ASSERT_TRUE(getStatus.ok());
        ASSERT_EQ(spaceId1, getStatus.value().get_space_id());
        ASSERT_EQ(10, getStatus.value().get_properties().get_partition_num());
        ASSERT_EQ(1, getStatus.value().get_properties().get_replica_factor());
        ASSERT_EQ(8, *getStatus.value().get_properties().get_vid_type().get_type_length());

        getStatus = metaClient->getSpace(spaceName2).get();
        ASSERT_TRUE(getStatus.ok());
        ASSERT_EQ(spaceId2, getStatus.value().get_space_id());
        ASSERT_EQ(100, getStatus.value().get_properties().get_partition_num());
        ASSERT_EQ(3, getStatus.value().get_properties().get_replica_factor());
        ASSERT_EQ(8, *getStatus.value().get_properties().get_vid_type().get_type_length());

        // List spaces
        auto listStatus = metaClient->listSpaces().get();
        ASSERT_TRUE(listStatus.ok());
        auto spaces = listStatus.value();
        ASSERT_EQ(2, spaces.size());

        std::vector<meta::SpaceIdName> expected;
        expected.emplace_back(spaceId2, spaceName2);
        expected.emplace_back(spaceId1, spaceName1);
        ASSERT_EQ(expected, spaces);
    }
    QueryExpressionContext ctx;
    // Test tag
    {
        // Create tag
        for (auto i = 0u; i < 10; i++) {
            meta::cpp2::Schema tagSchema;
            meta::cpp2::ColumnDef col;
            col.set_name(folly::stringPrintf("col_%d", i));
            col.type.set_type(meta::cpp2::PropertyType::STRING);
            ConstantExpression defaultValue("NULL");
            col.set_default_value(defaultValue.encode());
            std::vector<meta::cpp2::ColumnDef> cols;
            cols.emplace_back(col);
            tagSchema.set_columns(std::move(cols));
            auto status = metaClient->createTagSchema(spaceId1,
                    folly::stringPrintf("tag_%d", i), tagSchema, false).get();
            ASSERT_TRUE(status.ok());
        }

        // Get tag
        for (auto i = 0u; i < 10; i++) {
            auto status = metaClient->getTagSchema(spaceId1,
                    folly::stringPrintf("tag_%d", i)).get();
            ASSERT_TRUE(status.ok());
            auto schema = status.value();
            ASSERT_EQ(1, schema.get_columns().size());
            ASSERT_EQ(folly::stringPrintf("col_%d", i), schema.get_columns()[0].get_name());
            ASSERT_EQ(meta::cpp2::PropertyType::STRING,
                      schema.get_columns()[0].get_type().get_type());
            ASSERT_EQ("NULL", Expression::decode(
                    *schema.get_columns()[0].get_default_value())->eval(ctx(nullptr)).getStr());
        }

        // List tags
        auto listStatus = metaClient->listTagSchemas(spaceId1).get();
        ASSERT_TRUE(listStatus.ok());
        ASSERT_EQ(10, listStatus.value().size());

        // Drop tag
        for (auto i = 5u; i < 10; i++) {
            auto status = metaClient->dropTagSchema(spaceId1,
                    folly::stringPrintf("tag_%d", i), true).get();
            ASSERT_TRUE(status.ok());
        }

        // List tags
        listStatus = metaClient->listTagSchemas(spaceId1).get();
        ASSERT_TRUE(listStatus.ok());
        ASSERT_EQ(5, listStatus.value().size());
    }

    // Test edge
    {
        // Create edge
        for (auto i = 0u; i < 10; i++) {
            meta::cpp2::Schema edgeSchema;
            meta::cpp2::ColumnDef col;
            col.set_name(folly::stringPrintf("col_%d", i));
            col.type.set_type(meta::cpp2::PropertyType::STRING);
            ConstantExpression defaultValue("NULL");
            col.set_default_value(defaultValue.encode());
            std::vector<meta::cpp2::ColumnDef> cols;
            cols.emplace_back(col);
            edgeSchema.set_columns(std::move(cols));
            auto status = metaClient->createEdgeSchema(spaceId1,
                    folly::stringPrintf("edge_%d", i), edgeSchema, false).get();
            ASSERT_TRUE(status.ok());
        }

        // Get edge
        for (auto i = 0u; i < 10; i++) {
            auto status = metaClient->getEdgeSchema(spaceId1,
                    folly::stringPrintf("edge_%d", i)).get();
            ASSERT_TRUE(status.ok());
            auto schema = status.value();
            ASSERT_EQ(1, schema.get_columns().size());
            ASSERT_EQ(folly::stringPrintf("col_%d", i), schema.get_columns()[0].get_name());
            ASSERT_EQ(meta::cpp2::PropertyType::STRING,
                      schema.get_columns()[0].get_type().get_type());
            ASSERT_EQ("NULL", Expression::decode(
                    *schema.get_columns()[0].get_default_value())->eval(ctx(nullptr)).getStr());
        }

        // List edges
        auto listStatus = metaClient->listEdgeSchemas(spaceId1).get();
        ASSERT_TRUE(listStatus.ok());
        ASSERT_EQ(10, listStatus.value().size());

        // Drop edge
        for (auto i = 5u; i < 10; i++) {
            auto status = metaClient->dropEdgeSchema(spaceId1,
                    folly::stringPrintf("edge_%d", i), true).get();
            ASSERT_TRUE(status.ok());
        }

        // List edges
        listStatus = metaClient->listEdgeSchemas(spaceId1).get();
        ASSERT_TRUE(listStatus.ok());
        ASSERT_EQ(5, listStatus.value().size());
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
}

TEST_F(MockServerTest, DISABLED_TestStorage) {
    auto storageClient = gEnv->getStorageClient();
    GraphSpaceID space = 1;
    std::vector<Value> props;
    props.emplace_back("hello");
    storage::cpp2::NewTag tag;
    tag.set_tag_id(3);
    tag.set_props(std::move(props));
    std::vector<storage::cpp2::NewTag> tags;
    tags.emplace_back(tag);

    storage::cpp2::NewVertex vertex;
    vertex.set_id("2020");
    vertex.set_tags(std::move(tags));
    std::vector<storage::cpp2::NewVertex> vertices;
    vertices.emplace_back(std::move(vertex));

    std::unordered_map<TagID, std::vector<std::string>> propNames;
    propNames[3] = {"col"};
    auto resp = storageClient->addVertices(space, vertices, propNames, false).get();
    ASSERT_TRUE(resp.succeeded());
}

}   // namespace graph
}   // namespace nebula
