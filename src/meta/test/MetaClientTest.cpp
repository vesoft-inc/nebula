/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/clients/meta/MetaClient.h"
#include "common/network/NetworkUtils.h"
#include "common/meta/ServerBasedSchemaManager.h"
#include "common/meta/GflagsManager.h"
#include "common/conf/Configuration.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "meta/test/TestUtils.h"
#include "meta/MetaServiceUtils.h"
#include "mock/MockCluster.h"

DECLARE_int32(heartbeat_interval_secs);
DECLARE_string(rocksdb_db_options);


namespace nebula {
namespace meta {

using cpp2::PropertyType;
using nebula::Value;

TEST(MetaClientTest, InterfacesTest) {
    FLAGS_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/MetaClientTest.XXXXXX");

    mock::MockCluster cluster;
    cluster.startMeta(0, rootPath.path());
    cluster.initMetaClient();
    auto* kv = cluster.metaKV_.get();
    auto* client = cluster.metaClient_.get();

    GraphSpaceID spaceId = 0;
    {
        // Add hosts automatically, then testing listHosts interface.
        std::vector<HostAddr> hosts = {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}};
        TestUtils::registerHB(kv, hosts);
        auto ret = client->listHosts().get();
        ASSERT_TRUE(ret.ok());
        for (auto i = 0u; i < hosts.size(); i++) {
            auto tHost = ret.value()[i].hostAddr;
            auto hostAddr = HostAddr(tHost.host, tHost.port);
            ASSERT_EQ(hosts[i], hostAddr);
        }
    }
    {
        // Test createSpace, listSpaces, getPartsAlloc.
        {
            meta::cpp2::SpaceDesc spaceDesc;
            spaceDesc.set_space_name("default_space");
            spaceDesc.set_partition_num(8);
            spaceDesc.set_replica_factor(3);
            spaceDesc.set_charset_name("utf8");
            spaceDesc.set_collate_name("utf8_bin");
            auto ret = client->createSpace(spaceDesc).get();
            ASSERT_TRUE(ret.ok()) << ret.status();
            spaceId = ret.value();

            ret = client->createSpace(spaceDesc, true).get();
            ASSERT_TRUE(ret.ok()) << ret.status();
        }
        {
            meta::cpp2::SpaceDesc spaceDesc;
            spaceDesc.set_space_name("default_space");
            spaceDesc.set_partition_num(8);
            spaceDesc.set_replica_factor(3);
            auto ret = client->createSpace(spaceDesc).get();
            ASSERT_FALSE(ret.ok());
        }
        {
            auto ret = client->getSpace("default_space").get();
            ASSERT_TRUE(ret.ok()) << ret.status();
            meta::cpp2::SpaceDesc spaceDesc = ret.value().get_properties();
            ASSERT_EQ("default_space", spaceDesc.space_name);
            ASSERT_EQ(8, spaceDesc.partition_num);
            ASSERT_EQ(3, spaceDesc.replica_factor);
            ASSERT_EQ("utf8", spaceDesc.charset_name);
            ASSERT_EQ("utf8_bin", spaceDesc.collate_name);
        }
        {
            auto ret = client->listSpaces().get();
            ASSERT_TRUE(ret.ok()) << ret.status();
            ASSERT_EQ(1, ret.value().size());
            ASSERT_EQ(1, ret.value()[0].first);
            ASSERT_EQ("default_space", ret.value()[0].second);
        }
        {
            auto ret = client->getPartsAlloc(spaceId).get();
            ASSERT_TRUE(ret.ok()) << ret.status();
            ASSERT_EQ(8, ret.value().size());
            for (auto it = ret.value().begin(); it != ret.value().end(); it++) {
                ASSERT_EQ(3, it->second.size());
            }
        }
        {
            // Create tag schema
            cpp2::Schema schema;
            for (auto i = 0 ; i < 5; i++) {
                cpp2::ColumnDef column;
                column.name = "tagItem" + std::to_string(i);
                column.type.set_type(PropertyType::STRING);
                schema.columns.emplace_back(std::move(column));
            }
            auto ret = client->createTagSchema(spaceId, "tagName", schema).get();
            ASSERT_TRUE(ret.ok()) << ret.status();

            ret = client->createTagSchema(spaceId, "tagName", schema, true).get();
            ASSERT_TRUE(ret.ok()) << ret.status();
        }
        {
            // Create tag schema with default value
            cpp2::Schema schema;
            for (auto i = 0 ; i < 5; i++) {
                cpp2::ColumnDef column;
                column.name = "tagItem" + std::to_string(i);
                column.type.set_type(PropertyType::STRING);
                nebula::Value defaultValue;
                defaultValue.setStr(std::to_string(i));
                column.default_value = defaultValue;
                schema.columns.emplace_back(std::move(column));
            }
            auto ret = client->createTagSchema(spaceId, "tagWithDefault", schema).get();
            ASSERT_TRUE(ret.ok()) << ret.status();
        }
        {
            // Create edge schema
            cpp2::Schema schema;
            for (auto i = 0 ; i < 5; i++) {
                cpp2::ColumnDef column;
                column.name = "edgeItem" + std::to_string(i);
                column.type.set_type(PropertyType::STRING);
                schema.columns.emplace_back(std::move(column));
            }
            auto ret = client->createEdgeSchema(spaceId, "edgeName", schema).get();
            ASSERT_TRUE(ret.ok()) << ret.status();
            ret = client->createEdgeSchema(spaceId, "edgeName", schema, true).get();
            ASSERT_TRUE(ret.ok()) << ret.status();
        }
        {
            // Create edge schema with default value
            cpp2::Schema schema;
            for (auto i = 0 ; i < 5; i++) {
                cpp2::ColumnDef column;
                column.name = "edgeItem" + std::to_string(i);
                column.type.set_type(PropertyType::STRING);
                nebula::Value defaultValue;
                defaultValue.setStr(std::to_string(i));
                column.default_value = defaultValue;
                schema.columns.emplace_back(std::move(column));
            }
            auto ret = client->createEdgeSchema(spaceId, "edgeWithDefault", schema).get();
            ASSERT_TRUE(ret.ok()) << ret.status();
        }

        auto schemaMan = std::make_unique<ServerBasedSchemaManager>();
        schemaMan->init(client);
        {
            // listTagSchemas
            auto ret1 = client->listTagSchemas(spaceId).get();
            ASSERT_TRUE(ret1.ok()) << ret1.status();
            ASSERT_EQ(ret1.value().size(), 2);
            ASSERT_NE(ret1.value().begin()->tag_id, 0);
            ASSERT_EQ(ret1.value().begin()->schema.columns.size(), 5);

            // getTagSchemaFromCache
            sleep(FLAGS_heartbeat_interval_secs + 1);
            auto ret = client->getLatestTagVersionFromCache(spaceId,
                                                            ret1.value().begin()->tag_id);
            CHECK(ret.ok());
            auto ver = ret.value();
            auto ret2 = client->getTagSchemaFromCache(spaceId,
                                                      ret1.value().begin()->tag_id, ver);
            ASSERT_TRUE(ret2.ok()) << ret2.status();
            ASSERT_EQ(ret2.value()->getNumFields(), 5);

            // ServerBasedSchemaManager test
            auto status = schemaMan->toTagID(spaceId, "tagName");
            ASSERT_TRUE(status.ok());
            auto tagId = status.value();
            ASSERT_NE(-1, tagId);
            auto outSchema = schemaMan->getTagSchema(spaceId, tagId);
            ASSERT_EQ(5, outSchema->getNumFields());
            ASSERT_STREQ("tagItem0", outSchema->getFieldName(0));
            ASSERT_EQ(nullptr, outSchema->getFieldName(-1));
            ASSERT_EQ(nullptr, outSchema->getFieldName(5));
            ASSERT_EQ(nullptr, outSchema->getFieldName(6));
            auto retVer = schemaMan->getLatestTagSchemaVersion(spaceId, tagId);
            ASSERT_TRUE(retVer.ok());
            auto version = retVer.value();
            ASSERT_EQ(0, version);
            auto outSchema1 = schemaMan->getTagSchema(spaceId, tagId, version);
            ASSERT_TRUE(outSchema1 != nullptr);
            ASSERT_EQ(5, outSchema1->getNumFields());
            ASSERT_EQ(nullptr, outSchema1->getFieldName(-1));
            ASSERT_EQ(nullptr, outSchema1->getFieldName(5));
            ASSERT_EQ(nullptr, outSchema1->getFieldName(6));
            ASSERT_STREQ("tagItem0", outSchema1->getFieldName(0));
        }
        {
            // listEdgeSchemas
            auto ret1 = client->listEdgeSchemas(spaceId).get();
            ASSERT_TRUE(ret1.ok()) << ret1.status();
            ASSERT_EQ(ret1.value().size(), 2);
            ASSERT_NE(ret1.value().begin()->edge_type, 0);

            // getEdgeSchemaFromCache
            auto retVer = client->getLatestEdgeVersionFromCache(spaceId,
                                                                ret1.value().begin()->edge_type);
            CHECK(retVer.ok());
            auto ver = retVer.value();
            auto ret2 = client->getEdgeSchemaFromCache(spaceId,
                                                       ret1.value().begin()->edge_type, ver);
            ASSERT_TRUE(ret2.ok()) << ret2.status();
            ASSERT_EQ(ret2.value()->getNumFields(), 5);

            // ServerBasedSchemaManager test
            auto status = schemaMan->toEdgeType(spaceId, "edgeName");
            auto edgeType = status.value();
            ASSERT_NE(-1, edgeType);
            auto outSchema = schemaMan->getEdgeSchema(spaceId, edgeType);
            ASSERT_EQ(5, outSchema->getNumFields());
            ASSERT_STREQ("edgeItem0", outSchema->getFieldName(0));
            ASSERT_EQ(nullptr, outSchema->getFieldName(-1));
            ASSERT_EQ(nullptr, outSchema->getFieldName(5));
            ASSERT_EQ(nullptr, outSchema->getFieldName(6));
            auto versionRet = schemaMan->getLatestEdgeSchemaVersion(spaceId, edgeType);
            ASSERT_TRUE(versionRet.ok());
            auto version = versionRet.value();
            ASSERT_EQ(0, version);
            auto outSchema1 = schemaMan->getEdgeSchema(spaceId, edgeType, version);
            ASSERT_TRUE(outSchema1 != nullptr);
            ASSERT_EQ(5, outSchema1->getNumFields());
            ASSERT_EQ(nullptr, outSchema1->getFieldName(-1));
            ASSERT_EQ(nullptr, outSchema1->getFieldName(5));
            ASSERT_EQ(nullptr, outSchema1->getFieldName(6));
            ASSERT_STREQ("edgeItem0", outSchema1->getFieldName(0));
        }
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        // Test cache interfaces
        auto partsMap = client->getPartsMapFromCache(HostAddr("0", 0));
        ASSERT_EQ(1, partsMap.size());
        ASSERT_EQ(6, partsMap[spaceId].size());
    }
    {
        auto metaStatus = client->getPartHostsFromCache(spaceId, 1);
        ASSERT_TRUE(metaStatus.ok());
        auto partMeta = metaStatus.value();
        ASSERT_EQ(3, partMeta.hosts_.size());
        for (auto& h : partMeta.hosts_) {
            ASSERT_EQ(h.host, std::to_string(h.port));
        }
    }
    {
        auto ret = client->getSpaceIdByNameFromCache("default_space");
        ASSERT_TRUE(ret.ok()) << ret.status();
        ASSERT_EQ(1, ret.value());
    }
    {
        auto ret = client->getSpaceIdByNameFromCache("default_space_1");
        ASSERT_FALSE(ret.ok());
        ASSERT_EQ(Status::SpaceNotFound(), ret.status());
    }
    {
        // Multi Put Test
        std::vector<std::pair<std::string, std::string>> pairs;
        for (auto i = 0; i < 10; i++) {
            pairs.emplace_back(folly::stringPrintf("key_%d", i),
                               folly::stringPrintf("value_%d", i));
        }
        auto ret = client->multiPut("test", pairs).get();
        ASSERT_TRUE(ret.ok());
    }
    {
        // Get Test
        auto ret = client->get("test", "key_0").get();
        ASSERT_TRUE(ret.ok());
        ASSERT_EQ("value_0", ret.value());

        auto missedRet = client->get("test", "missed_key").get();
        ASSERT_FALSE(missedRet.ok());

        auto emptyRet = client->get("test", "").get();
        ASSERT_FALSE(emptyRet.ok());
    }
    {
        // Multi Get Test
        std::vector<std::string> keys;
        for (auto i = 0; i < 2; i++) {
            keys.emplace_back(folly::stringPrintf("key_%d", i));
        }
        auto ret = client->multiGet("test", keys).get();
        ASSERT_TRUE(ret.ok());
        ASSERT_EQ(2, ret.value().size());
        ASSERT_EQ("value_0", ret.value()[0]);
        ASSERT_EQ("value_1", ret.value()[1]);

        std::vector<std::string> emptyKeys;
        auto emptyRet = client->multiGet("test", emptyKeys).get();
        ASSERT_FALSE(emptyRet.ok());
    }
    {
        // Scan Test
        auto ret = client->scan("test", "key_0", "key_3").get();
        ASSERT_TRUE(ret.ok());
        ASSERT_EQ(3, ret.value().size());
        ASSERT_EQ("value_0", ret.value()[0]);
        ASSERT_EQ("value_1", ret.value()[1]);
        ASSERT_EQ("value_2", ret.value()[2]);
    }
    {
        // Remove Test
        auto ret = client->remove("test", "key_9").get();
        ASSERT_TRUE(ret.ok());
    }
    {
        // Remove Range Test
        auto ret = client->removeRange("test", "key_0", "key_4").get();
        ASSERT_TRUE(ret.ok());
    }
    {
        auto ret = client->remove("_test_", "key_8").get();
        ASSERT_FALSE(ret.ok());
    }
    {
        auto ret = client->dropSpace("default_space").get();
        ASSERT_TRUE(ret.ok());
        auto ret1 = client->listSpaces().get();
        ASSERT_TRUE(ret1.ok()) << ret1.status();
        ASSERT_EQ(0, ret1.value().size());
    }
}

TEST(MetaClientTest, TagTest) {
    FLAGS_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/MetaClientTagTest.XXXXXX");

    mock::MockCluster cluster;
    cluster.startMeta(0, rootPath.path());
    uint32_t localMetaPort = cluster.metaServer_->port_;
    auto* kv = cluster.metaKV_.get();
    auto localIp = cluster.localIP();

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    auto localhosts = std::vector<HostAddr>{HostAddr(localIp, localMetaPort)};
    auto client = std::make_shared<MetaClient>(threadPool, localhosts);
    client->waitForMetadReady();

    std::vector<HostAddr> hosts = {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}};
    TestUtils::registerHB(kv, hosts);
    meta::cpp2::SpaceDesc spaceDesc;
    spaceDesc.set_space_name("default");
    spaceDesc.set_partition_num(9);
    spaceDesc.set_replica_factor(3);
    auto ret = client->createSpace(spaceDesc).get();
    ASSERT_TRUE(ret.ok()) << ret.status();
    GraphSpaceID spaceId = ret.value();
    TagID id;
    int64_t version;

    {
        std::vector<cpp2::ColumnDef> columns;
        Value defaultValue;
        defaultValue.setInt(0);
        columns.emplace_back();
        columns.back().set_name("column_i");
        columns.back().set_default_value(defaultValue);
        columns.back().type.set_type(PropertyType::INT64);

        defaultValue.setFloat(3.14);
        columns.emplace_back();
        columns.back().set_default_value(defaultValue);
        columns.back().set_name("column_d");
        columns.back().type.set_type(PropertyType::DOUBLE);

        defaultValue.setStr("test");
        columns.emplace_back();
        columns.back().set_default_value(defaultValue);
        columns.back().set_name("column_s");
        columns.back().type.set_type(PropertyType::STRING);

        cpp2::Schema schema;
        schema.set_columns(std::move(columns));
        auto result = client->createTagSchema(spaceId, "test_tag", std::move(schema)).get();
        ASSERT_TRUE(result.ok());
        id = result.value();
    }
    {
        std::vector<cpp2::ColumnDef> columns;
        cpp2::ColumnDef intColumn;
        intColumn.set_name("column_i");
        intColumn.type.set_type(PropertyType::INT64);
        nebula::Value intValue;
        intValue.setInt(0);
        intColumn.set_default_value(intValue);
        columns.emplace_back(std::move(intColumn));

        cpp2::ColumnDef doubleColumn;
        doubleColumn.set_name("column_d");
        doubleColumn.type.set_type(PropertyType::STRING);
        nebula::Value doubleValue;
        doubleValue.setFloat(3.14);
        doubleColumn.set_default_value(doubleValue);
        columns.emplace_back(std::move(doubleColumn));

        cpp2::Schema schema;
        schema.set_columns(columns);

        auto result = client->createTagSchema(spaceId, "test_tag_type_mismatch", schema).get();
        ASSERT_FALSE(result.ok());
    }
    {
        auto result = client->listTagSchemas(spaceId).get();
        ASSERT_TRUE(result.ok());
        auto tags = result.value();
        ASSERT_EQ(1, tags.size());
        ASSERT_EQ(id, tags[0].get_tag_id());
        ASSERT_EQ("test_tag", tags[0].get_tag_name());
        version = tags[0].get_version();
    }
    {
        auto result1 = client->getTagSchema(spaceId, "test_tag", version).get();
        ASSERT_TRUE(result1.ok());
        auto result2 = client->getTagSchema(spaceId, "test_tag").get();
        ASSERT_TRUE(result2.ok());
        ASSERT_EQ(3, result2.value().columns.size());
        ASSERT_EQ(result1.value().columns.size(), result2.value().columns.size());
        for (auto i = 0u; i < result1.value().columns.size(); i++) {
            ASSERT_EQ(result1.value().columns[i].name, result2.value().columns[i].name);
            ASSERT_EQ(result1.value().columns[i].type, result2.value().columns[i].type);
        }
    }
    // Get wrong version
    {
        auto result = client->getTagSchema(spaceId, "test_tag", 100).get();
        ASSERT_FALSE(result.ok());
    }
    {
        auto result = client->dropTagSchema(spaceId, "test_tag").get();
        ASSERT_TRUE(result.ok());
    }
    {
        auto result = client->getTagSchema(spaceId, "test_tag", version).get();
        ASSERT_FALSE(result.ok());
    }
}

TEST(MetaClientTest, EdgeTest) {
    FLAGS_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/MetaClientEdgeTest.XXXXXX");

    mock::MockCluster cluster;
    cluster.startMeta(0, rootPath.path());
    cluster.initMetaClient();
    auto* kv = cluster.metaKV_.get();
    auto* client = cluster.metaClient_.get();

    std::vector<HostAddr> hosts = {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}};
    TestUtils::registerHB(kv, hosts);
    meta::cpp2::SpaceDesc spaceDesc;
    spaceDesc.set_space_name("default_space");
    spaceDesc.set_partition_num(9);
    spaceDesc.set_replica_factor(3);
    auto ret = client->createSpace(spaceDesc).get();
    ASSERT_TRUE(ret.ok()) << ret.status();
    GraphSpaceID space = ret.value();
    SchemaVer version;

    std::vector<cpp2::ColumnDef> expectedColumns;
    {
        std::vector<cpp2::ColumnDef> columns;

        cpp2::ColumnDef intColumn;
        intColumn.set_name("column_i");
        intColumn.type.set_type(PropertyType::INT64);
        nebula::Value intValue;
        intValue.setInt(0);
        intColumn.set_default_value(intValue);
        columns.emplace_back(std::move(intColumn));

        cpp2::ColumnDef doubleColumn;
        doubleColumn.set_name("column_d");
        doubleColumn.type.set_type(PropertyType::DOUBLE);
        nebula::Value doubleValue;
        doubleValue.setFloat(3.14);
        doubleColumn.set_default_value(doubleValue);
        columns.emplace_back(std::move(doubleColumn));

        cpp2::ColumnDef stringColumn;
        stringColumn.set_name("column_s");
        stringColumn.type.set_type(PropertyType::STRING);
        nebula::Value stringValue;
        stringValue.setStr("test");
        stringColumn.set_default_value(stringValue);
        columns.emplace_back(std::move(stringColumn));
        expectedColumns = columns;

        cpp2::Schema schema;
        schema.set_columns(std::move(columns));
        auto result = client->createEdgeSchema(space, "test_edge", schema).get();
        ASSERT_TRUE(result.ok());
    }
    {
        std::vector<cpp2::ColumnDef> columns;
        cpp2::ColumnDef intColumn;
        intColumn.set_name("column_i");
        intColumn.type.set_type(PropertyType::INT64);
        nebula::Value intValue;
        intValue.setInt(0);
        intColumn.set_default_value(intValue);
        columns.emplace_back(std::move(intColumn));

        cpp2::ColumnDef doubleColumn;
        doubleColumn.set_name("column_d");
        doubleColumn.type.set_type(PropertyType::STRING);
        nebula::Value doubleValue;
        doubleValue.setFloat(3.14);
        doubleColumn.set_default_value(doubleValue);
        columns.emplace_back(std::move(doubleColumn));
        cpp2::Schema schema;
        schema.set_columns(columns);

        auto result = client->createEdgeSchema(space, "test_edge_type_mismatch", schema).get();
        ASSERT_FALSE(result.ok());
    }
    {
        auto result = client->listEdgeSchemas(space).get();
        ASSERT_TRUE(result.ok());
        auto edges = result.value();
        ASSERT_EQ(1, edges.size());
        ASSERT_EQ(2, edges[0].get_edge_type());
        ASSERT_EQ("test_edge", edges[0].get_edge_name());
        version = edges[0].get_version();

        cpp2::Schema expected;
        expected.set_columns(std::move(expectedColumns));
        cpp2::Schema resultSchema = edges[0].get_schema();
        ASSERT_TRUE(TestUtils::verifySchema(resultSchema, expected));
    }
    {
        auto result1 = client->getEdgeSchema(space, "test_edge", version).get();
        ASSERT_TRUE(result1.ok());
        auto result2 = client->getEdgeSchema(space, "test_edge").get();
        ASSERT_TRUE(result2.ok());
        ASSERT_EQ(result1.value().columns.size(), result2.value().columns.size());
        for (auto i = 0u; i < result1.value().columns.size(); i++) {
            ASSERT_EQ(result1.value().columns[i].name, result2.value().columns[i].name);
            ASSERT_EQ(result1.value().columns[i].type, result2.value().columns[i].type);
        }
    }
    {
        auto result = client->dropEdgeSchema(space, "test_edge").get();
        ASSERT_TRUE(result.ok());
    }
    {
        auto result = client->getEdgeSchema(space, "test_edge", version).get();
        ASSERT_FALSE(result.ok());
    }
}

TEST(MetaClientTest, TagIndexTest) {
    FLAGS_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/MetaClientTagIndexTest.XXXXXX");

    mock::MockCluster cluster;
    cluster.startMeta(0, rootPath.path());
    cluster.initMetaClient();
    auto* kv = cluster.metaKV_.get();
    auto* client = cluster.metaClient_.get();

    std::vector<HostAddr> hosts = {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}};
    TestUtils::registerHB(kv, hosts);
    meta::cpp2::SpaceDesc spaceDesc;
    spaceDesc.set_space_name("default_space");
    spaceDesc.set_partition_num(8);
    spaceDesc.set_replica_factor(3);
    auto ret = client->createSpace(spaceDesc).get();
    ASSERT_TRUE(ret.ok()) << ret.status();
    GraphSpaceID space = ret.value();
    IndexID singleFieldIndexID;
    IndexID multiFieldIndexID;
    {
        for (auto i = 0; i < 2; i++) {
            std::vector<cpp2::ColumnDef> columns;
            cpp2::ColumnDef column0;
            column0.set_name(folly::stringPrintf("tag_%d_col_0", i));
            column0.type.set_type(PropertyType::INT64);
            columns.emplace_back(std::move(column0));

            cpp2::ColumnDef column1;
            column1.set_name(folly::stringPrintf("tag_%d_col_1", i));
            column1.type.set_type(PropertyType::STRING);
            columns.emplace_back(std::move(column1));

            cpp2::Schema schema;
            schema.set_columns(std::move(columns));
            auto result = client->createTagSchema(space, folly::stringPrintf("tag_%d", i),
                                                  schema).get();
            ASSERT_TRUE(result.ok());
        }
    }
    {
        std::vector<std::string>&& fields {"tag_0_col_0"};
        auto result = client->createTagIndex(space,
                                             "tag_single_field_index",
                                             "tag_0",
                                             std::move(fields)).get();
        ASSERT_TRUE(result.ok());
        singleFieldIndexID = result.value();
    }
    {
        std::vector<std::string>&& fields {"tag_0_col_0",  "tag_0_col_1"};
        auto result = client->createTagIndex(space,
                                             "tag_multi_field_index",
                                             "tag_0",
                                             std::move(fields)).get();
        ASSERT_TRUE(result.ok());
        multiFieldIndexID = result.value();
    }
    {
        std::vector<std::string>&& fields {"tag_0_col_0",  "not_exist_field"};
        auto result = client->createTagIndex(space,
                                             "tag_field_not_exist_index",
                                             "tag_0",
                                             std::move(fields)).get();
        ASSERT_FALSE(result.ok());
        ASSERT_EQ(Status::Error("not existed!"), result.status());
    }
    {
        std::vector<std::string>&& fields {"tag_0_col_0",  "tag_0_col_1"};
        auto result = client->createTagIndex(space,
                                             "tag_not_exist_index",
                                             "tag_not_exist",
                                             std::move(fields)).get();
        ASSERT_FALSE(result.ok());
        ASSERT_EQ(Status::Error("not existed!"), result.status());
    }
    {
        std::vector<std::string>&& fields {"tag_0_col_0",  "tag_0_col_0"};
        auto result = client->createTagIndex(space,
                                             "tag_conflict_index",
                                             "tag_0",
                                             std::move(fields)).get();
        ASSERT_FALSE(result.ok());
        ASSERT_EQ(Status::Error("conflict"), result.status());
    }
    {
        auto result = client->listTagIndexes(space).get();
        auto values = result.value();
        ASSERT_EQ(2, values.size());

        {
            cpp2::ColumnDef singleColumn;
            singleColumn.set_name("tag_0_col_0");
            singleColumn.type.set_type(PropertyType::INT64);
            std::vector<cpp2::ColumnDef> columns;
            columns.emplace_back(std::move(singleColumn));
            auto singleFieldResult = values[0].get_fields();
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
            stringColumn.type.set_type(PropertyType::STRING);
            columns.emplace_back(std::move(stringColumn));

            auto multiFieldResult = values[1].get_fields();
            ASSERT_TRUE(TestUtils::verifyResult(columns, multiFieldResult));
        }
    }
    sleep(FLAGS_heartbeat_interval_secs * 5);
    // Test Tag Index spaceDesc Cache
    {
        auto tagSingleFieldResult = client->getTagIndexFromCache(space, singleFieldIndexID);
        ASSERT_TRUE(tagSingleFieldResult.ok());
        auto tagMultiFieldResult = client->getTagIndexFromCache(space, multiFieldIndexID);
        ASSERT_TRUE(tagMultiFieldResult.ok());
    }
    {
        GraphSpaceID spaceNotExist = 99;
        IndexID tagIndexNotExist = 99;
        auto checkTagIndexNotExist = client->checkTagIndexed(space, tagIndexNotExist);
        ASSERT_EQ(Status::IndexNotFound(), checkTagIndexNotExist);

        auto checkSpaceNotExist = client->checkTagIndexed(spaceNotExist, singleFieldIndexID);
        ASSERT_EQ(Status::SpaceNotFound(), checkSpaceNotExist);

        auto spaceNotExistRet = client->getTagIndexFromCache(spaceNotExist, singleFieldIndexID);
        ASSERT_FALSE(spaceNotExistRet.ok());
    }
    {
        auto tagIndexes = client->getTagIndexesFromCache(space);
        ASSERT_TRUE(tagIndexes.ok());
        ASSERT_EQ(2, tagIndexes.value().size());
    }
    {
        GraphSpaceID spaceNotExist = 99;
        auto spaceNotExistRet = client->getTagIndexesFromCache(spaceNotExist);
        ASSERT_FALSE(spaceNotExistRet.ok());
    }
    {
        auto result = client->dropTagIndex(space, "tag_single_field_index").get();
        ASSERT_TRUE(result.ok());
    }
    {
        auto result = client->getTagIndex(space, "tag_single_field_index").get();
        ASSERT_FALSE(result.ok());
        ASSERT_EQ(Status::Error("not existed!"), result.status());
    }
    {
        auto result = client->dropTagIndex(space, "tag_single_field_index").get();
        ASSERT_FALSE(result.ok());
        ASSERT_EQ(Status::Error("not existed!"), result.status());
    }
}

TEST(MetaClientTest, EdgeIndexTest) {
    FLAGS_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/MetaClientEdgeIndexTest.XXXXXX");

    mock::MockCluster cluster;
    cluster.startMeta(0, rootPath.path());
    cluster.initMetaClient();
    auto* kv = cluster.metaKV_.get();
    auto* client = cluster.metaClient_.get();

    std::vector<HostAddr> hosts = {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}};
    TestUtils::registerHB(kv, hosts);
    meta::cpp2::SpaceDesc spaceDesc;
    spaceDesc.set_space_name("default_space");
    spaceDesc.set_partition_num(8);
    spaceDesc.set_replica_factor(3);
    auto ret = client->createSpace(spaceDesc).get();
    GraphSpaceID space = ret.value();
    IndexID singleFieldIndexID;
    IndexID multiFieldIndexID;
    {
        for (auto i = 0; i < 2; i++) {
            std::vector<cpp2::ColumnDef> columns;
            cpp2::ColumnDef column0;
            column0.set_name(folly::stringPrintf("edge_%d_col_0", i));
            column0.type.set_type(PropertyType::INT64);
            columns.emplace_back(std::move(column0));

            cpp2::ColumnDef column1;
            column1.set_name(folly::stringPrintf("edge_%d_col_1", i));
            column1.type.set_type(PropertyType::STRING);
            columns.emplace_back(std::move(column1));

            cpp2::Schema schema;
            schema.set_columns(std::move(columns));
            auto result = client->createEdgeSchema(space,
                                                   folly::stringPrintf("edge_%d", i),
                                                   schema).get();
            ASSERT_TRUE(result.ok());
        }
    }
    {
        std::vector<std::string>&& fields {"edge_0_col_0"};
        auto result = client->createEdgeIndex(space,
                                              "edge_single_field_index",
                                              "edge_0",
                                              std::move(fields)).get();
        ASSERT_TRUE(result.ok());
        singleFieldIndexID = result.value();
    }
    {
        std::vector<std::string>&& fields {"edge_0_col_0",  "edge_0_col_1"};
        auto result = client->createEdgeIndex(space,
                                              "edge_multi_field_index",
                                              "edge_0",
                                              std::move(fields)).get();
        ASSERT_TRUE(result.ok());
        multiFieldIndexID = result.value();
    }
    {
        std::vector<std::string>&& fields {"edge_0_col_0",  "edge_0_col_1"};
        auto result = client->createEdgeIndex(space,
                                              "edge_not_exist_index",
                                              "edge_not_exist",
                                              std::move(fields)).get();
        ASSERT_FALSE(result.ok());
        ASSERT_EQ(Status::Error("not existed!"), result.status());
    }
    {
        std::vector<std::string>&& fields {"edge_0_col_0",  "edge_0_col_0"};
        auto result = client->createEdgeIndex(space,
                                              "edge_conflict_index",
                                              "edge_0",
                                              std::move(fields)).get();
        ASSERT_FALSE(result.ok());
        ASSERT_EQ(Status::Error("conflict"), result.status());
    }
    {
        std::vector<std::string>&& fields {"edge_0_col_0",  "not_exist_field"};
        auto result = client->createEdgeIndex(space,
                                              "edge_field_not_exist_index",
                                              "edge_0",
                                              std::move(fields)).get();
        ASSERT_FALSE(result.ok());
        ASSERT_EQ(Status::Error("not existed!"), result.status());
    }
    {
        auto result = client->listEdgeIndexes(space).get();
        auto values = result.value();
        ASSERT_EQ(2, values.size());

        {
            cpp2::ColumnDef column;
            column.set_name("edge_0_col_0");
            column.type.set_type(PropertyType::INT64);
            std::vector<cpp2::ColumnDef> columns;
            columns.emplace_back(std::move(column));

            auto singleFieldResult = values[0].get_fields();
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
            stringColumn.type.set_type(PropertyType::STRING);
            columns.emplace_back(std::move(stringColumn));
            auto multiFieldResult = values[1].get_fields();
            ASSERT_TRUE(TestUtils::verifyResult(columns, multiFieldResult));
        }
    }
    sleep(FLAGS_heartbeat_interval_secs * 5);
    // Test Edge Index spaceDesc Cache
    {
        auto checkEdgeIndexed = client->checkEdgeIndexed(space, singleFieldIndexID);
        ASSERT_EQ(Status::OK(), checkEdgeIndexed);

        auto edgeSingleFieldResult = client->getEdgeIndexFromCache(space, singleFieldIndexID);
        ASSERT_TRUE(edgeSingleFieldResult.ok());
        auto edgeMultiFieldResult = client->getEdgeIndexFromCache(space, multiFieldIndexID);
        ASSERT_TRUE(edgeMultiFieldResult.ok());
    }
    {
        GraphSpaceID spaceNotExist = 99;
        IndexID edgeIndexNotExist = 99;
        auto checkEdgeIndexNotExist = client->checkEdgeIndexed(space, edgeIndexNotExist);
        ASSERT_EQ(Status::IndexNotFound(), checkEdgeIndexNotExist);

        auto checkSpaceNotExist = client->checkEdgeIndexed(spaceNotExist, singleFieldIndexID);
        ASSERT_EQ(Status::SpaceNotFound(), checkSpaceNotExist);

        auto spaceNotExistRet = client->getEdgeIndexFromCache(spaceNotExist, singleFieldIndexID);
        ASSERT_FALSE(spaceNotExistRet.ok());
    }
    {
        auto edgeIndexes = client->getEdgeIndexesFromCache(space);
        ASSERT_TRUE(edgeIndexes.ok());
        ASSERT_EQ(2, edgeIndexes.value().size());
    }
    {
        GraphSpaceID spaceNotExist = 99;
        auto spaceNotExistRet = client->getEdgeIndexesFromCache(spaceNotExist);
        ASSERT_FALSE(spaceNotExistRet.ok());
    }
    {
        auto result = client->dropEdgeIndex(space, "edge_single_field_index").get();
        ASSERT_TRUE(result.ok());
    }
    {
        auto result = client->getEdgeIndex(space, "edge_single_field_index").get();
        ASSERT_FALSE(result.ok());
        ASSERT_EQ(Status::Error("not existed!"), result.status());
    }
    {
        auto result = client->dropEdgeIndex(space, "edge_single_field_index").get();
        ASSERT_FALSE(result.ok());
        ASSERT_EQ(Status::Error("not existed!"), result.status());
    }
}

class TestListener : public MetaChangedListener {
public:
    virtual ~TestListener() = default;
    void onSpaceAdded(GraphSpaceID spaceId) override {
        LOG(INFO) << "Space " << spaceId << " added";
        spaceNum++;
    }

    void onSpaceRemoved(GraphSpaceID spaceId) override {
        LOG(INFO) << "Space " << spaceId << " removed";
        spaceNum--;
    }

    void onPartAdded(const PartHosts& partMeta) override {
        LOG(INFO) << "[" << partMeta.spaceId_ << ", " << partMeta.partId_ << "] added!";
        partNum++;
    }

    void onSpaceOptionUpdated(GraphSpaceID spaceId,
                              const std::unordered_map<std::string, std::string>& update)
                              override {
        UNUSED(spaceId);
        for (const auto& kv : update) {
            options[kv.first] = kv.second;
        }
    }

    void onPartRemoved(GraphSpaceID spaceId, PartitionID partId) override {
        LOG(INFO) << "[" << spaceId << ", " << partId << "] removed!";
        partNum--;
    }

    void onPartUpdated(const PartHosts& partMeta) override {
        LOG(INFO) << "[" << partMeta.spaceId_ << ", " << partMeta.partId_ << "] updated!";
        partChanged++;
    }

    void fetchLeaderInfo(std::unordered_map<GraphSpaceID,
                                            std::vector<PartitionID>>& leaderIds) override {
        LOG(INFO) << "Get leader distribution!";
        UNUSED(leaderIds);
    }

    HostAddr getLocalHost() {
        return HostAddr("0", 0);
    }

    int32_t spaceNum = 0;
    int32_t partNum = 0;
    int32_t partChanged = 0;
    std::unordered_map<std::string, std::string> options;
};

TEST(MetaClientTest, DiffTest) {
    FLAGS_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/MetaClientTest.XXXXXX");

    mock::MockCluster cluster;
    cluster.startMeta(0, rootPath.path());
    meta::MetaClientOptions options;
    options.role_ = meta::cpp2::HostRole::STORAGE;
    cluster.initMetaClient(options);
    auto* kv = cluster.metaKV_.get();
    auto* client = cluster.metaClient_.get();

    auto listener = std::make_unique<TestListener>();
    client->registerListener(listener.get());
    {
        // Add hosts automatically, then testing listHosts interface.
        std::vector<HostAddr> hosts = {{"", 0}};
        TestUtils::registerHB(kv, hosts);
        auto ret = client->listHosts().get();
        ASSERT_TRUE(ret.ok());
        for (auto i = 0u; i < hosts.size(); i++) {
            auto tHost = ret.value()[i].hostAddr;
            auto hostAddr = HostAddr(tHost.host, tHost.port);
            ASSERT_EQ(hosts[i], hostAddr);
        }
    }
    {
        // Test Create Space and List Spaces
        meta::cpp2::SpaceDesc spaceDesc;
        spaceDesc.set_space_name("default_space");
        spaceDesc.set_partition_num(9);
        spaceDesc.set_replica_factor(1);
        auto ret = client->createSpace(spaceDesc).get();
        ASSERT_TRUE(ret.ok()) << ret.status();
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    ASSERT_EQ(1, listener->spaceNum);
    ASSERT_EQ(9, listener->partNum);
    {
        meta::cpp2::SpaceDesc spaceDesc;
        spaceDesc.set_space_name("default_space_1");
        spaceDesc.set_partition_num(5);
        spaceDesc.set_replica_factor(1);
        auto ret = client->createSpace(spaceDesc).get();
        ASSERT_TRUE(ret.ok()) << ret.status();
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    ASSERT_EQ(2, listener->spaceNum);
    ASSERT_EQ(14, listener->partNum);
    {
        // Test Drop Space
        auto ret = client->dropSpace("default_space_1").get();
        ASSERT_TRUE(ret.ok()) << ret.status();
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    ASSERT_EQ(1, listener->spaceNum);
    ASSERT_EQ(9, listener->partNum);
    client->unRegisterListener();
}

TEST(MetaClientTest, HeartbeatTest) {
    FLAGS_heartbeat_interval_secs = 1;
    const nebula::ClusterID kClusterId = 10;
    fs::TempDir rootPath("/tmp/MetaClientTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.startMeta(0, rootPath.path());

    meta::MetaClientOptions options;
    HostAddr localHost(cluster.localIP(), network::NetworkUtils::getAvailablePort());
    options.localHost_ = localHost;
    options.clusterId_ = kClusterId;
    options.role_ = meta::cpp2::HostRole::STORAGE;
    cluster.initMetaClient(std::move(options));
    auto* client = cluster.metaClient_.get();

    auto listener = std::make_unique<TestListener>();
    client->registerListener(listener.get());
    {
        // Add hosts automatically, then testing listHosts interface.
        std::vector<HostAddr> hosts = {localHost};
        auto ret = client->listHosts().get();
        ASSERT_TRUE(ret.ok());
        for (auto i = 0u; i < hosts.size(); i++) {
            auto tHost = ret.value()[i].hostAddr;
            auto hostAddr = HostAddr(tHost.host, tHost.port);
            ASSERT_EQ(hosts[i], hostAddr);
        }
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    ASSERT_EQ(1, ActiveHostsMan::getActiveHosts(cluster.metaKV_.get()).size());
    client->unRegisterListener();
}


class TestMetaService : public cpp2::MetaServiceSvIf {
public:
    folly::Future<cpp2::HBResp>
    future_heartBeat(const cpp2::HBReq& req) override {
        UNUSED(req);
        folly::Promise<cpp2::HBResp> pro;
        auto f = pro.getFuture();
        cpp2::HBResp resp;
        resp.set_code(cpp2::ErrorCode::SUCCEEDED);
        pro.setValue(std::move(resp));
        return f;
    }
};

class TestMetaServiceRetry : public cpp2::MetaServiceSvIf {
public:
    void setLeader(HostAddr leader) {
        leader_ = leader;
    }

    void setAddr(HostAddr addr) {
        addr_ = addr;
    }

    folly::Future<cpp2::HBResp>
    future_heartBeat(const cpp2::HBReq& req) override {
        UNUSED(req);
        folly::Promise<cpp2::HBResp> pro;
        auto f = pro.getFuture();
        cpp2::HBResp resp;
        if (addr_ == leader_) {
            resp.set_code(cpp2::ErrorCode::SUCCEEDED);
        } else {
            resp.set_code(cpp2::ErrorCode::E_LEADER_CHANGED);
            resp.set_leader(leader_);
        }
        pro.setValue(std::move(resp));
        return f;
    }

private:
    HostAddr leader_;
    HostAddr addr_;
};

TEST(MetaClientTest, SimpleTest) {
    FLAGS_heartbeat_interval_secs = 3600;
    auto localIp = network::NetworkUtils::getHostname();


    auto mockMetad = std::make_unique<mock::RpcServer>();
    auto handler = std::make_shared<TestMetaService>();
    mockMetad->start("meta", 0, handler);

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    auto clientPort = network::NetworkUtils::getAvailablePort();
    HostAddr localHost{localIp, clientPort};
    auto client = std::make_shared<MetaClient>(
                                            threadPool,
                                            std::vector<HostAddr>{
                                                HostAddr(localIp, mockMetad->port_)});
    {
        LOG(INFO) << "Test heart beat...";
        folly::Baton<true, std::atomic> baton;
        client->heartbeat().thenValue([&baton] (auto&& status) {
            ASSERT_TRUE(status.ok());
            baton.post();
        });
        baton.wait();
    }
}

TEST(MetaClientTest, RetryWithExceptionTest) {
    FLAGS_heartbeat_interval_secs = 3600;
    auto localIp("127.0.0.1");

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    auto clientPort = network::NetworkUtils::getAvailablePort();
    HostAddr localHost{localIp, clientPort};
    auto client = std::make_shared<MetaClient>(threadPool,
                                               std::vector<HostAddr>{HostAddr("0", 0)});
    // Retry with exception, then failed
    {
        LOG(INFO) << "Test heart beat...";
        folly::Baton<true, std::atomic> baton;
        client->heartbeat().thenValue([&baton] (auto&& status) {
            ASSERT_TRUE(!status.ok());
            baton.post();
        });
        baton.wait();
    }
}

TEST(MetaClientTest, RetryOnceTest) {
    FLAGS_heartbeat_interval_secs = 3600;
    std::string localIp{"127.0.0.1"};

    std::vector<std::unique_ptr<mock::RpcServer>> metads;
    std::vector<std::shared_ptr<TestMetaServiceRetry>> handlers;
    std::vector<HostAddr> addrs;
    for (size_t i = 0; i < 3; i++) {
        auto metad = std::make_unique<mock::RpcServer>();
        auto handler = std::make_shared<TestMetaServiceRetry>();
        metad->start(folly::stringPrintf("meta-%ld", i), 0, handler);
        addrs.emplace_back(localIp, metad->port_);
        metads.emplace_back(std::move(metad));
        handlers.emplace_back(handler);
    }
    // set leaders to first service
    for (size_t i = 0; i < addrs.size(); i++) {
        handlers[i]->setLeader(addrs[0]);
        handlers[i]->setAddr(addrs[i]);
    }

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    auto clientPort = network::NetworkUtils::getAvailablePort();
    HostAddr localHost{localIp, clientPort};
    auto client = std::make_shared<MetaClient>(threadPool,
                                               std::vector<HostAddr>{addrs[1]});
    // First get leader changed and then succeeded
    {
        LOG(INFO) << "Test heart beat...";
        folly::Baton<true, std::atomic> baton;
        client->heartbeat().thenValue([&baton] (auto&& status) {
            ASSERT_TRUE(status.ok());
            baton.post();
        });
        baton.wait();
    }
}

TEST(MetaClientTest, RetryUntilLimitTest) {
    FLAGS_heartbeat_interval_secs = 3600;
    std::string localIp{"127.0.0.1"};

    std::vector<std::unique_ptr<mock::RpcServer>> metads;
    std::vector<std::shared_ptr<TestMetaServiceRetry>> handlers;
    std::vector<HostAddr> addrs;
    for (size_t i = 0; i < 3; i++) {
        auto metad = std::make_unique<mock::RpcServer>();
        auto handler = std::make_shared<TestMetaServiceRetry>();
        metad->start(folly::stringPrintf("meta-%ld", i), 0, handler);
        addrs.emplace_back(localIp, metad->port_);
        metads.emplace_back(std::move(metad));
        handlers.emplace_back(handler);
    }

    for (size_t i = 0; i < addrs.size(); i++) {
        handlers[i]->setLeader(addrs[(i + 1) % addrs.size()]);
        handlers[i]->setAddr(addrs[i]);
    }

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    auto clientPort = network::NetworkUtils::getAvailablePort();
    HostAddr localHost{localIp, clientPort};
    auto client = std::make_shared<MetaClient>(threadPool,
                                               std::vector<HostAddr>{addrs[1]});
    // always get response of leader changed, then failed
    {
        LOG(INFO) << "Test heart beat...";
        folly::Baton<true, std::atomic> baton;
        client->heartbeat().thenValue([&baton] (auto&& status) {
            ASSERT_TRUE(!status.ok());
            baton.post();
        });
        baton.wait();
    }
}

cpp2::ConfigItem initConfigItem(cpp2::ConfigModule module,
                                std::string name,
                                cpp2::ConfigMode mode,
                                Value value) {
    cpp2::ConfigItem configItem;
    configItem.set_module(module);
    configItem.set_name(name);
    configItem.set_mode(mode);
    configItem.set_value(value);
    return configItem;
}

TEST(MetaClientTest, Config) {
    FLAGS_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/MetaClientTest.Config.XXXXXX");

    mock::MockCluster cluster;
    cluster.startMeta(0, rootPath.path());

    MetaClientOptions options;
    // Now the `--local_config' option only affect if initialize the configuration from meta
    // not affect `show/update/get configs'
    options.skipConfig_ = false;

    cluster.initMetaClient(std::move(options));
    auto* client = cluster.metaClient_.get();

    // Empty Configuration
    // List
    {
        auto resp = client->listConfigs(cpp2::ConfigModule::GRAPH).get();
        EXPECT_TRUE(resp.ok());
        EXPECT_EQ(resp.value().size(), 0);
        resp = client->listConfigs(cpp2::ConfigModule::META).get();
        EXPECT_TRUE(resp.ok());
        EXPECT_EQ(resp.value().size(), 0);
        resp = client->listConfigs(cpp2::ConfigModule::STORAGE).get();
        EXPECT_TRUE(resp.ok());
        EXPECT_EQ(resp.value().size(), 0);
        resp = client->listConfigs(cpp2::ConfigModule::ALL).get();
        EXPECT_TRUE(resp.ok());
        EXPECT_EQ(resp.value().size(), 0);
    }
    // Set
    {
        auto resp = client->setConfig(cpp2::ConfigModule::GRAPH, "minloglevel", 3).get();
        EXPECT_TRUE(!resp.ok());
        resp = client->setConfig(cpp2::ConfigModule::META, "minloglevel", 3).get();
        EXPECT_TRUE(!resp.ok());
        resp = client->setConfig(cpp2::ConfigModule::STORAGE, "minloglevel", 3).get();
        EXPECT_TRUE(!resp.ok());
    }
    // Get
    {
        auto resp = client->getConfig(cpp2::ConfigModule::GRAPH, "minloglevel").get();
        EXPECT_TRUE(!resp.ok());

        resp = client->getConfig(cpp2::ConfigModule::META, "minloglevel").get();
        EXPECT_TRUE(!resp.ok());

        resp = client->getConfig(cpp2::ConfigModule::STORAGE, "minloglevel").get();
        EXPECT_TRUE(!resp.ok());
    }

    // Reg
    {
        std::vector<cpp2::ConfigItem> configItems;
        configItems.emplace_back(initConfigItem(
                cpp2::ConfigModule::GRAPH, "minloglevel", cpp2::ConfigMode::MUTABLE, 2));
        configItems.emplace_back(initConfigItem(
                cpp2::ConfigModule::META, "minloglevel", cpp2::ConfigMode::MUTABLE, 2));
        configItems.emplace_back(initConfigItem(
                cpp2::ConfigModule::STORAGE, "minloglevel", cpp2::ConfigMode::MUTABLE, 2));
        auto resp = client->regConfig(configItems).get();
        ASSERT(resp.ok());
    }
    // List
    {
        auto resp = client->listConfigs(cpp2::ConfigModule::GRAPH).get();
        EXPECT_TRUE(resp.ok());
        EXPECT_EQ(resp.value().size(), 1);
        resp = client->listConfigs(cpp2::ConfigModule::META).get();
        EXPECT_TRUE(resp.ok());
        EXPECT_EQ(resp.value().size(), 1);
        resp = client->listConfigs(cpp2::ConfigModule::STORAGE).get();
        EXPECT_TRUE(resp.ok());
        EXPECT_EQ(resp.value().size(), 1);
        resp = client->listConfigs(cpp2::ConfigModule::ALL).get();
        EXPECT_TRUE(resp.ok());
        EXPECT_EQ(resp.value().size(), 3);
    }
    // Set
    {
        auto resp = client->setConfig(cpp2::ConfigModule::GRAPH, "minloglevel", 3).get();
        EXPECT_TRUE(resp.ok());
        resp = client->setConfig(cpp2::ConfigModule::META, "minloglevel", 3).get();
        EXPECT_TRUE(resp.ok());
        resp = client->setConfig(cpp2::ConfigModule::STORAGE, "minloglevel", 3).get();
        EXPECT_TRUE(resp.ok());
    }
    // Get
    {
        auto resp = client->getConfig(cpp2::ConfigModule::GRAPH, "minloglevel").get();
        EXPECT_TRUE(resp.ok());
        auto configs = std::move(resp).value();
        EXPECT_EQ(configs[0].get_value(), Value(3));

        resp = client->getConfig(cpp2::ConfigModule::META, "minloglevel").get();
        EXPECT_TRUE(resp.ok());
        configs = std::move(resp).value();
        EXPECT_EQ(configs[0].get_value(), Value(3));

        resp = client->getConfig(cpp2::ConfigModule::STORAGE, "minloglevel").get();
        EXPECT_TRUE(resp.ok());
        configs = std::move(resp).value();
        EXPECT_EQ(configs[0].get_value(), Value(3));
    }
    // Just avoid memory leak error of clang asan. to waitting asynchronous thread done.
    sleep(FLAGS_heartbeat_interval_secs * 5);
}

TEST(MetaClientTest, RocksdbOptionsTest) {
    FLAGS_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/RocksdbOptionsTest.XXXXXX");

    mock::MockCluster cluster;
    cluster.startMeta(0, rootPath.path());

    MetaClientOptions options;
    // Now the `--local_config' option only affect if initialize the configuration from meta
    // not affect `show/update/get configs'
    options.skipConfig_ = false;

    cluster.initMetaClient(std::move(options));
    auto* client = cluster.metaClient_.get();

    auto listener = std::make_unique<TestListener>();
    auto module = cpp2::ConfigModule::STORAGE;
    auto mode = meta::cpp2::ConfigMode::MUTABLE;

    client->registerListener(listener.get());
    client->gflagsModule_ = module;

    // mock some rocksdb gflags to meta
    {
        auto name = "rocksdb_db_options";
        std::vector<cpp2::ConfigItem> configItems;
        FLAGS_rocksdb_db_options = R"({"disable_auto_compactions":"false","write_buffer_size":"1048576"})";
        Map map;
        map.kvs.emplace("disable_auto_compactions", "false");
        map.kvs.emplace("write_buffer_size", "1048576");
        configItems.emplace_back(initConfigItem(
            module, name,
            mode, Value(map)));
        client->regConfig(configItems);

        // get from meta server
        auto getRet = client->getConfig(module, name).get();
        ASSERT_TRUE(getRet.ok());
        auto item = getRet.value().front();

        sleep(FLAGS_heartbeat_interval_secs + 1);
        ASSERT_EQ(FLAGS_rocksdb_db_options, GflagsManager::ValueToGflagString(item.get_value()));
    }
    {
        std::vector<HostAddr> hosts = {{"0", 0}};
        TestUtils::registerHB(cluster.metaKV_.get(), hosts);
        meta::cpp2::SpaceDesc spaceDesc;
        spaceDesc.set_space_name("default_space");
        spaceDesc.set_partition_num(9);
        spaceDesc.set_replica_factor(1);
        client->createSpace(spaceDesc).get();
        sleep(FLAGS_heartbeat_interval_secs + 1);
    }
    {
        std::string name = "rocksdb_db_options";
        Map map;
        map.kvs.emplace("disable_auto_compactions", "true");
        map.kvs.emplace("level0_file_num_compaction_trigger", "4");

        // update config
        auto setRet = client->setConfig(module, name, Value(map)).get();
        ASSERT_TRUE(setRet.ok());

        // get from meta server
        auto getRet = client->getConfig(module, name).get();
        ASSERT_TRUE(getRet.ok());
        auto item = getRet.value().front();

        sleep(FLAGS_heartbeat_interval_secs + 1);
        ASSERT_EQ(FLAGS_rocksdb_db_options, GflagsManager::ValueToGflagString(item.get_value()));
        ASSERT_EQ(listener->options["disable_auto_compactions"], "true");
        ASSERT_EQ(listener->options["level0_file_num_compaction_trigger"], "4");
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
