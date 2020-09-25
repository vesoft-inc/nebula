/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "base/Base.h"
#include "dataman/ResultSchemaProvider.h"
#include "fs/TempDir.h"
#include "meta/ClientBasedGflagsManager.h"
#include "meta/MetaServiceUtils.h"
#include "meta/ServerBasedSchemaManager.h"
#include "meta/client/MetaClient.h"
#include "meta/test/TestUtils.h"
#include "network/NetworkUtils.h"

DECLARE_int32(heartbeat_interval_secs);
DECLARE_string(rocksdb_db_options);

namespace nebula {
namespace meta {

using nebula::cpp2::SupportedType;
using nebula::cpp2::Value;
using nebula::cpp2::ValueType;

TEST(MetaClientTest, InterfacesTest) {
    FLAGS_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/MetaClientTest.XXXXXX");

    // Let the system choose an available port for us
    uint32_t localMetaPort = 0;
    auto sc = TestUtils::mockMetaServer(localMetaPort, rootPath.path());

    GraphSpaceID spaceId = 0;
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);
    auto clientPort = network::NetworkUtils::getAvailablePort();
    network::InetAddress localHost{localIp, clientPort};
    auto client = std::make_shared<MetaClient>(
        threadPool, std::vector<network::InetAddress>{network::InetAddress(localIp, sc->port_)});
    client->waitForMetadReady();
    {
        // Add hosts automatically, then testing listHosts interface.
        std::vector<network::InetAddress> hosts = {{0, 0}, {1, 1}, {2, 2}, {3, 3}};
        TestUtils::registerHB(sc->kvStore_.get(), hosts);
        auto ret = client->listHosts().get();
        ASSERT_TRUE(ret.ok());
        for (auto i = 0u; i < hosts.size(); i++) {
            auto tHost = ret.value()[i].hostAddr;
            auto hostAddr = network::InetAddress(tHost);
            ASSERT_EQ(hosts[i], hostAddr);
        }
    }
    {
        // Test createSpace, listSpaces, getPartsAlloc.
        {
            SpaceDesc spaceDesc("default_space", 8, 3, "utf8", "utf8_bin");
            auto ret = client->createSpace(spaceDesc).get();
            ASSERT_TRUE(ret.ok()) << ret.status();
            spaceId = ret.value();

            ret = client->createSpace(spaceDesc, true).get();
            ASSERT_TRUE(ret.ok()) << ret.status();
        }
        {
            SpaceDesc spaceDesc("default_space", 8, 3);
            auto ret = client->createSpace(spaceDesc).get();
            ASSERT_FALSE(ret.ok());
        }
        {
            auto ret = client->getSpace("default_space").get();
            ASSERT_TRUE(ret.ok()) << ret.status();
            meta::cpp2::SpaceProperties properties = ret.value().get_properties();
            ASSERT_EQ("default_space", properties.space_name);
            ASSERT_EQ(8, properties.partition_num);
            ASSERT_EQ(3, properties.replica_factor);
            ASSERT_EQ("utf8", properties.charset_name);
            ASSERT_EQ("utf8_bin", properties.collate_name);
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
            nebula::cpp2::Schema schema;
            for (auto i = 0; i < 5; i++) {
                nebula::cpp2::ColumnDef column;
                column.name = "tagItem" + std::to_string(i);
                column.type.type = nebula::cpp2::SupportedType::STRING;
                schema.columns.emplace_back(std::move(column));
            }
            auto ret = client->createTagSchema(spaceId, "tagName", schema).get();
            ASSERT_TRUE(ret.ok()) << ret.status();

            ret = client->createTagSchema(spaceId, "tagName", schema, true).get();
            ASSERT_TRUE(ret.ok()) << ret.status();
        }
        {
            // Create tag schema with default value
            nebula::cpp2::Schema schema;
            for (auto i = 0; i < 5; i++) {
                nebula::cpp2::ColumnDef column;
                column.name = "tagItem" + std::to_string(i);
                column.type.type = nebula::cpp2::SupportedType::STRING;
                nebula::cpp2::Value defaultValue;
                defaultValue.set_string_value(std::to_string(i));
                column.default_value = defaultValue;
                schema.columns.emplace_back(std::move(column));
            }
            auto ret = client->createTagSchema(spaceId, "tagWithDefault", schema).get();
            ASSERT_TRUE(ret.ok()) << ret.status();
        }
        {
            // Create edge schema
            nebula::cpp2::Schema schema;
            for (auto i = 0; i < 5; i++) {
                nebula::cpp2::ColumnDef column;
                column.name = "edgeItem" + std::to_string(i);
                column.type.type = nebula::cpp2::SupportedType::STRING;
                schema.columns.emplace_back(std::move(column));
            }
            auto ret = client->createEdgeSchema(spaceId, "edgeName", schema).get();
            ASSERT_TRUE(ret.ok()) << ret.status();
            ret = client->createEdgeSchema(spaceId, "edgeName", schema, true).get();
            ASSERT_TRUE(ret.ok()) << ret.status();
        }
        {
            // Create edge schema with default value
            nebula::cpp2::Schema schema;
            for (auto i = 0; i < 5; i++) {
                nebula::cpp2::ColumnDef column;
                column.name = "edgeItem" + std::to_string(i);
                column.type.type = nebula::cpp2::SupportedType::STRING;
                nebula::cpp2::Value defaultValue;
                defaultValue.set_string_value(std::to_string(i));
                column.default_value = defaultValue;
                schema.columns.emplace_back(std::move(column));
            }
            auto ret = client->createEdgeSchema(spaceId, "edgeWithDefault", schema).get();
            ASSERT_TRUE(ret.ok()) << ret.status();
        }

        auto schemaMan = std::make_unique<ServerBasedSchemaManager>();
        schemaMan->init(client.get());
        {
            // listTagSchemas
            auto ret1 = client->listTagSchemas(spaceId).get();
            ASSERT_TRUE(ret1.ok()) << ret1.status();
            ASSERT_EQ(ret1.value().size(), 2);
            ASSERT_NE(ret1.value().begin()->tag_id, 0);
            ASSERT_EQ(ret1.value().begin()->schema.columns.size(), 5);

            // getTagSchemaFromCache
            sleep(FLAGS_heartbeat_interval_secs + 1);
            auto ret = client->getLatestTagVersionFromCache(spaceId, ret1.value().begin()->tag_id);
            CHECK(ret.ok());
            auto ver = ret.value();
            auto ret2 = client->getTagSchemaFromCache(spaceId, ret1.value().begin()->tag_id, ver);
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
            auto retVer =
                client->getLatestEdgeVersionFromCache(spaceId, ret1.value().begin()->edge_type);
            CHECK(retVer.ok());
            auto ver = retVer.value();
            auto ret2 =
                client->getEdgeSchemaFromCache(spaceId, ret1.value().begin()->edge_type, ver);
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
        auto partsMap = client->getPartsMapFromCache(network::InetAddress(0, 0));
        ASSERT_EQ(1, partsMap.size());
        ASSERT_EQ(6, partsMap[spaceId].size());
    }
    {
        auto metaStatus = client->getPartMetaFromCache(spaceId, 1);
        ASSERT_TRUE(metaStatus.ok());
        auto partMeta = metaStatus.value();
        ASSERT_EQ(3, partMeta.peers_.size());
        for (auto& h : partMeta.peers_) {
            ASSERT_EQ(h.toLongHBO(), h.getPort());
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

    client.reset();
}

TEST(MetaClientTest, TagTest) {
    FLAGS_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/MetaClientTagTest.XXXXXX");

    // Let the system choose an available port for us
    int32_t localMetaPort = 0;
    auto sc = TestUtils::mockMetaServer(localMetaPort, rootPath.path());

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);
    auto localhosts = std::vector<network::InetAddress>{network::InetAddress(localIp, sc->port_)};
    auto client = std::make_shared<MetaClient>(threadPool, localhosts);
    std::vector<network::InetAddress> hosts = {{0, 0}, {1, 1}, {2, 2}, {3, 3}};
    client->waitForMetadReady();
    TestUtils::registerHB(sc->kvStore_.get(), hosts);
    SpaceDesc spaceDesc("default", 9, 3);
    auto ret = client->createSpace(spaceDesc).get();
    ASSERT_TRUE(ret.ok()) << ret.status();
    GraphSpaceID spaceId = ret.value();
    TagID id;
    int64_t version;

    {
        std::vector<nebula::cpp2::ColumnDef> columns;
        ValueType vt;
        Value defaultValue;
        vt.set_type(SupportedType::INT);
        defaultValue.set_int_value(0);
        columns.emplace_back();
        columns.back().set_name("column_i");
        columns.back().set_default_value(defaultValue);
        columns.back().set_type(vt);

        vt.set_type(SupportedType::DOUBLE);
        defaultValue.set_double_value(3.14);
        columns.emplace_back();
        columns.back().set_default_value(defaultValue);
        columns.back().set_name("column_d");
        columns.back().set_type(vt);

        vt.set_type(SupportedType::STRING);
        defaultValue.set_string_value("test");
        columns.emplace_back();
        columns.back().set_default_value(defaultValue);
        columns.back().set_name("column_s");
        columns.back().set_type(vt);

        nebula::cpp2::Schema schema;
        schema.set_columns(std::move(columns));
        auto result = client->createTagSchema(spaceId, "test_tag", std::move(schema)).get();
        ASSERT_TRUE(result.ok());
        id = result.value();
    }
    {
        std::vector<nebula::cpp2::ColumnDef> columns;
        nebula::cpp2::ColumnDef intColumn;
        intColumn.set_name("column_i");
        nebula::cpp2::ValueType intType;
        intType.set_type(SupportedType::INT);
        intColumn.set_type(std::move(intType));
        nebula::cpp2::Value intValue;
        intValue.set_int_value(0);
        intColumn.set_default_value(intValue);
        columns.emplace_back(std::move(intColumn));

        nebula::cpp2::ColumnDef doubleColumn;
        doubleColumn.set_name("column_d");
        nebula::cpp2::ValueType stringType;
        stringType.set_type(SupportedType::STRING);
        doubleColumn.set_type(std::move(stringType));
        nebula::cpp2::Value doubleValue;
        doubleValue.set_double_value(3.14);
        doubleColumn.set_default_value(doubleValue);
        columns.emplace_back(std::move(doubleColumn));

        nebula::cpp2::Schema schema;
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

    // Let the system choose an available port for us
    int32_t localMetaPort = 0;
    auto sc = TestUtils::mockMetaServer(localMetaPort, rootPath.path());

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);
    auto localhosts = std::vector<network::InetAddress>{network::InetAddress(localIp, sc->port_)};
    auto client = std::make_shared<MetaClient>(threadPool, localhosts);
    std::vector<network::InetAddress> hosts = {{0, 0}, {1, 1}, {2, 2}, {3, 3}};
    client->waitForMetadReady();
    TestUtils::registerHB(sc->kvStore_.get(), hosts);
    SpaceDesc spaceDesc("default_space", 9, 3);
    auto ret = client->createSpace(spaceDesc).get();
    ASSERT_TRUE(ret.ok()) << ret.status();
    GraphSpaceID space = ret.value();
    SchemaVer version;

    std::vector<nebula::cpp2::ColumnDef> expectedColumns;
    {
        std::vector<nebula::cpp2::ColumnDef> columns;

        nebula::cpp2::ColumnDef intColumn;
        intColumn.set_name("column_i");
        nebula::cpp2::ValueType intType;
        intType.set_type(SupportedType::INT);
        intColumn.set_type(std::move(intType));
        nebula::cpp2::Value intValue;
        intValue.set_int_value(0);
        intColumn.set_default_value(intValue);
        columns.emplace_back(std::move(intColumn));

        nebula::cpp2::ColumnDef doubleColumn;
        doubleColumn.set_name("column_d");
        nebula::cpp2::ValueType doubleType;
        doubleType.set_type(SupportedType::DOUBLE);
        doubleColumn.set_type(std::move(doubleType));
        nebula::cpp2::Value doubleValue;
        doubleValue.set_double_value(3.14);
        doubleColumn.set_default_value(doubleValue);
        columns.emplace_back(std::move(doubleColumn));

        nebula::cpp2::ColumnDef stringColumn;
        stringColumn.set_name("column_s");
        nebula::cpp2::ValueType stringType;
        stringType.set_type(SupportedType::STRING);
        stringColumn.set_type(stringType);
        nebula::cpp2::Value stringValue;
        stringValue.set_string_value("test");
        stringColumn.set_default_value(stringValue);
        columns.emplace_back(std::move(stringColumn));
        expectedColumns = columns;

        nebula::cpp2::Schema schema;
        schema.set_columns(std::move(columns));
        auto result = client->createEdgeSchema(space, "test_edge", schema).get();
        ASSERT_TRUE(result.ok());
    }
    {
        std::vector<nebula::cpp2::ColumnDef> columns;
        nebula::cpp2::ColumnDef intColumn;
        intColumn.set_name("column_i");
        nebula::cpp2::ValueType intType;
        intType.set_type(SupportedType::INT);
        intColumn.set_type(std::move(intType));
        nebula::cpp2::Value intValue;
        intValue.set_int_value(0);
        intColumn.set_default_value(intValue);
        columns.emplace_back(std::move(intColumn));

        nebula::cpp2::ColumnDef doubleColumn;
        doubleColumn.set_name("column_d");
        nebula::cpp2::ValueType stringType;
        stringType.set_type(SupportedType::STRING);
        doubleColumn.set_type(std::move(stringType));
        nebula::cpp2::Value doubleValue;
        doubleValue.set_double_value(3.14);
        doubleColumn.set_default_value(doubleValue);
        columns.emplace_back(std::move(doubleColumn));
        nebula::cpp2::Schema schema;
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

        nebula::cpp2::Schema expected;
        expected.set_columns(std::move(expectedColumns));
        nebula::cpp2::Schema resultSchema = edges[0].get_schema();
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

    // Let the system choose an available port for us
    int32_t localMetaPort = 0;
    auto sc = TestUtils::mockMetaServer(localMetaPort, rootPath.path());

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);
    auto localhosts = std::vector<network::InetAddress>{network::InetAddress(localIp, sc->port_)};
    std::vector<network::InetAddress> hosts = {{0, 0}, {1, 1}, {2, 2}, {3, 3}};
    auto client = std::make_shared<MetaClient>(threadPool, localhosts);
    client->waitForMetadReady();
    TestUtils::registerHB(sc->kvStore_.get(), hosts);

    SpaceDesc spaceDesc("default_space", 8, 3);
    auto ret = client->createSpace(spaceDesc).get();
    ASSERT_TRUE(ret.ok()) << ret.status();
    GraphSpaceID space = ret.value();
    IndexID singleFieldIndexID;
    IndexID multiFieldIndexID;
    {
        for (auto i = 0; i < 2; i++) {
            std::vector<nebula::cpp2::ColumnDef> columns;
            nebula::cpp2::ColumnDef column0;
            column0.set_name(folly::stringPrintf("tag_%d_col_0", i));
            nebula::cpp2::ValueType intType;
            intType.set_type(SupportedType::INT);
            column0.set_type(std::move(intType));
            columns.emplace_back(std::move(column0));

            nebula::cpp2::ColumnDef column1;
            column1.set_name(folly::stringPrintf("tag_%d_col_1", i));
            nebula::cpp2::ValueType stringType;
            stringType.set_type(SupportedType::STRING);
            column1.set_type(std::move(stringType));
            columns.emplace_back(std::move(column1));

            nebula::cpp2::Schema schema;
            schema.set_columns(std::move(columns));
            auto result =
                client->createTagSchema(space, folly::stringPrintf("tag_%d", i), schema).get();
            ASSERT_TRUE(result.ok());
        }
    }
    {
        std::vector<std::string>&& fields{"tag_0_col_0"};
        auto result =
            client->createTagIndex(space, "tag_single_field_index", "tag_0", std::move(fields))
                .get();
        ASSERT_TRUE(result.ok());
        singleFieldIndexID = result.value();
    }
    {
        std::vector<std::string>&& fields{"tag_0_col_0"};
        auto result =
            client->createTagIndex(space, "tag_duplicate_field_index", "tag_0", std::move(fields))
                .get();
        ASSERT_FALSE(result.ok());
    }
    {
        std::vector<std::string>&& fields{"tag_0_col_0", "tag_0_col_1"};
        auto result =
            client->createTagIndex(space, "tag_multi_field_index", "tag_0", std::move(fields))
                .get();
        ASSERT_TRUE(result.ok());
        multiFieldIndexID = result.value();
    }
    {
        std::vector<std::string>&& fields{"tag_0_col_1", "tag_0_col_0"};
        auto result =
            client->createTagIndex(space, "tag_disorder_field_index", "tag_0", std::move(fields))
                .get();
        ASSERT_TRUE(result.ok());
    }
    {
        std::vector<std::string>&& fields{"tag_0_col_0", "tag_0_col_1"};
        auto result =
            client->createTagIndex(space, "tag_duplicate_field_index", "tag_0", std::move(fields))
                .get();
        ASSERT_FALSE(result.ok());
    }
    {
        std::vector<std::string>&& fields{"tag_0_col_0"};
        auto result =
            client->createTagIndex(space, "tag_duplicate_field_index", "tag_0", std::move(fields))
                .get();
        ASSERT_FALSE(result.ok());
    }
    {
        std::vector<std::string>&& fields{"tag_0_col_0", "not_exist_field"};
        auto result =
            client->createTagIndex(space, "tag_field_not_exist_index", "tag_0", std::move(fields))
                .get();
        ASSERT_FALSE(result.ok());
        ASSERT_EQ(Status::Error("not existed!"), result.status());
    }
    {
        std::vector<std::string>&& fields{"tag_0_col_0", "tag_0_col_1"};
        auto result =
            client->createTagIndex(space, "tag_not_exist_index", "tag_not_exist", std::move(fields))
                .get();
        ASSERT_FALSE(result.ok());
        ASSERT_EQ(Status::Error("not existed!"), result.status());
    }
    {
        std::vector<std::string>&& fields{"tag_0_col_0", "tag_0_col_0"};
        auto result =
            client->createTagIndex(space, "tag_conflict_index", "tag_0", std::move(fields)).get();
        ASSERT_FALSE(result.ok());
        ASSERT_EQ(Status::Error("conflict"), result.status());
    }
    {
        auto result = client->listTagIndexes(space).get();
        auto values = result.value();
        ASSERT_EQ(3, values.size());

        {
            nebula::cpp2::ColumnDef singleColumn;
            singleColumn.set_name("tag_0_col_0");
            nebula::cpp2::ValueType intType;
            intType.set_type(SupportedType::INT);
            singleColumn.set_type(std::move(intType));
            std::vector<nebula::cpp2::ColumnDef> columns;
            columns.emplace_back(std::move(singleColumn));

            auto singleFieldResult = values[0].get_fields();
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

            auto multiFieldResult = values[1].get_fields();
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

            auto disorderFieldResult = values[2].get_fields();
            ASSERT_TRUE(TestUtils::verifyResult(columns, disorderFieldResult));
        }
    }
    sleep(FLAGS_heartbeat_interval_secs * 5);
    // Test Tag Index Properties Cache
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
        ASSERT_EQ(3, tagIndexes.value().size());
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

    // Let the system choose an available port for us
    int32_t localMetaPort = 0;
    auto sc = TestUtils::mockMetaServer(localMetaPort, rootPath.path());

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);
    auto localhosts = std::vector<network::InetAddress>{network::InetAddress(localIp, sc->port_)};
    auto client = std::make_shared<MetaClient>(threadPool, localhosts);

    client->waitForMetadReady();
    std::vector<network::InetAddress> hosts = {{0, 0}, {1, 1}, {2, 2}, {3, 3}};
    TestUtils::registerHB(sc->kvStore_.get(), hosts);
    SpaceDesc spaceDesc("default_space", 8, 3);
    auto ret = client->createSpace(spaceDesc).get();
    GraphSpaceID space = ret.value();
    IndexID singleFieldIndexID;
    IndexID multiFieldIndexID;
    {
        for (auto i = 0; i < 2; i++) {
            std::vector<nebula::cpp2::ColumnDef> columns;
            nebula::cpp2::ColumnDef column0;
            column0.set_name(folly::stringPrintf("edge_%d_col_0", i));
            nebula::cpp2::ValueType intType;
            intType.set_type(SupportedType::INT);
            column0.set_type(std::move(intType));
            columns.emplace_back(std::move(column0));

            nebula::cpp2::ColumnDef column1;
            column1.set_name(folly::stringPrintf("edge_%d_col_1", i));
            nebula::cpp2::ValueType stringType;
            stringType.set_type(SupportedType::STRING);
            column1.set_type(std::move(stringType));
            columns.emplace_back(std::move(column1));

            nebula::cpp2::Schema schema;
            schema.set_columns(std::move(columns));
            auto result =
                client->createEdgeSchema(space, folly::stringPrintf("edge_%d", i), schema).get();
            ASSERT_TRUE(result.ok());
        }
    }
    {
        std::vector<std::string>&& fields{"edge_0_col_0"};
        auto result =
            client->createEdgeIndex(space, "edge_single_field_index", "edge_0", std::move(fields))
                .get();
        ASSERT_TRUE(result.ok());
        singleFieldIndexID = result.value();
    }
    {
        std::vector<std::string>&& fields{"edge_0_col_0"};
        auto result =
            client
                ->createEdgeIndex(space, "edge_duplicate_field_index", "edge_0", std::move(fields))
                .get();
        ASSERT_FALSE(result.ok());
    }
    {
        std::vector<std::string>&& fields{"edge_0_col_0", "edge_0_col_1"};
        auto result =
            client->createEdgeIndex(space, "edge_multi_field_index", "edge_0", std::move(fields))
                .get();
        ASSERT_TRUE(result.ok());
        multiFieldIndexID = result.value();
    }
    {
        std::vector<std::string>&& fields{"edge_0_col_1", "edge_0_col_0"};
        auto result =
            client->createEdgeIndex(space, "edge_disorder_field_index", "edge_0", std::move(fields))
                .get();
        ASSERT_TRUE(result.ok());
    }
    {
        std::vector<std::string>&& fields{"edge_0_col_0", "edge_0_col_1"};
        auto result =
            client
                ->createEdgeIndex(space, "edge_duplicate_field_index", "edge_0", std::move(fields))
                .get();
        ASSERT_FALSE(result.ok());
    }
    {
        std::vector<std::string>&& fields{"edge_0_col_0"};
        auto result =
            client
                ->createEdgeIndex(space, "edge_duplicate_field_index", "edge_0", std::move(fields))
                .get();
        ASSERT_FALSE(result.ok());
    }
    {
        std::vector<std::string>&& fields{"edge_0_col_0", "edge_0_col_1"};
        auto result = client
                          ->createEdgeIndex(
                              space, "edge_not_exist_index", "edge_not_exist", std::move(fields))
                          .get();
        ASSERT_FALSE(result.ok());
        ASSERT_EQ(Status::Error("not existed!"), result.status());
    }
    {
        std::vector<std::string>&& fields{"edge_0_col_0", "edge_0_col_0"};
        auto result =
            client->createEdgeIndex(space, "edge_conflict_index", "edge_0", std::move(fields))
                .get();
        ASSERT_FALSE(result.ok());
        ASSERT_EQ(Status::Error("conflict"), result.status());
    }
    {
        std::vector<std::string>&& fields{"edge_0_col_0", "not_exist_field"};
        auto result =
            client
                ->createEdgeIndex(space, "edge_field_not_exist_index", "edge_0", std::move(fields))
                .get();
        ASSERT_FALSE(result.ok());
        ASSERT_EQ(Status::Error("not existed!"), result.status());
    }
    {
        auto result = client->listEdgeIndexes(space).get();
        auto values = result.value();
        ASSERT_EQ(3, values.size());

        {
            nebula::cpp2::ColumnDef column;
            column.set_name("edge_0_col_0");
            nebula::cpp2::ValueType type;
            type.set_type(SupportedType::INT);
            column.set_type(std::move(type));
            std::vector<nebula::cpp2::ColumnDef> columns;
            columns.emplace_back(std::move(column));

            auto singleFieldResult = values[0].get_fields();
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
            auto multiFieldResult = values[1].get_fields();
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
            auto disorderFieldResult = values[2].get_fields();
            ASSERT_TRUE(TestUtils::verifyResult(columns, disorderFieldResult));
        }
    }
    sleep(FLAGS_heartbeat_interval_secs * 5);
    // Test Edge Index Properties Cache
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
        ASSERT_EQ(3, edgeIndexes.value().size());
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

    void onPartAdded(const PartMeta& partMeta) override {
        LOG(INFO) << "[" << partMeta.spaceId_ << ", " << partMeta.partId_ << "] added!";
        partNum++;
    }

    void onSpaceOptionUpdated(GraphSpaceID spaceId,
                              const std::unordered_map<std::string, std::string>& update) override {
        UNUSED(spaceId);
        for (const auto& kv : update) {
            options[kv.first] = kv.second;
        }
    }

    void onPartRemoved(GraphSpaceID spaceId, PartitionID partId) override {
        LOG(INFO) << "[" << spaceId << ", " << partId << "] removed!";
        partNum--;
    }

    void onPartUpdated(const PartMeta& partMeta) override {
        LOG(INFO) << "[" << partMeta.spaceId_ << ", " << partMeta.partId_ << "] updated!";
        partChanged++;
    }

    void fetchLeaderInfo(
        std::unordered_map<GraphSpaceID, std::vector<PartitionID>>& leaderIds) override {
        LOG(INFO) << "Get leader distribution!";
        UNUSED(leaderIds);
    }

    network::InetAddress getLocalHost() {
        return network::InetAddress(0, 0);
    }

    int32_t spaceNum = 0;
    int32_t partNum = 0;
    int32_t partChanged = 0;
    std::unordered_map<std::string, std::string> options;
};

TEST(MetaClientTest, DiffTest) {
    FLAGS_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/MetaClientTest.XXXXXX");

    // Let the system choose an available port for us
    int32_t localMetaPort = 0;
    auto sc = TestUtils::mockMetaServer(localMetaPort, rootPath.path());

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);
    auto listener = std::make_unique<TestListener>();
    auto client = std::make_shared<MetaClient>(
        threadPool, std::vector<network::InetAddress>{network::InetAddress(localIp, sc->port_)});
    client->waitForMetadReady();
    client->registerListener(listener.get());
    {
        // Add hosts automatically, then testing listHosts interface.
        std::vector<network::InetAddress> hosts = {{0, 0}};
        TestUtils::registerHB(sc->kvStore_.get(), hosts);
        auto ret = client->listHosts().get();
        ASSERT_TRUE(ret.ok());
        for (auto i = 0u; i < hosts.size(); i++) {
            auto tHost = ret.value()[i].hostAddr;
            auto hostAddr = network::InetAddress(tHost);
            ASSERT_EQ(hosts[i], hostAddr);
        }
    }
    {
        // Test Create Space and List Spaces
        SpaceDesc spaceDesc("default_space", 9, 1);
        auto ret = client->createSpace(spaceDesc).get();
        ASSERT_TRUE(ret.ok()) << ret.status();
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    ASSERT_EQ(1, listener->spaceNum);
    ASSERT_EQ(9, listener->partNum);
    {
        SpaceDesc spaceDesc("default_space_1", 5, 1);
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
}

TEST(MetaClientTest, HeartbeatTest) {
    FLAGS_heartbeat_interval_secs = 1;
    const nebula::ClusterID kClusterId = 10;
    fs::TempDir rootPath("/tmp/MetaClientTest.XXXXXX");
    // Let the system choose an available port for us
    int32_t localMetaPort = 0;
    auto sc = TestUtils::mockMetaServer(localMetaPort, rootPath.path(), kClusterId);

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);
    auto listener = std::make_unique<TestListener>();
    auto clientPort = network::NetworkUtils::getAvailablePort();
    network::InetAddress localHost{localIp, clientPort};

    MetaClientOptions options;
    options.localHost_ = localHost;
    options.clusterId_ = kClusterId;
    options.inStoraged_ = true;
    auto client = std::make_shared<MetaClient>(
        threadPool,
        std::vector<network::InetAddress>{network::InetAddress(localIp, sc->port_)},
        options);
    client->waitForMetadReady();
    client->registerListener(listener.get());
    {
        // Add hosts automatically, then testing listHosts interface.
        std::vector<network::InetAddress> hosts = {localHost};
        auto ret = client->listHosts().get();
        ASSERT_TRUE(ret.ok());
        for (auto i = 0u; i < hosts.size(); i++) {
            auto tHost = ret.value()[i].hostAddr;
            auto hostAddr = network::InetAddress(tHost);
            ASSERT_EQ(hosts[i], hostAddr);
        }
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    ASSERT_EQ(1, ActiveHostsMan::getActiveHosts(sc->kvStore_.get()).size());
}

class TestMetaService : public cpp2::MetaServiceSvIf {
public:
    folly::Future<cpp2::HBResp> future_heartBeat(const cpp2::HBReq& req) override {
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
    void setLeader(network::InetAddress leader) {
        leader_.set_ip(leader.toLongHBO());
        leader_.set_port(leader.getPort());
    }

    void setAddr(network::InetAddress addr) {
        addr_.set_ip(addr.toLongHBO());
        addr_.set_port(addr.getPort());
    }

    folly::Future<cpp2::HBResp> future_heartBeat(const cpp2::HBReq& req) override {
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
    nebula::cpp2::HostAddr leader_;
    nebula::cpp2::HostAddr addr_;
};

TEST(MetaClientTest, SimpleTest) {
    FLAGS_heartbeat_interval_secs = 3600;
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);

    auto sc = std::make_unique<test::ServerContext>();
    auto handler = std::make_shared<TestMetaService>();
    sc->mockCommon("meta", 0, handler);

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    auto clientPort = network::NetworkUtils::getAvailablePort();
    network::InetAddress localHost{localIp, clientPort};
    auto client = std::make_shared<MetaClient>(
        threadPool, std::vector<network::InetAddress>{network::InetAddress(localIp, sc->port_)});
    {
        LOG(INFO) << "Test heart beat...";
        folly::Baton<true, std::atomic> baton;
        client->heartbeat().thenValue([&baton](auto&& status) {
            ASSERT_TRUE(status.ok());
            baton.post();
        });
        baton.wait();
    }
}

TEST(MetaClientTest, RetryWithExceptionTest) {
    FLAGS_heartbeat_interval_secs = 3600;
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    auto clientPort = network::NetworkUtils::getAvailablePort();
    network::InetAddress localHost{localIp, clientPort};
    auto client = std::make_shared<MetaClient>(
        threadPool, std::vector<network::InetAddress>{network::InetAddress(0, 0)});
    // Retry with exception, then failed
    {
        LOG(INFO) << "Test heart beat...";
        folly::Baton<true, std::atomic> baton;
        client->heartbeat().thenValue([&baton](auto&& status) {
            ASSERT_TRUE(!status.ok());
            baton.post();
        });
        baton.wait();
    }
}

TEST(MetaClientTest, RetryOnceTest) {
    FLAGS_heartbeat_interval_secs = 3600;
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);

    std::vector<std::unique_ptr<test::ServerContext>> contexts;
    std::vector<std::shared_ptr<TestMetaServiceRetry>> handlers;
    std::vector<network::InetAddress> addrs;
    for (size_t i = 0; i < 3; i++) {
        auto sc = std::make_unique<test::ServerContext>();
        auto handler = std::make_shared<TestMetaServiceRetry>();
        sc->mockCommon("meta", 0, handler);
        addrs.emplace_back(localIp, sc->port_);
        contexts.emplace_back(std::move(sc));
        handlers.emplace_back(handler);
    }
    // set leaders to first service
    for (size_t i = 0; i < addrs.size(); i++) {
        handlers[i]->setLeader(addrs[0]);
        handlers[i]->setAddr(addrs[i]);
    }

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    auto clientPort = network::NetworkUtils::getAvailablePort();
    network::InetAddress localHost{localIp, clientPort};
    auto client =
        std::make_shared<MetaClient>(threadPool, std::vector<network::InetAddress>{addrs[1]});
    // First get leader changed and then succeeded
    {
        LOG(INFO) << "Test heart beat...";
        folly::Baton<true, std::atomic> baton;
        client->heartbeat().thenValue([&baton](auto&& status) {
            ASSERT_TRUE(status.ok());
            baton.post();
        });
        baton.wait();
    }
}

TEST(MetaClientTest, RetryUntilLimitTest) {
    FLAGS_heartbeat_interval_secs = 3600;
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);

    std::vector<std::unique_ptr<test::ServerContext>> contexts;
    std::vector<std::shared_ptr<TestMetaServiceRetry>> handlers;
    std::vector<network::InetAddress> addrs;
    for (size_t i = 0; i < 3; i++) {
        auto sc = std::make_unique<test::ServerContext>();
        auto handler = std::make_shared<TestMetaServiceRetry>();
        sc->mockCommon("meta", 0, handler);
        addrs.emplace_back(localIp, sc->port_);
        contexts.emplace_back(std::move(sc));
        handlers.emplace_back(handler);
    }
    for (size_t i = 0; i < addrs.size(); i++) {
        handlers[i]->setLeader(addrs[(i + 1) % addrs.size()]);
        handlers[i]->setAddr(addrs[i]);
    }

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    auto clientPort = network::NetworkUtils::getAvailablePort();
    network::InetAddress localHost{localIp, clientPort};
    auto client =
        std::make_shared<MetaClient>(threadPool, std::vector<network::InetAddress>{addrs[1]});
    // always get response of leader changed, then failed
    {
        LOG(INFO) << "Test heart beat...";
        folly::Baton<true, std::atomic> baton;
        client->heartbeat().thenValue([&baton](auto&& status) {
            ASSERT_TRUE(!status.ok());
            baton.post();
        });
        baton.wait();
    }
}

TEST(MetaClientTest, Config) {
    FLAGS_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/MetaClientTest.Config.XXXXXX");

    // Let the system choose an available port for us
    int32_t localMetaPort = 0;
    auto sc = TestUtils::mockMetaServer(localMetaPort, rootPath.path());

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);
    MetaClientOptions options;
    // Now the `--local_config' option only affect if initialize the configuration from meta
    // not affect `show/update/get configs'
    options.skipConfig_ = true;
    auto client = std::make_shared<MetaClient>(
        threadPool,
        std::vector<network::InetAddress>{network::InetAddress(localIp, sc->port_)},
        options);
    client->waitForMetadReady();

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
        auto resp =
            client
                ->setConfig(cpp2::ConfigModule::GRAPH,
                            "minloglevel",
                            cpp2::ConfigType::INT64,
                            toThriftValueStr(cpp2::ConfigType::INT64, static_cast<int64_t>(3)))
                .get();
        EXPECT_TRUE(!resp.ok());
        resp = client
                   ->setConfig(cpp2::ConfigModule::META,
                               "minloglevel",
                               cpp2::ConfigType::INT64,
                               toThriftValueStr(cpp2::ConfigType::INT64, static_cast<int64_t>(3)))
                   .get();
        EXPECT_TRUE(!resp.ok());
        resp = client
                   ->setConfig(cpp2::ConfigModule::STORAGE,
                               "minloglevel",
                               cpp2::ConfigType::INT64,
                               toThriftValueStr(cpp2::ConfigType::INT64, static_cast<int64_t>(3)))
                   .get();
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

    auto item1 =
        toThriftConfigItem(cpp2::ConfigModule::GRAPH,
                           "minloglevel",
                           cpp2::ConfigType::INT64,
                           cpp2::ConfigMode::MUTABLE,
                           toThriftValueStr(cpp2::ConfigType::INT64, static_cast<int64_t>(2)));
    auto item2 =
        toThriftConfigItem(cpp2::ConfigModule::META,
                           "minloglevel",
                           cpp2::ConfigType::INT64,
                           cpp2::ConfigMode::MUTABLE,
                           toThriftValueStr(cpp2::ConfigType::INT64, static_cast<int64_t>(2)));
    auto item3 =
        toThriftConfigItem(cpp2::ConfigModule::STORAGE,
                           "minloglevel",
                           cpp2::ConfigType::INT64,
                           cpp2::ConfigMode::MUTABLE,
                           toThriftValueStr(cpp2::ConfigType::INT64, static_cast<int64_t>(2)));
    // Reg
    {
        std::vector<cpp2::ConfigItem> configItems{
            item1,
            item2,
            item3,
        };
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
        auto resp =
            client
                ->setConfig(cpp2::ConfigModule::GRAPH,
                            "minloglevel",
                            cpp2::ConfigType::INT64,
                            toThriftValueStr(cpp2::ConfigType::INT64, static_cast<int64_t>(3)))
                .get();
        EXPECT_TRUE(resp.ok());
        resp = client
                   ->setConfig(cpp2::ConfigModule::META,
                               "minloglevel",
                               cpp2::ConfigType::INT64,
                               toThriftValueStr(cpp2::ConfigType::INT64, static_cast<int64_t>(3)))
                   .get();
        EXPECT_TRUE(resp.ok());
        resp = client
                   ->setConfig(cpp2::ConfigModule::STORAGE,
                               "minloglevel",
                               cpp2::ConfigType::INT64,
                               toThriftValueStr(cpp2::ConfigType::INT64, static_cast<int64_t>(3)))
                   .get();
        EXPECT_TRUE(resp.ok());
    }
    // Get
    {
        item1.set_value(toThriftValueStr(cpp2::ConfigType::INT64, static_cast<int64_t>(3)));
        auto resp = client->getConfig(cpp2::ConfigModule::GRAPH, "minloglevel").get();
        EXPECT_TRUE(resp.ok());
        auto config = std::move(resp).value();
        EXPECT_EQ(item1, config[0]);

        item2.set_value(toThriftValueStr(cpp2::ConfigType::INT64, static_cast<int64_t>(3)));
        resp = client->getConfig(cpp2::ConfigModule::META, "minloglevel").get();
        EXPECT_TRUE(resp.ok());
        config = std::move(resp).value();
        EXPECT_EQ(item2, config[0]);

        item3.set_value(toThriftValueStr(cpp2::ConfigType::INT64, static_cast<int64_t>(3)));
        resp = client->getConfig(cpp2::ConfigModule::STORAGE, "minloglevel").get();
        EXPECT_TRUE(resp.ok());
        config = std::move(resp).value();
        EXPECT_EQ(item3, config[0]);
    }
}

TEST(MetaClientTest, RocksdbOptionsTest) {
    FLAGS_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/RocksdbOptionsTest.XXXXXX");
    uint32_t localMetaPort = 0;
    auto sc = TestUtils::mockMetaServer(localMetaPort, rootPath.path());
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    IPv4 localIp;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);

    auto listener = std::make_unique<TestListener>();
    auto module = cpp2::ConfigModule::STORAGE;
    auto type = cpp2::ConfigType::NESTED;
    auto mode = meta::cpp2::ConfigMode::MUTABLE;

    auto client = std::make_shared<MetaClient>(
        threadPool, std::vector<network::InetAddress>{network::InetAddress(localIp, sc->port_)});
    client->waitForMetadReady();
    client->registerListener(listener.get());
    client->gflagsModule_ = module;

    ClientBasedGflagsManager cfgMan(client.get());
    // mock some rocksdb gflags to meta
    {
        std::vector<cpp2::ConfigItem> configItems;
        FLAGS_rocksdb_db_options = R"({
            "disable_auto_compactions":"false",
            "write_buffer_size":"1048576"
        })";
        configItems.emplace_back(
            toThriftConfigItem(module,
                               "rocksdb_db_options",
                               type,
                               mode,
                               toThriftValueStr(type, FLAGS_rocksdb_db_options)));
        cfgMan.registerGflags(configItems);
    }
    {
        std::vector<network::InetAddress> hosts = {{0, 0}};
        TestUtils::registerHB(sc->kvStore_.get(), hosts);
        SpaceDesc spaceDesc("default_space", 9, 1);
        client->createSpace(spaceDesc).get();
        sleep(FLAGS_heartbeat_interval_secs + 1);
    }
    {
        std::string name = "rocksdb_db_options";
        std::string updateValue = "disable_auto_compactions=true,"
                                  "level0_file_num_compaction_trigger=4";
        // update config
        auto setRet = cfgMan.setConfig(module, name, type, updateValue).get();
        ASSERT_TRUE(setRet.ok());

        // get from meta server
        auto getRet = cfgMan.getConfig(module, name).get();
        ASSERT_TRUE(getRet.ok());
        auto item = getRet.value().front();
        auto value = boost::get<std::string>(item.get_value());

        sleep(FLAGS_heartbeat_interval_secs + 1);
        ASSERT_EQ(FLAGS_rocksdb_db_options, value);
        ASSERT_EQ(listener->options["disable_auto_compactions"], "true");
        ASSERT_EQ(listener->options["level0_file_num_compaction_trigger"], "4");
    }
}

}   // namespace meta
}   // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
