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
#include "common/expression/ArithmeticExpression.h"
#include "common/expression/FunctionCallExpression.h"
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
    cluster.startMeta(rootPath.path());
    cluster.initMetaClient();
    auto* kv = cluster.metaKV_.get();
    auto* client = cluster.metaClient_.get();

    GraphSpaceID spaceId = 0;
    TestUtils::createSomeHosts(kv);
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
            ASSERT_EQ("default_space", spaceDesc.get_space_name());
            ASSERT_EQ(8, spaceDesc.get_partition_num());
            ASSERT_EQ(3, spaceDesc.get_replica_factor());
            ASSERT_EQ("utf8", spaceDesc.get_charset_name());
            ASSERT_EQ("utf8_bin", spaceDesc.get_collate_name());
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
                (*schema.columns_ref()).emplace_back(std::move(column));
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
                const auto& defaultValue = *ConstantExpression::make(metaPool, std::to_string(i));
                column.set_default_value(Expression::encode(defaultValue));
                (*schema.columns_ref()).emplace_back(std::move(column));
            }
            auto ret = client->createTagSchema(spaceId, "tagWithDefault", schema).get();
            ASSERT_TRUE(ret.ok()) << ret.status();
        }
        {
            // Create tag schema with default value with wrong null type
            cpp2::Schema schema;
            cpp2::ColumnDef column;
            column.name = "tagItem";
            column.type.set_type(PropertyType::STRING);
            column.set_nullable(true);
            const auto& defaultValue =
                *ArithmeticExpression::makeDivision(metaPool,
                                                    ConstantExpression::make(metaPool, 1),
                                                    ConstantExpression::make(metaPool, 0));
            column.set_default_value(Expression::encode(defaultValue));
            (*schema.columns_ref()).emplace_back(std::move(column));
            auto ret = client->createTagSchema(spaceId, "tagWithWrongDefault", schema).get();
            ASSERT_FALSE(ret.ok()) << ret.status();
        }
        {
            // Create edge schema
            cpp2::Schema schema;
            for (auto i = 0; i < 5; i++) {
                cpp2::ColumnDef column;
                column.name = "edgeItem" + std::to_string(i);
                column.type.set_type(PropertyType::STRING);
                (*schema.columns_ref()).emplace_back(std::move(column));
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
                const auto& defaultValue = *ConstantExpression::make(metaPool, std::to_string(i));
                column.set_default_value(Expression::encode(defaultValue));
                (*schema.columns_ref()).emplace_back(std::move(column));
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
            ASSERT_NE(ret1.value().begin()->get_tag_id(), 0);
            ASSERT_EQ((*ret1.value().begin()->get_schema().columns_ref()).size(), 5);

            // getTagSchemaFromCache
            sleep(FLAGS_heartbeat_interval_secs + 1);
            auto ret = client->getLatestTagVersionFromCache(spaceId,
                                                            ret1.value().begin()->get_tag_id());
            CHECK(ret.ok());
            auto ver = ret.value();
            auto ret2 = client->getTagSchemaFromCache(spaceId,
                                                      ret1.value().begin()->get_tag_id(), ver);
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
            ASSERT_NE(ret1.value().begin()->get_edge_type(), 0);

            // getEdgeSchemaFromCache
            auto retVer =
                client->getLatestEdgeVersionFromCache(spaceId,
                        ret1.value().begin()->get_edge_type());
            CHECK(retVer.ok());
            auto ver = retVer.value();
            auto ret2 = client->getEdgeSchemaFromCache(spaceId,
                                                       ret1.value().begin()->get_edge_type(), ver);
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


TEST(MetaClientTest, SpaceWithGroupTest) {
    FLAGS_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/SpaceWithGroupTest.XXXXXX");

    mock::MockCluster cluster;
    cluster.startMeta(rootPath.path());
    cluster.initMetaClient();
    auto* kv = cluster.metaKV_.get();
    auto* client = cluster.metaClient_.get();

    // Prepare
    {
        {
            // Add hosts automatically, then testing listHosts interface.
            std::vector<HostAddr> addresses;
            for (int32_t i = 0; i < 10; i++) {
               addresses.emplace_back(std::to_string(i), i);
            }
            TestUtils::registerHB(kv, addresses);
            auto ret = client->listHosts().get();
            ASSERT_TRUE(ret.ok());
            for (auto i = 0u; i < addresses.size(); i++) {
                auto tHost = ret.value()[i].get_hostAddr();
                auto hostAddr = HostAddr(tHost.host, tHost.port);
                ASSERT_EQ(addresses[i], hostAddr);
            }
        }
        // Add Zone
        {
            std::vector<HostAddr> nodes = {{"0", 0}, {"1", 1}};
            auto result = client->addZone("zone_0", nodes).get();
            ASSERT_TRUE(result.ok());
        }
        {
            std::vector<HostAddr> nodes = {{"2", 2}, {"3", 3}};
            auto result = client->addZone("zone_1", nodes).get();
            ASSERT_TRUE(result.ok());
        }
        {
            std::vector<HostAddr> nodes = {{"4", 4}, {"5", 5}};
            auto result = client->addZone("zone_2", nodes).get();
            ASSERT_TRUE(result.ok());
        }
        {
            std::vector<HostAddr> nodes = {{"6", 6}, {"7", 7}};
            auto result = client->addZone("zone_3", nodes).get();
            ASSERT_TRUE(result.ok());
        }
        {
            std::vector<HostAddr> nodes = {{"8", 8}, {"9", 9}};
            auto result = client->addZone("zone_4", nodes).get();
            ASSERT_TRUE(result.ok());
        }
        // List Zones
        {
            auto result = client->listZones().get();
            ASSERT_TRUE(result.ok());
            ASSERT_EQ(5, result.value().size());
        }
        // Add Group
        {
            std::vector<std::string> zones = {"zone_0", "zone_1", "zone_2"};
            auto result = client->addGroup("group_0", std::move(zones)).get();
            ASSERT_TRUE(result.ok());
        }
        {
            std::vector<std::string> zones = {"zone_0", "zone_1", "zone_2", "zone_3", "zone_4"};
            auto result = client->addGroup("group_1", std::move(zones)).get();
            ASSERT_TRUE(result.ok());
        }
        {
           auto result = client->listGroups().get();
            ASSERT_TRUE(result.ok());
            ASSERT_EQ(2, result.value().size());
        }
    }
    // Create Space without Group
    {
        meta::cpp2::SpaceDesc spaceDesc;
        spaceDesc.set_space_name("default_space");
        spaceDesc.set_partition_num(9);
        spaceDesc.set_replica_factor(3);
        auto ret = client->createSpace(spaceDesc).get();
        ASSERT_TRUE(ret.ok()) << ret.status();

        ret = client->createSpace(spaceDesc, true).get();
        ASSERT_TRUE(ret.ok()) << ret.status();
    }
    // Create Space on group_0, replica factor is equal with zone size
    {
        meta::cpp2::SpaceDesc spaceDesc;
        spaceDesc.set_space_name("space_on_group_0_3");
        spaceDesc.set_partition_num(9);
        spaceDesc.set_replica_factor(3);
        spaceDesc.set_group_name("group_0");
        auto ret = client->createSpace(spaceDesc).get();
        ASSERT_TRUE(ret.ok()) << ret.status();

        ret = client->createSpace(spaceDesc, true).get();
        ASSERT_TRUE(ret.ok()) << ret.status();
    }
    // Drop Group should failed
    {
        auto result = client->dropGroup("group_0").get();
        ASSERT_FALSE(result.ok());
    }
    // Create Space on group_0, replica factor is less than zone size
    {
        meta::cpp2::SpaceDesc spaceDesc;
        spaceDesc.set_space_name("space_on_group_0_1");
        spaceDesc.set_partition_num(9);
        spaceDesc.set_replica_factor(1);
        spaceDesc.set_group_name("group_0");
        auto ret = client->createSpace(spaceDesc).get();
        ASSERT_TRUE(ret.ok()) << ret.status();

        ret = client->createSpace(spaceDesc, true).get();
        ASSERT_TRUE(ret.ok()) << ret.status();
    }
    // Create Space on group_0, replica factor is larger than zone size
    {
        meta::cpp2::SpaceDesc spaceDesc;
        spaceDesc.set_space_name("space_on_group_0_4");
        spaceDesc.set_partition_num(9);
        spaceDesc.set_replica_factor(4);
        spaceDesc.set_group_name("group_0");
        auto ret = client->createSpace(spaceDesc).get();
        ASSERT_FALSE(ret.ok()) << ret.status();

        ret = client->createSpace(spaceDesc, true).get();
        ASSERT_FALSE(ret.ok()) << ret.status();
    }
    {
        auto result = client->addZoneIntoGroup("zone_3", "group_0").get();
        ASSERT_TRUE(result.ok());
    }
    {
        meta::cpp2::SpaceDesc spaceDesc;
        spaceDesc.set_space_name("space_on_group_0_4");
        spaceDesc.set_partition_num(9);
        spaceDesc.set_replica_factor(4);
        spaceDesc.set_group_name("group_0");
        auto ret = client->createSpace(spaceDesc).get();
        ASSERT_TRUE(ret.ok()) << ret.status();

        ret = client->createSpace(spaceDesc, true).get();
        ASSERT_TRUE(ret.ok()) << ret.status();
    }
    // Create Space on a group which is not exist
    {
        meta::cpp2::SpaceDesc spaceDesc;
        spaceDesc.set_space_name("space_on_group_not_exist");
        spaceDesc.set_partition_num(9);
        spaceDesc.set_replica_factor(4);
        spaceDesc.set_group_name("group_not_exist");
        auto ret = client->createSpace(spaceDesc).get();
        ASSERT_FALSE(ret.ok()) << ret.status();

        ret = client->createSpace(spaceDesc, true).get();
        ASSERT_FALSE(ret.ok()) << ret.status();
    }
}

TEST(MetaClientTest, TagTest) {
    FLAGS_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/MetaClientTagTest.XXXXXX");

    mock::MockCluster cluster;
    cluster.startMeta(rootPath.path());
    cluster.initMetaClient();
    auto* kv = cluster.metaKV_.get();
    auto* client = cluster.metaClient_.get();

    TestUtils::createSomeHosts(kv);
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
        columns.emplace_back();
        columns.back().set_name("column_i");
        const auto& intValue = *ConstantExpression::make(metaPool, Value(0L));
        columns.back().set_default_value(Expression::encode(intValue));
        columns.back().type.set_type(PropertyType::INT64);

        columns.emplace_back();
        const auto& floatValue = *ConstantExpression::make(metaPool, Value(3.14));
        columns.back().set_default_value(Expression::encode(floatValue));
        columns.back().set_name("column_d");
        columns.back().type.set_type(PropertyType::DOUBLE);

        columns.emplace_back();
        const auto& strValue = *ConstantExpression::make(metaPool, "test");
        columns.back().set_default_value(Expression::encode(strValue));
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
        const auto& intValue = *ConstantExpression::make(metaPool, Value(0L));
        intColumn.set_default_value(Expression::encode(intValue));
        columns.emplace_back(std::move(intColumn));

        cpp2::ColumnDef doubleColumn;
        doubleColumn.set_name("column_d");
        doubleColumn.type.set_type(PropertyType::STRING);
        const auto& floatValue = *ConstantExpression::make(metaPool, Value(3.14));
        doubleColumn.set_default_value(Expression::encode(floatValue));
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
        ASSERT_EQ(3, (*result2.value().columns_ref()).size());
        ASSERT_EQ((*result1.value().columns_ref()).size(), (*result2.value().columns_ref()).size());
        for (auto i = 0u; i < (*result1.value().columns_ref()).size(); i++) {
            ASSERT_EQ((*result1.value().columns_ref())[i].name,
                    (*result2.value().columns_ref())[i].name);
            ASSERT_EQ((*result1.value().columns_ref())[i].type,
                    (*result2.value().columns_ref())[i].type);
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
    auto genSchema = [] (Value value, PropertyType type, bool isFunction = false) -> auto {
        std::vector<cpp2::ColumnDef> columns;
        columns.emplace_back();
        columns.back().set_name("colName");
        auto valueExpr = ConstantExpression::make(metaPool, std::move(value));
        if (isFunction) {
            ArgumentList *argList = ArgumentList::make(metaPool);
            argList->addArgument(valueExpr);
            auto fExpr = FunctionCallExpression::make(metaPool, "timestamp", argList);
            columns.back().set_default_value(Expression::encode(*fExpr));
        } else {
            columns.back().set_default_value(Expression::encode(*valueExpr));
        }
        columns.back().type.set_type(type);

        cpp2::Schema schema;
        schema.set_columns(std::move(columns));
        return schema;
    };
    // Test wrong format timestamp in default value
    {
        cpp2::Schema schema = genSchema(-10L, PropertyType::TIMESTAMP);
        auto result = client->createTagSchema(
                spaceId, "test_tag_wrong_default_timestamp1", std::move(schema)).get();
        ASSERT_FALSE(result.ok());
    }
    // Test wrong format timestamp in default value
    {
        cpp2::Schema schema = genSchema("2010-10-10X10:00:00", PropertyType::TIMESTAMP, true);
        auto result = client->createTagSchema(
                spaceId, "test_tag_wrong_default_timestamp2", std::move(schema)).get();
        ASSERT_FALSE(result.ok());
    }
    // Test right format timestamp in default value
    {
        cpp2::Schema schema = genSchema("2010-10-10T10:00:00", PropertyType::TIMESTAMP, true);
        auto result = client->createTagSchema(
                spaceId, "test_tag_right_default_timestamp2", std::move(schema)).get();
        ASSERT_TRUE(result.ok());
    }
    {
        cpp2::Schema schema = genSchema("2010-10-10 10:00:00", PropertyType::TIMESTAMP, true);
        auto result = client->createTagSchema(
                spaceId, "test_tag_wrong_default_timestamp2", std::move(schema)).get();
        ASSERT_TRUE(result.ok());
    }
    // Test out of range of int8
    {
        cpp2::Schema schema = genSchema(-129, PropertyType::INT8);
        auto result = client->createTagSchema(
                spaceId, "test_tag_less_min_in8", std::move(schema)).get();
        ASSERT_FALSE(result.ok());
    }
    // Test out of range of int8
    {
        cpp2::Schema schema = genSchema(128, PropertyType::INT8);
        auto result = client->createTagSchema(
                spaceId, "test_tag_large_max_in8", std::move(schema)).get();
        ASSERT_FALSE(result.ok());
    }
    // Test border of int8
    {
        cpp2::Schema schema = genSchema(-128, PropertyType::INT8);
        auto result = client->createTagSchema(
                spaceId, "test_tag_min_int8", std::move(schema)).get();
        ASSERT_TRUE(result.ok());
    }
    // Test border of int8
    {
        cpp2::Schema schema = genSchema(127, PropertyType::INT8);
        auto result = client->createTagSchema(
                spaceId, "test_tag_max_int8", std::move(schema)).get();
        ASSERT_TRUE(result.ok());
    }
    // Test out of range of int16
    {
        cpp2::Schema schema = genSchema(32768, PropertyType::INT16);
        auto result = client->createTagSchema(
                spaceId, "test_tag_large_max_int16", std::move(schema)).get();
        ASSERT_FALSE(result.ok());
    }
    // Test out of range of int16
    {
        cpp2::Schema schema = genSchema(-32769, PropertyType::INT16);
        auto result = client->createTagSchema(
                spaceId, "test_tag_less_min_int16", std::move(schema)).get();
        ASSERT_FALSE(result.ok());
    }
    // Test border of int16
    {
        cpp2::Schema schema = genSchema(32767, PropertyType::INT16);
        auto result = client->createTagSchema(
                spaceId, "test_tag_max_int16", std::move(schema)).get();
        ASSERT_TRUE(result.ok());
    }
    // Test border of int16
    {
        cpp2::Schema schema = genSchema(-32768, PropertyType::INT16);
        auto result = client->createTagSchema(
                spaceId, "test_tag_min_int16", std::move(schema)).get();
        ASSERT_TRUE(result.ok());
    }
    // Test out of range of int32
    {
        cpp2::Schema schema = genSchema(2147483648, PropertyType::INT32);
        auto result = client->createTagSchema(
                spaceId, "test_tag_large_max_int32", std::move(schema)).get();
        ASSERT_FALSE(result.ok());
    }
    // Test out of range of int32
    {
        cpp2::Schema schema = genSchema(-2147483649, PropertyType::INT32);
        auto result = client->createTagSchema(
                spaceId, "test_tag_less_min_int32", std::move(schema)).get();
        ASSERT_FALSE(result.ok());
    }
    // Test border of int32
    {
        cpp2::Schema schema = genSchema(2147483647, PropertyType::INT32);
        auto result = client->createTagSchema(
                spaceId, "test_tag_max_int32", std::move(schema)).get();
        ASSERT_TRUE(result.ok());
    }
    // Test border of int32
    {
        cpp2::Schema schema = genSchema(-2147483648, PropertyType::INT32);
        auto result = client->createTagSchema(
                spaceId, "test_tag_min_int32", std::move(schema)).get();
        ASSERT_TRUE(result.ok());
    }
}

TEST(MetaClientTest, EdgeTest) {
    FLAGS_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/MetaClientEdgeTest.XXXXXX");

    mock::MockCluster cluster;
    cluster.startMeta(rootPath.path());
    cluster.initMetaClient();
    auto* kv = cluster.metaKV_.get();
    auto* client = cluster.metaClient_.get();

    TestUtils::createSomeHosts(kv);
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
        const auto& intValue = *ConstantExpression::make(metaPool, Value(0L));
        intColumn.set_default_value(Expression::encode(intValue));
        columns.emplace_back(std::move(intColumn));

        cpp2::ColumnDef doubleColumn;
        doubleColumn.set_name("column_d");
        doubleColumn.type.set_type(PropertyType::DOUBLE);
        const auto& floatValue = *ConstantExpression::make(metaPool, Value(3.14));
        doubleColumn.set_default_value(Expression::encode(floatValue));
        columns.emplace_back(std::move(doubleColumn));

        cpp2::ColumnDef stringColumn;
        stringColumn.set_name("column_s");
        stringColumn.type.set_type(PropertyType::STRING);
        const auto& strValue = *ConstantExpression::make(metaPool, "test");
        stringColumn.set_default_value(Expression::encode(strValue));
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
        const auto& intValue = *ConstantExpression::make(metaPool, Value(0L));
        intColumn.set_default_value(Expression::encode(intValue));
        columns.emplace_back(std::move(intColumn));

        cpp2::ColumnDef doubleColumn;
        doubleColumn.set_name("column_d");
        doubleColumn.type.set_type(PropertyType::STRING);
        const auto& floatValue = *ConstantExpression::make(metaPool, Value(3.14));
        doubleColumn.set_default_value(Expression::encode(floatValue));
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
        ASSERT_EQ((*result1.value().columns_ref()).size(), (*result2.value().columns_ref()).size());
        for (auto i = 0u; i < (*result1.value().columns_ref()).size(); i++) {
            ASSERT_EQ((*result1.value().columns_ref())[i].name,
                    (*result2.value().columns_ref())[i].name);
            ASSERT_EQ((*result1.value().columns_ref())[i].type,
                    (*result2.value().columns_ref())[i].type);
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
    cluster.startMeta(rootPath.path());
    cluster.initMetaClient();
    auto* kv = cluster.metaKV_.get();
    auto* client = cluster.metaClient_.get();

    TestUtils::createSomeHosts(kv);
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
            column1.type.set_type(PropertyType::FIXED_STRING);
            column1.type.set_type_length(50);
            columns.emplace_back(std::move(column1));

            cpp2::Schema schema;
            schema.set_columns(std::move(columns));
            auto result = client->createTagSchema(space, folly::stringPrintf("tag_%d", i),
                                                  schema).get();
            ASSERT_TRUE(result.ok());
        }
    }
    {
        cpp2::IndexFieldDef field;
        field.set_name("tag_0_col_0");
        auto result = client->createTagIndex(space,
                                             "tag_single_field_index",
                                             "tag_0",
                                             {field}).get();
        ASSERT_TRUE(result.ok());
        singleFieldIndexID = result.value();
    }
    {
        std::vector<cpp2::IndexFieldDef> fields;
        cpp2::IndexFieldDef field1, field2;
        field1.set_name("tag_0_col_0");
        field2.set_name("tag_0_col_1");
        fields.emplace_back(std::move(field1));
        fields.emplace_back(std::move(field2));
        auto result = client->createTagIndex(space,
                                             "tag_multi_field_index",
                                             "tag_0",
                                             std::move(fields)).get();
        ASSERT_TRUE(result.ok());
        multiFieldIndexID = result.value();
    }
    {
        std::vector<cpp2::IndexFieldDef> fields;
        cpp2::IndexFieldDef field1, field2;
        field1.set_name("tag_0_col_0");
        field2.set_name("not_exist_field");
        fields.emplace_back(std::move(field1));
        fields.emplace_back(std::move(field2));

        auto result = client->createTagIndex(space,
                                             "tag_field_not_exist_index",
                                             "tag_0",
                                             std::move(fields)).get();
        ASSERT_FALSE(result.ok());
        ASSERT_EQ(Status::Error("not existed!"), result.status());
    }
    {
        std::vector<cpp2::IndexFieldDef> fields;
        cpp2::IndexFieldDef field1, field2;
        field1.set_name("tag_0_col_0");
        field2.set_name("tag_0_col_1");
        fields.emplace_back(std::move(field1));
        fields.emplace_back(std::move(field2));

        auto result = client->createTagIndex(space,
                                             "tag_not_exist_index",
                                             "tag_not_exist",
                                             std::move(fields)).get();
        ASSERT_FALSE(result.ok());
        ASSERT_EQ(Status::Error("not existed!"), result.status());
    }
    {
        std::vector<cpp2::IndexFieldDef> fields;
        cpp2::IndexFieldDef field1, field2;
        field1.set_name("tag_0_col_0");
        field2.set_name("tag_0_col_0");
        fields.emplace_back(std::move(field1));
        fields.emplace_back(std::move(field2));

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
            stringColumn.type.set_type(PropertyType::FIXED_STRING);
            stringColumn.type.set_type_length(50);
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
        ASSERT_FALSE(checkTagIndexNotExist.ok());

        auto checkSpaceNotExist = client->checkTagIndexed(spaceNotExist, singleFieldIndexID);
        ASSERT_FALSE(checkSpaceNotExist.ok());

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
    }
    {
        auto result = client->dropTagIndex(space, "tag_single_field_index").get();
        ASSERT_FALSE(result.ok());
    }
}

TEST(MetaClientTest, EdgeIndexTest) {
    FLAGS_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/MetaClientEdgeIndexTest.XXXXXX");

    mock::MockCluster cluster;
    cluster.startMeta(rootPath.path());
    cluster.initMetaClient();
    auto* kv = cluster.metaKV_.get();
    auto* client = cluster.metaClient_.get();

    TestUtils::createSomeHosts(kv);
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
            column1.type.set_type(PropertyType::FIXED_STRING);
            column1.type.set_type_length(50);
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
        cpp2::IndexFieldDef field;
        field.set_name("edge_0_col_0");
        auto result = client->createEdgeIndex(space,
                                              "edge_single_field_index",
                                              "edge_0",
                                              {field}).get();
        ASSERT_TRUE(result.ok());
        singleFieldIndexID = result.value();
    }
    {
        std::vector<cpp2::IndexFieldDef> fields;
        cpp2::IndexFieldDef field1, field2;
        field1.set_name("edge_0_col_0");
        field2.set_name("edge_0_col_1");
        fields.emplace_back(std::move(field1));
        fields.emplace_back(std::move(field2));
        auto result = client->createEdgeIndex(space,
                                              "edge_multi_field_index",
                                              "edge_0",
                                              std::move(fields)).get();
        ASSERT_TRUE(result.ok());
        multiFieldIndexID = result.value();
    }
    {
        std::vector<cpp2::IndexFieldDef> fields;
        cpp2::IndexFieldDef field1, field2;
        field1.set_name("edge_0_col_0");
        field2.set_name("edge_0_col_1");
        fields.emplace_back(std::move(field1));
        fields.emplace_back(std::move(field2));
        auto result = client->createEdgeIndex(space,
                                              "edge_not_exist_index",
                                              "edge_not_exist",
                                              std::move(fields)).get();
        ASSERT_FALSE(result.ok());
    }
    {
        std::vector<cpp2::IndexFieldDef> fields;
        cpp2::IndexFieldDef field1, field2;
        field1.set_name("edge_0_col_0");
        field2.set_name("edge_0_col_0");
        fields.emplace_back(std::move(field1));
        fields.emplace_back(std::move(field2));
        auto result = client->createEdgeIndex(space,
                                              "edge_conflict_index",
                                              "edge_0",
                                              std::move(fields)).get();
        ASSERT_FALSE(result.ok());
        ASSERT_EQ(Status::Error("conflict"), result.status());
    }
    {
        std::vector<cpp2::IndexFieldDef> fields;
        cpp2::IndexFieldDef field1, field2;
        field1.set_name("edge_0_col_0");
        field2.set_name("not_exist_field");
        fields.emplace_back(std::move(field1));
        fields.emplace_back(std::move(field2));
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
            stringColumn.type.set_type(PropertyType::FIXED_STRING);
            stringColumn.type.set_type_length(50);
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
        ASSERT_FALSE(checkEdgeIndexNotExist.ok());

        auto checkSpaceNotExist = client->checkEdgeIndexed(spaceNotExist, singleFieldIndexID);
        ASSERT_FALSE(checkSpaceNotExist.ok());

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
    }
    {
        auto result = client->dropEdgeIndex(space, "edge_single_field_index").get();
        ASSERT_FALSE(result.ok());
    }
}

TEST(MetaClientTest, GroupAndZoneTest) {
    FLAGS_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/GroupAndZoneTest.XXXXXX");

    mock::MockCluster cluster;
    cluster.startMeta(rootPath.path());
    cluster.initMetaClient();
    auto* kv = cluster.metaKV_.get();
    auto* client = cluster.metaClient_.get();
    std::vector<HostAddr> hosts;
    for (int32_t i = 0; i < 13; i++) {
        hosts.emplace_back(std::to_string(i), i);
    }
    TestUtils::createSomeHosts(kv, std::move(hosts));

    // Add Zone
    {
        std::vector<HostAddr> nodes = {{"0", 0}, {"1", 1}, {"2", 2}};
        auto result = client->addZone("zone_0", nodes).get();
        ASSERT_TRUE(result.ok());
    }
    {
        std::vector<HostAddr> nodes = {{"3", 3}, {"4", 4}, {"5", 5}};
        auto result = client->addZone("zone_1", nodes).get();
        ASSERT_TRUE(result.ok());
    }
    {
        std::vector<HostAddr> nodes = {{"6", 6}, {"7", 7}, {"8", 8}};
        auto result = client->addZone("zone_2", nodes).get();
        ASSERT_TRUE(result.ok());
    }
    // Host have overlap
    {
        std::vector<HostAddr> nodes = {{"8", 8}, {"9", 9}, {"10", 10}};
        auto result = client->addZone("zone_3", nodes).get();
        ASSERT_FALSE(result.ok());
    }
    // Add Zone with empty node list
    {
        std::vector<HostAddr> nodes;
        auto result = client->addZone("zone_0", nodes).get();
        ASSERT_FALSE(result.ok());
    }
    // Add Zone with duplicate node
    {
        std::vector<HostAddr> nodes = {{"0", 0}, {"0", 0}};
        auto result = client->addZone("zone_0", nodes).get();
        ASSERT_FALSE(result.ok());
    }
    // Add Zone which node not exist
    {
        std::vector<HostAddr> nodes = {{"zone_not_exist", 0}};
        auto result = client->addZone("zone_4", nodes).get();
        ASSERT_FALSE(result.ok());
    }
    // Add Zone already existed
    {
        std::vector<HostAddr> nodes = {{"0", 0}, {"1", 1}, {"2", 2}};
        auto result = client->addZone("zone_0", nodes).get();
        ASSERT_FALSE(result.ok());
    }
    // Get Zone
    {
        auto result = client->getZone("zone_0").get();
        ASSERT_TRUE(result.ok());
    }
    // Get Zone which is not exist
    {
        auto result = client->getZone("zone_not_exist").get();
        ASSERT_FALSE(result.ok());
    }
    // List Zones
    {
        auto result = client->listZones().get();
        ASSERT_TRUE(result.ok());
        ASSERT_EQ(3, result.value().size());
    }
    // Add host into zone
    {
        HostAddr node("12", 12);
        auto result = client->addHostIntoZone(node, "zone_0").get();
        ASSERT_TRUE(result.ok());
    }
    // Add host into zone overlap with another zone
    {
        HostAddr node("3", 3);
        auto result = client->addHostIntoZone(node, "zone_0").get();
        ASSERT_FALSE(result.ok());
    }
    // Add host into zone which zone is not exist
    {
        HostAddr node("4", 4);
        auto result = client->addHostIntoZone(node, "zone_not_exist").get();
        ASSERT_FALSE(result.ok());
    }
    // Add host into zone which the node have existed
    {
        HostAddr node("0", 0);
        auto result = client->addHostIntoZone(node, "zone_0").get();
        ASSERT_FALSE(result.ok());
    }
    // Add host into zone which the node not existed
    {
        HostAddr node("99", 99);
        auto result = client->addHostIntoZone(node, "zone_0").get();
        ASSERT_FALSE(result.ok());
    }
    // Drop host from zone
    {
        HostAddr node("12", 12);
        auto result = client->dropHostFromZone(node, "zone_0").get();
        ASSERT_TRUE(result.ok());
    }
    // Drop host from zone which zone is not exist
    {
        HostAddr node("4", 4);
        auto result = client->dropHostFromZone(node, "zone_not_exist").get();
        ASSERT_FALSE(result.ok());
    }
    // Drop host from zone which the node not exist
    {
        HostAddr node("4", 4);
        auto result = client->dropHostFromZone(node, "zone_0").get();
        ASSERT_FALSE(result.ok());
    }
    // Add Group
    {
        std::vector<std::string> zones = {"zone_0", "zone_1", "zone_2"};
        auto result = client->addGroup("group_0", std::move(zones)).get();
        ASSERT_TRUE(result.ok());
    }
    // Add Group with empty zone name list
    {
        std::vector<std::string> zones = {};
        auto result = client->addGroup("group_0", std::move(zones)).get();
        ASSERT_FALSE(result.ok());
    }
    // Add Group with duplicate zone name
    {
        std::vector<std::string> zones = {"zone_0", "zone_0", "zone_2"};
        auto result = client->addGroup("group_0", std::move(zones)).get();
        ASSERT_FALSE(result.ok());
    }
    // Add Group already existed
    {
        std::vector<std::string> zones = {"zone_0", "zone_1", "zone_2"};
        auto result = client->addGroup("group_0", std::move(zones)).get();
        ASSERT_FALSE(result.ok());
    }
    {
        std::vector<std::string> zones = {"zone_1", "zone_2"};
        auto result = client->addGroup("group_1", std::move(zones)).get();
        ASSERT_TRUE(result.ok());
    }
    // Get Group
    {
        auto result = client->getGroup("group_0").get();
        ASSERT_TRUE(result.ok());
    }
    // Get Group which is not exist
    {
        auto result = client->getGroup("group_not_exist").get();
        ASSERT_FALSE(result.ok());
    }
    // List Groups
    {
        auto result = client->listGroups().get();
        ASSERT_TRUE(result.ok());
    }
    {
        std::vector<HostAddr> nodes = {{"9", 9}, {"10", 10}, {"11", 11}};
        auto result = client->addZone("zone_3", nodes).get();
        ASSERT_TRUE(result.ok());
    }
    // Add zone into group
    {
        auto result = client->addZoneIntoGroup("zone_3", "group_0").get();
        ASSERT_TRUE(result.ok());
    }
    // Add zone into group which group not exist
    {
        auto result = client->addZoneIntoGroup("zone_0", "group_not_exist").get();
        ASSERT_FALSE(result.ok());
    }
    // Add zone into group which zone already exist
    {
        auto result = client->addZoneIntoGroup("zone_0", "group_0").get();
        ASSERT_FALSE(result.ok());
    }
    // Add zone into group which zone not exist
    {
        auto result = client->addZoneIntoGroup("zone_not_exist", "group_0").get();
        ASSERT_FALSE(result.ok());
    }
    // Drop zone from group
    {
        auto result = client->dropZoneFromGroup("zone_3", "group_0").get();
        ASSERT_TRUE(result.ok());
    }
    // Drop zone from group which group not exist
    {
        auto result = client->dropZoneFromGroup("zone_0", "group_not_exist").get();
        ASSERT_FALSE(result.ok());
    }
    // Drop zone from group which zone not exist
    {
        auto result = client->dropZoneFromGroup("zone_not_exist", "group_0").get();
        ASSERT_FALSE(result.ok());
    }
    // Drop Group
    {
        auto result = client->dropGroup("group_0").get();
        ASSERT_TRUE(result.ok());
    }
    // Drop Group which is not exist
    {
        auto result = client->dropGroup("group_0").get();
        ASSERT_FALSE(result.ok());
    }
    // Drop Zone
    {
        auto result = client->dropZone("zone_0").get();
        ASSERT_TRUE(result.ok());
    }
    // Drop Zone which is not exist
    {
        auto result = client->dropZone("zone_0").get();
        ASSERT_FALSE(result.ok());
    }
}

TEST(MetaClientTest, FTServiceTest) {
    FLAGS_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/FTServiceTest.XXXXXX");

    mock::MockCluster cluster;
    cluster.startMeta(rootPath.path());
    uint32_t localMetaPort = cluster.metaServer_->port_;
    auto* kv = cluster.metaKV_.get();
    auto localIp = cluster.localIP();

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    auto localhosts = std::vector<HostAddr>{HostAddr(localIp, localMetaPort)};
    auto client = std::make_shared<MetaClient>(threadPool, localhosts);
    client->waitForMetadReady();

    std::vector<HostAddr> hosts = {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}};
    TestUtils::registerHB(kv, hosts);

    std::vector<cpp2::FTClient> clients;
    cpp2::FTClient c1, c2;
    c1.set_host({"0", 0});
    c1.set_user("u1");
    c1.set_pwd("pwd");
    clients.emplace_back(c1);
    c2.set_host({"1", 1});
    c2.set_user("u2");
    clients.emplace_back(c2);
    {
        cpp2::FTServiceType type = cpp2::FTServiceType::ELASTICSEARCH;
        auto result = client->signInFTService(type, clients).get();
        ASSERT_TRUE(result.ok());
    }
    {
        auto result = client->listFTClients().get();
        ASSERT_TRUE(result.ok());
        ASSERT_EQ(clients, result.value());
    }
    {
        auto result = client->signOutFTService().get();
        ASSERT_TRUE(result.ok());
    }
    {
        auto result = client->listFTClients().get();
        ASSERT_TRUE(result.ok());
        ASSERT_TRUE(result.value().empty());
    }
}

class TestListener : public MetaChangedListener {
public:
    virtual ~TestListener() = default;
    void onSpaceAdded(GraphSpaceID spaceId, bool isListener) override {
        LOG(INFO) << "Space " << spaceId << " added";
        if (!isListener) {
            spaceNum++;
        } else {
            listenerSpaceNum++;
        }
    }

    void onSpaceRemoved(GraphSpaceID spaceId, bool isListener) override {
        LOG(INFO) << "Space " << spaceId << " removed";
        if (!isListener) {
            spaceNum--;
        } else {
            listenerSpaceNum--;
        }
    }

    void onPartAdded(const PartHosts& partMeta) override {
        LOG(INFO) << "[" << partMeta.spaceId_ << ", " << partMeta.partId_ << "] added!";
        partNum++;
    }

    void onSpaceOptionUpdated(GraphSpaceID,
                              const std::unordered_map<std::string, std::string>& update)
                              override {
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
                                            std::vector<cpp2::LeaderInfo>>&) override {
        LOG(INFO) << "Get leader distribution!";
    }

    void onListenerAdded(GraphSpaceID spaceId,
                         PartitionID partId,
                         const ListenerHosts& listenerHosts) override {
        UNUSED(spaceId); UNUSED(partId); UNUSED(listenerHosts);
        listenerPartNum++;
    }

    void onListenerRemoved(GraphSpaceID spaceId,
                           PartitionID partId,
                           cpp2::ListenerType type) override {
        UNUSED(spaceId); UNUSED(partId); UNUSED(type);
        listenerPartNum--;
    }

    void onCheckRemoteListeners(GraphSpaceID spaceId,
                                PartitionID partId,
                                const std::vector<HostAddr>& remoteListeners) override {
        UNUSED(spaceId); UNUSED(partId); UNUSED(remoteListeners);
    }

    int32_t spaceNum = 0;
    int32_t partNum = 0;
    int32_t partChanged = 0;
    std::unordered_map<std::string, std::string> options;
    int32_t listenerSpaceNum = 0;
    int32_t listenerPartNum = 0;
};

TEST(MetaClientTest, DiffTest) {
    FLAGS_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/MetaClientDiffTest.XXXXXX");

    mock::MockCluster cluster;
    cluster.startMeta(rootPath.path());
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
            auto tHost = ret.value()[i].get_hostAddr();
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
}

TEST(MetaClientTest, ListenerDiffTest) {
    FLAGS_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/MetaClientTest.XXXXXX");

    mock::MockCluster cluster;
    cluster.startMeta(rootPath.path());
    meta::MetaClientOptions options;
    options.localHost_ = {"", 0};
    options.role_ = meta::cpp2::HostRole::STORAGE;
    cluster.initMetaClient(options);
    auto* kv = cluster.metaKV_.get();
    auto* console = cluster.metaClient_.get();
    auto testListener = std::make_unique<TestListener>();
    console->registerListener(testListener.get());

    // create another meta client for listener host
    HostAddr listenerHost("listener", 0);
    options.localHost_ = listenerHost;
    options.role_ = meta::cpp2::HostRole::UNKNOWN;
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    auto metaAddrs = {HostAddr(cluster.localIP(), cluster.metaServer_->port_)};
    auto client = std::make_unique<meta::MetaClient>(threadPool, metaAddrs, options);
    client->waitForMetadReady();

    auto listener = std::make_unique<TestListener>();
    client->registerListener(listener.get());

    // register HB for storage
    std::vector<HostAddr> hosts = {{"", 0}};
    TestUtils::registerHB(kv, hosts);
    {
        // create two space
        meta::cpp2::SpaceDesc spaceDesc;
        spaceDesc.set_space_name("listener_space");
        spaceDesc.set_partition_num(9);
        spaceDesc.set_replica_factor(1);
        auto ret = console->createSpace(spaceDesc).get();
        ASSERT_TRUE(ret.ok()) << ret.status();
        auto spaceId = ret.value();

        spaceDesc.set_space_name("no_listener_space");
        ret = console->createSpace(spaceDesc).get();
        ASSERT_TRUE(ret.ok()) << ret.status();

        sleep(FLAGS_heartbeat_interval_secs + 1);
        ASSERT_EQ(0, listener->spaceNum);
        ASSERT_EQ(0, listener->partNum);
        ASSERT_EQ(0, listener->listenerSpaceNum);
        ASSERT_EQ(0, listener->listenerPartNum);

        // add listener hosts to space and check num
        auto addRet = console->addListener(spaceId,
                                           cpp2::ListenerType::ELASTICSEARCH,
                                           {listenerHost}).get();
        ASSERT_TRUE(addRet.ok()) << addRet.status();
        sleep(FLAGS_heartbeat_interval_secs + 1);
        ASSERT_EQ(0, listener->spaceNum);
        ASSERT_EQ(0, listener->partNum);
        ASSERT_EQ(1, listener->listenerSpaceNum);
        ASSERT_EQ(9, listener->listenerPartNum);

        // drop other space should not effect listener space
        auto dropRet = console->dropSpace("no_listener_space").get();
        ASSERT_TRUE(dropRet.ok()) << dropRet.status();
        sleep(FLAGS_heartbeat_interval_secs + 1);
        ASSERT_EQ(0, listener->spaceNum);
        ASSERT_EQ(0, listener->partNum);
        ASSERT_EQ(1, listener->listenerSpaceNum);
        ASSERT_EQ(9, listener->listenerPartNum);

        // remove listener hosts from space
        auto removeRet = console->removeListener(spaceId, cpp2::ListenerType::ELASTICSEARCH).get();
        ASSERT_TRUE(removeRet.ok()) << removeRet.status();
        sleep(FLAGS_heartbeat_interval_secs + 1);
        ASSERT_EQ(0, listener->spaceNum);
        ASSERT_EQ(0, listener->partNum);
        ASSERT_EQ(0, listener->listenerSpaceNum);
        ASSERT_EQ(0, listener->listenerPartNum);

        // add listener again
        addRet = console->addListener(spaceId,
                                      cpp2::ListenerType::ELASTICSEARCH,
                                      {listenerHost}).get();
        ASSERT_TRUE(addRet.ok()) << addRet.status();
        sleep(FLAGS_heartbeat_interval_secs + 1);
        ASSERT_EQ(0, listener->spaceNum);
        ASSERT_EQ(0, listener->partNum);
        ASSERT_EQ(1, listener->listenerSpaceNum);
        ASSERT_EQ(9, listener->listenerPartNum);

        // drop listener space
        dropRet = console->dropSpace("listener_space").get();
        ASSERT_TRUE(dropRet.ok()) << dropRet.status();
        sleep(FLAGS_heartbeat_interval_secs + 1);
        ASSERT_EQ(0, listener->spaceNum);
        ASSERT_EQ(0, listener->partNum);
        ASSERT_EQ(0, listener->listenerSpaceNum);
        ASSERT_EQ(0, listener->listenerPartNum);
    }
    client->unRegisterListener();
}

TEST(MetaClientTest, HeartbeatTest) {
    FLAGS_heartbeat_interval_secs = 1;
    const nebula::ClusterID kClusterId = 10;
    fs::TempDir rootPath("/tmp/HeartbeatTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.startMeta(rootPath.path());

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
            auto tHost = ret.value()[i].get_hostAddr();
            auto hostAddr = HostAddr(tHost.host, tHost.port);
            ASSERT_EQ(hosts[i], hostAddr);
        }
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    auto hostsRet =  ActiveHostsMan::getActiveHosts(cluster.metaKV_.get());;
    ASSERT_TRUE(nebula::ok(hostsRet));
    ASSERT_EQ(1, nebula::value(hostsRet).size());
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
        resp.set_code(nebula::cpp2::ErrorCode::SUCCEEDED);
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
            resp.set_code(nebula::cpp2::ErrorCode::SUCCEEDED);
        } else {
            resp.set_code(nebula::cpp2::ErrorCode::E_LEADER_CHANGED);
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
    auto localIp = "127.0.0.1";

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
    cluster.startMeta(rootPath.path());

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

TEST(MetaClientTest, ListenerTest) {
    FLAGS_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/MetaClientListenerTest.XXXXXX");

    mock::MockCluster cluster;
    cluster.startMeta(rootPath.path(), HostAddr("127.0.0.1", 0));
    uint32_t localMetaPort = cluster.metaServer_->port_;
    auto* kv = cluster.metaKV_.get();
    auto localIp = cluster.localIP();

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    auto localhosts = std::vector<HostAddr>{HostAddr(localIp, localMetaPort)};
    auto client = std::make_shared<MetaClient>(threadPool, localhosts);
    client->waitForMetadReady();

    TestUtils::createSomeHosts(kv);
    meta::cpp2::SpaceDesc spaceDesc;
    spaceDesc.set_space_name("default");
    spaceDesc.set_partition_num(9);
    spaceDesc.set_replica_factor(3);
    auto ret = client->createSpace(spaceDesc).get();
    ASSERT_TRUE(ret.ok()) << ret.status();
    GraphSpaceID space = ret.value();
    std::vector<HostAddr> listenerHosts = {{"1", 0}, {"1", 1}, {"1", 2}, {"1", 3}};
    {
        TestUtils::setupHB(
            kv, listenerHosts, cpp2::HostRole::LISTENER, gitInfoSha());
        auto addRet =
            client->addListener(space, cpp2::ListenerType::ELASTICSEARCH, listenerHosts).get();
        ASSERT_TRUE(addRet.ok()) << addRet.status();
    }
    {
        auto listRet = client->listListener(space).get();
        ASSERT_TRUE(listRet.ok()) << listRet.status();
        auto listeners = listRet.value();
        ASSERT_EQ(9, listeners.size());
        std::vector<cpp2::ListenerInfo> expected;
        for (size_t i = 0; i < 9; i++) {
            cpp2::ListenerInfo l;
            l.set_type(cpp2::ListenerType::ELASTICSEARCH);
            l.set_host(listenerHosts[i % 4]);
            l.set_part_id(i + 1);
            l.set_status(cpp2::HostStatus::ONLINE);
            expected.emplace_back(std::move(l));
        }
        ASSERT_EQ(expected, listeners);
    }
    {
       auto removeRet = client->removeListener(space, cpp2::ListenerType::ELASTICSEARCH).get();
       ASSERT_TRUE(removeRet.ok()) << removeRet.status();
    }
    {
        auto listRet = client->listListener(space).get();
        ASSERT_TRUE(listRet.ok()) << listRet.status();
        auto listeners = listRet.value();
        ASSERT_EQ(0, listeners.size());
    }
}

TEST(MetaClientTest, RocksdbOptionsTest) {
    FLAGS_heartbeat_interval_secs = 1;
    fs::TempDir rootPath("/tmp/RocksdbOptionsTest.XXXXXX");

    mock::MockCluster cluster;
    cluster.startMeta(rootPath.path(), HostAddr("127.0.0.1", 0));

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
        ASSERT_EQ(listener->options["disable_auto_compactions"], "\"true\"");
        ASSERT_EQ(listener->options["level0_file_num_compaction_trigger"], "\"4\"");
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
