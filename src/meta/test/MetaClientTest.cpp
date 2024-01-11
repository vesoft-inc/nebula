/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>
#include <rocksdb/db.h>

#include "clients/meta/MetaClient.h"
#include "common/base/Base.h"
#include "common/conf/Configuration.h"
#include "common/expression/ArithmeticExpression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/fs/TempDir.h"
#include "common/meta/GflagsManager.h"
#include "common/meta/ServerBasedSchemaManager.h"
#include "common/network/NetworkUtils.h"
#include "common/time/TimezoneInfo.h"
#include "common/utils/MetaKeyUtils.h"
#include "interface/gen-cpp2/common_constants.h"
#include "meta/processors/zone/AddHostsProcessor.h"
#include "meta/test/TestUtils.h"
#include "mock/MockCluster.h"

DECLARE_int32(heartbeat_interval_secs);
DECLARE_string(rocksdb_db_options);
DECLARE_bool(enable_client_white_list);
DECLARE_string(client_white_list);

namespace nebula {
namespace meta {

using nebula::Value;
using nebula::cpp2::PropertyType;

TEST(MetaClientTest, InterfacesTest) {
  FLAGS_heartbeat_interval_secs = 1;
  fs::TempDir rootPath("/tmp/MetaClientTest.XXXXXX");

  mock::MockCluster cluster;
  cluster.startMeta(rootPath.path());
  auto options = meta::MetaClientOptions();
  cluster.initMetaClient();
  auto* client = cluster.metaClient_.get();

  GraphSpaceID spaceId = 0;
  {
    std::vector<HostAddr> hosts = {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}};
    auto result = client->addHosts(hosts).get();
    TestUtils::registerHB(cluster.metaKV_.get(), hosts);
    EXPECT_TRUE(result.ok());
  }
  {
    // Test createSpace, listSpaces, getPartsAlloc.
    {
      meta::cpp2::SpaceDesc spaceDesc;
      spaceDesc.space_name_ref() = "default_space";
      spaceDesc.partition_num_ref() = 8;
      spaceDesc.replica_factor_ref() = 3;
      spaceDesc.charset_name_ref() = "utf8";
      spaceDesc.collate_name_ref() = "utf8_bin";
      auto ret = client->createSpace(spaceDesc).get();
      ASSERT_TRUE(ret.ok()) << ret.status();
      spaceId = ret.value();

      ret = client->createSpace(spaceDesc, true).get();
      ASSERT_TRUE(ret.ok()) << ret.status();
    }
    {
      meta::cpp2::SpaceDesc spaceDesc;
      spaceDesc.space_name_ref() = "default_space";
      spaceDesc.partition_num_ref() = 8;
      spaceDesc.replica_factor_ref() = 3;
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
      for (auto i = 0; i < 5; i++) {
        cpp2::ColumnDef column;
        column.name = "tagItem" + std::to_string(i);
        column.type.type_ref() = PropertyType::STRING;
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
      for (auto i = 0; i < 5; i++) {
        cpp2::ColumnDef column;
        column.name = "tagItem" + std::to_string(i);
        column.type.type_ref() = PropertyType::STRING;
        const auto& defaultValue = *ConstantExpression::make(metaPool, std::to_string(i));
        column.default_value_ref() = Expression::encode(defaultValue);
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
      column.type.type_ref() = PropertyType::STRING;
      column.nullable_ref() = true;
      const auto& defaultValue = *ArithmeticExpression::makeDivision(
          metaPool, ConstantExpression::make(metaPool, 1), ConstantExpression::make(metaPool, 0));
      column.default_value_ref() = Expression::encode(defaultValue);
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
        column.type.type_ref() = PropertyType::STRING;
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
      for (auto i = 0; i < 5; i++) {
        cpp2::ColumnDef column;
        column.name = "edgeItem" + std::to_string(i);
        column.type.type_ref() = PropertyType::STRING;
        const auto& defaultValue = *ConstantExpression::make(metaPool, std::to_string(i));
        column.default_value_ref() = Expression::encode(defaultValue);
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
      auto ret = client->getLatestTagVersionFromCache(spaceId, ret1.value().begin()->get_tag_id());
      CHECK(ret.ok());
      auto ver = ret.value();
      auto ret2 = client->getTagSchemaFromCache(spaceId, ret1.value().begin()->get_tag_id(), ver);
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
          client->getLatestEdgeVersionFromCache(spaceId, ret1.value().begin()->get_edge_type());
      CHECK(retVer.ok());
      auto ver = retVer.value();
      auto ret2 =
          client->getEdgeSchemaFromCache(spaceId, ret1.value().begin()->get_edge_type(), ver);
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
    auto ret = client->dropSpace("default_space").get();
    ASSERT_TRUE(ret.ok());
    auto ret1 = client->listSpaces().get();
    ASSERT_TRUE(ret1.ok()) << ret1.status();
    ASSERT_EQ(0, ret1.value().size());
  }
  cluster.stop();
}

TEST(MetaClientTest, TagTest) {
  FLAGS_heartbeat_interval_secs = 1;
  fs::TempDir rootPath("/tmp/MetaClientTagTest.XXXXXX");

  mock::MockCluster cluster;
  cluster.startMeta(rootPath.path());
  cluster.initMetaClient();
  auto* client = cluster.metaClient_.get();
  {
    std::vector<HostAddr> hosts = {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}};
    auto result = client->addHosts(hosts).get();
    TestUtils::registerHB(cluster.metaKV_.get(), hosts);
    EXPECT_TRUE(result.ok());
  }
  meta::cpp2::SpaceDesc spaceDesc;
  spaceDesc.space_name_ref() = "default";
  spaceDesc.partition_num_ref() = 9;
  spaceDesc.replica_factor_ref() = 3;
  auto ret = client->createSpace(spaceDesc).get();
  ASSERT_TRUE(ret.ok()) << ret.status();
  GraphSpaceID spaceId = ret.value();
  TagID id;
  int64_t version;

  {
    std::vector<cpp2::ColumnDef> columns;
    columns.emplace_back();
    columns.back().name_ref() = "column_i";
    const auto& intValue = *ConstantExpression::make(metaPool, Value(0L));
    columns.back().default_value_ref() = Expression::encode(intValue);
    columns.back().type.type_ref() = PropertyType::INT64;

    columns.emplace_back();
    const auto& floatValue = *ConstantExpression::make(metaPool, Value(3.14));
    columns.back().default_value_ref() = Expression::encode(floatValue);
    columns.back().name_ref() = "column_d";
    columns.back().type.type_ref() = PropertyType::DOUBLE;

    columns.emplace_back();
    const auto& strValue = *ConstantExpression::make(metaPool, "test");
    columns.back().default_value_ref() = Expression::encode(strValue);
    columns.back().name_ref() = "column_s";
    columns.back().type.type_ref() = PropertyType::STRING;

    cpp2::Schema schema;
    schema.columns_ref() = std::move(columns);
    auto result = client->createTagSchema(spaceId, "test_tag", std::move(schema)).get();
    ASSERT_TRUE(result.ok());
    id = result.value();
  }
  {
    std::vector<cpp2::ColumnDef> columns;
    cpp2::ColumnDef intColumn;
    intColumn.name_ref() = "column_i";
    intColumn.type.type_ref() = PropertyType::INT64;
    const auto& intValue = *ConstantExpression::make(metaPool, Value(0L));
    intColumn.default_value_ref() = Expression::encode(intValue);
    columns.emplace_back(std::move(intColumn));

    cpp2::ColumnDef doubleColumn;
    doubleColumn.name_ref() = "column_d";
    doubleColumn.type.type_ref() = PropertyType::STRING;
    const auto& floatValue = *ConstantExpression::make(metaPool, Value(3.14));
    doubleColumn.default_value_ref() = Expression::encode(floatValue);
    columns.emplace_back(std::move(doubleColumn));

    cpp2::Schema schema;
    schema.columns_ref() = columns;

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
      ASSERT_EQ((*result1.value().columns_ref())[i].name, (*result2.value().columns_ref())[i].name);
      ASSERT_EQ((*result1.value().columns_ref())[i].type, (*result2.value().columns_ref())[i].type);
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
  auto genSchema = [](Value value, PropertyType type, bool isFunction = false) -> auto {
    std::vector<cpp2::ColumnDef> columns;
    columns.emplace_back();
    columns.back().name_ref() = "colName";
    auto valueExpr = ConstantExpression::make(metaPool, std::move(value));
    if (isFunction) {
      ArgumentList* argList = ArgumentList::make(metaPool);
      argList->addArgument(valueExpr);
      auto fExpr = FunctionCallExpression::make(metaPool, "timestamp", argList);
      columns.back().default_value_ref() = Expression::encode(*fExpr);
    } else {
      columns.back().default_value_ref() = Expression::encode(*valueExpr);
    }
    columns.back().type.type_ref() = type;

    cpp2::Schema schema;
    schema.columns_ref() = std::move(columns);
    return schema;
  };
  // Test wrong format timestamp in default value
  {
    cpp2::Schema schema = genSchema(-10L, PropertyType::TIMESTAMP);
    auto result =
        client->createTagSchema(spaceId, "test_tag_wrong_default_timestamp1", std::move(schema))
            .get();
    ASSERT_FALSE(result.ok());
  }
  // Test wrong format timestamp in default value
  {
    cpp2::Schema schema = genSchema("2010-10-10X10:00:00", PropertyType::TIMESTAMP, true);
    auto result =
        client->createTagSchema(spaceId, "test_tag_wrong_default_timestamp2", std::move(schema))
            .get();
    ASSERT_FALSE(result.ok());
  }
  // Test right format timestamp in default value
  {
    cpp2::Schema schema = genSchema("2010-10-10T10:00:00", PropertyType::TIMESTAMP, true);
    auto result =
        client->createTagSchema(spaceId, "test_tag_right_default_timestamp2", std::move(schema))
            .get();
    ASSERT_TRUE(result.ok());
  }
  {
    cpp2::Schema schema = genSchema("2010-10-10 10:00:00", PropertyType::TIMESTAMP, true);
    auto result =
        client->createTagSchema(spaceId, "test_tag_wrong_default_timestamp2", std::move(schema))
            .get();
    ASSERT_TRUE(result.ok());
  }
  // Test out of range of int8
  {
    cpp2::Schema schema = genSchema(-129, PropertyType::INT8);
    auto result =
        client->createTagSchema(spaceId, "test_tag_less_min_in8", std::move(schema)).get();
    ASSERT_FALSE(result.ok());
  }
  // Test out of range of int8
  {
    cpp2::Schema schema = genSchema(128, PropertyType::INT8);
    auto result =
        client->createTagSchema(spaceId, "test_tag_large_max_in8", std::move(schema)).get();
    ASSERT_FALSE(result.ok());
  }
  // Test border of int8
  {
    cpp2::Schema schema = genSchema(-128, PropertyType::INT8);
    auto result = client->createTagSchema(spaceId, "test_tag_min_int8", std::move(schema)).get();
    ASSERT_TRUE(result.ok());
  }
  // Test border of int8
  {
    cpp2::Schema schema = genSchema(127, PropertyType::INT8);
    auto result = client->createTagSchema(spaceId, "test_tag_max_int8", std::move(schema)).get();
    ASSERT_TRUE(result.ok());
  }
  // Test out of range of int16
  {
    cpp2::Schema schema = genSchema(32768, PropertyType::INT16);
    auto result =
        client->createTagSchema(spaceId, "test_tag_large_max_int16", std::move(schema)).get();
    ASSERT_FALSE(result.ok());
  }
  // Test out of range of int16
  {
    cpp2::Schema schema = genSchema(-32769, PropertyType::INT16);
    auto result =
        client->createTagSchema(spaceId, "test_tag_less_min_int16", std::move(schema)).get();
    ASSERT_FALSE(result.ok());
  }
  // Test border of int16
  {
    cpp2::Schema schema = genSchema(32767, PropertyType::INT16);
    auto result = client->createTagSchema(spaceId, "test_tag_max_int16", std::move(schema)).get();
    ASSERT_TRUE(result.ok());
  }
  // Test border of int16
  {
    cpp2::Schema schema = genSchema(-32768, PropertyType::INT16);
    auto result = client->createTagSchema(spaceId, "test_tag_min_int16", std::move(schema)).get();
    ASSERT_TRUE(result.ok());
  }
  // Test out of range of int32
  {
    cpp2::Schema schema = genSchema(2147483648, PropertyType::INT32);
    auto result =
        client->createTagSchema(spaceId, "test_tag_large_max_int32", std::move(schema)).get();
    ASSERT_FALSE(result.ok());
  }
  // Test out of range of int32
  {
    cpp2::Schema schema = genSchema(-2147483649, PropertyType::INT32);
    auto result =
        client->createTagSchema(spaceId, "test_tag_less_min_int32", std::move(schema)).get();
    ASSERT_FALSE(result.ok());
  }
  // Test border of int32
  {
    cpp2::Schema schema = genSchema(2147483647, PropertyType::INT32);
    auto result = client->createTagSchema(spaceId, "test_tag_max_int32", std::move(schema)).get();
    ASSERT_TRUE(result.ok());
  }
  // Test border of int32
  {
    cpp2::Schema schema = genSchema(-2147483648, PropertyType::INT32);
    auto result = client->createTagSchema(spaceId, "test_tag_min_int32", std::move(schema)).get();
    ASSERT_TRUE(result.ok());
  }
  cluster.stop();
}

TEST(MetaClientTest, EdgeTest) {
  FLAGS_heartbeat_interval_secs = 1;
  fs::TempDir rootPath("/tmp/MetaClientEdgeTest.XXXXXX");

  mock::MockCluster cluster;
  cluster.startMeta(rootPath.path());
  cluster.initMetaClient();
  auto* client = cluster.metaClient_.get();
  {
    std::vector<HostAddr> hosts = {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}};
    auto result = client->addHosts(hosts).get();
    TestUtils::registerHB(cluster.metaKV_.get(), hosts);
    EXPECT_TRUE(result.ok());
  }
  meta::cpp2::SpaceDesc spaceDesc;
  spaceDesc.space_name_ref() = "default_space";
  spaceDesc.partition_num_ref() = 9;
  spaceDesc.replica_factor_ref() = 3;
  auto ret = client->createSpace(spaceDesc).get();
  ASSERT_TRUE(ret.ok()) << ret.status();
  GraphSpaceID space = ret.value();
  SchemaVer version;

  std::vector<cpp2::ColumnDef> expectedColumns;
  {
    std::vector<cpp2::ColumnDef> columns;

    cpp2::ColumnDef intColumn;
    intColumn.name_ref() = "column_i";
    intColumn.type.type_ref() = PropertyType::INT64;
    const auto& intValue = *ConstantExpression::make(metaPool, Value(0L));
    intColumn.default_value_ref() = Expression::encode(intValue);
    columns.emplace_back(std::move(intColumn));

    cpp2::ColumnDef doubleColumn;
    doubleColumn.name_ref() = "column_d";
    doubleColumn.type.type_ref() = PropertyType::DOUBLE;
    const auto& floatValue = *ConstantExpression::make(metaPool, Value(3.14));
    doubleColumn.default_value_ref() = Expression::encode(floatValue);
    columns.emplace_back(std::move(doubleColumn));

    cpp2::ColumnDef stringColumn;
    stringColumn.name_ref() = "column_s";
    stringColumn.type.type_ref() = PropertyType::STRING;
    const auto& strValue = *ConstantExpression::make(metaPool, "test");
    stringColumn.default_value_ref() = Expression::encode(strValue);
    columns.emplace_back(std::move(stringColumn));
    expectedColumns = columns;

    cpp2::Schema schema;
    schema.columns_ref() = std::move(columns);
    auto result = client->createEdgeSchema(space, "test_edge", schema).get();
    ASSERT_TRUE(result.ok());
  }
  {
    std::vector<cpp2::ColumnDef> columns;
    cpp2::ColumnDef intColumn;
    intColumn.name_ref() = "column_i";
    intColumn.type.type_ref() = PropertyType::INT64;
    const auto& intValue = *ConstantExpression::make(metaPool, Value(0L));
    intColumn.default_value_ref() = Expression::encode(intValue);
    columns.emplace_back(std::move(intColumn));

    cpp2::ColumnDef doubleColumn;
    doubleColumn.name_ref() = "column_d";
    doubleColumn.type.type_ref() = PropertyType::STRING;
    const auto& floatValue = *ConstantExpression::make(metaPool, Value(3.14));
    doubleColumn.default_value_ref() = Expression::encode(floatValue);
    columns.emplace_back(std::move(doubleColumn));
    cpp2::Schema schema;
    schema.columns_ref() = columns;

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
    expected.columns_ref() = std::move(expectedColumns);
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
      ASSERT_EQ((*result1.value().columns_ref())[i].name, (*result2.value().columns_ref())[i].name);
      ASSERT_EQ((*result1.value().columns_ref())[i].type, (*result2.value().columns_ref())[i].type);
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
  cluster.stop();
}

TEST(MetaClientTest, TagIndexTest) {
  FLAGS_heartbeat_interval_secs = 1;
  fs::TempDir rootPath("/tmp/MetaClientTagIndexTest.XXXXXX");

  mock::MockCluster cluster;
  cluster.startMeta(rootPath.path());
  cluster.initMetaClient();
  auto* client = cluster.metaClient_.get();
  {
    std::vector<HostAddr> hosts = {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}};
    auto result = client->addHosts(hosts).get();
    TestUtils::registerHB(cluster.metaKV_.get(), hosts);
    EXPECT_TRUE(result.ok());
  }

  meta::cpp2::SpaceDesc spaceDesc;
  spaceDesc.space_name_ref() = "default_space";
  spaceDesc.partition_num_ref() = 8;
  spaceDesc.replica_factor_ref() = 3;
  auto ret = client->createSpace(spaceDesc).get();
  ASSERT_TRUE(ret.ok()) << ret.status();
  GraphSpaceID space = ret.value();
  IndexID singleFieldIndexID;
  IndexID multiFieldIndexID;
  {
    for (auto i = 0; i < 2; i++) {
      std::vector<cpp2::ColumnDef> columns;
      cpp2::ColumnDef column0;
      column0.name_ref() = folly::stringPrintf("tag_%d_col_0", i);
      column0.type.type_ref() = PropertyType::INT64;
      columns.emplace_back(std::move(column0));

      cpp2::ColumnDef column1;
      column1.name_ref() = folly::stringPrintf("tag_%d_col_1", i);
      column1.type.type_ref() = PropertyType::FIXED_STRING;
      column1.type.type_length_ref() = 50;
      columns.emplace_back(std::move(column1));

      cpp2::Schema schema;
      schema.columns_ref() = std::move(columns);
      auto result = client->createTagSchema(space, folly::stringPrintf("tag_%d", i), schema).get();
      ASSERT_TRUE(result.ok());
    }
  }
  {
    cpp2::IndexFieldDef field;
    field.name_ref() = "tag_0_col_0";
    auto result = client->createTagIndex(space, "tag_single_field_index", "tag_0", {field}).get();
    ASSERT_TRUE(result.ok());
    singleFieldIndexID = result.value();
  }
  {
    std::vector<cpp2::IndexFieldDef> fields;
    cpp2::IndexFieldDef field1, field2;
    field1.name_ref() = "tag_0_col_0";
    field2.name_ref() = "tag_0_col_1";
    fields.emplace_back(std::move(field1));
    fields.emplace_back(std::move(field2));
    auto result =
        client->createTagIndex(space, "tag_multi_field_index", "tag_0", std::move(fields)).get();
    ASSERT_TRUE(result.ok());
    multiFieldIndexID = result.value();
  }
  {
    std::vector<cpp2::IndexFieldDef> fields;
    cpp2::IndexFieldDef field1, field2;
    field1.name_ref() = "tag_0_col_0";
    field2.name_ref() = "not_exist_field";
    fields.emplace_back(std::move(field1));
    fields.emplace_back(std::move(field2));

    auto result =
        client->createTagIndex(space, "tag_field_not_exist_index", "tag_0", std::move(fields))
            .get();
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(Status::Error("not existed!"), result.status());
  }
  {
    std::vector<cpp2::IndexFieldDef> fields;
    cpp2::IndexFieldDef field1, field2;
    field1.name_ref() = "tag_0_col_0";
    field2.name_ref() = "tag_0_col_1";
    fields.emplace_back(std::move(field1));
    fields.emplace_back(std::move(field2));

    auto result =
        client->createTagIndex(space, "tag_not_exist_index", "tag_not_exist", std::move(fields))
            .get();
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(Status::TagNotFound("not existed!"), result.status());
  }
  {
    std::vector<cpp2::IndexFieldDef> fields;
    cpp2::IndexFieldDef field1, field2;
    field1.name_ref() = "tag_0_col_0";
    field2.name_ref() = "tag_0_col_0";
    fields.emplace_back(std::move(field1));
    fields.emplace_back(std::move(field2));

    auto result =
        client->createTagIndex(space, "tag_conflict_index", "tag_0", std::move(fields)).get();
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(Status::Error("conflict"), result.status());
  }
  {
    // meta will be read in the order of indexname
    auto result = client->listTagIndexes(space).get();
    auto values = result.value();
    ASSERT_EQ(2, values.size());

    {
      std::vector<cpp2::ColumnDef> columns;
      cpp2::ColumnDef intColumn;
      intColumn.name_ref() = "tag_0_col_0";
      intColumn.type.type_ref() = PropertyType::INT64;
      columns.emplace_back(std::move(intColumn));

      cpp2::ColumnDef stringColumn;
      stringColumn.name_ref() = "tag_0_col_1";
      stringColumn.type.type_ref() = PropertyType::FIXED_STRING;
      stringColumn.type.type_length_ref() = 50;
      columns.emplace_back(std::move(stringColumn));

      auto multiFieldResult = values[0].get_fields();
      ASSERT_TRUE(TestUtils::verifyResult(columns, multiFieldResult));
    }

    {
      cpp2::ColumnDef singleColumn;
      singleColumn.name_ref() = "tag_0_col_0";
      singleColumn.type.type_ref() = PropertyType::INT64;
      std::vector<cpp2::ColumnDef> columns;
      columns.emplace_back(std::move(singleColumn));
      auto singleFieldResult = values[1].get_fields();
      ASSERT_TRUE(TestUtils::verifyResult(columns, singleFieldResult));
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
  cluster.stop();
}

TEST(MetaClientTest, EdgeIndexTest) {
  FLAGS_heartbeat_interval_secs = 1;
  fs::TempDir rootPath("/tmp/MetaClientEdgeIndexTest.XXXXXX");

  mock::MockCluster cluster;
  cluster.startMeta(rootPath.path());
  cluster.initMetaClient();
  auto* client = cluster.metaClient_.get();
  {
    std::vector<HostAddr> hosts = {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}};
    auto result = client->addHosts(hosts).get();
    TestUtils::registerHB(cluster.metaKV_.get(), hosts);
    EXPECT_TRUE(result.ok());
  }

  meta::cpp2::SpaceDesc spaceDesc;
  spaceDesc.space_name_ref() = "default_space";
  spaceDesc.partition_num_ref() = 8;
  spaceDesc.replica_factor_ref() = 3;
  auto ret = client->createSpace(spaceDesc).get();
  GraphSpaceID space = ret.value();
  IndexID singleFieldIndexID;
  IndexID multiFieldIndexID;
  {
    for (auto i = 0; i < 2; i++) {
      std::vector<cpp2::ColumnDef> columns;
      cpp2::ColumnDef column0;
      column0.name_ref() = folly::stringPrintf("edge_%d_col_0", i);
      column0.type.type_ref() = PropertyType::INT64;
      columns.emplace_back(std::move(column0));

      cpp2::ColumnDef column1;
      column1.name_ref() = folly::stringPrintf("edge_%d_col_1", i);
      column1.type.type_ref() = PropertyType::FIXED_STRING;
      column1.type.type_length_ref() = 50;
      columns.emplace_back(std::move(column1));

      cpp2::Schema schema;
      schema.columns_ref() = std::move(columns);
      auto result =
          client->createEdgeSchema(space, folly::stringPrintf("edge_%d", i), schema).get();
      ASSERT_TRUE(result.ok());
    }
  }
  {
    cpp2::IndexFieldDef field;
    field.name_ref() = "edge_0_col_0";
    auto result =
        client->createEdgeIndex(space, "edge_single_field_index", "edge_0", {field}).get();
    ASSERT_TRUE(result.ok());
    singleFieldIndexID = result.value();
  }
  {
    std::vector<cpp2::IndexFieldDef> fields;
    cpp2::IndexFieldDef field1, field2;
    field1.name_ref() = "edge_0_col_0";
    field2.name_ref() = "edge_0_col_1";
    fields.emplace_back(std::move(field1));
    fields.emplace_back(std::move(field2));
    auto result =
        client->createEdgeIndex(space, "edge_multi_field_index", "edge_0", std::move(fields)).get();
    ASSERT_TRUE(result.ok());
    multiFieldIndexID = result.value();
  }
  {
    std::vector<cpp2::IndexFieldDef> fields;
    cpp2::IndexFieldDef field1, field2;
    field1.name_ref() = "edge_0_col_0";
    field2.name_ref() = "edge_0_col_1";
    fields.emplace_back(std::move(field1));
    fields.emplace_back(std::move(field2));
    auto result =
        client->createEdgeIndex(space, "edge_not_exist_index", "edge_not_exist", std::move(fields))
            .get();
    ASSERT_FALSE(result.ok());
  }
  {
    std::vector<cpp2::IndexFieldDef> fields;
    cpp2::IndexFieldDef field1, field2;
    field1.name_ref() = "edge_0_col_0";
    field2.name_ref() = "edge_0_col_0";
    fields.emplace_back(std::move(field1));
    fields.emplace_back(std::move(field2));
    auto result =
        client->createEdgeIndex(space, "edge_conflict_index", "edge_0", std::move(fields)).get();
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(Status::Error("conflict"), result.status());
  }
  {
    std::vector<cpp2::IndexFieldDef> fields;
    cpp2::IndexFieldDef field1, field2;
    field1.name_ref() = "edge_0_col_0";
    field2.name_ref() = "not_exist_field";
    fields.emplace_back(std::move(field1));
    fields.emplace_back(std::move(field2));
    auto result =
        client->createEdgeIndex(space, "edge_field_not_exist_index", "edge_0", std::move(fields))
            .get();
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(Status::Error("not existed!"), result.status());
  }
  {
    // meta will be read in the order of indexname
    auto result = client->listEdgeIndexes(space).get();
    auto values = result.value();
    ASSERT_EQ(2, values.size());

    {
      std::vector<cpp2::ColumnDef> columns;
      cpp2::ColumnDef intColumn;
      intColumn.name_ref() = "edge_0_col_0";
      intColumn.type.type_ref() = PropertyType::INT64;
      columns.emplace_back(std::move(intColumn));
      cpp2::ColumnDef stringColumn;
      stringColumn.name_ref() = "edge_0_col_1";
      stringColumn.type.type_ref() = PropertyType::FIXED_STRING;
      stringColumn.type.type_length_ref() = 50;
      columns.emplace_back(std::move(stringColumn));
      auto multiFieldResult = values[0].get_fields();
      ASSERT_TRUE(TestUtils::verifyResult(columns, multiFieldResult));
    }

    {
      cpp2::ColumnDef column;
      column.name_ref() = "edge_0_col_0";
      column.type.type_ref() = PropertyType::INT64;
      std::vector<cpp2::ColumnDef> columns;
      columns.emplace_back(std::move(column));

      auto singleFieldResult = values[1].get_fields();
      ASSERT_TRUE(TestUtils::verifyResult(columns, singleFieldResult));
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
  cluster.stop();
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

  std::vector<cpp2::ServiceClient> clients;
  cpp2::ServiceClient c1, c2;
  c1.host_ref() = {"0", 0};
  c1.user_ref() = "u1";
  c1.pwd_ref() = "pwd";
  clients.emplace_back(c1);
  c2.host_ref() = {"1", 1};
  c2.user_ref() = "u2";
  clients.emplace_back(c2);
  {
    cpp2::ExternalServiceType type = cpp2::ExternalServiceType::ELASTICSEARCH;
    auto result = client->signInService(type, clients).get();
    ASSERT_TRUE(result.ok());
  }
  {
    auto result = client->listServiceClients(cpp2::ExternalServiceType::ELASTICSEARCH).get();
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(clients, result.value()[cpp2::ExternalServiceType::ELASTICSEARCH]);
  }
  {
    auto result = client->signOutService(cpp2::ExternalServiceType::ELASTICSEARCH).get();
    ASSERT_TRUE(result.ok());
  }
  {
    auto result = client->listServiceClients(cpp2::ExternalServiceType::ELASTICSEARCH).get();
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.value().empty());
  }
  cluster.stop();
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

  void onSpaceOptionUpdated(GraphSpaceID,
                            const std::unordered_map<std::string, std::string>& update) override {
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

  void fetchLeaderInfo(std::unordered_map<GraphSpaceID, std::vector<cpp2::LeaderInfo>>&) override {
    LOG(INFO) << "Get leader distribution!";
  }

  void onListenerSpaceAdded(GraphSpaceID spaceId, cpp2::ListenerType type) override {
    UNUSED(spaceId);
    UNUSED(type);
    listenerSpaceNum++;
  }

  void onListenerSpaceRemoved(GraphSpaceID spaceId, cpp2::ListenerType type) override {
    UNUSED(spaceId);
    UNUSED(type);
    listenerSpaceNum--;
  }

  void onListenerPartAdded(GraphSpaceID spaceId,
                           PartitionID partId,
                           cpp2::ListenerType type,
                           const std::vector<HostAddr>& peers) override {
    UNUSED(spaceId);
    UNUSED(partId);
    UNUSED(type);
    UNUSED(peers);
    listenerPartNum++;
  }

  void onListenerPartRemoved(GraphSpaceID spaceId,
                             PartitionID partId,
                             cpp2::ListenerType type) override {
    UNUSED(spaceId);
    UNUSED(partId);
    UNUSED(type);
    listenerPartNum--;
  }

  void onCheckRemoteListeners(GraphSpaceID spaceId,
                              PartitionID partId,
                              const std::vector<HostAddr>& remoteListeners) override {
    UNUSED(spaceId);
    UNUSED(partId);
    UNUSED(remoteListeners);
  }

  void fetchDiskParts(kvstore::SpaceDiskPartsMap& diskParts) override {
    UNUSED(diskParts);
    LOG(INFO) << "Fetch Disk Paths";
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
  auto* kv = cluster.metaKV_.get();
  std::vector<HostAddr> hosts = {{"0", 0}};
  {
    cpp2::AddHostsReq req;
    req.hosts_ref() = hosts;
    auto* processor = AddHostsProcessor::instance(kv);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  { TestUtils::registerHB(kv, {{"0", 0}}); }

  meta::MetaClientOptions options;
  options.localHost_ = {"0", 0};
  options.role_ = meta::cpp2::HostRole::STORAGE;
  options.clusterId_ = 10;
  cluster.initMetaClient(options);
  auto* client = cluster.metaClient_.get();
  auto listener = std::make_unique<TestListener>();
  client->registerListener(listener.get());
  {
    // Add hosts automatically, then testing listHosts interface.
    std::vector<HostAddr> hbHosts = {{"0", 0}};
    TestUtils::registerHB(kv, hbHosts);
    auto ret = client->listHosts().get();
    ASSERT_TRUE(ret.ok());
    for (auto i = 0u; i < hosts.size(); i++) {
      auto tHost = ret.value()[i].get_hostAddr();
      auto hostAddr = HostAddr(tHost.host, tHost.port);
      ASSERT_EQ(hosts[i], hostAddr);
    }
  }
  sleep(FLAGS_heartbeat_interval_secs + 1);
  {
    // Test Create Space and List Spaces
    meta::cpp2::SpaceDesc spaceDesc;
    spaceDesc.space_name_ref() = "default_space";
    spaceDesc.partition_num_ref() = 9;
    spaceDesc.replica_factor_ref() = 1;
    auto ret = client->createSpace(spaceDesc).get();
    ASSERT_TRUE(ret.ok()) << ret.status();
  }
  sleep(FLAGS_heartbeat_interval_secs + 1);
  ASSERT_EQ(1, listener->spaceNum);
  ASSERT_EQ(9, listener->partNum);
  {
    meta::cpp2::SpaceDesc spaceDesc;
    spaceDesc.space_name_ref() = "default_space_1";
    spaceDesc.partition_num_ref() = 5;
    spaceDesc.replica_factor_ref() = 1;
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
  cluster.stop();
}

TEST(MetaClientTest, ListenerDiffTest) {
  FLAGS_heartbeat_interval_secs = 1;
  fs::TempDir rootPath("/tmp/MetaClientTest.XXXXXX");

  mock::MockCluster cluster;
  cluster.startMeta(rootPath.path());
  cluster.initMetaClient();
  auto* kv = cluster.metaKV_.get();
  auto* console = cluster.metaClient_.get();
  auto testListener = std::make_unique<TestListener>();
  console->registerListener(testListener.get());

  // create another meta client for listener host
  HostAddr listenerHost("listener", 0);
  meta::MetaClientOptions options;
  options.localHost_ = listenerHost;
  options.role_ = meta::cpp2::HostRole::UNKNOWN;
  auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
  auto metaAddrs = {HostAddr(cluster.localIP(), cluster.metaServer_->port_)};
  auto client = std::make_unique<meta::MetaClient>(threadPool, metaAddrs, options);
  {
    std::vector<HostAddr> hosts = {{"0", 0}};
    auto result = client->addHosts(hosts).get();
    TestUtils::registerHB(cluster.metaKV_.get(), hosts);
    EXPECT_TRUE(result.ok());
  }
  client->waitForMetadReady();

  auto listener = std::make_unique<TestListener>();
  client->registerListener(listener.get());

  // register HB for storage
  std::vector<HostAddr> hbHosts = {{"0", 0}};
  TestUtils::registerHB(kv, hbHosts);
  {
    // create two space
    meta::cpp2::SpaceDesc spaceDesc;
    spaceDesc.space_name_ref() = "listener_space";
    spaceDesc.partition_num_ref() = 9;
    spaceDesc.replica_factor_ref() = 1;
    auto ret = console->createSpace(spaceDesc).get();
    ASSERT_TRUE(ret.ok()) << ret.status();
    auto spaceId = ret.value();

    spaceDesc.space_name_ref() = "no_listener_space";
    ret = console->createSpace(spaceDesc).get();
    ASSERT_TRUE(ret.ok()) << ret.status();

    sleep(FLAGS_heartbeat_interval_secs + 1);
    ASSERT_EQ(0, listener->spaceNum);
    ASSERT_EQ(0, listener->partNum);
    ASSERT_EQ(0, listener->listenerSpaceNum);
    ASSERT_EQ(0, listener->listenerPartNum);

    // add listener hosts to space and check num
    auto addRet =
        console->addListener(spaceId, cpp2::ListenerType::ELASTICSEARCH, {listenerHost}).get();
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
    addRet = console->addListener(spaceId, cpp2::ListenerType::ELASTICSEARCH, {listenerHost}).get();
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
  cluster.stop();
}

TEST(MetaClientTest, HeartbeatTest) {
  FLAGS_heartbeat_interval_secs = 1;
  fs::TempDir rootPath("/tmp/HeartbeatTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.startMeta(rootPath.path());
  auto* kv = cluster.metaKV_.get();

  TestUtils::createSomeHosts(kv, {{"0", 0}});

  HostAddr localHost("0", 0);
  cluster.initMetaClient();
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
  auto hostsRet = ActiveHostsMan::getActiveHosts(cluster.metaKV_.get());
  ASSERT_TRUE(nebula::ok(hostsRet));
  ASSERT_EQ(1, nebula::value(hostsRet).size());
  client->unRegisterListener();
  cluster.stop();
}

class TestMetaService : public cpp2::MetaServiceSvIf {
 public:
  folly::Future<cpp2::HBResp> future_heartBeat(const cpp2::HBReq& req) override {
    UNUSED(req);
    folly::Promise<cpp2::HBResp> pro;
    auto f = pro.getFuture();
    cpp2::HBResp resp;
    resp.code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
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

  folly::Future<cpp2::HBResp> future_heartBeat(const cpp2::HBReq& req) override {
    UNUSED(req);
    folly::Promise<cpp2::HBResp> pro;
    auto f = pro.getFuture();
    cpp2::HBResp resp;
    if (addr_ == leader_) {
      resp.code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
    } else {
      resp.code_ref() = nebula::cpp2::ErrorCode::E_LEADER_CHANGED;
      resp.leader_ref() = leader_;
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
      threadPool, std::vector<HostAddr>{HostAddr(localIp, mockMetad->port_)});
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
  auto localIp("127.0.0.1");

  auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
  auto clientPort = network::NetworkUtils::getAvailablePort();
  HostAddr localHost{localIp, clientPort};
  auto client = std::make_shared<MetaClient>(threadPool, std::vector<HostAddr>{{"0", 0}});
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
  auto client = std::make_shared<MetaClient>(threadPool, std::vector<HostAddr>{addrs[1]});
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
  auto client = std::make_shared<MetaClient>(threadPool, std::vector<HostAddr>{addrs[1]});
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

cpp2::ConfigItem initConfigItem(cpp2::ConfigModule module,
                                std::string name,
                                cpp2::ConfigMode mode,
                                Value value) {
  cpp2::ConfigItem configItem;
  configItem.module_ref() = module;
  configItem.name_ref() = name;
  configItem.mode_ref() = mode;
  configItem.value_ref() = value;
  return configItem;
}

TEST(MetaClientTest, Config) {
  FLAGS_heartbeat_interval_secs = 1;
  fs::TempDir rootPath("/tmp/MetaClientTest.Config.XXXXXX");

  mock::MockCluster cluster;
  cluster.startMeta(rootPath.path());

  MetaClientOptions options;
  // Now the `--local_config' option only affect if initialize the configuration
  // from meta not affect `show/update/get configs'
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
    configItems.emplace_back(
        initConfigItem(cpp2::ConfigModule::GRAPH, "minloglevel", cpp2::ConfigMode::MUTABLE, 2));
    configItems.emplace_back(
        initConfigItem(cpp2::ConfigModule::META, "minloglevel", cpp2::ConfigMode::MUTABLE, 2));
    configItems.emplace_back(
        initConfigItem(cpp2::ConfigModule::STORAGE, "minloglevel", cpp2::ConfigMode::MUTABLE, 2));
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
  // Just avoid memory leak error of clang asan. to waiting asynchronous thread
  // done.
  sleep(FLAGS_heartbeat_interval_secs * 5);
  cluster.stop();
}

TEST(MetaClientTest, ListenerTest) {
  FLAGS_heartbeat_interval_secs = 1;
  fs::TempDir rootPath("/tmp/MetaClientListenerTest.XXXXXX");

  mock::MockCluster cluster;
  cluster.startMeta(rootPath.path());
  uint32_t localMetaPort = cluster.metaServer_->port_;
  auto* kv = cluster.metaKV_.get();
  auto localIp = cluster.localIP();

  auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
  auto localhosts = std::vector<HostAddr>{HostAddr(localIp, localMetaPort)};
  auto client = std::make_shared<MetaClient>(threadPool, localhosts);
  {
    std::vector<HostAddr> hosts = {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}};
    auto result = client->addHosts(hosts).get();
    TestUtils::registerHB(cluster.metaKV_.get(), hosts);
    EXPECT_TRUE(result.ok());
  }
  client->waitForMetadReady();

  meta::cpp2::SpaceDesc spaceDesc;
  spaceDesc.space_name_ref() = "default";
  spaceDesc.partition_num_ref() = 9;
  spaceDesc.replica_factor_ref() = 3;
  auto ret = client->createSpace(spaceDesc).get();
  ASSERT_TRUE(ret.ok()) << ret.status();
  GraphSpaceID space = ret.value();
  std::vector<HostAddr> listenerHosts = {{"1", 0}, {"1", 1}, {"1", 2}, {"1", 3}};
  {
    TestUtils::setupHB(kv, listenerHosts, cpp2::HostRole::STORAGE_LISTENER, gitInfoSha());
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
      l.type_ref() = cpp2::ListenerType::ELASTICSEARCH;
      l.host_ref() = listenerHosts[i % 4];
      l.part_id_ref() = i + 1;
      l.status_ref() = cpp2::HostStatus::ONLINE;
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
  cluster.stop();
}

TEST(MetaClientTest, VerifyClientTest) {
  FLAGS_heartbeat_interval_secs = 1;
  fs::TempDir rootPath("/tmp/VerifyClientTest.XXXXXX");

  mock::MockCluster cluster;
  cluster.startMeta(rootPath.path());
  cluster.initMetaClient();
  auto* client = cluster.metaClient_.get();

  FLAGS_enable_client_white_list = true;
  {
    FLAGS_client_white_list = nebula::cpp2::common_constants::version();
    auto status = client->verifyVersion("");
    EXPECT_TRUE(status.ok());
  }
  {
    FLAGS_client_white_list = "";
    auto status = client->verifyVersion("");
    EXPECT_FALSE(status.ok());
  }
  {
    FLAGS_client_white_list = "1.0.0:1.2.0:";
    auto status = client->verifyVersion("");
    EXPECT_FALSE(status.ok());
  }
  {
    FLAGS_enable_client_white_list = false;
    FLAGS_client_white_list = "1.0.0:1.2.0:";
    auto status = client->verifyVersion("");
    EXPECT_TRUE(status.ok());
  }
  FLAGS_enable_client_white_list = false;
  cluster.stop();
}

TEST(MetaClientTest, HostsTest) {
  FLAGS_heartbeat_interval_secs = 1;
  fs::TempDir rootPath("/tmp/HostsTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.startMeta(rootPath.path());
  cluster.initMetaClient();
  auto* client = cluster.metaClient_.get();
  {
    // Add single host
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8989}};
    auto result = client->addHosts(std::move(hosts)).get();
    EXPECT_TRUE(result.ok());
  }
  {
    // Add multi host
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8987}, {"127.0.0.1", 8988}};
    auto result = client->addHosts(std::move(hosts)).get();
    EXPECT_TRUE(result.ok());
  }
  {
    // Add duplicated hosts
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8986}, {"127.0.0.1", 8986}};
    auto result = client->addHosts(std::move(hosts)).get();
    EXPECT_FALSE(result.ok());
  }
  {
    // Add empty hosts
    std::vector<HostAddr> hosts = {};
    auto result = client->addHosts(std::move(hosts)).get();
    EXPECT_FALSE(result.ok());
  }
  {
    // Add hosts which is existed
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8988}};
    auto result = client->addHosts(std::move(hosts)).get();
    EXPECT_FALSE(result.ok());
  }
  {
    // Add hosts which is existed
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8986}, {"127.0.0.1", 8988}};
    auto result = client->addHosts(std::move(hosts)).get();
    EXPECT_FALSE(result.ok());
  }
  {
    // Show the zones created by add hosts
    auto result = client->listZones().get();
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(3, result.value().size());
  }
  {
    // Drop hosts with duplicate element
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8987}, {"127.0.0.1", 8987}};
    auto result = client->dropHosts(std::move(hosts)).get();
    EXPECT_FALSE(result.ok());
  }
  {
    // Drop hosts which is empty
    std::vector<HostAddr> hosts = {};
    auto result = client->dropHosts(std::move(hosts)).get();
    EXPECT_FALSE(result.ok());
  }
  cluster.stop();
}

TEST(MetaClientTest, AddHostsIntoNewZoneTest) {
  FLAGS_heartbeat_interval_secs = 1;
  fs::TempDir rootPath("/tmp/AddHostsIntoNewZoneTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.startMeta(rootPath.path());
  cluster.initMetaClient();
  auto* client = cluster.metaClient_.get();
  {
    std::vector<HostAddr> hosts = {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}};
    auto result = client->addHosts(std::move(hosts)).get();
    EXPECT_TRUE(result.ok());
  }
  {
    // Add host into zone with duplicate hosts
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8988}, {"127.0.0.1", 8988}, {"127.0.0.1", 8989}};
    auto result = client->addHostsIntoZone(std::move(hosts), "zone_0", true).get();
    EXPECT_FALSE(result.ok());
  }
  {
    // Add host into new zone with empty hosts.
    std::vector<HostAddr> hosts = {};
    auto result = client->addHostsIntoZone(std::move(hosts), "zone_0", true).get();
    EXPECT_FALSE(result.ok());
  }
  {
    // Add host into new zone.
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8987}, {"127.0.0.1", 8988}, {"127.0.0.1", 8989}};
    auto result = client->addHostsIntoZone(std::move(hosts), "zone_0", true).get();
    EXPECT_TRUE(result.ok());
  }
  {
    // Add host into new zone with zone name conflict.
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8977}, {"127.0.0.1", 8978}, {"127.0.0.1", 8979}};
    auto result = client->addHostsIntoZone(std::move(hosts), "zone_0", true).get();
    EXPECT_FALSE(result.ok());
  }
  {
    // the hosts have exist in another zones.
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8977}, {"127.0.0.1", 8978}, {"127.0.0.1", 8989}};
    auto result = client->addHostsIntoZone(std::move(hosts), "zone_1", true).get();
    EXPECT_FALSE(result.ok());
  }
  cluster.stop();
}

TEST(MetaClientTest, AddHostsIntoZoneTest) {
  FLAGS_heartbeat_interval_secs = 1;
  fs::TempDir rootPath("/tmp/AddHostsIntoZoneTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.startMeta(rootPath.path());
  cluster.initMetaClient();
  auto* client = cluster.metaClient_.get();
  auto* kv = cluster.metaKV_.get();
  {
    // Add host into zone with duplicate hosts
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8988}, {"127.0.0.1", 8988}, {"127.0.0.1", 8989}};
    auto result = client->addHostsIntoZone(std::move(hosts), "zone_0", false).get();
    EXPECT_FALSE(result.ok());
  }
  {
    // Add host into zone with empty hosts
    std::vector<HostAddr> hosts;
    auto result = client->addHostsIntoZone(std::move(hosts), "zone_0", false).get();
    EXPECT_FALSE(result.ok());
  }
  {
    // Add host into zone which zone is not exist.
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8987}, {"127.0.0.1", 8988}, {"127.0.0.1", 8989}};
    auto result = client->addHostsIntoZone(std::move(hosts), "zone_not_existed", false).get();
    EXPECT_FALSE(result.ok());
  }
  {
    // Add host into zone.
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8987}, {"127.0.0.1", 8988}, {"127.0.0.1", 8989}};
    auto result = client->addHostsIntoZone(std::move(hosts), "zone_0", true).get();
    EXPECT_TRUE(result.ok());
  }
  {
    // Add host into zone with zone name conflict.
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8977}, {"127.0.0.1", 8978}, {"127.0.0.1", 8979}};
    auto result = client->addHostsIntoZone(std::move(hosts), "zone_0", true).get();
    EXPECT_FALSE(result.ok());
  }
  {
    // Add existed hosts
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8988}};
    auto result = client->addHostsIntoZone(std::move(hosts), "zone_0", true).get();
    EXPECT_FALSE(result.ok());
  }
  {
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8977}, {"127.0.0.1", 8978}, {"127.0.0.1", 8979}};
    auto result = client->addHostsIntoZone(std::move(hosts), "zone_1", true).get();
    EXPECT_TRUE(result.ok());
  }
  {
    // Add existed hosts.
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8988}};
    auto result = client->addHosts(std::move(hosts)).get();
    EXPECT_FALSE(result.ok());
  }
  {
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8976}, {"127.0.0.1", 8988}};
    auto result = client->addHostsIntoZone(std::move(hosts), "zone_1", false).get();
    EXPECT_FALSE(result.ok());
  }
  { TestUtils::registerHB(kv, {{"127.0.0.1", 8987}, {"127.0.0.1", 8988}, {"127.0.0.1", 8989}}); }
  {
    // Drop hosts which is empty.
    std::vector<HostAddr> hosts = {};
    auto result = client->dropHosts(std::move(hosts)).get();
    EXPECT_FALSE(result.ok());
  }
  {
    // Drop hosts which have duplicate element.
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8988}, {"127.0.0.1", 8988}, {"127.0.0.1", 8989}};
    auto result = client->dropHosts(std::move(hosts)).get();
    EXPECT_FALSE(result.ok());
  }
  {
    // Drop hosts.
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8987}, {"127.0.0.1", 8988}, {"127.0.0.1", 8989}};
    auto result = client->dropHosts(std::move(hosts)).get();
    EXPECT_TRUE(result.ok());
  }
  cluster.stop();
}

TEST(MetaClientTest, DropHostsTest) {
  FLAGS_heartbeat_interval_secs = 1;
  fs::TempDir rootPath("/tmp/DropHostsTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.startMeta(rootPath.path());
  cluster.initMetaClient();
  auto* client = cluster.metaClient_.get();
  auto* kv = cluster.metaKV_.get();
  {
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8987}, {"127.0.0.1", 8988}, {"127.0.0.1", 8989}};
    auto result = client->addHosts(std::move(hosts)).get();
    EXPECT_TRUE(result.ok());
  }
  TestUtils::registerHB(kv, {{"127.0.0.1", 8987}, {"127.0.0.1", 8988}, {"127.0.0.1", 8989}});
  {
    auto result = client->listZones().get();
    ASSERT_TRUE(result.ok());
    auto zones = result.value();
    ASSERT_EQ(3, zones.size());
    ASSERT_EQ("default_zone_127.0.0.1_8987", zones[0].get_zone_name());
    ASSERT_EQ("default_zone_127.0.0.1_8988", zones[1].get_zone_name());
    ASSERT_EQ("default_zone_127.0.0.1_8989", zones[2].get_zone_name());
  }
  {
    // Create Space on cluster, the replica number same with the zone size.
    meta::cpp2::SpaceDesc properties;
    properties.space_name_ref() = "default_space_0";
    properties.partition_num_ref() = 9;
    properties.replica_factor_ref() = 3;
    properties.charset_name_ref() = "utf8";
    properties.collate_name_ref() = "utf8_bin";
    auto ret = client->createSpace(properties).get();
    ASSERT_TRUE(ret.ok()) << ret.status();
    ASSERT_EQ(1, ret.value());
  }
  {
    // Create Space on cluster, the replica number less than the zone size.
    cpp2::SpaceDesc properties;
    properties.space_name_ref() = "default_space_1";
    properties.partition_num_ref() = 9;
    properties.replica_factor_ref() = 1;
    properties.charset_name_ref() = "utf8";
    properties.collate_name_ref() = "utf8_bin";
    auto ret = client->createSpace(properties).get();
    ASSERT_TRUE(ret.ok()) << ret.status();
    ASSERT_EQ(2, ret.value());
  }
  {
    // Create Space on cluster, the replica number greater than the zone size.
    cpp2::SpaceDesc properties;
    properties.space_name_ref() = "default_space_2";
    properties.partition_num_ref() = 9;
    properties.replica_factor_ref() = 6;
    properties.charset_name_ref() = "utf8";
    properties.collate_name_ref() = "utf8_bin";
    auto ret = client->createSpace(properties).get();
    ASSERT_FALSE(ret.ok()) << ret.status();
  }
  {
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8976}, {"127.0.0.1", 8977}};
    auto result = client->addHostsIntoZone(std::move(hosts), "zone_0", true).get();
    EXPECT_TRUE(result.ok());
  }
  {
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8978}};
    auto result = client->addHostsIntoZone(std::move(hosts), "zone_1", true).get();
    EXPECT_TRUE(result.ok());
  }
  {
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8979}};
    auto result = client->addHostsIntoZone(std::move(hosts), "zone_2", true).get();
    EXPECT_TRUE(result.ok());
  }
  TestUtils::registerHB(
      kv, {{"127.0.0.1", 8976}, {"127.0.0.1", 8977}, {"127.0.0.1", 8978}, {"127.0.0.1", 8979}});
  {
    auto result = client->listZones().get();
    ASSERT_TRUE(result.ok());
    auto zones = result.value();
    ASSERT_EQ(6, zones.size());
    ASSERT_EQ("default_zone_127.0.0.1_8987", zones[0].get_zone_name());
    ASSERT_EQ("default_zone_127.0.0.1_8988", zones[1].get_zone_name());
    ASSERT_EQ("default_zone_127.0.0.1_8989", zones[2].get_zone_name());
    ASSERT_EQ("zone_0", zones[3].get_zone_name());
    ASSERT_EQ("zone_1", zones[4].get_zone_name());
    ASSERT_EQ("zone_2", zones[5].get_zone_name());
  }
  {
    // Create Space on cluster, the replica number greater than the zone size.
    cpp2::SpaceDesc properties;
    properties.space_name_ref() = "default_space_on_zone_3";
    properties.partition_num_ref() = 9;
    properties.replica_factor_ref() = 3;
    properties.charset_name_ref() = "utf8";
    properties.collate_name_ref() = "utf8_bin";
    std::vector<std::string> zones = {"zone_0", "zone_1", "zone_2"};
    properties.zone_names_ref() = std::move(zones);
    auto ret = client->createSpace(properties).get();
    ASSERT_TRUE(ret.ok()) << ret.status();
    ASSERT_EQ(4, ret.value());
  }
  {
    // Create Space on cluster, the replica number less than the zone size
    cpp2::SpaceDesc properties;
    properties.space_name_ref() = "default_space_on_zone_1";
    properties.partition_num_ref() = 9;
    properties.replica_factor_ref() = 1;
    properties.charset_name_ref() = "utf8";
    properties.collate_name_ref() = "utf8_bin";
    std::vector<std::string> zones = {"zone_0"};
    properties.zone_names_ref() = std::move(zones);
    auto ret = client->createSpace(properties).get();
    ASSERT_TRUE(ret.ok()) << ret.status();
    ASSERT_EQ(5, ret.value());
  }
  {
    // Create Space on cluster, the replica number greater than the zone size
    cpp2::SpaceDesc properties;
    properties.space_name_ref() = "default_space_on_zone_6";
    properties.partition_num_ref() = 9;
    properties.replica_factor_ref() = 6;
    properties.charset_name_ref() = "utf8";
    properties.collate_name_ref() = "utf8_bin";
    std::vector<std::string> zones = {"zone_0"};
    properties.zone_names_ref() = std::move(zones);
    auto ret = client->createSpace(properties).get();
    ASSERT_FALSE(ret.ok()) << ret.status();
  }
  {
    // Drop hosts which hold partition.
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8987}};
    auto result = client->dropHosts(std::move(hosts)).get();
    EXPECT_FALSE(result.ok());
  }
  {
    // Drop hosts which hold partition.
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8977}};
    auto result = client->dropHosts(std::move(hosts)).get();
    EXPECT_FALSE(result.ok());
  }
  {
    auto ret = client->dropSpace("default_space_on_zone_1").get();
    ASSERT_TRUE(ret.ok());
  }
  {
    auto ret = client->dropSpace("default_space_on_zone_3").get();
    ASSERT_TRUE(ret.ok());
  }
  {
    auto ret = client->dropSpace("default_space_0").get();
    ASSERT_TRUE(ret.ok());
  }
  {
    auto ret = client->dropSpace("default_space_1").get();
    ASSERT_TRUE(ret.ok());
  }
  {
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8987}};
    auto result = client->dropHosts(std::move(hosts)).get();
    EXPECT_TRUE(result.ok());
  }
  {
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8976}};
    auto result = client->dropHosts(std::move(hosts)).get();
    EXPECT_TRUE(result.ok());
  }
  {
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8978}};
    auto result = client->dropHosts(std::move(hosts)).get();
    EXPECT_TRUE(result.ok());
  }
  {
    auto result = client->listZones().get();
    ASSERT_TRUE(result.ok());
    auto zones = result.value();
    ASSERT_EQ(4, zones.size());
    ASSERT_EQ("default_zone_127.0.0.1_8988", zones[0].get_zone_name());
    ASSERT_EQ("default_zone_127.0.0.1_8989", zones[1].get_zone_name());
    ASSERT_EQ("zone_0", zones[2].get_zone_name());
    ASSERT_EQ("zone_2", zones[3].get_zone_name());
  }
  cluster.stop();
}

TEST(MetaClientTest, RenameZoneTest) {
  FLAGS_heartbeat_interval_secs = 1;
  fs::TempDir rootPath("/tmp/RenameZoneTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.startMeta(rootPath.path());
  cluster.initMetaClient();
  auto* client = cluster.metaClient_.get();
  auto* kv = cluster.metaKV_.get();
  {
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8987}};
    auto result = client->addHostsIntoZone(std::move(hosts), "zone_0", true).get();
    EXPECT_TRUE(result.ok());
  }
  {
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8988}};
    auto result = client->addHostsIntoZone(std::move(hosts), "zone_1", true).get();
    EXPECT_TRUE(result.ok());
  }
  {
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8989}};
    auto result = client->addHostsIntoZone(std::move(hosts), "zone_2", true).get();
    EXPECT_TRUE(result.ok());
  }
  TestUtils::registerHB(kv, {{"127.0.0.1", 8987}, {"127.0.0.1", 8988}, {"127.0.0.1", 8989}});
  {
    auto result = client->listZones().get();
    ASSERT_TRUE(result.ok());
    auto zones = result.value();
    ASSERT_EQ(3, zones.size());
    ASSERT_EQ("zone_0", zones[0].get_zone_name());
    ASSERT_EQ("zone_1", zones[1].get_zone_name());
    ASSERT_EQ("zone_2", zones[2].get_zone_name());
  }
  {
    cpp2::SpaceDesc properties;
    properties.space_name_ref() = "default";
    properties.partition_num_ref() = 9;
    properties.replica_factor_ref() = 3;
    properties.charset_name_ref() = "utf8";
    properties.collate_name_ref() = "utf8_bin";
    std::vector<std::string> zones = {"zone_0", "zone_1", "zone_2"};
    properties.zone_names_ref() = std::move(zones);
    auto ret = client->createSpace(properties).get();
    ASSERT_TRUE(ret.ok()) << ret.status();
    ASSERT_EQ(1, ret.value());
  }
  {
    auto ret = client->getSpace("default").get();
    ASSERT_TRUE(ret.ok()) << ret.status();
    auto spaceDesc = ret.value().get_properties();
    ASSERT_EQ("default", spaceDesc.get_space_name());
    ASSERT_EQ(9, spaceDesc.get_partition_num());
    ASSERT_EQ(3, spaceDesc.get_replica_factor());
    ASSERT_EQ("utf8", spaceDesc.get_charset_name());
    ASSERT_EQ("utf8_bin", spaceDesc.get_collate_name());
    auto zones = spaceDesc.get_zone_names();
    ASSERT_EQ(3, zones.size());
    ASSERT_EQ("zone_0", zones[0]);
    ASSERT_EQ("zone_1", zones[1]);
    ASSERT_EQ("zone_2", zones[2]);
  }
  {
    auto result = client->renameZone("zone_not_exist", "new_zone_name").get();
    EXPECT_FALSE(result.ok());
  }
  {
    auto result = client->renameZone("zone_0", "zone_1").get();
    EXPECT_FALSE(result.ok());
  }
  {
    auto result = client->renameZone("zone_1", "z_1").get();
    EXPECT_TRUE(result.ok());
  }
  {
    auto ret = client->getSpace("default").get();
    ASSERT_TRUE(ret.ok()) << ret.status();
    auto spaceDesc = ret.value().get_properties();
    ASSERT_EQ("default", spaceDesc.get_space_name());
    ASSERT_EQ(9, spaceDesc.get_partition_num());
    ASSERT_EQ(3, spaceDesc.get_replica_factor());
    ASSERT_EQ("utf8", spaceDesc.get_charset_name());
    ASSERT_EQ("utf8_bin", spaceDesc.get_collate_name());
    auto zones = spaceDesc.get_zone_names();
    ASSERT_EQ(3, zones.size());
    ASSERT_EQ("zone_0", zones[0]);
    ASSERT_EQ("z_1", zones[1]);
    ASSERT_EQ("zone_2", zones[2]);
  }
  cluster.stop();
}

TEST(MetaClientTest, MergeZoneTest) {
  FLAGS_heartbeat_interval_secs = 1;
  fs::TempDir rootPath("/tmp/MergeZoneTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.startMeta(rootPath.path());
  cluster.initMetaClient();
  auto* client = cluster.metaClient_.get();
  auto* kv = cluster.metaKV_.get();
  {
    std::vector<HostAddr> hosts = {
        {"127.0.0.1", 8986}, {"127.0.0.1", 8987}, {"127.0.0.1", 8988}, {"127.0.0.1", 8989}};
    auto result = client->addHosts(std::move(hosts)).get();
    EXPECT_TRUE(result.ok());
  }
  TestUtils::registerHB(
      kv, {{"127.0.0.1", 8986}, {"127.0.0.1", 8987}, {"127.0.0.1", 8988}, {"127.0.0.1", 8989}});
  {
    auto result = client->listZones().get();
    ASSERT_TRUE(result.ok());
    auto zones = result.value();
    ASSERT_EQ(4, zones.size());
    ASSERT_EQ("default_zone_127.0.0.1_8986", zones[0].get_zone_name());
    ASSERT_EQ("default_zone_127.0.0.1_8987", zones[1].get_zone_name());
    ASSERT_EQ("default_zone_127.0.0.1_8988", zones[2].get_zone_name());
    ASSERT_EQ("default_zone_127.0.0.1_8989", zones[3].get_zone_name());
  }
  {
    auto result = client
                      ->mergeZone({"default_zone_127.0.0.1_8986", "default_zone_127.0.0.1_8987"},
                                  "default_zone_127.0.0.1_8988")
                      .get();
    EXPECT_FALSE(result.ok());
  }
  {
    // Merge zones is empty
    auto result = client->mergeZone({}, "new_zone").get();
    EXPECT_FALSE(result.ok());
  }
  {
    // Merge only zone
    auto result = client->mergeZone({"zone_0"}, "new_zone").get();
    EXPECT_FALSE(result.ok());
  }
  {
    // Merge zones have duplicate
    auto result = client->mergeZone({"zone_0", "zone_0"}, "new_zone").get();
    EXPECT_FALSE(result.ok());
  }
  {
    // Merge zones not exist
    auto result = client->mergeZone({"zone_0", "zone_not_exist"}, "new_zone").get();
    EXPECT_FALSE(result.ok());
  }
  {
    // Merge zones not exist
    auto result = client->mergeZone({"zone_not_exist_0", "zone_not_exist_1"}, "new_zone").get();
    EXPECT_FALSE(result.ok());
  }
  {
    auto result = client
                      ->mergeZone({"default_zone_127.0.0.1_8986",
                                   "default_zone_127.0.0.1_8987",
                                   "default_zone_127.0.0.1_8988"},
                                  "zone_1")
                      .get();
    EXPECT_TRUE(result.ok());
  }
  {
    auto result = client->mergeZone({"default_zone_127.0.0.1_8989", "zone_1"}, "zone_1").get();
    EXPECT_TRUE(result.ok());
  }
  {
    auto result = client->listZones().get();
    ASSERT_TRUE(result.ok());
    auto zones = result.value();
    ASSERT_EQ(1, zones.size());
    ASSERT_EQ("zone_1", zones[0].get_zone_name());
  }
  {
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8976}, {"127.0.0.1", 8977}, {"127.0.0.1", 8978}};
    auto result = client->addHosts(std::move(hosts)).get();
    EXPECT_TRUE(result.ok());
  }
  TestUtils::registerHB(kv, {{"127.0.0.1", 8976}, {"127.0.0.1", 8977}, {"127.0.0.1", 8978}});
  // Merge zone with space test
  {
    cpp2::SpaceDesc properties;
    properties.space_name_ref() = "default_space";
    properties.partition_num_ref() = 9;
    properties.replica_factor_ref() = 3;
    properties.charset_name_ref() = "utf8";
    properties.collate_name_ref() = "utf8_bin";
    std::vector<std::string> zones = {"default_zone_127.0.0.1_8976",
                                      "default_zone_127.0.0.1_8977",
                                      "default_zone_127.0.0.1_8978"};
    properties.zone_names_ref() = std::move(zones);
    auto ret = client->createSpace(properties).get();
    ASSERT_TRUE(ret.ok()) << ret.status();
    ASSERT_EQ(1, ret.value());
  }
  {
    // zone size should be greater than part size
    auto result =
        client->mergeZone({"default_zone_127.0.0.1_8976", "default_zone_127.0.0.1_8977"}, "zone")
            .get();
    EXPECT_FALSE(result.ok());
  }
  {
    auto result = client->dropSpace("default_space").get();
    EXPECT_TRUE(result.ok());
  }
  {
    cpp2::SpaceDesc properties;
    properties.space_name_ref() = "default_space";
    properties.partition_num_ref() = 9;
    properties.replica_factor_ref() = 1;
    properties.charset_name_ref() = "utf8";
    properties.collate_name_ref() = "utf8_bin";
    std::vector<std::string> zones = {"default_zone_127.0.0.1_8976",
                                      "default_zone_127.0.0.1_8977",
                                      "default_zone_127.0.0.1_8978"};
    properties.zone_names_ref() = std::move(zones);
    auto ret = client->createSpace(properties).get();
    ASSERT_TRUE(ret.ok()) << ret.status();
    ASSERT_EQ(2, ret.value());
  }
  {
    auto result =
        client->mergeZone({"default_zone_127.0.0.1_8976", "default_zone_127.0.0.1_8977"}, "z_1")
            .get();
    EXPECT_TRUE(result.ok());
  }
  {
    auto result = client->mergeZone({"default_zone_127.0.0.1_8978", "z_1"}, "z_1").get();
    EXPECT_TRUE(result.ok());
  }
  {
    auto result = client->getSpace("default_space").get();
    auto properties = result.value().get_properties();
    ASSERT_EQ("default_space", properties.get_space_name());
    ASSERT_EQ(9, properties.get_partition_num());
    ASSERT_EQ(1, properties.get_replica_factor());
    ASSERT_EQ("utf8", properties.get_charset_name());
    ASSERT_EQ("utf8_bin", properties.get_collate_name());
    auto zones = properties.get_zone_names();
    ASSERT_EQ(1, zones.size());
    ASSERT_EQ("z_1", zones[0]);
  }
  {
    auto result = client->listZones().get();
    ASSERT_TRUE(result.ok());
    auto zones = result.value();
    ASSERT_EQ(2, zones.size());
    ASSERT_EQ("z_1", zones[0].get_zone_name());
    ASSERT_EQ("zone_1", zones[1].get_zone_name());
  }
  cluster.stop();
}

TEST(MetaClientTest, DivideZoneTest) {
  FLAGS_heartbeat_interval_secs = 1;
  fs::TempDir rootPath("/tmp/DivideZoneTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.startMeta(rootPath.path());
  cluster.initMetaClient();
  auto* client = cluster.metaClient_.get();
  auto* kv = cluster.metaKV_.get();
  {
    std::vector<HostAddr> hosts = {
        {"127.0.0.1", 8986}, {"127.0.0.1", 8987}, {"127.0.0.1", 8988}, {"127.0.0.1", 8989}};
    auto result = client->addHostsIntoZone(std::move(hosts), "default_zone", true).get();
    EXPECT_TRUE(result.ok());
  }
  TestUtils::registerHB(
      kv, {{"127.0.0.1", 8986}, {"127.0.0.1", 8987}, {"127.0.0.1", 8988}, {"127.0.0.1", 8989}});
  {
    auto result = client->listZones().get();
    ASSERT_TRUE(result.ok());
    auto zones = result.value();
    ASSERT_EQ(1, zones.size());
    ASSERT_EQ("default_zone", zones[0].get_zone_name());
  }
  {
    // Split zone which not exist
    std::unordered_map<std::string, std::vector<HostAddr>> zoneItems;
    std::vector<HostAddr> oneHosts = {{"127.0.0.1", 8986}, {"127.0.0.1", 8987}};
    zoneItems.emplace("one_zone", std::move(oneHosts));
    std::vector<HostAddr> anotherHosts = {{"127.0.0.1", 8988}, {"127.0.0.1", 8989}};
    zoneItems.emplace("another_zone", std::move(anotherHosts));
    auto result = client->divideZone("zone_not_exist", std::move(zoneItems)).get();
    EXPECT_FALSE(result.ok());
  }
  {
    // Split zone with empty hosts
    std::unordered_map<std::string, std::vector<HostAddr>> zoneItems;
    std::vector<HostAddr> oneHosts = {};
    zoneItems.emplace("one_zone", std::move(oneHosts));
    std::vector<HostAddr> anotherHosts = {{"127.0.0.1", 8988}, {"127.0.0.1", 8989}};
    zoneItems.emplace("another_zone", std::move(anotherHosts));

    auto result = client->divideZone("default_zone", std::move(zoneItems)).get();
    EXPECT_FALSE(result.ok());
  }
  {
    // Split zone with empty hosts
    std::unordered_map<std::string, std::vector<HostAddr>> zoneItems;
    std::vector<HostAddr> oneHosts = {{"127.0.0.1", 8986}, {"127.0.0.1", 8987}};
    zoneItems.emplace("one_zone", std::move(oneHosts));
    std::vector<HostAddr> anotherHosts = {};
    zoneItems.emplace("another_zone", std::move(anotherHosts));
    auto result = client->divideZone("default_zone", std::move(zoneItems)).get();
    EXPECT_FALSE(result.ok());
  }
  {
    // Split zone and the sum is not all
    std::unordered_map<std::string, std::vector<HostAddr>> zoneItems;
    std::vector<HostAddr> oneHosts = {{"127.0.0.1", 8986}};
    zoneItems.emplace("one_zone", std::move(oneHosts));
    std::vector<HostAddr> anotherHosts = {{"127.0.0.1", 8988}, {"127.0.0.1", 8989}};
    zoneItems.emplace("another_zone", std::move(anotherHosts));
    auto result = client->divideZone("default_zone", std::move(zoneItems)).get();
    EXPECT_FALSE(result.ok());
  }
  {
    // Split zone and the hosts is more than the total
    std::unordered_map<std::string, std::vector<HostAddr>> zoneItems;
    std::vector<HostAddr> oneHosts = {
        {"127.0.0.1", 8986}, {"127.0.0.1", 8987}, {"127.0.0.1", 8985}};
    zoneItems.emplace("one_zone", std::move(oneHosts));
    std::vector<HostAddr> anotherHosts = {{"127.0.0.1", 8988}, {"127.0.0.1", 8989}};
    zoneItems.emplace("another_zone", std::move(anotherHosts));
    auto result = client->divideZone("default_zone", std::move(zoneItems)).get();
    EXPECT_FALSE(result.ok());
  }
  {
    std::unordered_map<std::string, std::vector<HostAddr>> zoneItems;
    std::vector<HostAddr> oneHosts = {
        {"127.0.0.1", 8986}, {"127.0.0.1", 8987}, {"127.0.0.1", 8988}};
    zoneItems.emplace("one_zone", std::move(oneHosts));
    std::vector<HostAddr> anotherHosts = {{"127.0.0.1", 8988}, {"127.0.0.1", 8989}};
    zoneItems.emplace("another_zone", std::move(anotherHosts));
    auto result = client->divideZone("default_zone", std::move(zoneItems)).get();
    EXPECT_FALSE(result.ok());
  }
  {
    std::unordered_map<std::string, std::vector<HostAddr>> zoneItems;
    std::vector<HostAddr> oneHosts = {
        {"127.0.0.1", 8986}, {"127.0.0.1", 8987}, {"127.0.0.1", 8987}};
    zoneItems.emplace("one_zone", std::move(oneHosts));
    std::vector<HostAddr> anotherHosts = {{"127.0.0.1", 8988}, {"127.0.0.1", 8989}};
    zoneItems.emplace("another_zone", std::move(anotherHosts));
    auto result = client->divideZone("default_zone", std::move(zoneItems)).get();
    EXPECT_FALSE(result.ok());
  }
  {
    std::unordered_map<std::string, std::vector<HostAddr>> zoneItems;
    std::vector<HostAddr> hosts0 = {};
    zoneItems.emplace("zone_0", std::move(hosts0));
    std::vector<HostAddr> hosts1 = {};
    zoneItems.emplace("zone_1", std::move(hosts1));
    std::vector<HostAddr> hosts2 = {};
    zoneItems.emplace("zone_2", std::move(hosts2));
    std::vector<HostAddr> hosts3 = {};
    zoneItems.emplace("zone_3", std::move(hosts3));
    std::vector<HostAddr> hosts4 = {};
    zoneItems.emplace("zone_4", std::move(hosts4));
    std::vector<HostAddr> hosts5 = {};
    zoneItems.emplace("zone_5", std::move(hosts5));
    auto result = client->divideZone("default_zone", std::move(zoneItems)).get();
    EXPECT_FALSE(result.ok());
  }
  {
    // Split zone successfully
    std::unordered_map<std::string, std::vector<HostAddr>> zoneItems;
    std::vector<HostAddr> oneHosts = {{"127.0.0.1", 8986}, {"127.0.0.1", 8987}};
    zoneItems.emplace("one_zone", std::move(oneHosts));
    std::vector<HostAddr> anotherHosts = {{"127.0.0.1", 8988}, {"127.0.0.1", 8989}};
    zoneItems.emplace("another_zone", std::move(anotherHosts));
    auto result = client->divideZone("default_zone", std::move(zoneItems)).get();
    EXPECT_TRUE(result.ok());
  }
  {
    auto result = client->listZones().get();
    ASSERT_TRUE(result.ok());
    auto zones = result.value();
    ASSERT_EQ(2, zones.size());
    ASSERT_EQ("another_zone", zones[0].get_zone_name());
    ASSERT_EQ("one_zone", zones[1].get_zone_name());
  }
  {
    std::vector<HostAddr> hosts = {
        {"127.0.0.1", 8976}, {"127.0.0.1", 8977}, {"127.0.0.1", 8978}, {"127.0.0.1", 8979}};
    auto result = client->addHostsIntoZone(std::move(hosts), "default_zone", true).get();
    EXPECT_TRUE(result.ok());
  }
  TestUtils::registerHB(
      kv, {{"127.0.0.1", 8976}, {"127.0.0.1", 8977}, {"127.0.0.1", 8978}, {"127.0.0.1", 8979}});
  {
    auto result = client->listZones().get();
    ASSERT_TRUE(result.ok());
    auto zones = result.value();
    ASSERT_EQ(3, zones.size());
    ASSERT_EQ("another_zone", zones[0].get_zone_name());
    ASSERT_EQ("default_zone", zones[1].get_zone_name());
    ASSERT_EQ("one_zone", zones[2].get_zone_name());
  }
  {
    cpp2::SpaceDesc properties;
    properties.space_name_ref() = "default_space";
    properties.partition_num_ref() = 9;
    properties.replica_factor_ref() = 3;
    properties.charset_name_ref() = "utf8";
    properties.collate_name_ref() = "utf8_bin";
    auto ret = client->createSpace(properties).get();
    ASSERT_TRUE(ret.ok()) << ret.status();
    ASSERT_EQ(1, ret.value());
  }
  {
    auto ret = client->getSpace("default_space").get();
    ASSERT_TRUE(ret.ok()) << ret.status();
    auto spaceDesc = ret.value().get_properties();
    ASSERT_EQ("default_space", spaceDesc.get_space_name());
    ASSERT_EQ(9, spaceDesc.get_partition_num());
    ASSERT_EQ(3, spaceDesc.get_replica_factor());
    ASSERT_EQ("utf8", spaceDesc.get_charset_name());
    ASSERT_EQ("utf8_bin", spaceDesc.get_collate_name());
    auto zones = spaceDesc.get_zone_names();
    ASSERT_EQ(3, zones.size());
    ASSERT_EQ("another_zone", zones[0]);
    ASSERT_EQ("default_zone", zones[1]);
    ASSERT_EQ("one_zone", zones[2]);
  }
  {
    // Zone name conflict
    std::unordered_map<std::string, std::vector<HostAddr>> zoneItems;
    std::vector<HostAddr> oneHosts = {{"127.0.0.1", 8976}, {"127.0.0.1", 8977}};
    zoneItems.emplace("one_zone", std::move(oneHosts));
    std::vector<HostAddr> anotherHosts = {{"127.0.0.1", 8978}, {"127.0.0.1", 8979}};
    zoneItems.emplace("another_zone_1", std::move(anotherHosts));
    auto result = client->divideZone("default_zone", std::move(zoneItems)).get();
    EXPECT_FALSE(result.ok());
  }
  {
    // Zone name conflict
    std::unordered_map<std::string, std::vector<HostAddr>> zoneItems;
    std::vector<HostAddr> oneHosts = {{"127.0.0.1", 8976}, {"127.0.0.1", 8977}};
    zoneItems.emplace("one_zone_1", std::move(oneHosts));
    std::vector<HostAddr> anotherHosts = {{"127.0.0.1", 8978}, {"127.0.0.1", 8979}};
    zoneItems.emplace("another_zone", std::move(anotherHosts));
    auto result = client->divideZone("default_zone", std::move(zoneItems)).get();
    EXPECT_FALSE(result.ok());
  }
  {
    std::unordered_map<std::string, std::vector<HostAddr>> zoneItems;
    std::vector<HostAddr> oneHosts = {{"127.0.0.1", 8976}};
    zoneItems.emplace("one_zone_1", std::move(oneHosts));
    std::vector<HostAddr> anotherHosts = {
        {"127.0.0.1", 8977}, {"127.0.0.1", 8978}, {"127.0.0.1", 8979}};
    zoneItems.emplace("another_zone_1", std::move(anotherHosts));
    auto result = client->divideZone("default_zone", std::move(zoneItems)).get();
    EXPECT_TRUE(result.ok());
  }
  {
    auto result = client->listZones().get();
    ASSERT_TRUE(result.ok());
    auto zones = result.value();
    ASSERT_EQ(4, zones.size());
    ASSERT_EQ("another_zone", zones[0].get_zone_name());
    ASSERT_EQ("another_zone_1", zones[1].get_zone_name());
    ASSERT_EQ("one_zone", zones[2].get_zone_name());
    ASSERT_EQ("one_zone_1", zones[3].get_zone_name());
  }
  {
    auto ret = client->getSpace("default_space").get();
    ASSERT_TRUE(ret.ok()) << ret.status();
    auto spaceDesc = ret.value().get_properties();
    ASSERT_EQ("default_space", spaceDesc.get_space_name());
    ASSERT_EQ(9, spaceDesc.get_partition_num());
    ASSERT_EQ(3, spaceDesc.get_replica_factor());
    ASSERT_EQ("utf8", spaceDesc.get_charset_name());
    ASSERT_EQ("utf8_bin", spaceDesc.get_collate_name());
    auto zones = spaceDesc.get_zone_names();
    ASSERT_EQ(4, zones.size());
    ASSERT_EQ("another_zone", zones[0]);
    ASSERT_EQ("one_zone", zones[1]);
    ASSERT_EQ("one_zone_1", zones[2]);
    ASSERT_EQ("another_zone_1", zones[3]);
  }
  {
    std::unordered_map<std::string, std::vector<HostAddr>> zoneItems;
    std::vector<HostAddr> oneHosts = {{"127.0.0.1", 8976}};
    zoneItems.emplace("one_zone_1_1", std::move(oneHosts));
    std::vector<HostAddr> anotherHosts = {{"127.0.0.1", 8976}};
    zoneItems.emplace("one_zone_1_2", std::move(anotherHosts));
    auto result = client->divideZone("one_zone_1", std::move(zoneItems)).get();
    EXPECT_FALSE(result.ok());
  }
  {
    std::unordered_map<std::string, std::vector<HostAddr>> zoneItems;
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8977}, {"127.0.0.1", 8978}, {"127.0.0.1", 8979}};
    zoneItems.emplace("another_zone_1_1", std::move(hosts));
    auto result = client->divideZone("another_zone_1", std::move(zoneItems)).get();
    EXPECT_FALSE(result.ok());
  }
  {
    std::unordered_map<std::string, std::vector<HostAddr>> zoneItems;
    std::vector<HostAddr> oneHosts = {{"127.0.0.1", 8977}};
    zoneItems.emplace("another_zone_1", std::move(oneHosts));
    std::vector<HostAddr> anotherHosts = {{"127.0.0.1", 8978}, {"127.0.0.1", 8979}};
    zoneItems.emplace("another_zone_1_1", std::move(anotherHosts));
    auto result = client->divideZone("another_zone_1", std::move(zoneItems)).get();
    EXPECT_TRUE(result.ok());
  }
  {
    auto result = client->listZones().get();
    ASSERT_TRUE(result.ok());
    auto zones = result.value();
    ASSERT_EQ(5, zones.size());
    ASSERT_EQ("another_zone", zones[0].get_zone_name());
    ASSERT_EQ("another_zone_1", zones[1].get_zone_name());
    ASSERT_EQ("another_zone_1_1", zones[2].get_zone_name());
    ASSERT_EQ("one_zone", zones[3].get_zone_name());
    ASSERT_EQ("one_zone_1", zones[4].get_zone_name());
  }
  {
    std::unordered_map<std::string, std::vector<HostAddr>> zoneItems;
    std::vector<HostAddr> oneHosts = {{"127.0.0.1", 8977}};
    zoneItems.emplace("another_zone_1", std::move(oneHosts));
    std::vector<HostAddr> anotherHosts = {{"127.0.0.1", 8978}, {"127.0.0.1", 8979}};
    zoneItems.emplace("another_zone_1", std::move(anotherHosts));
    auto result = client->divideZone("another_zone_1", std::move(zoneItems)).get();
    EXPECT_FALSE(result.ok());
  }
  cluster.stop();
}

TEST(MetaClientTest, DropZoneTest) {
  FLAGS_heartbeat_interval_secs = 1;
  fs::TempDir rootPath("/tmp/DropZoneTest.XXXXXX");
  mock::MockCluster cluster;
  cluster.startMeta(rootPath.path());
  cluster.initMetaClient();
  auto* client = cluster.metaClient_.get();
  auto* kv = cluster.metaKV_.get();
  {
    // Add single host
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8986}};
    auto result = client->addHostsIntoZone(std::move(hosts), "zone_0", true).get();
    EXPECT_TRUE(result.ok());
  }
  {
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8987}};
    auto result = client->addHostsIntoZone(std::move(hosts), "zone_1", true).get();
    EXPECT_TRUE(result.ok());
  }
  {
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8988}};
    auto result = client->addHostsIntoZone(std::move(hosts), "zone_2", true).get();
    EXPECT_TRUE(result.ok());
  }
  {
    std::vector<HostAddr> hosts = {{"127.0.0.1", 8989}};
    auto result = client->addHostsIntoZone(std::move(hosts), "zone_3", true).get();
    EXPECT_TRUE(result.ok());
  }
  TestUtils::registerHB(
      kv, {{"127.0.0.1", 8986}, {"127.0.0.1", 8987}, {"127.0.0.1", 8988}, {"127.0.0.1", 8989}});
  {
    auto result = client->listZones().get();
    ASSERT_TRUE(result.ok());
    auto zones = result.value();
    ASSERT_EQ(4, zones.size());
    ASSERT_EQ("zone_0", zones[0].get_zone_name());
    ASSERT_EQ("zone_1", zones[1].get_zone_name());
    ASSERT_EQ("zone_2", zones[2].get_zone_name());
    ASSERT_EQ("zone_3", zones[3].get_zone_name());
  }
  {
    auto result = client->dropZone("zone_not_exist").get();
    ASSERT_FALSE(result.ok());
  }
  {
    auto result = client->dropZone("zone_0").get();
    ASSERT_TRUE(result.ok());
  }
  {
    auto result = client->dropZone("zone_0").get();
    ASSERT_FALSE(result.ok());
  }
  {
    cpp2::SpaceDesc properties;
    properties.space_name_ref() = "default_space";
    properties.partition_num_ref() = 9;
    properties.replica_factor_ref() = 3;
    properties.charset_name_ref() = "utf8";
    properties.collate_name_ref() = "utf8_bin";
    auto ret = client->createSpace(properties).get();
    ASSERT_TRUE(ret.ok()) << ret.status();
    ASSERT_EQ(1, ret.value());
  }
  {
    auto result = client->dropZone("zone_1").get();
    ASSERT_FALSE(result.ok());
  }
  {
    auto result = client->dropSpace("default_space").get();
    ASSERT_TRUE(result.ok());
  }
  {
    cpp2::SpaceDesc properties;
    properties.space_name_ref() = "default_space";
    properties.partition_num_ref() = 9;
    properties.replica_factor_ref() = 3;
    properties.charset_name_ref() = "utf8";
    properties.collate_name_ref() = "utf8_bin";
    auto ret = client->createSpace(properties).get();
    ASSERT_TRUE(ret.ok()) << ret.status();
    ASSERT_EQ(2, ret.value());
  }
  {
    auto result = client->dropZone("zone_1").get();
    ASSERT_FALSE(result.ok());
  }
  cluster.stop();
}

TEST(MetaClientTest, RocksdbOptionsTest) {
  FLAGS_heartbeat_interval_secs = 1;
  fs::TempDir rootPath("/tmp/RocksdbOptionsTest.XXXXXX");

  mock::MockCluster cluster;
  cluster.startMeta(rootPath.path());
  // auto* kv = cluster.metaKV_.get();
  // TestUtils::createSomeHosts(kv, {{"0", 0}});

  MetaClientOptions options;
  // Now the `--local_config' option only affect if initialize the configuration
  // from meta not affect `show/update/get configs'
  options.skipConfig_ = false;

  cluster.initMetaClient(std::move(options));
  auto* client = cluster.metaClient_.get();
  {
    // Add single host
    std::vector<HostAddr> hosts = {{"0", 0}};
    auto result = client->addHosts(std::move(hosts)).get();
    EXPECT_TRUE(result.ok());
  }

  auto listener = std::make_unique<TestListener>();
  auto module = cpp2::ConfigModule::STORAGE;
  auto mode = meta::cpp2::ConfigMode::MUTABLE;

  client->registerListener(listener.get());
  client->gflagsModule_ = module;

  // mock some rocksdb gflags to meta
  {
    auto name = "rocksdb_db_options";
    std::vector<cpp2::ConfigItem> configItems;
    FLAGS_rocksdb_db_options =
        R"({"disable_auto_compactions":"false","write_buffer_size":"1048576"})";
    Map map;
    map.kvs.emplace("disable_auto_compactions", "false");
    map.kvs.emplace("write_buffer_size", "1048576");
    configItems.emplace_back(initConfigItem(module, name, mode, Value(map)));
    client->regConfig(configItems);
  }
  {
    std::vector<HostAddr> hosts = {{"0", 0}};
    TestUtils::registerHB(cluster.metaKV_.get(), hosts);
    meta::cpp2::SpaceDesc spaceDesc;
    spaceDesc.space_name_ref() = "default_space";
    spaceDesc.partition_num_ref() = 9;
    spaceDesc.replica_factor_ref() = 1;
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
  cluster.stop();
}

}  // namespace meta
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  // Initialize the global timezone, it's only used for datetime type compute
  // won't affect the process timezone.
  auto status = nebula::time::Timezone::initializeGlobalTimezone();
  if (!status.ok()) {
    LOG(ERROR) << status;
    return EXIT_FAILURE;
  }
  return RUN_ALL_TESTS();
}
