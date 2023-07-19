/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include <gtest/gtest.h>

#include "common/base/Base.h"
#include "common/base/CommonMacro.h"
#include "common/fs/TempDir.h"
#include "meta/processors/index/CreateEdgeIndexProcessor.h"
#include "meta/processors/index/CreateTagIndexProcessor.h"
#include "meta/processors/index/DropEdgeIndexProcessor.h"
#include "meta/processors/index/DropTagIndexProcessor.h"
#include "meta/processors/index/FTIndexProcessor.h"
#include "meta/processors/index/GetEdgeIndexProcessor.h"
#include "meta/processors/index/GetTagIndexProcessor.h"
#include "meta/processors/index/ListEdgeIndexesProcessor.h"
#include "meta/processors/index/ListTagIndexesProcessor.h"
#include "meta/processors/job/ListEdgeIndexStatusProcessor.h"
#include "meta/processors/job/ListTagIndexStatusProcessor.h"
#include "meta/processors/parts/CreateSpaceProcessor.h"
#include "meta/processors/schema/AlterEdgeProcessor.h"
#include "meta/processors/schema/AlterTagProcessor.h"
#include "meta/processors/schema/CreateEdgeProcessor.h"
#include "meta/processors/schema/CreateTagProcessor.h"
#include "meta/processors/schema/DropEdgeProcessor.h"
#include "meta/processors/schema/DropTagProcessor.h"
#include "meta/processors/zone/AddHostsProcessor.h"
#include "meta/test/TestUtils.h"

namespace nebula {
namespace meta {

using nebula::cpp2::PropertyType;

TEST(IndexProcessorTest, AlterEdgeWithTTLTest) {
  fs::TempDir rootPath("/tmp/AlterEdgeWithTTLTest.XXXXXX");
  auto kv = MockCluster::initMetaKV(rootPath.path());
  TestUtils::assembleSpace(kv.get(), 1, 1);
  TestUtils::mockEdge(kv.get(), 1);
  // verify alter ttl prop with index exists
  {
    cpp2::Schema schema;
    cpp2::SchemaProp schemaProp;
    std::vector<cpp2::ColumnDef> cols;
    cols.emplace_back(TestUtils::columnDef(0, PropertyType::INT64));
    schemaProp.ttl_duration_ref() = 100;
    schemaProp.ttl_col_ref() = "col_0";
    schema.schema_prop_ref() = std::move(schemaProp);
    schema.columns_ref() = std::move(cols);

    cpp2::CreateEdgeReq req;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "ttl";
    req.schema_ref() = schema;
    auto* processor = CreateEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    cpp2::CreateEdgeIndexReq req;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "ttl";
    cpp2::IndexFieldDef field;
    field.name_ref() = "col_0";
    req.fields_ref() = {field};
    req.index_name_ref() = "ttl_index";
    auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    cpp2::AlterEdgeReq req;
    cpp2::SchemaProp schemaProp;
    schemaProp.ttl_duration_ref() = 1000;
    schemaProp.ttl_col_ref() = "col_0";
    req.space_id_ref() = 1;
    req.edge_name_ref() = "ttl";
    req.schema_prop_ref() = std::move(schemaProp);
    auto* processor = AlterEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_UNSUPPORTED, resp.get_code());
  }
}

TEST(IndexProcessorTest, AlterTagWithTTLTest) {
  fs::TempDir rootPath("/tmp/AlterTagWithTTLTest.XXXXXX");
  auto kv = MockCluster::initMetaKV(rootPath.path());
  TestUtils::assembleSpace(kv.get(), 1, 1);
  TestUtils::mockTag(kv.get(), 1);
  // verify alter ttl prop with index exists
  {
    cpp2::Schema schema;
    cpp2::SchemaProp schemaProp;
    std::vector<cpp2::ColumnDef> cols;
    cols.emplace_back(TestUtils::columnDef(0, PropertyType::INT64));
    schemaProp.ttl_duration_ref() = 100;
    schemaProp.ttl_col_ref() = "col_0";
    schema.schema_prop_ref() = std::move(schemaProp);
    schema.columns_ref() = std::move(cols);

    cpp2::CreateTagReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "ttl";
    req.schema_ref() = schema;
    auto* processor = CreateTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    cpp2::CreateTagIndexReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "ttl";
    cpp2::IndexFieldDef field;
    field.name_ref() = "col_0";
    req.fields_ref() = {field};
    req.index_name_ref() = "ttl_index";
    auto* processor = CreateTagIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    cpp2::AlterTagReq req;
    cpp2::SchemaProp schemaProp;
    schemaProp.ttl_duration_ref() = 1000;
    schemaProp.ttl_col_ref() = "col_0";
    req.space_id_ref() = 1;
    req.tag_name_ref() = "ttl";
    req.schema_prop_ref() = std::move(schemaProp);
    auto* processor = AlterTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_UNSUPPORTED, resp.get_code());
  }
}

TEST(IndexProcessorTest, TagIndexTest) {
  fs::TempDir rootPath("/tmp/TagIndexTest.XXXXXX");
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
  TestUtils::createSomeHosts(kv.get());
  TestUtils::assembleSpace(kv.get(), 1, 1);
  TestUtils::mockTag(kv.get(), 2);
  {
    // Allow to create tag index on no fields
    cpp2::CreateTagIndexReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    std::vector<cpp2::IndexFieldDef> fields{};
    req.fields_ref() = std::move(fields);
    req.index_name_ref() = "no_field_index";
    auto* processor = CreateTagIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    // Duplicate tag index on no fields
    cpp2::CreateTagIndexReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    std::vector<cpp2::IndexFieldDef> fields{};
    req.fields_ref() = std::move(fields);
    req.index_name_ref() = "no_field_index_1";
    auto* processor = CreateTagIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_NE(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    cpp2::CreateTagIndexReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    cpp2::IndexFieldDef field;
    field.name_ref() = "tag_0_col_0";
    req.fields_ref() = {field};
    req.index_name_ref() = "single_field_index";
    auto* processor = CreateTagIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    cpp2::CreateTagIndexReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    cpp2::IndexFieldDef field;
    field.name_ref() = "tag_0_col_0";
    req.fields_ref() = {field};
    req.index_name_ref() = "duplicate_field_index";
    auto* processor = CreateTagIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_EXISTED, resp.get_code());
  }
  {
    cpp2::DropTagIndexReq req;
    req.space_id_ref() = 1;
    req.index_name_ref() = "no_field_index";
    auto* processor = DropTagIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    cpp2::CreateTagIndexReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    std::vector<cpp2::IndexFieldDef> fields;
    cpp2::IndexFieldDef field1, field2;
    field1.name_ref() = "tag_0_col_0";
    field2.name_ref() = "tag_0_col_1";
    fields.emplace_back(std::move(field1));
    fields.emplace_back(std::move(field2));
    req.fields_ref() = std::move(fields);
    req.index_name_ref() = "multi_field_index";
    auto* processor = CreateTagIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    cpp2::CreateTagIndexReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    std::vector<cpp2::IndexFieldDef> fields;
    cpp2::IndexFieldDef field1, field2;
    field1.name_ref() = "tag_0_col_0";
    field2.name_ref() = "tag_0_col_1";
    fields.emplace_back(std::move(field1));
    fields.emplace_back(std::move(field2));
    req.fields_ref() = std::move(fields);
    req.index_name_ref() = "duplicate_field_index";
    auto* processor = CreateTagIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_EXISTED, resp.get_code());
  }
  {
    cpp2::CreateTagIndexReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    cpp2::IndexFieldDef field;
    field.name_ref() = "tag_0_col_0";
    req.fields_ref() = {field};
    req.index_name_ref() = "duplicate_field_index";
    auto* processor = CreateTagIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_EXISTED, resp.get_code());
  }
  {
    cpp2::CreateTagIndexReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    std::vector<cpp2::IndexFieldDef> fields;
    cpp2::IndexFieldDef field1, field2;
    field1.name_ref() = "tag_0_col_1";
    field2.name_ref() = "tag_0_col_0";
    fields.emplace_back(std::move(field1));
    fields.emplace_back(std::move(field2));
    req.fields_ref() = std::move(fields);
    req.index_name_ref() = "disorder_field_index";
    auto* processor = CreateTagIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    cpp2::CreateTagIndexReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    std::vector<cpp2::IndexFieldDef> fields;
    cpp2::IndexFieldDef field1, field2;
    field1.name_ref() = "tag_0_col_0";
    field2.name_ref() = "tag_0_col_0";
    fields.emplace_back(std::move(field1));
    fields.emplace_back(std::move(field2));
    req.fields_ref() = std::move(fields);
    req.index_name_ref() = "conflict_index";
    auto* processor = CreateTagIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_INVALID_PARM, resp.get_code());
  }
  {
    cpp2::CreateTagIndexReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_not_exist";
    cpp2::IndexFieldDef field;
    field.name_ref() = "tag_0_col_0";
    req.fields_ref() = {field};
    req.index_name_ref() = "tag_not_exist_index";
    auto* processor = CreateTagIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND, resp.get_code());
  }
  {
    cpp2::CreateTagIndexReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    cpp2::IndexFieldDef field;
    field.name_ref() = "field_not_exist";
    req.fields_ref() = {field};
    req.index_name_ref() = "field_not_exist_index";
    auto* processor = CreateTagIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND, resp.get_code());
  }
  {
    // Test index have exist
    cpp2::CreateTagIndexReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    cpp2::IndexFieldDef field;
    field.name_ref() = "tag_0_col_0";
    req.fields_ref() = {field};
    req.index_name_ref() = "single_field_index";
    auto* processor = CreateTagIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_EXISTED, resp.get_code());
  }
  {
    cpp2::ListTagIndexesReq req;
    req.space_id_ref() = 1;
    auto* processor = ListTagIndexesProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    auto items = resp.get_items();

    ASSERT_EQ(3, items.size());
    // meta will be read in the order of indexname
    {
      std::vector<cpp2::ColumnDef> columns;
      cpp2::ColumnDef stringColumn;
      stringColumn.name_ref() = "tag_0_col_1";
      stringColumn.type.type_ref() = PropertyType::FIXED_STRING;
      stringColumn.type.type_length_ref() = MAX_INDEX_TYPE_LENGTH;
      columns.emplace_back(std::move(stringColumn));
      cpp2::ColumnDef intColumn;
      intColumn.name_ref() = "tag_0_col_0";
      intColumn.type.type_ref() = PropertyType::INT64;
      columns.emplace_back(std::move(intColumn));

      auto disorderItem = items[0];
      ASSERT_EQ(4, disorderItem.get_index_id());
      auto disorderFieldResult = disorderItem.get_fields();
      ASSERT_TRUE(TestUtils::verifyResult(columns, disorderFieldResult));
    }
    {
      std::vector<cpp2::ColumnDef> columns;
      cpp2::ColumnDef intColumn;
      intColumn.name_ref() = "tag_0_col_0";
      intColumn.type.type_ref() = PropertyType::INT64;
      columns.emplace_back(std::move(intColumn));

      cpp2::ColumnDef stringColumn;
      stringColumn.name_ref() = "tag_0_col_1";
      stringColumn.type.type_ref() = PropertyType::FIXED_STRING;
      stringColumn.type.type_length_ref() = MAX_INDEX_TYPE_LENGTH;
      columns.emplace_back(std::move(stringColumn));

      auto multiItem = items[1];
      ASSERT_EQ(3, multiItem.get_index_id());
      auto multiFieldResult = multiItem.get_fields();
      ASSERT_TRUE(TestUtils::verifyResult(columns, multiFieldResult));
    }
    {
      cpp2::ColumnDef column;
      column.name_ref() = "tag_0_col_0";
      column.type.type_ref() = PropertyType::INT64;
      std::vector<cpp2::ColumnDef> columns;
      columns.emplace_back(std::move(column));

      auto singleItem = items[2];
      ASSERT_EQ(2, singleItem.get_index_id());
      ASSERT_EQ("single_field_index", singleItem.get_index_name());
      auto singleFieldResult = singleItem.get_fields();
      ASSERT_TRUE(TestUtils::verifyResult(columns, singleFieldResult));
    }
  }
  {
    cpp2::GetTagIndexReq req;
    req.space_id_ref() = 1;
    req.index_name_ref() = "single_field_index";

    auto* processor = GetTagIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    auto item = resp.get_item();
    auto fields = item.get_fields();
    ASSERT_EQ(2, item.get_index_id());

    cpp2::ColumnDef column;
    column.name_ref() = "tag_0_col_0";
    column.type.type_ref() = PropertyType::INT64;
    std::vector<cpp2::ColumnDef> columns;
    columns.emplace_back(std::move(column));

    ASSERT_TRUE(TestUtils::verifyResult(columns, fields));
  }
  {
    cpp2::DropTagIndexReq req;
    req.space_id_ref() = 1;
    req.index_name_ref() = "single_field_index";

    auto* processor = DropTagIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    cpp2::GetTagIndexReq req;
    req.space_id_ref() = 1;
    req.index_name_ref() = "single_field_index";

    auto* processor = GetTagIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND, resp.get_code());
  }
  {
    cpp2::DropTagIndexReq req;
    req.space_id_ref() = 1;
    req.index_name_ref() = "single_field_index";

    auto* processor = DropTagIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND, resp.get_code());
  }
}

TEST(IndexProcessorTest, TagIndexTestV2) {
  fs::TempDir rootPath("/tmp/TagIndexTestV2.XXXXXX");
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
  TestUtils::createSomeHosts(kv.get());
  TestUtils::assembleSpace(kv.get(), 1, 1);
  TestUtils::mockTag(kv.get(), 2, 0, true);
  {
    cpp2::CreateTagIndexReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    cpp2::IndexFieldDef field;
    field.name_ref() = "tag_0_col_0";
    req.fields_ref() = {field};
    req.index_name_ref() = "single_field_index";
    auto* processor = CreateTagIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    cpp2::GetTagIndexReq req;
    req.space_id_ref() = 1;
    req.index_name_ref() = "single_field_index";

    auto* processor = GetTagIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    auto item = resp.get_item();
    auto fields = item.get_fields();
    ASSERT_EQ(1, item.get_index_id());

    cpp2::ColumnDef column;
    column.name_ref() = "tag_0_col_0";
    column.type.type_ref() = PropertyType::INT64;
    column.nullable_ref() = true;
    std::vector<cpp2::ColumnDef> columns;
    columns.emplace_back(std::move(column));

    ASSERT_TRUE(TestUtils::verifyResult(columns, fields));
  }
}

TEST(IndexProcessorTest, EdgeIndexTest) {
  fs::TempDir rootPath("/tmp/EdgeIndexTest.XXXXXX");
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
  TestUtils::createSomeHosts(kv.get());
  TestUtils::assembleSpace(kv.get(), 1, 1);
  TestUtils::mockEdge(kv.get(), 2);
  {
    // Allow to create edge index on no fields
    cpp2::CreateEdgeIndexReq req;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    std::vector<cpp2::IndexFieldDef> fields{};
    req.fields_ref() = std::move(fields);
    req.index_name_ref() = "no_field_index";
    auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    // Duplicate edge index on no fields
    cpp2::CreateEdgeIndexReq req;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    std::vector<cpp2::IndexFieldDef> fields{};
    req.fields_ref() = std::move(fields);
    req.index_name_ref() = "no_field_index_1";
    auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_NE(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    cpp2::CreateEdgeIndexReq req;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    cpp2::IndexFieldDef field;
    field.name_ref() = "edge_0_col_0";
    req.fields_ref() = {field};
    req.index_name_ref() = "single_field_index";
    auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    cpp2::CreateEdgeIndexReq req;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    cpp2::IndexFieldDef field;
    field.name_ref() = "edge_0_col_0";
    req.fields_ref() = {field};
    req.index_name_ref() = "duplicate_field_index";
    auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_EXISTED, resp.get_code());
  }
  {
    cpp2::DropEdgeIndexReq req;
    req.space_id_ref() = 1;
    req.index_name_ref() = "no_field_index";
    auto* processor = DropEdgeIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    cpp2::CreateEdgeIndexReq req;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    std::vector<cpp2::IndexFieldDef> fields;
    cpp2::IndexFieldDef field1, field2;
    field1.name_ref() = "edge_0_col_0";
    field2.name_ref() = "edge_0_col_1";
    fields.emplace_back(std::move(field1));
    fields.emplace_back(std::move(field2));
    req.fields_ref() = std::move(fields);
    req.index_name_ref() = "multi_field_index";
    auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    cpp2::CreateEdgeIndexReq req;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    std::vector<cpp2::IndexFieldDef> fields;
    cpp2::IndexFieldDef field1, field2;
    field1.name_ref() = "edge_0_col_0";
    field2.name_ref() = "edge_0_col_1";
    fields.emplace_back(std::move(field1));
    fields.emplace_back(std::move(field2));
    req.fields_ref() = std::move(fields);
    req.index_name_ref() = "duplicate_field_index";
    auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_EXISTED, resp.get_code());
  }
  {
    cpp2::CreateEdgeIndexReq req;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    cpp2::IndexFieldDef field;
    field.name_ref() = "edge_0_col_0";
    req.fields_ref() = {field};
    req.index_name_ref() = "duplicate_field_index";
    auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_EXISTED, resp.get_code());
  }
  {
    cpp2::CreateEdgeIndexReq req;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    std::vector<cpp2::IndexFieldDef> fields;
    cpp2::IndexFieldDef field1, field2;
    field1.name_ref() = "edge_0_col_1";
    field2.name_ref() = "edge_0_col_0";
    fields.emplace_back(std::move(field1));
    fields.emplace_back(std::move(field2));
    req.fields_ref() = std::move(fields);
    req.index_name_ref() = "disorder_field_index";
    auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    cpp2::CreateEdgeIndexReq req;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    std::vector<cpp2::IndexFieldDef> fields;
    cpp2::IndexFieldDef field1, field2;
    field1.name_ref() = "edge_0_col_0";
    field2.name_ref() = "edge_0_col_0";
    fields.emplace_back(std::move(field1));
    fields.emplace_back(std::move(field2));
    req.fields_ref() = std::move(fields);
    req.index_name_ref() = "conflict_index";
    auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_INVALID_PARM, resp.get_code());
  }
  {
    cpp2::CreateEdgeIndexReq req;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_not_exist";
    cpp2::IndexFieldDef field;
    field.name_ref() = "edge_0_col_0";
    req.fields_ref() = {field};
    req.index_name_ref() = "edge_not_exist_index";
    auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND, resp.get_code());
  }
  {
    cpp2::CreateEdgeIndexReq req;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    cpp2::IndexFieldDef field;
    field.name_ref() = "edge_field_not_exist";
    req.fields_ref() = {field};
    req.index_name_ref() = "field_not_exist_index";
    auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND, resp.get_code());
  }
  {
    cpp2::CreateEdgeIndexReq req;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    cpp2::IndexFieldDef field;
    field.name_ref() = "edge_0_col_0";
    req.fields_ref() = {field};
    req.index_name_ref() = "single_field_index";
    auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_EXISTED, resp.get_code());
  }
  // meta will be read in the order of indexname
  {
    cpp2::ListEdgeIndexesReq req;
    req.space_id_ref() = 1;
    auto* processor = ListEdgeIndexesProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    auto items = resp.get_items();
    ASSERT_EQ(3, items.size());
    {
      std::vector<cpp2::ColumnDef> columns;
      cpp2::ColumnDef stringColumn;
      stringColumn.name_ref() = "edge_0_col_1";
      stringColumn.type.type_ref() = PropertyType::FIXED_STRING;
      stringColumn.type.type_length_ref() = MAX_INDEX_TYPE_LENGTH;
      columns.emplace_back(std::move(stringColumn));
      cpp2::ColumnDef intColumn;
      intColumn.name_ref() = "edge_0_col_0";
      intColumn.type.type_ref() = PropertyType::INT64;
      columns.emplace_back(std::move(intColumn));

      auto disorderItem = items[0];
      ASSERT_EQ(4, disorderItem.get_index_id());
      auto disorderFieldResult = disorderItem.get_fields();
      ASSERT_TRUE(TestUtils::verifyResult(columns, disorderFieldResult));
    }
    {
      std::vector<cpp2::ColumnDef> columns;
      cpp2::ColumnDef intColumn;
      intColumn.name_ref() = "edge_0_col_0";
      intColumn.type.type_ref() = PropertyType::INT64;
      columns.emplace_back(std::move(intColumn));

      cpp2::ColumnDef stringColumn;
      stringColumn.name_ref() = "edge_0_col_1";
      stringColumn.type.type_ref() = PropertyType::FIXED_STRING;
      stringColumn.type.type_length_ref() = MAX_INDEX_TYPE_LENGTH;
      columns.emplace_back(std::move(stringColumn));

      auto multiItem = items[1];
      ASSERT_EQ(3, multiItem.get_index_id());
      auto multiFieldResult = multiItem.get_fields();
      ASSERT_TRUE(TestUtils::verifyResult(columns, multiFieldResult));
    }
    {
      cpp2::ColumnDef column;
      column.name_ref() = "edge_0_col_0";
      column.type.type_ref() = PropertyType::INT64;
      std::vector<cpp2::ColumnDef> columns;
      columns.emplace_back(std::move(column));

      auto singleItem = items[2];
      ASSERT_EQ(2, singleItem.get_index_id());
      auto singleFieldResult = singleItem.get_fields();
      ASSERT_TRUE(TestUtils::verifyResult(columns, singleFieldResult));
    }
  }
  {
    cpp2::GetEdgeIndexReq req;
    req.space_id_ref() = 1;
    req.index_name_ref() = "single_field_index";

    auto* processor = GetEdgeIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    auto item = resp.get_item();
    auto properties = item.get_fields();
    ASSERT_EQ(2, item.get_index_id());
  }
  {
    cpp2::DropEdgeIndexReq req;
    req.space_id_ref() = 1;
    req.index_name_ref() = "single_field_index";

    auto* processor = DropEdgeIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    cpp2::GetEdgeIndexReq req;
    req.space_id_ref() = 1;
    req.index_name_ref() = "single_field_index";

    auto* processor = GetEdgeIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND, resp.get_code());
  }
  {
    cpp2::DropEdgeIndexReq req;
    req.space_id_ref() = 1;
    req.index_name_ref() = "single_field_index";

    auto* processor = DropEdgeIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND, resp.get_code());
  }
  // Test the maximum limit for index columns
  std::vector<cpp2::IndexFieldDef> bigFields;
  {
    for (auto i = 1; i < 18; i++) {
      cpp2::IndexFieldDef field;
      field.name_ref() = folly::stringPrintf("col-%d", i);
      bigFields.emplace_back(std::move(field));
    }
  }
  {
    cpp2::CreateTagIndexReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    req.fields_ref() = bigFields;
    req.index_name_ref() = "index_0";
    auto* processor = CreateTagIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_INVALID_PARM, resp.get_code());
  }
  {
    cpp2::CreateEdgeIndexReq req;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    req.fields_ref() = std::move(bigFields);
    req.index_name_ref() = "index_0";
    auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_INVALID_PARM, resp.get_code());
  }
}

TEST(IndexProcessorTest, EdgeIndexTestV2) {
  fs::TempDir rootPath("/tmp/EdgeIndexTestV2.XXXXXX");
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
  TestUtils::createSomeHosts(kv.get());
  TestUtils::assembleSpace(kv.get(), 1, 1);
  TestUtils::mockEdge(kv.get(), 2, 0, true);
  {
    cpp2::CreateEdgeIndexReq req;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    cpp2::IndexFieldDef field;
    field.name_ref() = "edge_0_col_0";
    req.fields_ref() = {field};
    req.index_name_ref() = "single_field_index";
    auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    cpp2::GetEdgeIndexReq req;
    req.space_id_ref() = 1;
    req.index_name_ref() = "single_field_index";
    auto* processor = GetEdgeIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    auto item = resp.get_item();
    auto fields = item.get_fields();
    ASSERT_EQ(1, item.get_index_id());

    cpp2::ColumnDef column;
    column.name_ref() = "edge_0_col_0";
    column.type.type_ref() = PropertyType::INT64;
    column.nullable_ref() = true;
    std::vector<cpp2::ColumnDef> columns;
    columns.emplace_back(std::move(column));
    ASSERT_TRUE(TestUtils::verifyResult(columns, fields));
  }
}

TEST(IndexProcessorTest, IndexCheckAlterEdgeTest) {
  fs::TempDir rootPath("/tmp/IndexCheckAlterEdgeTest.XXXXXX");
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
  TestUtils::createSomeHosts(kv.get());
  TestUtils::assembleSpace(kv.get(), 1, 1);
  TestUtils::mockEdge(kv.get(), 2);
  {
    cpp2::CreateEdgeIndexReq req;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    cpp2::IndexFieldDef field;
    field.name_ref() = "edge_0_col_0";
    req.fields_ref() = {field};
    req.index_name_ref() = "single_field_index";
    auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    cpp2::AlterEdgeReq req;
    cpp2::Schema addSch;
    cpp2::ColumnDef column;
    std::vector<cpp2::AlterSchemaItem> items;
    for (int32_t i = 2; i < 4; i++) {
      column.name = folly::stringPrintf("edge_0_col_%d", i);
      (*addSch.columns_ref()).emplace_back(std::move(column));
    }

    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::ADD;
    items.back().schema_ref() = std::move(addSch);
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    req.edge_items_ref() = items;
    auto* processor = AlterEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    cpp2::AlterEdgeReq req;
    std::vector<cpp2::AlterSchemaItem> items;
    cpp2::Schema changeSch;
    cpp2::ColumnDef column;
    column.name = "edge_0_col_2";
    column.type.type_ref() = PropertyType::INT64;
    (*changeSch.columns_ref()).emplace_back(std::move(column));

    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::CHANGE;
    items.back().schema_ref() = std::move(changeSch);

    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    req.edge_items_ref() = items;
    auto* processor = AlterEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // Verify ErrorCode of drop
  {
    cpp2::AlterEdgeReq req;
    std::vector<cpp2::AlterSchemaItem> items;
    cpp2::Schema dropSch;
    cpp2::ColumnDef column;
    column.name = "edge_0_col_3";
    column.type.type_ref() = PropertyType::INT64;
    (*dropSch.columns_ref()).emplace_back(std::move(column));

    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::DROP;
    items.back().schema_ref() = std::move(dropSch);

    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    req.edge_items_ref() = items;
    auto* processor = AlterEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // Verify ErrorCode of change
  {
    cpp2::AlterEdgeReq req;
    std::vector<cpp2::AlterSchemaItem> items;
    cpp2::Schema changeSch;
    cpp2::ColumnDef column;
    column.name = "edge_0_col_0";
    column.type.type_ref() = PropertyType::INT64;
    (*changeSch.columns_ref()).emplace_back(std::move(column));

    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::CHANGE;
    items.back().schema_ref() = std::move(changeSch);

    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    req.edge_items_ref() = items;
    auto* processor = AlterEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_RELATED_INDEX_EXISTS, resp.get_code());
  }
  // Verify ErrorCode of drop
  {
    cpp2::AlterEdgeReq req;
    std::vector<cpp2::AlterSchemaItem> items;
    cpp2::Schema dropSch;
    cpp2::ColumnDef column;
    column.name = "edge_0_col_0";
    column.type.type_ref() = PropertyType::INT64;
    (*dropSch.columns_ref()).emplace_back(std::move(column));

    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::DROP;
    items.back().schema_ref() = std::move(dropSch);

    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    req.edge_items_ref() = items;
    auto* processor = AlterEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_RELATED_INDEX_EXISTS, resp.get_code());
  }
}

TEST(IndexProcessorTest, IndexCheckAlterTagTest) {
  fs::TempDir rootPath("/tmp/IndexCheckAlterTagTest.XXXXXX");
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
  TestUtils::createSomeHosts(kv.get());
  TestUtils::assembleSpace(kv.get(), 1, 1);
  TestUtils::mockTag(kv.get(), 2);
  {
    cpp2::CreateTagIndexReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    cpp2::IndexFieldDef field;
    field.name_ref() = "tag_0_col_0";
    req.fields_ref() = {field};
    req.index_name_ref() = "single_field_index";
    auto* processor = CreateTagIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    cpp2::AlterTagReq req;
    std::vector<cpp2::AlterSchemaItem> items;
    cpp2::Schema addSch;
    cpp2::ColumnDef column;
    column.name = "tag_0_col_2";
    column.type.type_ref() = PropertyType::INT64;
    (*addSch.columns_ref()).emplace_back(std::move(column));

    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::ADD;
    items.back().schema_ref() = std::move(addSch);
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    req.tag_items_ref() = items;
    auto* processor = AlterTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    cpp2::AlterTagReq req;
    std::vector<cpp2::AlterSchemaItem> items;
    cpp2::Schema changeSch;
    cpp2::ColumnDef column;
    column.name = "tag_0_col_2";
    column.type.type_ref() = PropertyType::INT64;
    (*changeSch.columns_ref()).emplace_back(std::move(column));
    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::CHANGE;
    items.back().schema_ref() = std::move(changeSch);
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    req.tag_items_ref() = items;
    auto* processor = AlterTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    cpp2::AlterTagReq req;
    std::vector<cpp2::AlterSchemaItem> items;
    cpp2::Schema dropSch;
    cpp2::ColumnDef column;
    column.name = "tag_0_col_2";
    column.type.type_ref() = PropertyType::INT64;
    (*dropSch.columns_ref()).emplace_back(std::move(column));

    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::DROP;
    items.back().schema_ref() = std::move(dropSch);
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    req.tag_items_ref() = items;
    auto* processor = AlterTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    cpp2::AlterTagReq req;
    std::vector<cpp2::AlterSchemaItem> items;
    cpp2::Schema changeSch;
    cpp2::ColumnDef column;
    column.name = "tag_0_col_0";
    column.type.type_ref() = PropertyType::INT64;
    (*changeSch.columns_ref()).emplace_back(std::move(column));
    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::CHANGE;
    items.back().schema_ref() = std::move(changeSch);
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    req.tag_items_ref() = items;
    auto* processor = AlterTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_RELATED_INDEX_EXISTS, resp.get_code());
  }
  {
    cpp2::AlterTagReq req;
    std::vector<cpp2::AlterSchemaItem> items;
    cpp2::Schema dropSch;
    cpp2::ColumnDef column;
    column.name = "tag_0_col_0";
    column.type.type_ref() = PropertyType::INT64;
    (*dropSch.columns_ref()).emplace_back(std::move(column));

    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::DROP;
    items.back().schema_ref() = std::move(dropSch);
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    req.tag_items_ref() = items;
    auto* processor = AlterTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_RELATED_INDEX_EXISTS, resp.get_code());
  }
}

TEST(IndexProcessorTest, IndexCheckDropEdgeTest) {
  fs::TempDir rootPath("/tmp/IndexCheckDropEdgeTest.XXXXXX");
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
  TestUtils::createSomeHosts(kv.get());
  TestUtils::assembleSpace(kv.get(), 1, 1);
  TestUtils::mockEdge(kv.get(), 2);
  {
    cpp2::CreateEdgeIndexReq req;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    cpp2::IndexFieldDef field;
    field.name_ref() = "edge_0_col_0";
    req.fields_ref() = {field};
    req.index_name_ref() = "single_field_index";
    auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    cpp2::DropEdgeReq req;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    auto* processor = DropEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_RELATED_INDEX_EXISTS, resp.get_code());
  }
}

TEST(IndexProcessorTest, IndexCheckDropTagTest) {
  fs::TempDir rootPath("/tmp/IndexCheckDropTagTest.XXXXXX");
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
  TestUtils::createSomeHosts(kv.get());
  TestUtils::assembleSpace(kv.get(), 1, 1);
  TestUtils::mockTag(kv.get(), 2);
  {
    cpp2::CreateTagIndexReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    cpp2::IndexFieldDef field;
    field.name_ref() = "tag_0_col_0";
    req.fields_ref() = {field};
    req.index_name_ref() = "single_field_index";
    auto* processor = CreateTagIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    cpp2::DropTagReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    auto* processor = DropTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_RELATED_INDEX_EXISTS, resp.get_code());
  }
}

TEST(IndexProcessorTest, IndexTTLTagTest) {
  fs::TempDir rootPath("/tmp/IndexTTLTagTest.XXXXXX");
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
  TestUtils::createSomeHosts(kv.get());
  TestUtils::assembleSpace(kv.get(), 1, 1);
  TestUtils::mockTag(kv.get(), 1);
  {
    cpp2::CreateTagIndexReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    cpp2::IndexFieldDef field;
    field.name_ref() = "tag_0_col_0";
    req.fields_ref() = {field};
    req.index_name_ref() = "single_field_index";

    auto* processor = CreateTagIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    cpp2::AlterTagReq req;
    std::vector<cpp2::AlterSchemaItem> items;
    cpp2::Schema addSch;
    for (auto i = 0; i < 2; i++) {
      cpp2::ColumnDef column;
      column.name = folly::stringPrintf("tag_0_col_%d", i + 10);
      column.type.type_ref() = (i < 1 ? PropertyType::INT64 : PropertyType::STRING);
      (*addSch.columns_ref()).emplace_back(std::move(column));
    }
    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::ADD;
    items.back().schema_ref() = std::move(addSch);
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    req.tag_items_ref() = items;

    auto* processor = AlterTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // Tag with index add ttl property on index col, failed
  {
    cpp2::AlterTagReq req;
    cpp2::SchemaProp schemaProp;
    schemaProp.ttl_col_ref() = "tag_0_col_0";
    schemaProp.ttl_duration_ref() = 100;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    req.schema_prop_ref() = std::move(schemaProp);

    auto* processor = AlterTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_UNSUPPORTED, resp.get_code());
  }
  // Tag with index add ttl property on no index col, failed
  {
    cpp2::AlterTagReq req;
    cpp2::SchemaProp schemaProp;
    schemaProp.ttl_col_ref() = "tag_0_col_10";
    schemaProp.ttl_duration_ref() = 100;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    req.schema_prop_ref() = std::move(schemaProp);

    auto* processor = AlterTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_UNSUPPORTED, resp.get_code());
  }
  // Drop index
  {
    cpp2::DropTagIndexReq req;
    req.space_id_ref() = 1;
    req.index_name_ref() = "single_field_index";

    auto* processor = DropTagIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // Add ttl property, succeed
  {
    cpp2::AlterTagReq req;
    cpp2::SchemaProp schemaProp;
    schemaProp.ttl_col_ref() = "tag_0_col_0";
    schemaProp.ttl_duration_ref() = 100;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    req.schema_prop_ref() = std::move(schemaProp);

    auto* processor = AlterTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // Tag with ttl to create index on ttl col
  {
    cpp2::CreateTagIndexReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    cpp2::IndexFieldDef field;
    field.name_ref() = "tag_0_col_0";
    req.fields_ref() = {field};
    req.index_name_ref() = "ttl_with_index";

    auto* processor = CreateTagIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // Drop ttl property
  {
    cpp2::AlterTagReq req;
    cpp2::SchemaProp schemaProp;
    schemaProp.ttl_duration_ref() = 0;
    schemaProp.ttl_col_ref() = "";
    req.space_id_ref() = 1;
    req.tag_name_ref() = "tag_0";
    req.schema_prop_ref() = std::move(schemaProp);

    auto* processor = AlterTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_UNSUPPORTED, resp.get_code());
  }
  {
    cpp2::DropTagIndexReq req;
    req.space_id_ref() = 1;
    req.index_name_ref() = "ttl_with_index";

    auto* processor = DropTagIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
}

TEST(IndexProcessorTest, IndexTTLEdgeTest) {
  fs::TempDir rootPath("/tmp/IndexTTLEdgeTest.XXXXXX");
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
  TestUtils::createSomeHosts(kv.get());
  TestUtils::assembleSpace(kv.get(), 1, 1);
  TestUtils::mockEdge(kv.get(), 1);
  {
    cpp2::CreateEdgeIndexReq req;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    cpp2::IndexFieldDef field;
    field.name_ref() = "edge_0_col_0";
    req.fields_ref() = {field};
    req.index_name_ref() = "single_field_index";

    auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    cpp2::AlterEdgeReq req;
    std::vector<cpp2::AlterSchemaItem> items;
    cpp2::Schema addSch;
    for (auto i = 0; i < 2; i++) {
      cpp2::ColumnDef column;
      column.name = folly::stringPrintf("edge_0_col_%d", i + 10);
      column.type.type_ref() = (i < 1 ? PropertyType::INT64 : PropertyType::STRING);
      (*addSch.columns_ref()).emplace_back(std::move(column));
    }
    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::ADD;
    items.back().schema_ref() = std::move(addSch);

    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    req.edge_items_ref() = items;
    auto* processor = AlterEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // Edge with index add ttl property on index col
  {
    cpp2::AlterEdgeReq req;
    cpp2::SchemaProp schemaProp;
    schemaProp.ttl_col_ref() = "edge_0_col_0";
    schemaProp.ttl_duration_ref() = 100;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    req.schema_prop_ref() = std::move(schemaProp);

    auto* processor = AlterEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_UNSUPPORTED, resp.get_code());
  }
  // Edge with index add ttl property on no index col
  {
    cpp2::AlterEdgeReq req;
    cpp2::SchemaProp schemaProp;
    schemaProp.ttl_col_ref() = "edge_0_col_10";
    schemaProp.ttl_duration_ref() = 100;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    req.schema_prop_ref() = std::move(schemaProp);

    auto* processor = AlterEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_UNSUPPORTED, resp.get_code());
  }
  // Drop index
  {
    cpp2::DropEdgeIndexReq req;
    req.space_id_ref() = 1;
    req.index_name_ref() = "single_field_index";

    auto* processor = DropEdgeIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // Drop index, then add ttl property, succeed
  {
    cpp2::AlterEdgeReq req;
    cpp2::SchemaProp schemaProp;
    schemaProp.ttl_col_ref() = "edge_0_col_0";
    schemaProp.ttl_duration_ref() = 100;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    req.schema_prop_ref() = std::move(schemaProp);

    auto* processor = AlterEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // Edge with ttl to create index on ttl col
  {
    cpp2::CreateEdgeIndexReq req;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    cpp2::IndexFieldDef field;
    field.name_ref() = "edge_0_col_0";
    req.fields_ref() = {field};
    req.index_name_ref() = "ttl_with_index";

    auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // Drop ttl property
  {
    cpp2::AlterEdgeReq req;
    cpp2::SchemaProp schemaProp;
    schemaProp.ttl_duration_ref() = 0;
    schemaProp.ttl_col_ref() = "";
    req.space_id_ref() = 1;
    req.edge_name_ref() = "edge_0";
    req.schema_prop_ref() = std::move(schemaProp);

    auto* processor = AlterEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_UNSUPPORTED, resp.get_code());
  }
  {
    cpp2::DropEdgeIndexReq req;
    req.space_id_ref() = 1;
    req.index_name_ref() = "ttl_with_index";

    auto* processor = DropEdgeIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
}

void mockSchemas(kvstore::KVStore* kv) {
  std::vector<nebula::kvstore::KV> schemas;
  SchemaVer ver = 0;
  TagID tagId = 5;
  EdgeType edgeType = 6;

  cpp2::Schema srcsch;
  // data type is string
  {
    cpp2::ColumnDef col;
    col.name_ref() = "col_string";
    col.type.type_ref() = PropertyType::STRING;
    (*srcsch.columns_ref()).emplace_back(std::move(col));
  }
  // non-string type
  {
    cpp2::ColumnDef col;
    col.name_ref() = "col_int";
    col.type.type_ref() = PropertyType::INT64;
    (*srcsch.columns_ref()).emplace_back(std::move(col));
  }
  // data type is fixed_string, length <= MAX_INDEX_TYPE_LENGTH
  {
    cpp2::ColumnDef col;
    col.name_ref() = "col_fixed_string_1";
    col.type.type_ref() = PropertyType::FIXED_STRING;
    col.type.type_length_ref() = MAX_INDEX_TYPE_LENGTH;
    (*srcsch.columns_ref()).emplace_back(std::move(col));
  }
  // data type is fixed_string, length > MAX_INDEX_TYPE_LENGTH
  {
    cpp2::ColumnDef col;
    col.name_ref() = "col_fixed_string_2";
    col.type.type_ref() = PropertyType::FIXED_STRING;
    col.type.type_length_ref() = 257;
    (*srcsch.columns_ref()).emplace_back(std::move(col));
  }
  auto tagIdVal = std::string(reinterpret_cast<const char*>(&tagId), sizeof(tagId));
  schemas.emplace_back(MetaKeyUtils::indexTagKey(1, "test_tag"), tagIdVal);
  schemas.emplace_back(MetaKeyUtils::schemaTagKey(1, tagId, ver),
                       MetaKeyUtils::schemaVal("test_tag", srcsch));

  auto edgeTypeVal = std::string(reinterpret_cast<const char*>(&edgeType), sizeof(edgeType));
  schemas.emplace_back(MetaKeyUtils::indexEdgeKey(1, "test_edge"), edgeTypeVal);
  schemas.emplace_back(MetaKeyUtils::schemaEdgeKey(1, edgeType, ver),
                       MetaKeyUtils::schemaVal("test_edge", srcsch));

  // space 2
  schemas.emplace_back(MetaKeyUtils::indexTagKey(2, "test_tag"), tagIdVal);
  schemas.emplace_back(MetaKeyUtils::schemaTagKey(2, tagId, ver),
                       MetaKeyUtils::schemaVal("test_tag", srcsch));

  schemas.emplace_back(MetaKeyUtils::indexEdgeKey(2, "test_edge"), edgeTypeVal);
  schemas.emplace_back(MetaKeyUtils::schemaEdgeKey(2, edgeType, ver),
                       MetaKeyUtils::schemaVal("test_edge", srcsch));

  folly::Baton<true, std::atomic> baton;
  kv->asyncMultiPut(
      kDefaultSpaceId, kDefaultPartId, std::move(schemas), [&](nebula::cpp2::ErrorCode code) {
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
        baton.post();
      });
  baton.wait();
}

TEST(IndexProcessorTest, DISABLED_CreateFTIndexTest) {
  fs::TempDir rootPath("/tmp/CreateFTIndexTest.XXXXXX");
  auto kv = MockCluster::initMetaKV(rootPath.path());
  TestUtils::assembleSpace(kv.get(), 1, 1);
  TestUtils::assembleSpace(kv.get(), 2, 1, 1, 1, true);
  mockSchemas(kv.get());
  for (auto id : {5, 6}) {
    // expected error. column col_fixed_string_2 is fixed_string,
    // and type length > MAX_INDEX_TYPE_LENGTH
    {
      cpp2::CreateFTIndexReq req;
      cpp2::FTIndex index;
      nebula::cpp2::SchemaID schemaId;
      if (id == 5) {
        schemaId.tag_id_ref() = 5;
      } else {
        schemaId.edge_type_ref() = 6;
      }
      index.space_id_ref() = 1;
      index.depend_schema_ref() = std::move(schemaId);
      index.fields_ref() = {"col_string", "col_fixed_string_2"};
      req.fulltext_index_name_ref() = "test_ft_index";
      req.index_ref() = std::move(index);

      auto* processor = CreateFTIndexProcessor::instance(kv.get());
      auto f = processor->getFuture();
      processor->process(req);
      auto resp = std::move(f).get();
      ASSERT_EQ(nebula::cpp2::ErrorCode::E_UNSUPPORTED, resp.get_code());
    }
    // expected error. column col_int is not string type.
    {
      cpp2::CreateFTIndexReq req;
      cpp2::FTIndex index;
      nebula::cpp2::SchemaID schemaId;
      if (id == 5) {
        schemaId.tag_id_ref() = 5;
      } else {
        schemaId.edge_type_ref() = 6;
      }
      index.space_id_ref() = 1;
      index.depend_schema_ref() = std::move(schemaId);
      index.fields_ref() = {"col_string", "col_int"};
      req.fulltext_index_name_ref() = "test_ft_index";
      req.index_ref() = std::move(index);

      auto* processor = CreateFTIndexProcessor::instance(kv.get());
      auto f = processor->getFuture();
      processor->process(req);
      auto resp = std::move(f).get();
      ASSERT_EQ(nebula::cpp2::ErrorCode::E_UNSUPPORTED, resp.get_code());
    }
    // expected error. space not found.
    {
      cpp2::CreateFTIndexReq req;
      cpp2::FTIndex index;
      nebula::cpp2::SchemaID schemaId;
      if (id == 5) {
        schemaId.tag_id_ref() = 5;
      } else {
        schemaId.edge_type_ref() = 6;
      }
      index.space_id_ref() = 3;
      index.depend_schema_ref() = std::move(schemaId);
      index.fields_ref() = {"col_string"};
      req.fulltext_index_name_ref() = "test_ft_index";
      req.index_ref() = std::move(index);

      auto* processor = CreateFTIndexProcessor::instance(kv.get());
      auto f = processor->getFuture();
      processor->process(req);
      auto resp = std::move(f).get();
      ASSERT_EQ(nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND, resp.get_code());
    }
    // expected error. schema not found.
    {
      cpp2::CreateFTIndexReq req;
      cpp2::FTIndex index;
      nebula::cpp2::SchemaID schemaId;
      if (id == 5) {
        schemaId.tag_id_ref() = 55;
      } else {
        schemaId.edge_type_ref() = 66;
      }
      index.space_id_ref() = 1;
      index.depend_schema_ref() = std::move(schemaId);
      index.fields_ref() = {"col_string"};
      req.fulltext_index_name_ref() = "test_ft_index";
      req.index_ref() = std::move(index);

      auto* processor = CreateFTIndexProcessor::instance(kv.get());
      auto f = processor->getFuture();
      processor->process(req);
      auto resp = std::move(f).get();
      auto eCode = id == 5 ? nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND
                           : nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND;
      ASSERT_EQ(eCode, resp.get_code());
    }
    // expected error. column not found.
    {
      cpp2::CreateFTIndexReq req;
      cpp2::FTIndex index;
      nebula::cpp2::SchemaID schemaId;
      if (id == 5) {
        schemaId.tag_id_ref() = 5;
      } else {
        schemaId.edge_type_ref() = 6;
      }
      index.space_id_ref() = 1;
      index.depend_schema_ref() = std::move(schemaId);
      index.fields_ref() = {"col_bool"};
      req.fulltext_index_name_ref() = "test_ft_index";
      req.index_ref() = std::move(index);

      auto* processor = CreateFTIndexProcessor::instance(kv.get());
      auto f = processor->getFuture();
      processor->process(req);
      auto resp = std::move(f).get();
      auto eCode = id == 5 ? nebula::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND
                           : nebula::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
      ASSERT_EQ(eCode, resp.get_code());
    }
  }
  // expected success
  {
    cpp2::CreateFTIndexReq req;
    cpp2::FTIndex index;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = 5;
    index.space_id_ref() = 1;
    index.depend_schema_ref() = std::move(schemaId);
    index.fields_ref() = {"col_string", "col_fixed_string_1"};
    req.fulltext_index_name_ref() = "ft_tag_index";
    req.index_ref() = std::move(index);

    auto* processor = CreateFTIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  // expected error. same name fulltext index.
  {
    cpp2::CreateFTIndexReq req;
    cpp2::FTIndex index;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = 5;
    index.space_id_ref() = 1;
    index.depend_schema_ref() = std::move(schemaId);
    index.fields_ref() = {"col_string", "col_fixed_string_1"};
    req.fulltext_index_name_ref() = "ft_tag_index";
    req.index_ref() = (std::move(index));

    auto* processor = CreateFTIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_EXISTED, resp.get_code());
  }
  // expected error. same schema.
  {
    cpp2::CreateFTIndexReq req;
    cpp2::FTIndex index;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = 5;
    index.space_id_ref() = 1;
    index.depend_schema_ref() = std::move(schemaId);
    index.fields_ref() = {"col_string", "col_fixed_string_1"};
    req.fulltext_index_name_ref() = "ft_tag_index_re";
    req.index_ref() = std::move(index);

    auto* processor = CreateFTIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_EXISTED, resp.get_code());
  }
  // expected error. different column.
  {
    cpp2::CreateFTIndexReq req;
    cpp2::FTIndex index;
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = 5;
    index.space_id_ref() = 1;
    index.depend_schema_ref() = std::move(schemaId);
    index.fields_ref() = {"col_string"};
    req.fulltext_index_name_ref() = "ft_tag_index_re";
    req.index_ref() = std::move(index);

    auto* processor = CreateFTIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_EXISTED, resp.get_code());
  }
  // expected error. different schema, but same name.
  {
    cpp2::CreateFTIndexReq req;
    cpp2::FTIndex index;
    nebula::cpp2::SchemaID schemaId;
    schemaId.edge_type_ref() = 6;
    index.space_id_ref() = 1;
    index.depend_schema_ref() = std::move(schemaId);
    index.fields_ref() = {"col_string"};
    req.fulltext_index_name_ref() = "ft_tag_index";
    req.index_ref() = std::move(index);

    auto* processor = CreateFTIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_EXISTED, resp.get_code());
  }
  {
    cpp2::ListFTIndexesReq req;
    auto* processor = ListFTIndexesProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(1, resp.get_indexes().size());
    auto index = resp.get_indexes().begin();
    ASSERT_EQ("ft_tag_index", index->first);
    std::vector<std::string> fields = {"col_string", "col_fixed_string_1"};
    ASSERT_EQ(fields, index->second.get_fields());
    ASSERT_EQ(1, index->second.get_space_id());
    nebula::cpp2::SchemaID schemaId;
    schemaId.tag_id_ref() = 5;
    ASSERT_EQ(schemaId, index->second.get_depend_schema());
  }
  {
    cpp2::DropFTIndexReq req;
    req.space_id_ref() = 1;
    req.fulltext_index_name_ref() = "ft_tag_index";
    auto* processor = DropFTIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    cpp2::ListFTIndexesReq req;
    auto* processor = ListFTIndexesProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(0, resp.get_indexes().size());
  }
  // expected error. fulltext index not found.
  {
    cpp2::DropFTIndexReq req;
    req.space_id_ref() = 1;
    req.fulltext_index_name_ref() = "ft_tag_index";
    auto* processor = DropFTIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND, resp.get_code());
  }

  // expected success
  // Different spaces, the same tag name(same tagId), create full-text indexes with different names.
  {
    {
      for (auto i = 0; i < 2; ++i) {
        cpp2::CreateFTIndexReq req;
        cpp2::FTIndex index;
        nebula::cpp2::SchemaID schemaId;
        schemaId.tag_id_ref() = 5;
        index.space_id_ref() = i + 1;
        index.depend_schema_ref() = std::move(schemaId);
        index.fields_ref() = {"col_string", "col_fixed_string_1"};
        req.fulltext_index_name_ref() = folly::stringPrintf("ft_tag_index_space%d", i + 1);
        req.index_ref() = std::move(index);

        auto* processor = CreateFTIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
      }
    }
    {
      cpp2::ListFTIndexesReq req;
      auto* processor = ListFTIndexesProcessor::instance(kv.get());
      auto f = processor->getFuture();
      processor->process(req);
      auto resp = std::move(f).get();
      ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
      auto indexes = resp.get_indexes();
      ASSERT_EQ(2, indexes.size());
      for (auto i = 0u; i < indexes.size(); ++i) {
        auto key = folly::stringPrintf("ft_tag_index_space%u", i + 1);
        auto iter = indexes.find(key);
        ASSERT_NE(indexes.end(), iter);
        std::vector<std::string> fields = {"col_string", "col_fixed_string_1"};
        ASSERT_EQ(fields, iter->second.get_fields());
        ASSERT_EQ(i + 1, iter->second.get_space_id());
        nebula::cpp2::SchemaID schemaId;
        schemaId.tag_id_ref() = 5;
        ASSERT_EQ(schemaId, iter->second.get_depend_schema());
      }
    }
    {
      for (auto i = 0; i < 2; ++i) {
        cpp2::DropFTIndexReq req;
        req.space_id_ref() = i + 1;
        req.fulltext_index_name_ref() = folly::stringPrintf("ft_tag_index_space%d", i + 1);
        auto* processor = DropFTIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
      }
    }
  }
}

TEST(IndexProcessorTest, DISABLED_DropWithFTIndexTest) {
  fs::TempDir rootPath("/tmp/DropWithFTIndexTest.XXXXXX");
  auto kv = MockCluster::initMetaKV(rootPath.path());
  TestUtils::assembleSpace(kv.get(), 1, 1);
  mockSchemas(kv.get());

  for (auto id : {5, 6}) {
    {
      cpp2::CreateFTIndexReq req;
      cpp2::FTIndex index;
      nebula::cpp2::SchemaID schemaId;
      if (id == 5) {
        schemaId.tag_id_ref() = 5;
      } else {
        schemaId.edge_type_ref() = 6;
      }
      index.space_id_ref() = 1;
      index.depend_schema_ref() = std::move(schemaId);
      index.fields_ref() = {"col_string", "col_fixed_string_1"};
      req.fulltext_index_name_ref() = folly::stringPrintf("test_ft_index_%d", id);
      req.index_ref() = (std::move(index));

      auto* processor = CreateFTIndexProcessor::instance(kv.get());
      auto f = processor->getFuture();
      processor->process(req);
      auto resp = std::move(f).get();
      ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
  }
  {
    cpp2::DropTagReq req;
    req.space_id_ref() = 1;
    req.tag_name_ref() = "test_tag";
    auto* processor = DropTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_RELATED_INDEX_EXISTS, resp.get_code());
  }
  {
    cpp2::DropEdgeReq req;
    req.space_id_ref() = 1;
    req.edge_name_ref() = "test_edge";
    auto* processor = DropEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_RELATED_INDEX_EXISTS, resp.get_code());
  }
}

TEST(IndexProcessorTest, DISABLED_AlterWithFTIndexTest) {
  fs::TempDir rootPath("/tmp/AlterWithFTIndexTest.XXXXXX");
  auto kv = MockCluster::initMetaKV(rootPath.path());
  TestUtils::assembleSpace(kv.get(), 1, 1);
  mockSchemas(kv.get());

  for (auto id : {5, 6}) {
    {
      cpp2::CreateFTIndexReq req;
      cpp2::FTIndex index;
      nebula::cpp2::SchemaID schemaId;
      if (id == 5) {
        schemaId.tag_id_ref() = 5;
      } else {
        schemaId.edge_type_ref() = 6;
      }
      index.space_id_ref() = 1;
      index.depend_schema_ref() = std::move(schemaId);
      index.fields_ref() = {"col_string", "col_fixed_string_1"};
      req.fulltext_index_name_ref() = folly::stringPrintf("test_ft_index_%d", id);
      req.index_ref() = std::move(index);

      auto* processor = CreateFTIndexProcessor::instance(kv.get());
      auto f = processor->getFuture();
      processor->process(req);
      auto resp = std::move(f).get();
      ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
  }
  {
    cpp2::AlterTagReq req;
    std::vector<cpp2::AlterSchemaItem> items;
    cpp2::Schema changeSch;
    cpp2::ColumnDef column;
    column.name = "col_string";
    column.type.type_ref() = PropertyType::INT64;
    (*changeSch.columns_ref()).emplace_back(std::move(column));
    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::CHANGE;
    items.back().schema_ref() = std::move(changeSch);
    req.space_id_ref() = 1;
    req.tag_name_ref() = "test_tag";
    req.tag_items_ref() = items;
    auto* processor = AlterTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_RELATED_FULLTEXT_INDEX_EXISTS, resp.get_code());
  }
  {
    cpp2::AlterTagReq req;
    std::vector<cpp2::AlterSchemaItem> items;
    cpp2::Schema dropSch;
    cpp2::ColumnDef column;
    column.name = "col_string";
    column.type.type_ref() = PropertyType::INT64;
    (*dropSch.columns_ref()).emplace_back(std::move(column));

    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::DROP;
    items.back().schema_ref() = std::move(dropSch);
    req.space_id_ref() = 1;
    req.tag_name_ref() = "test_tag";
    req.tag_items_ref() = items;
    auto* processor = AlterTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_RELATED_FULLTEXT_INDEX_EXISTS, resp.get_code());
  }
  {
    cpp2::AlterTagReq req;
    std::vector<cpp2::AlterSchemaItem> items;
    cpp2::Schema addSch;
    cpp2::ColumnDef column;
    column.name = "col_bool";
    column.type.type_ref() = PropertyType::BOOL;
    (*addSch.columns_ref()).emplace_back(std::move(column));

    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::ADD;
    items.back().schema_ref() = std::move(addSch);
    req.space_id_ref() = 1;
    req.tag_name_ref() = "test_tag";
    req.tag_items_ref() = items;
    auto* processor = AlterTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }

  {
    cpp2::AlterEdgeReq req;
    std::vector<cpp2::AlterSchemaItem> items;
    cpp2::Schema changeSch;
    cpp2::ColumnDef column;
    column.name = "col_string";
    column.type.type_ref() = PropertyType::INT64;
    (*changeSch.columns_ref()).emplace_back(std::move(column));
    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::CHANGE;
    items.back().schema_ref() = std::move(changeSch);
    req.space_id_ref() = 1;
    req.edge_name_ref() = "test_edge";
    req.edge_items_ref() = items;
    auto* processor = AlterEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_RELATED_FULLTEXT_INDEX_EXISTS, resp.get_code());
  }
  {
    cpp2::AlterEdgeReq req;
    std::vector<cpp2::AlterSchemaItem> items;
    cpp2::Schema dropSch;
    cpp2::ColumnDef column;
    column.name = "col_string";
    column.type.type_ref() = PropertyType::INT64;
    (*dropSch.columns_ref()).emplace_back(std::move(column));

    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::DROP;
    items.back().schema_ref() = std::move(dropSch);
    req.space_id_ref() = 1;
    req.edge_name_ref() = "test_edge";
    req.edge_items_ref() = items;
    auto* processor = AlterEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::E_RELATED_FULLTEXT_INDEX_EXISTS, resp.get_code());
  }
  {
    cpp2::AlterEdgeReq req;
    std::vector<cpp2::AlterSchemaItem> items;
    cpp2::Schema addSch;
    cpp2::ColumnDef column;
    column.name = "col_bool";
    column.type.type_ref() = PropertyType::BOOL;
    (*addSch.columns_ref()).emplace_back(std::move(column));

    items.emplace_back();
    items.back().op_ref() = cpp2::AlterSchemaOp::ADD;
    items.back().schema_ref() = std::move(addSch);
    req.space_id_ref() = 1;
    req.edge_name_ref() = "test_edge";
    req.edge_items_ref() = items;
    auto* processor = AlterEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
}

TEST(ProcessorTest, IndexIdInSpaceRangeTest) {
  fs::TempDir rootPath("/tmp/IndexIdInSpaceRangeTest.XXXXXX");
  auto kv = MockCluster::initMetaKV(rootPath.path());
  {
    cpp2::AddHostsReq req;
    std::vector<HostAddr> hosts = {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}};
    req.hosts_ref() = std::move(hosts);
    auto* processor = AddHostsProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  }
  {
    std::vector<HostAddr> hosts = {{"0", 0}, {"1", 1}, {"2", 2}, {"3", 3}};
    TestUtils::registerHB(kv.get(), hosts);
  }
  // mock one space and ten tag, ten edge
  {
    // space Id is 1
    TestUtils::assembleSpace(kv.get(), 1, 1);
    // tagId is 2 in space 1
    TestUtils::mockTag(kv.get(), 1, 0, false, 2, 1);
    // edgeType is 3 in space 1
    TestUtils::mockEdge(kv.get(), 1, 0, false, 3, 1);

    std::vector<cpp2::ColumnDef> columns;
    TestUtils::mockTagIndex(kv.get(), 2, "tag_2", 4, "tag_index_name", columns);
    TestUtils::mockEdgeIndex(kv.get(), 3, "edge_3", 5, "edge_index_name", columns);

    // check tag and edge count
    int count = 0;

    auto tagprefix = MetaKeyUtils::schemaTagsPrefix(1);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto retCode = kv->prefix(kDefaultSpaceId, kDefaultPartId, tagprefix, &iter);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, retCode);
    while (iter->valid()) {
      count++;
      iter->next();
    }
    ASSERT_EQ(1, count);

    auto edgeprefix = MetaKeyUtils::schemaEdgesPrefix(1);
    retCode = kv->prefix(kDefaultSpaceId, kDefaultPartId, edgeprefix, &iter);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, retCode);
    while (iter->valid()) {
      count++;
      iter->next();
    }
    ASSERT_EQ(2, count);

    auto indexPrefix = MetaKeyUtils::indexPrefix(1);
    retCode = kv->prefix(kDefaultSpaceId, kDefaultPartId, indexPrefix, &iter);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, retCode);
    while (iter->valid()) {
      count++;
      iter->next();
    }
    ASSERT_EQ(4, count);

    // modify id to 5 for mock some schema
    std::string kId = "__id__";
    int32_t id = 5;
    std::vector<kvstore::KV> data;
    data.emplace_back(kId, std::string(reinterpret_cast<const char*>(&id), sizeof(id)));
    folly::Baton<true, std::atomic> baton;
    auto ret = nebula::cpp2::ErrorCode::SUCCEEDED;
    kv->asyncMultiPut(
        kDefaultSpaceId, kDefaultPartId, std::move(data), [&](nebula::cpp2::ErrorCode code) {
          ret = code;
          baton.post();
        });
    baton.wait();
    ASSERT_EQ(ret, nebula::cpp2::ErrorCode::SUCCEEDED);
  }

  cpp2::Schema schema;
  std::vector<cpp2::ColumnDef> cols;
  cols.emplace_back(TestUtils::columnDef(0, PropertyType::INT64));
  cols.emplace_back(TestUtils::columnDef(1, PropertyType::FLOAT));
  cols.emplace_back(TestUtils::columnDef(2, PropertyType::STRING));
  schema.columns_ref() = std::move(cols);

  // create space, use global id
  {
    cpp2::SpaceDesc properties;
    properties.space_name_ref() = "my_space";
    properties.partition_num_ref() = 9;
    properties.replica_factor_ref() = 1;
    cpp2::CreateSpaceReq req;
    req.properties_ref() = std::move(properties);

    auto* processor = CreateSpaceProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(6, resp.get_id().get_space_id());
  }
  // create tag in my_space, because there is no local_id key, use global id + 1
  {
    // Succeeded
    cpp2::CreateTagReq req;
    req.space_id_ref() = 6;
    req.tag_name_ref() = "default_tag";
    req.schema_ref() = schema;
    auto* processor = CreateTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(7, resp.get_id().get_tag_id());
  }
  // create tag index in my_space, because there is local_id key, use local_id + 1
  {
    cpp2::CreateTagIndexReq req;
    req.space_id_ref() = 6;
    req.tag_name_ref() = "default_tag";
    std::vector<cpp2::IndexFieldDef> fields;
    cpp2::IndexFieldDef field1;
    field1.name_ref() = "col_0";
    fields.emplace_back(std::move(field1));
    req.fields_ref() = std::move(fields);
    req.index_name_ref() = "default_tag_index";
    auto* processor = CreateTagIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(8, resp.get_id().get_index_id());
  }
  // create edge in my_space, because there is local_id key, use local_id + 1
  {
    // Succeeded
    cpp2::CreateEdgeReq req;
    req.space_id_ref() = 6;
    req.edge_name_ref() = "default_edge";
    req.schema_ref() = schema;
    auto* processor = CreateEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(9, resp.get_id().get_edge_type());
  }
  // create edge index in my_space, because there is local_id key, use local_id + 1
  {
    cpp2::CreateEdgeIndexReq req;
    req.space_id_ref() = 6;
    req.edge_name_ref() = "default_edge";
    std::vector<cpp2::IndexFieldDef> fields;
    cpp2::IndexFieldDef field1;
    field1.name_ref() = "col_0";
    fields.emplace_back(std::move(field1));
    req.fields_ref() = (std::move(fields));
    req.index_name_ref() = ("default_edge_index");
    auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(10, resp.get_id().get_index_id());
  }

  // create space, space Id is global id + 1
  {
    cpp2::SpaceDesc properties;
    properties.space_name_ref() = "last_space";
    properties.partition_num_ref() = 9;
    properties.replica_factor_ref() = 1;
    cpp2::CreateSpaceReq req;
    req.properties_ref() = std::move(properties);

    auto* processor = CreateSpaceProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(7, resp.get_id().get_space_id());
  }
  // create tag in my_space, because there is local_id key, use local id + 1
  {
    // Succeeded
    cpp2::CreateTagReq req;
    req.space_id_ref() = (6);
    req.tag_name_ref() = ("my_tag");
    req.schema_ref() = (schema);
    auto* processor = CreateTagProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(11, resp.get_id().get_tag_id());
  }
  // create tag index in my_space, because there is local_id key, use local_id + 1
  {
    cpp2::CreateTagIndexReq req;
    req.space_id_ref() = 6;
    req.tag_name_ref() = "my_tag";
    std::vector<cpp2::IndexFieldDef> fields;
    cpp2::IndexFieldDef field1;
    field1.name_ref() = "col_0";
    fields.emplace_back(std::move(field1));
    req.fields_ref() = std::move(fields);
    req.index_name_ref() = "my_tag_index";
    auto* processor = CreateTagIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(12, resp.get_id().get_index_id());
  }
  // create edge in my_space, because there is local_id key use local_id + 1
  {
    // Succeeded
    cpp2::CreateEdgeReq req;
    req.space_id_ref() = 6;
    req.edge_name_ref() = "my_edge";
    req.schema_ref() = schema;
    auto* processor = CreateEdgeProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(13, resp.get_id().get_edge_type());
  }
  // create edge index in my_space, because there is local_id key, use local_id + 1
  {
    // Allow to create edge index on no fields
    cpp2::CreateEdgeIndexReq req;
    req.space_id_ref() = 6;
    req.edge_name_ref() = "my_edge";
    std::vector<cpp2::IndexFieldDef> fields;
    cpp2::IndexFieldDef field1;
    field1.name_ref() = "col_0";
    fields.emplace_back(std::move(field1));
    req.fields_ref() = std::move(fields);
    req.index_name_ref() = "my_edge_index";
    auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    ASSERT_EQ(14, resp.get_id().get_index_id());
  }
}

// Check list tag index status
TEST(IndexProcessorTest, ListTagIndexStatusTest) {
  fs::TempDir rootPath("/tmp/ListTagIndexStatusTest.XXXXXX");
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
  TestUtils::createSomeHosts(kv.get());
  GraphSpaceID spaceId = 1;
  TestUtils::assembleSpace(kv.get(), spaceId, 1);
  std::vector<kvstore::KV> data;

  for (JobID jobId = 1; jobId < 3; jobId++) {
    auto jobKey = MetaKeyUtils::jobKey(spaceId, jobId);
    std::vector<std::string> paras{"tag_index_name"};
    cpp2::JobStatus jstatus;
    if (jobId == 1) {
      jstatus = cpp2::JobStatus::FAILED;
    } else {
      jstatus = cpp2::JobStatus::FINISHED;
    }
    auto jobVal = MetaKeyUtils::jobVal(
        cpp2::JobType::REBUILD_TAG_INDEX, paras, jstatus, 1, 2, nebula::cpp2::ErrorCode::SUCCEEDED);
    data.emplace_back(std::move(jobKey), std::move(jobVal));
  }
  folly::Baton<true, std::atomic> baton;
  auto ret = nebula::cpp2::ErrorCode::SUCCEEDED;
  kv->asyncMultiPut(
      kDefaultSpaceId, kDefaultPartId, std::move(data), [&](nebula::cpp2::ErrorCode code) {
        ret = code;
        baton.post();
      });
  baton.wait();
  ASSERT_EQ(ret, nebula::cpp2::ErrorCode::SUCCEEDED);

  // Allow to create edge index on no fields
  cpp2::ListIndexStatusReq req;
  req.space_id_ref() = 1;
  auto* processor = ListTagIndexStatusProcessor::instance(kv.get());
  auto f = processor->getFuture();
  processor->process(req);
  auto resp = std::move(f).get();
  ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  auto statuses = resp.get_statuses();
  ASSERT_EQ(statuses.size(), 1);
  ASSERT_TRUE(!(statuses[0].get_status().compare("FINISHED")));
}

// Check list edge index status
TEST(IndexProcessorTest, ListEdgeIndexStatusTest) {
  fs::TempDir rootPath("/tmp/ListEdgeIndexStatusTest.XXXXXX");
  std::unique_ptr<kvstore::KVStore> kv(MockCluster::initMetaKV(rootPath.path()));
  TestUtils::createSomeHosts(kv.get());
  GraphSpaceID spaceId = 1;
  TestUtils::assembleSpace(kv.get(), spaceId, 1);
  std::vector<kvstore::KV> data;

  for (JobID jobId = 1; jobId < 3; jobId++) {
    auto jobKey = MetaKeyUtils::jobKey(spaceId, jobId);
    std::vector<std::string> paras{"edge_index_name"};
    cpp2::JobStatus jstatus;
    if (jobId == 1) {
      jstatus = cpp2::JobStatus::FAILED;
    } else {
      jstatus = cpp2::JobStatus::FINISHED;
    }
    auto jobVal = MetaKeyUtils::jobVal(cpp2::JobType::REBUILD_EDGE_INDEX,
                                       paras,
                                       jstatus,
                                       1,
                                       2,
                                       nebula::cpp2::ErrorCode::SUCCEEDED);
    data.emplace_back(std::move(jobKey), std::move(jobVal));
  }
  folly::Baton<true, std::atomic> baton;
  auto ret = nebula::cpp2::ErrorCode::SUCCEEDED;
  kv->asyncMultiPut(
      kDefaultSpaceId, kDefaultPartId, std::move(data), [&](nebula::cpp2::ErrorCode code) {
        ret = code;
        baton.post();
      });
  baton.wait();
  ASSERT_EQ(ret, nebula::cpp2::ErrorCode::SUCCEEDED);

  // Allow to create edge index on no fields
  cpp2::ListIndexStatusReq req;
  req.space_id_ref() = 1;
  auto* processor = ListEdgeIndexStatusProcessor::instance(kv.get());
  auto f = processor->getFuture();
  processor->process(req);
  auto resp = std::move(f).get();
  ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
  auto statuses = resp.get_statuses();
  ASSERT_EQ(statuses.size(), 1);
  ASSERT_TRUE(!(statuses[0].get_status().compare("FINISHED")));
}

}  // namespace meta
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}
