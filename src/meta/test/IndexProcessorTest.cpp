/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include <gtest/gtest.h>
#include "common/base/Base.h"
#include "common/base/CommonMacro.h"
#include "common/fs/TempDir.h"
#include "meta/test/TestUtils.h"
#include "meta/processors/schemaMan/CreateTagProcessor.h"
#include "meta/processors/schemaMan/CreateEdgeProcessor.h"
#include "meta/processors/schemaMan/DropTagProcessor.h"
#include "meta/processors/schemaMan/DropEdgeProcessor.h"
#include "meta/processors/schemaMan/AlterTagProcessor.h"
#include "meta/processors/schemaMan/AlterEdgeProcessor.h"
#include "meta/processors/indexMan/CreateTagIndexProcessor.h"
#include "meta/processors/indexMan/DropTagIndexProcessor.h"
#include "meta/processors/indexMan/GetTagIndexProcessor.h"
#include "meta/processors/indexMan/ListTagIndexesProcessor.h"
#include "meta/processors/indexMan/CreateEdgeIndexProcessor.h"
#include "meta/processors/indexMan/DropEdgeIndexProcessor.h"
#include "meta/processors/indexMan/GetEdgeIndexProcessor.h"
#include "meta/processors/indexMan/ListEdgeIndexesProcessor.h"
#include "meta/processors/indexMan/FTIndexProcessor.h"

namespace nebula {
namespace meta {

using cpp2::PropertyType;

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
        schemaProp.set_ttl_duration(100);
        schemaProp.set_ttl_col("col_0");
        schema.set_schema_prop(std::move(schemaProp));
        schema.set_columns(std::move(cols));

        cpp2::CreateEdgeReq req;
        req.set_space_id(1);
        req.set_edge_name("ttl");
        req.set_schema(schema);
        auto* processor = CreateEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("ttl");
        cpp2::IndexFieldDef field;
        field.set_name("col_0");
        req.set_fields({field});
        req.set_index_name("ttl_index");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::AlterEdgeReq req;
        cpp2::SchemaProp schemaProp;
        schemaProp.set_ttl_duration(1000);
        schemaProp.set_ttl_col("col_0");
        req.set_space_id(1);
        req.set_edge_name("ttl");
        req.set_schema_prop(std::move(schemaProp));
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
        schemaProp.set_ttl_duration(100);
        schemaProp.set_ttl_col("col_0");
        schema.set_schema_prop(std::move(schemaProp));
        schema.set_columns(std::move(cols));

        cpp2::CreateTagReq req;
        req.set_space_id(1);
        req.set_tag_name("ttl");
        req.set_schema(schema);
        auto* processor = CreateTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("ttl");
        cpp2::IndexFieldDef field;
        field.set_name("col_0");
        req.set_fields({field});
        req.set_index_name("ttl_index");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::AlterTagReq req;
        cpp2::SchemaProp schemaProp;
        schemaProp.set_ttl_duration(1000);
        schemaProp.set_ttl_col("col_0");
        req.set_space_id(1);
        req.set_tag_name("ttl");
        req.set_schema_prop(std::move(schemaProp));
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
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        std::vector<cpp2::IndexFieldDef> fields{};
        req.set_fields(std::move(fields));
        req.set_index_name("no_field_index");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        // Duplicate tag index on no fields
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        std::vector<cpp2::IndexFieldDef> fields{};
        req.set_fields(std::move(fields));
        req.set_index_name("no_field_index_1");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        cpp2::IndexFieldDef field;
        field.set_name("tag_0_col_0");
        req.set_fields({field});
        req.set_index_name("single_field_index");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        cpp2::IndexFieldDef field;
        field.set_name("tag_0_col_0");
        req.set_fields({field});
        req.set_index_name("duplicate_field_index");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_EXISTED, resp.get_code());
    }
    {
        cpp2::DropTagIndexReq req;
        req.set_space_id(1);
        req.set_index_name("no_field_index");
        auto* processor = DropTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        // Allow to create tag index on no fields
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        std::vector<cpp2::IndexFieldDef> fields{};
        req.set_fields(std::move(fields));
        req.set_index_name("no_field_index");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        std::vector<cpp2::IndexFieldDef> fields;
        cpp2::IndexFieldDef field1, field2;
        field1.set_name("tag_0_col_0");
        field2.set_name("tag_0_col_1");
        fields.emplace_back(std::move(field1));
        fields.emplace_back(std::move(field2));
        req.set_fields(std::move(fields));
        req.set_index_name("multi_field_index");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        std::vector<cpp2::IndexFieldDef> fields;
        cpp2::IndexFieldDef field1, field2;
        field1.set_name("tag_0_col_0");
        field2.set_name("tag_0_col_1");
        fields.emplace_back(std::move(field1));
        fields.emplace_back(std::move(field2));
        req.set_fields(std::move(fields));
        req.set_index_name("duplicate_field_index");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_EXISTED, resp.get_code());
    }
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        cpp2::IndexFieldDef field;
        field.set_name("tag_0_col_0");
        req.set_fields({field});
        req.set_index_name("duplicate_field_index");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_EXISTED, resp.get_code());
    }
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        std::vector<cpp2::IndexFieldDef> fields;
        cpp2::IndexFieldDef field1, field2;
        field1.set_name("tag_0_col_1");
        field2.set_name("tag_0_col_0");
        fields.emplace_back(std::move(field1));
        fields.emplace_back(std::move(field2));
        req.set_fields(std::move(fields));
        req.set_index_name("disorder_field_index");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        std::vector<cpp2::IndexFieldDef> fields;
        cpp2::IndexFieldDef field1, field2;
        field1.set_name("tag_0_col_0");
        field2.set_name("tag_0_col_0");
        fields.emplace_back(std::move(field1));
        fields.emplace_back(std::move(field2));
        req.set_fields(std::move(fields));
        req.set_index_name("conflict_index");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_CONFLICT, resp.get_code());
    }
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_not_exist");
        cpp2::IndexFieldDef field;
        field.set_name("tag_0_col_0");
        req.set_fields({field});
        req.set_index_name("tag_not_exist_index");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND, resp.get_code());
    }
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        cpp2::IndexFieldDef field;
        field.set_name("field_not_exist");
        req.set_fields({field});
        req.set_index_name("field_not_exist_index");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND, resp.get_code());
    }
    {
        // Test index have exist
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        cpp2::IndexFieldDef field;
        field.set_name("tag_0_col_0");
        req.set_fields({field});
        req.set_index_name("single_field_index");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_EXISTED, resp.get_code());
    }
    {
        cpp2::ListTagIndexesReq req;
        req.set_space_id(1);
        auto* processor = ListTagIndexesProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        auto items = resp.get_items();

        ASSERT_EQ(3, items.size());
        {
            cpp2::ColumnDef column;
            column.set_name("tag_0_col_0");
            column.type.set_type(PropertyType::INT64);
            std::vector<cpp2::ColumnDef> columns;
            columns.emplace_back(std::move(column));

            auto singleItem = items[0];
            ASSERT_EQ(2, singleItem.get_index_id());
            ASSERT_EQ("single_field_index", singleItem.get_index_name());
            auto singleFieldResult = singleItem.get_fields();
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
            stringColumn.type.set_type_length(MAX_INDEX_TYPE_LENGTH);
            columns.emplace_back(std::move(stringColumn));

            auto multiItem = items[1];
            ASSERT_EQ(3, multiItem.get_index_id());
            auto multiFieldResult = multiItem.get_fields();
            ASSERT_TRUE(TestUtils::verifyResult(columns, multiFieldResult));
        }
        {
            std::vector<cpp2::ColumnDef> columns;
            cpp2::ColumnDef stringColumn;
            stringColumn.set_name("tag_0_col_1");
            stringColumn.type.set_type(PropertyType::FIXED_STRING);
            stringColumn.type.set_type_length(MAX_INDEX_TYPE_LENGTH);
            columns.emplace_back(std::move(stringColumn));
            cpp2::ColumnDef intColumn;
            intColumn.set_name("tag_0_col_0");
            intColumn.type.set_type(PropertyType::INT64);
            columns.emplace_back(std::move(intColumn));

            auto disorderItem = items[2];
            ASSERT_EQ(4, disorderItem.get_index_id());
            auto disorderFieldResult = disorderItem.get_fields();
            ASSERT_TRUE(TestUtils::verifyResult(columns, disorderFieldResult));
        }
    }
    {
        cpp2::GetTagIndexReq req;
        req.set_space_id(1);
        req.set_index_name("single_field_index");

        auto* processor = GetTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        auto item = resp.get_item();
        auto fields = item.get_fields();
        ASSERT_EQ(2, item.get_index_id());

        cpp2::ColumnDef column;
        column.set_name("tag_0_col_0");
        column.type.set_type(PropertyType::INT64);
        std::vector<cpp2::ColumnDef> columns;
        columns.emplace_back(std::move(column));

        ASSERT_TRUE(TestUtils::verifyResult(columns, fields));
    }
    {
        cpp2::DropTagIndexReq req;
        req.set_space_id(1);
        req.set_index_name("single_field_index");

        auto* processor = DropTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::GetTagIndexReq req;
        req.set_space_id(1);
        req.set_index_name("single_field_index");

        auto* processor = GetTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND, resp.get_code());
    }
    {
        cpp2::DropTagIndexReq req;
        req.set_space_id(1);
        req.set_index_name("single_field_index");

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
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        cpp2::IndexFieldDef field;
        field.set_name("tag_0_col_0");
        req.set_fields({field});
        req.set_index_name("single_field_index");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::GetTagIndexReq req;
        req.set_space_id(1);
        req.set_index_name("single_field_index");

        auto* processor = GetTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        auto item = resp.get_item();
        auto fields = item.get_fields();
        ASSERT_EQ(1, item.get_index_id());

        cpp2::ColumnDef column;
        column.set_name("tag_0_col_0");
        column.type.set_type(PropertyType::INT64);
        column.set_nullable(true);
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
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        std::vector<cpp2::IndexFieldDef> fields{};
        req.set_fields(std::move(fields));
        req.set_index_name("no_field_index");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        // Duplicate edge index on no fields
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        std::vector<cpp2::IndexFieldDef> fields{};
        req.set_fields(std::move(fields));
        req.set_index_name("no_field_index_1");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        cpp2::IndexFieldDef field;
        field.set_name("edge_0_col_0");
        req.set_fields({field});
        req.set_index_name("single_field_index");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        cpp2::IndexFieldDef field;
        field.set_name("edge_0_col_0");
        req.set_fields({field});
        req.set_index_name("duplicate_field_index");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_EXISTED, resp.get_code());
    }
    {
        cpp2::DropEdgeIndexReq req;
        req.set_space_id(1);
        req.set_index_name("no_field_index");
        auto* processor = DropEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        // Allow to create edge index on no fields
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        std::vector<cpp2::IndexFieldDef> fields{};
        req.set_fields(std::move(fields));
        req.set_index_name("no_field_index");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_NE(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        std::vector<cpp2::IndexFieldDef> fields;
        cpp2::IndexFieldDef field1, field2;
        field1.set_name("edge_0_col_0");
        field2.set_name("edge_0_col_1");
        fields.emplace_back(std::move(field1));
        fields.emplace_back(std::move(field2));
        req.set_fields(std::move(fields));
        req.set_index_name("multi_field_index");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        std::vector<cpp2::IndexFieldDef> fields;
        cpp2::IndexFieldDef field1, field2;
        field1.set_name("edge_0_col_0");
        field2.set_name("edge_0_col_1");
        fields.emplace_back(std::move(field1));
        fields.emplace_back(std::move(field2));
        req.set_fields(std::move(fields));
        req.set_index_name("duplicate_field_index");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_EXISTED, resp.get_code());
    }
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        cpp2::IndexFieldDef field;
        field.set_name("edge_0_col_0");
        req.set_fields({field});
        req.set_index_name("duplicate_field_index");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_EXISTED, resp.get_code());
    }
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        std::vector<cpp2::IndexFieldDef> fields;
        cpp2::IndexFieldDef field1, field2;
        field1.set_name("edge_0_col_1");
        field2.set_name("edge_0_col_0");
        fields.emplace_back(std::move(field1));
        fields.emplace_back(std::move(field2));
        req.set_fields(std::move(fields));
        req.set_index_name("disorder_field_index");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        std::vector<cpp2::IndexFieldDef> fields;
        cpp2::IndexFieldDef field1, field2;
        field1.set_name("edge_0_col_0");
        field2.set_name("edge_0_col_0");
        fields.emplace_back(std::move(field1));
        fields.emplace_back(std::move(field2));
        req.set_fields(std::move(fields));
        req.set_index_name("conflict_index");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_CONFLICT, resp.get_code());
    }
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_not_exist");
        cpp2::IndexFieldDef field;
        field.set_name("edge_0_col_0");
        req.set_fields({field});
        req.set_index_name("edge_not_exist_index");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND, resp.get_code());
    }
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        cpp2::IndexFieldDef field;
        field.set_name("edge_field_not_exist");
        req.set_fields({field});
        req.set_index_name("field_not_exist_index");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND, resp.get_code());
    }
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        cpp2::IndexFieldDef field;
        field.set_name("edge_0_col_0");
        req.set_fields({field});
        req.set_index_name("single_field_index");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_EXISTED, resp.get_code());
    }
    {
        cpp2::ListEdgeIndexesReq req;
        req.set_space_id(1);
        auto* processor = ListEdgeIndexesProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        auto items = resp.get_items();
        ASSERT_EQ(3, items.size());
        {
            cpp2::ColumnDef column;
            column.set_name("edge_0_col_0");
            column.type.set_type(PropertyType::INT64);
            std::vector<cpp2::ColumnDef> columns;
            columns.emplace_back(std::move(column));

            auto singleItem = items[0];
            ASSERT_EQ(2, singleItem.get_index_id());
            auto singleFieldResult = singleItem.get_fields();
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
            stringColumn.type.set_type_length(MAX_INDEX_TYPE_LENGTH);
            columns.emplace_back(std::move(stringColumn));

            auto multiItem = items[1];
            ASSERT_EQ(3, multiItem.get_index_id());
            auto multiFieldResult = multiItem.get_fields();
            ASSERT_TRUE(TestUtils::verifyResult(columns, multiFieldResult));
        }
        {
            std::vector<cpp2::ColumnDef> columns;
            cpp2::ColumnDef stringColumn;
            stringColumn.set_name("edge_0_col_1");
            stringColumn.type.set_type(PropertyType::FIXED_STRING);
            stringColumn.type.set_type_length(MAX_INDEX_TYPE_LENGTH);
            columns.emplace_back(std::move(stringColumn));
            cpp2::ColumnDef intColumn;
            intColumn.set_name("edge_0_col_0");
            intColumn.type.set_type(PropertyType::INT64);
            columns.emplace_back(std::move(intColumn));

            auto disorderItem = items[2];
            ASSERT_EQ(4, disorderItem.get_index_id());
            auto disorderFieldResult = disorderItem.get_fields();
            ASSERT_TRUE(TestUtils::verifyResult(columns, disorderFieldResult));
        }
    }
    {
        cpp2::GetEdgeIndexReq req;
        req.set_space_id(1);
        req.set_index_name("single_field_index");

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
        req.set_space_id(1);
        req.set_index_name("single_field_index");

        auto* processor = DropEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::GetEdgeIndexReq req;
        req.set_space_id(1);
        req.set_index_name("single_field_index");

        auto* processor = GetEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND, resp.get_code());
    }
    {
        cpp2::DropEdgeIndexReq req;
        req.set_space_id(1);
        req.set_index_name("single_field_index");

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
            field.set_name(folly::stringPrintf("col-%d", i));
            bigFields.emplace_back(std::move(field));
        }
    }
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_fields(bigFields);
        req.set_index_name("index_0");
        auto* processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_CONFLICT, resp.get_code());
    }
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_fields(std::move(bigFields));
        req.set_index_name("index_0");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_CONFLICT, resp.get_code());
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
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        cpp2::IndexFieldDef field;
        field.set_name("edge_0_col_0");
        req.set_fields({field});
        req.set_index_name("single_field_index");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::GetEdgeIndexReq req;
        req.set_space_id(1);
        req.set_index_name("single_field_index");
        auto* processor = GetEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        auto item = resp.get_item();
        auto fields = item.get_fields();
        ASSERT_EQ(1, item.get_index_id());

        cpp2::ColumnDef column;
        column.set_name("edge_0_col_0");
        column.type.set_type(PropertyType::INT64);
        column.set_nullable(true);
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
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        cpp2::IndexFieldDef field;
        field.set_name("edge_0_col_0");
        req.set_fields({field});
        req.set_index_name("single_field_index");
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
        items.back().set_op(cpp2::AlterSchemaOp::ADD);
        items.back().set_schema(std::move(addSch));
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_edge_items(items);
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
        column.type.set_type(PropertyType::INT64);
        (*changeSch.columns_ref()).emplace_back(std::move(column));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::CHANGE);
        items.back().set_schema(std::move(changeSch));

        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_edge_items(items);
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
        column.type.set_type(PropertyType::INT64);
        (*dropSch.columns_ref()).emplace_back(std::move(column));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::DROP);
        items.back().set_schema(std::move(dropSch));

        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_edge_items(items);
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
        column.type.set_type(PropertyType::INT64);
        (*changeSch.columns_ref()).emplace_back(std::move(column));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::CHANGE);
        items.back().set_schema(std::move(changeSch));

        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_edge_items(items);
        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_CONFLICT, resp.get_code());
    }
    // Verify ErrorCode of drop
    {
        cpp2::AlterEdgeReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        cpp2::Schema dropSch;
        cpp2::ColumnDef column;
        column.name = "edge_0_col_0";
        column.type.set_type(PropertyType::INT64);
        (*dropSch.columns_ref()).emplace_back(std::move(column));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::DROP);
        items.back().set_schema(std::move(dropSch));

        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_edge_items(items);
        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_CONFLICT, resp.get_code());
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
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        cpp2::IndexFieldDef field;
        field.set_name("tag_0_col_0");
        req.set_fields({field});
        req.set_index_name("single_field_index");
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
        column.type.set_type(PropertyType::INT64);
        (*addSch.columns_ref()).emplace_back(std::move(column));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::ADD);
        items.back().set_schema(std::move(addSch));
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_tag_items(items);
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
        column.type.set_type(PropertyType::INT64);
        (*changeSch.columns_ref()).emplace_back(std::move(column));
        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::CHANGE);
        items.back().set_schema(std::move(changeSch));
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_tag_items(items);
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
        column.type.set_type(PropertyType::INT64);
        (*dropSch.columns_ref()).emplace_back(std::move(column));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::DROP);
        items.back().set_schema(std::move(dropSch));
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_tag_items(items);
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
        column.type.set_type(PropertyType::INT64);
        (*changeSch.columns_ref()).emplace_back(std::move(column));
        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::CHANGE);
        items.back().set_schema(std::move(changeSch));
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_tag_items(items);
        auto* processor = AlterTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_CONFLICT, resp.get_code());
    }
    {
        cpp2::AlterTagReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        cpp2::Schema dropSch;
        cpp2::ColumnDef column;
        column.name = "tag_0_col_0";
        column.type.set_type(PropertyType::INT64);
        (*dropSch.columns_ref()).emplace_back(std::move(column));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::DROP);
        items.back().set_schema(std::move(dropSch));
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_tag_items(items);
        auto* processor = AlterTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_CONFLICT, resp.get_code());
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
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        cpp2::IndexFieldDef field;
        field.set_name("edge_0_col_0");
        req.set_fields({field});
        req.set_index_name("single_field_index");
        auto* processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::DropEdgeReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        auto* processor = DropEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_CONFLICT, resp.get_code());
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
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        cpp2::IndexFieldDef field;
        field.set_name("tag_0_col_0");
        req.set_fields({field});
        req.set_index_name("single_field_index");
        auto *processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::DropTagReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        auto* processor = DropTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_CONFLICT, resp.get_code());
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
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        cpp2::IndexFieldDef field;
        field.set_name("tag_0_col_0");
        req.set_fields({field});
        req.set_index_name("single_field_index");

        auto *processor = CreateTagIndexProcessor::instance(kv.get());
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
            column.type.set_type(i < 1 ? PropertyType::INT64 : PropertyType::STRING);
            (*addSch.columns_ref()).emplace_back(std::move(column));
        }
        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::ADD);
        items.back().set_schema(std::move(addSch));
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_tag_items(items);

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
        schemaProp.set_ttl_col("tag_0_col_0");
        schemaProp.set_ttl_duration(100);
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_schema_prop(std::move(schemaProp));

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
        schemaProp.set_ttl_col("tag_0_col_10");
        schemaProp.set_ttl_duration(100);
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_schema_prop(std::move(schemaProp));

        auto* processor = AlterTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_UNSUPPORTED, resp.get_code());
    }
    // Drop index
    {
        cpp2::DropTagIndexReq req;
        req.set_space_id(1);
        req.set_index_name("single_field_index");

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
        schemaProp.set_ttl_col("tag_0_col_0");
        schemaProp.set_ttl_duration(100);
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_schema_prop(std::move(schemaProp));

        auto* processor = AlterTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Tag with ttl to creat index on ttl col
    {
        cpp2::CreateTagIndexReq req;
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        cpp2::IndexFieldDef field;
        field.set_name("tag_0_col_0");
        req.set_fields({field});
        req.set_index_name("ttl_with_index");

        auto *processor = CreateTagIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Drop ttl property
    {
        cpp2::AlterTagReq req;
        cpp2::SchemaProp schemaProp;
        schemaProp.set_ttl_duration(0);
        schemaProp.set_ttl_col("");
        req.set_space_id(1);
        req.set_tag_name("tag_0");
        req.set_schema_prop(std::move(schemaProp));

        auto* processor = AlterTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_UNSUPPORTED, resp.get_code());
    }
    {
        cpp2::DropTagIndexReq req;
        req.set_space_id(1);
        req.set_index_name("ttl_with_index");

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
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        cpp2::IndexFieldDef field;
        field.set_name("edge_0_col_0");
        req.set_fields({field});
        req.set_index_name("single_field_index");

        auto *processor = CreateEdgeIndexProcessor::instance(kv.get());
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
            column.type.set_type(i < 1 ? PropertyType::INT64 : PropertyType::STRING);
            (*addSch.columns_ref()).emplace_back(std::move(column));
        }
        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::ADD);
        items.back().set_schema(std::move(addSch));

        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_edge_items(items);
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
        schemaProp.set_ttl_col("edge_0_col_0");
        schemaProp.set_ttl_duration(100);
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_schema_prop(std::move(schemaProp));

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
        schemaProp.set_ttl_col("edge_0_col_10");
        schemaProp.set_ttl_duration(100);
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_schema_prop(std::move(schemaProp));

        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_UNSUPPORTED, resp.get_code());
    }
    // Drop index
    {
        cpp2::DropEdgeIndexReq req;
        req.set_space_id(1);
        req.set_index_name("single_field_index");

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
        schemaProp.set_ttl_col("edge_0_col_0");
        schemaProp.set_ttl_duration(100);
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_schema_prop(std::move(schemaProp));

        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Edge with ttl to create index on ttl col
    {
        cpp2::CreateEdgeIndexReq req;
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        cpp2::IndexFieldDef field;
        field.set_name("edge_0_col_0");
        req.set_fields({field});
        req.set_index_name("ttl_with_index");

        auto *processor = CreateEdgeIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // Drop ttl property
    {
        cpp2::AlterEdgeReq req;
        cpp2::SchemaProp schemaProp;
        schemaProp.set_ttl_duration(0);
        schemaProp.set_ttl_col("");
        req.set_space_id(1);
        req.set_edge_name("edge_0");
        req.set_schema_prop(std::move(schemaProp));

        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_UNSUPPORTED, resp.get_code());
    }
    {
        cpp2::DropEdgeIndexReq req;
        req.set_space_id(1);
        req.set_index_name("ttl_with_index");

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
        col.set_name("col_string");
        col.type.set_type(PropertyType::STRING);
        (*srcsch.columns_ref()).emplace_back(std::move(col));
    }
    // non-string type
    {
        cpp2::ColumnDef col;
        col.set_name("col_int");
        col.type.set_type(PropertyType::INT64);
        (*srcsch.columns_ref()).emplace_back(std::move(col));
    }
    // data type is fixed_string, length <= MAX_INDEX_TYPE_LENGTH
    {
        cpp2::ColumnDef col;
        col.set_name("col_fixed_string_1");
        col.type.set_type(meta::cpp2::PropertyType::FIXED_STRING);
        col.type.set_type_length(MAX_INDEX_TYPE_LENGTH);
        (*srcsch.columns_ref()).emplace_back(std::move(col));
    }
    // data type is fixed_string, length > MAX_INDEX_TYPE_LENGTH
    {
        cpp2::ColumnDef col;
        col.set_name("col_fixed_string_2");
        col.type.set_type(meta::cpp2::PropertyType::FIXED_STRING);
        col.type.set_type_length(257);
        (*srcsch.columns_ref()).emplace_back(std::move(col));
    }
    auto tagIdVal = std::string(reinterpret_cast<const char*>(&tagId), sizeof(tagId));
    schemas.emplace_back(MetaServiceUtils::indexTagKey(1, "test_tag"), tagIdVal);
    schemas.emplace_back(MetaServiceUtils::schemaTagKey(1, tagId, ver),
                         MetaServiceUtils::schemaVal("test_tag", srcsch));

    auto edgeTypeVal = std::string(reinterpret_cast<const char*>(&edgeType), sizeof(edgeType));
    schemas.emplace_back(MetaServiceUtils::indexEdgeKey(1, "test_edge"), edgeTypeVal);
    schemas.emplace_back(MetaServiceUtils::schemaEdgeKey(1, edgeType, ver),
                         MetaServiceUtils::schemaVal("test_edge", srcsch));

    folly::Baton<true, std::atomic> baton;
    kv->asyncMultiPut(0, 0, std::move(schemas),
        [&] (nebula::cpp2::ErrorCode code) {
            ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
            baton.post();
        });
    baton.wait();
}

TEST(IndexProcessorTest, CreateFTIndexTest) {
    fs::TempDir rootPath("/tmp/CreateFTIndexTest.XXXXXX");
    auto kv = MockCluster::initMetaKV(rootPath.path());
    TestUtils::assembleSpace(kv.get(), 1, 1);
    mockSchemas(kv.get());
    for (auto id : {5, 6}) {
        // expected error. column col_fixed_string_2 is fixed_string,
        // and type length > MAX_INDEX_TYPE_LENGTH
        {
            cpp2::CreateFTIndexReq req;
            cpp2::FTIndex index;
            cpp2::SchemaID schemaId;
            if (id == 5) {
                schemaId.set_tag_id(5);
            } else {
                schemaId.set_edge_type(6);
            }
            index.set_space_id(1);
            index.set_depend_schema(std::move(schemaId));
            index.set_fields({"col_string", "col_fixed_string_2"});
            req.set_fulltext_index_name("test_ft_index");
            req.set_index(std::move(index));

            auto *processor = CreateFTIndexProcessor::instance(kv.get());
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ASSERT_EQ(nebula::cpp2::ErrorCode::E_UNSUPPORTED, resp.get_code());
        }
        // expected error. column col_int is not string type.
        {
            cpp2::CreateFTIndexReq req;
            cpp2::FTIndex index;
            cpp2::SchemaID schemaId;
            if (id == 5) {
                schemaId.set_tag_id(5);
            } else {
                schemaId.set_edge_type(6);
            }
            index.set_space_id(1);
            index.set_depend_schema(std::move(schemaId));
            index.set_fields({"col_string", "col_int"});
            req.set_fulltext_index_name("test_ft_index");
            req.set_index(std::move(index));

            auto *processor = CreateFTIndexProcessor::instance(kv.get());
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ASSERT_EQ(nebula::cpp2::ErrorCode::E_UNSUPPORTED, resp.get_code());
        }
        // expected error. space not found.
        {
            cpp2::CreateFTIndexReq req;
            cpp2::FTIndex index;
            cpp2::SchemaID schemaId;
            if (id == 5) {
                schemaId.set_tag_id(5);
            } else {
                schemaId.set_edge_type(6);
            }
            index.set_space_id(2);
            index.set_depend_schema(std::move(schemaId));
            index.set_fields({"col_string"});
            req.set_fulltext_index_name("test_ft_index");
            req.set_index(std::move(index));

            auto *processor = CreateFTIndexProcessor::instance(kv.get());
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ASSERT_EQ(nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND, resp.get_code());
        }
        // expected error. schema not found.
        {
            cpp2::CreateFTIndexReq req;
            cpp2::FTIndex index;
            cpp2::SchemaID schemaId;
            if (id == 5) {
                schemaId.set_tag_id(55);
            } else {
                schemaId.set_edge_type(66);
            }
            index.set_space_id(1);
            index.set_depend_schema(std::move(schemaId));
            index.set_fields({"col_string"});
            req.set_fulltext_index_name("test_ft_index");
            req.set_index(std::move(index));

            auto *processor = CreateFTIndexProcessor::instance(kv.get());
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            auto eCode = id == 5
                       ? nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND
                       : nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND;
            ASSERT_EQ(eCode, resp.get_code());
        }
        // expected error. column not found.
        {
            cpp2::CreateFTIndexReq req;
            cpp2::FTIndex index;
            cpp2::SchemaID schemaId;
            if (id == 5) {
                schemaId.set_tag_id(5);
            } else {
                schemaId.set_edge_type(6);
            }
            index.set_space_id(1);
            index.set_depend_schema(std::move(schemaId));
            index.set_fields({"col_bool"});
            req.set_fulltext_index_name("test_ft_index");
            req.set_index(std::move(index));

            auto *processor = CreateFTIndexProcessor::instance(kv.get());
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            auto eCode = id == 5
                       ? nebula::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND
                       : nebula::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
            ASSERT_EQ(eCode, resp.get_code());
        }
    }
    // expected success
    {
        cpp2::CreateFTIndexReq req;
        cpp2::FTIndex index;
        cpp2::SchemaID schemaId;
        schemaId.set_tag_id(5);
        index.set_space_id(1);
        index.set_depend_schema(std::move(schemaId));
        index.set_fields({"col_string", "col_fixed_string_1"});
        req.set_fulltext_index_name("ft_tag_index");
        req.set_index(std::move(index));

        auto *processor = CreateFTIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    // expected error. same name fulltext index.
    {
        cpp2::CreateFTIndexReq req;
        cpp2::FTIndex index;
        cpp2::SchemaID schemaId;
        schemaId.set_tag_id(5);
        index.set_space_id(1);
        index.set_depend_schema(std::move(schemaId));
        index.set_fields({"col_string", "col_fixed_string_1"});
        req.set_fulltext_index_name("ft_tag_index");
        req.set_index(std::move(index));

        auto *processor = CreateFTIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_EXISTED, resp.get_code());
    }
    // expected error. same schema.
    {
        cpp2::CreateFTIndexReq req;
        cpp2::FTIndex index;
        cpp2::SchemaID schemaId;
        schemaId.set_tag_id(5);
        index.set_space_id(1);
        index.set_depend_schema(std::move(schemaId));
        index.set_fields({"col_string", "col_fixed_string_1"});
        req.set_fulltext_index_name("ft_tag_index_re");
        req.set_index(std::move(index));

        auto *processor = CreateFTIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_EXISTED, resp.get_code());
    }
    // expected error. different column.
    {
        cpp2::CreateFTIndexReq req;
        cpp2::FTIndex index;
        cpp2::SchemaID schemaId;
        schemaId.set_tag_id(5);
        index.set_space_id(1);
        index.set_depend_schema(std::move(schemaId));
        index.set_fields({"col_string"});
        req.set_fulltext_index_name("ft_tag_index_re");
        req.set_index(std::move(index));

        auto *processor = CreateFTIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_EXISTED, resp.get_code());
    }
    // expected error. different schema, but same name.
    {
        cpp2::CreateFTIndexReq req;
        cpp2::FTIndex index;
        cpp2::SchemaID schemaId;
        schemaId.set_edge_type(6);
        index.set_space_id(1);
        index.set_depend_schema(std::move(schemaId));
        index.set_fields({"col_string"});
        req.set_fulltext_index_name("ft_tag_index");
        req.set_index(std::move(index));

        auto *processor = CreateFTIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_EXISTED, resp.get_code());
    }
    {
        cpp2::ListFTIndexesReq req;
        auto *processor = ListFTIndexesProcessor::instance(kv.get());
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
        cpp2::SchemaID schemaId;
        schemaId.set_tag_id(5);
        ASSERT_EQ(schemaId, index->second.get_depend_schema());
    }
    {
        cpp2::DropFTIndexReq req;
        req.set_space_id(1);
        req.set_fulltext_index_name("ft_tag_index");
        auto *processor = DropFTIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
    }
    {
        cpp2::ListFTIndexesReq req;
        auto *processor = ListFTIndexesProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        ASSERT_EQ(0, resp.get_indexes().size());
    }
    // expected error. fulltext index not found.
    {
        cpp2::DropFTIndexReq req;
        req.set_space_id(1);
        req.set_fulltext_index_name("ft_tag_index");
        auto *processor = DropFTIndexProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND, resp.get_code());
    }
}

TEST(IndexProcessorTest, DropWithFTIndexTest) {
    fs::TempDir rootPath("/tmp/DropWithFTIndexTest.XXXXXX");
    auto kv = MockCluster::initMetaKV(rootPath.path());
    TestUtils::assembleSpace(kv.get(), 1, 1);
    mockSchemas(kv.get());

    for (auto id : {5, 6}) {
        {
            cpp2::CreateFTIndexReq req;
            cpp2::FTIndex index;
            cpp2::SchemaID schemaId;
            if (id == 5) {
                schemaId.set_tag_id(5);
            } else {
                schemaId.set_edge_type(6);
            }
            index.set_space_id(1);
            index.set_depend_schema(std::move(schemaId));
            index.set_fields({"col_string", "col_fixed_string_1"});
            req.set_fulltext_index_name(folly::stringPrintf("test_ft_index_%d", id));
            req.set_index(std::move(index));

            auto *processor = CreateFTIndexProcessor::instance(kv.get());
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
        }
    }
    {
        cpp2::DropTagReq req;
        req.set_space_id(1);
        req.set_tag_name("test_tag");
        auto* processor = DropTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_CONFLICT, resp.get_code());
    }
    {
        cpp2::DropEdgeReq req;
        req.set_space_id(1);
        req.set_edge_name("test_edge");
        auto* processor = DropEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_CONFLICT, resp.get_code());
    }
}

TEST(IndexProcessorTest, AlterWithFTIndexTest) {
    fs::TempDir rootPath("/tmp/AlterWithFTIndexTest.XXXXXX");
    auto kv = MockCluster::initMetaKV(rootPath.path());
    TestUtils::assembleSpace(kv.get(), 1, 1);
    mockSchemas(kv.get());

    for (auto id : {5, 6}) {
        {
            cpp2::CreateFTIndexReq req;
            cpp2::FTIndex index;
            cpp2::SchemaID schemaId;
            if (id == 5) {
                schemaId.set_tag_id(5);
            } else {
                schemaId.set_edge_type(6);
            }
            index.set_space_id(1);
            index.set_depend_schema(std::move(schemaId));
            index.set_fields({"col_string", "col_fixed_string_1"});
            req.set_fulltext_index_name(folly::stringPrintf("test_ft_index_%d", id));
            req.set_index(std::move(index));

            auto *processor = CreateFTIndexProcessor::instance(kv.get());
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
        column.type.set_type(PropertyType::INT64);
        (*changeSch.columns_ref()).emplace_back(std::move(column));
        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::CHANGE);
        items.back().set_schema(std::move(changeSch));
        req.set_space_id(1);
        req.set_tag_name("test_tag");
        req.set_tag_items(items);
        auto* processor = AlterTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_CONFLICT, resp.get_code());
    }
    {
        cpp2::AlterTagReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        cpp2::Schema dropSch;
        cpp2::ColumnDef column;
        column.name = "col_string";
        column.type.set_type(PropertyType::INT64);
        (*dropSch.columns_ref()).emplace_back(std::move(column));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::DROP);
        items.back().set_schema(std::move(dropSch));
        req.set_space_id(1);
        req.set_tag_name("test_tag");
        req.set_tag_items(items);
        auto* processor = AlterTagProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_CONFLICT, resp.get_code());
    }
    {
        cpp2::AlterTagReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        cpp2::Schema addSch;
        cpp2::ColumnDef column;
        column.name = "col_bool";
        column.type.set_type(PropertyType::BOOL);
        (*addSch.columns_ref()).emplace_back(std::move(column));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::ADD);
        items.back().set_schema(std::move(addSch));
        req.set_space_id(1);
        req.set_tag_name("test_tag");
        req.set_tag_items(items);
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
        column.type.set_type(PropertyType::INT64);
        (*changeSch.columns_ref()).emplace_back(std::move(column));
        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::CHANGE);
        items.back().set_schema(std::move(changeSch));
        req.set_space_id(1);
        req.set_edge_name("test_edge");
        req.set_edge_items(items);
        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_CONFLICT, resp.get_code());
    }
    {
        cpp2::AlterEdgeReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        cpp2::Schema dropSch;
        cpp2::ColumnDef column;
        column.name = "col_string";
        column.type.set_type(PropertyType::INT64);
        (*dropSch.columns_ref()).emplace_back(std::move(column));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::DROP);
        items.back().set_schema(std::move(dropSch));
        req.set_space_id(1);
        req.set_edge_name("test_edge");
        req.set_edge_items(items);
        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::E_CONFLICT, resp.get_code());
    }
    {
        cpp2::AlterEdgeReq req;
        std::vector<cpp2::AlterSchemaItem> items;
        cpp2::Schema addSch;
        cpp2::ColumnDef column;
        column.name = "col_bool";
        column.type.set_type(PropertyType::BOOL);
        (*addSch.columns_ref()).emplace_back(std::move(column));

        items.emplace_back();
        items.back().set_op(cpp2::AlterSchemaOp::ADD);
        items.back().set_schema(std::move(addSch));
        req.set_space_id(1);
        req.set_edge_name("test_edge");
        req.set_edge_items(items);
        auto* processor = AlterEdgeProcessor::instance(kv.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, resp.get_code());
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
