/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "base/NebulaKeyUtils.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "fs/TempDir.h"
#include "storage/test/TestUtils.h"
#include "storage/mutate/AddVerticesProcessor.h"
#include "storage/mutate/AddEdgesProcessor.h"
#include "storage/mutate/DeleteVertexProcessor.h"
#include "storage/mutate/DeleteEdgesProcessor.h"
#include "storage/mutate/UpdateEdgeProcessor.h"
#include "storage/mutate/UpdateVertexProcessor.h"

namespace nebula {
namespace storage {

static std::shared_ptr<meta::SchemaProviderIf> genTagSchema(
        TagID tagId,
        int32_t intFieldsNum,
        int32_t stringFieldsNum) {
    nebula::cpp2::Schema schema;
    for (auto i = 0; i < intFieldsNum; i++) {
        nebula::cpp2::ColumnDef column;
        column.name = folly::stringPrintf("tag_%d_col_%d", tagId, i);
        column.type.type = nebula::cpp2::SupportedType::INT;
        schema.columns.emplace_back(std::move(column));
    }
    for (auto i = intFieldsNum; i < intFieldsNum + stringFieldsNum; i++) {
        nebula::cpp2::ColumnDef column;
        column.name = folly::stringPrintf("tag_%d_col_%d", tagId, i);
        column.type.type = nebula::cpp2::SupportedType::STRING;
        schema.columns.emplace_back(std::move(column));
    }
    return std::make_shared<ResultSchemaProvider>(std::move(schema));
}

static std::shared_ptr<meta::SchemaProviderIf> genEdgeSchema(
        int32_t intFieldsNum,
        int32_t stringFieldsNum) {
    nebula::cpp2::Schema schema;
    for (auto i = 0; i < intFieldsNum; i++) {
        nebula::cpp2::ColumnDef column;
        column.name = folly::stringPrintf("col_%d", i);
        column.type.type = nebula::cpp2::SupportedType::INT;
        schema.columns.emplace_back(std::move(column));
    }
    for (auto i = intFieldsNum; i < intFieldsNum + stringFieldsNum; i++) {
        nebula::cpp2::ColumnDef column;
        column.name = folly::stringPrintf("col_%d", i);
        column.type.type = nebula::cpp2::SupportedType::STRING;
        schema.columns.emplace_back(std::move(column));
    }
    return std::make_shared<ResultSchemaProvider>(std::move(schema));
}

static std::unique_ptr<meta::SchemaManager> mockSchemaMan(
        GraphSpaceID spaceId,
        const std::vector<nebula::cpp2::IndexItem>& tagIndexes,
        const std::vector<nebula::cpp2::IndexItem>& edgeIndexes) {
    auto* schemaMan = new AdHocSchemaManager();
    for (auto edgeType = 101; edgeType < 110; edgeType++) {
        schemaMan->addEdgeSchema(spaceId /*space id*/, edgeType /*edge type*/,
                                 genEdgeSchema(10, 10));
    }
    for (auto tagId = 3001; tagId < 3010; tagId++) {
        schemaMan->addTagSchema(
                spaceId /*space id*/, tagId, genTagSchema(tagId, 3, 3));
    }
    for (auto& index : tagIndexes) {
        schemaMan->addTagIndex(spaceId, index);
    }
    for (auto& index : edgeIndexes) {
        schemaMan->addEdgeIndex(spaceId, index);
    }
    std::unique_ptr<meta::SchemaManager> sm(schemaMan);
    return sm;
}

static std::vector<nebula::cpp2::IndexItem> mockIndexes(bool isEdge,
                                                        int32_t intFieldsNum,
                                                        int32_t stringFieldsNum) {
    std::vector<nebula::cpp2::IndexItem> indexes;
    if (isEdge) {
        for (auto edgeType = 101; edgeType < 110; edgeType++) {
            std::vector<nebula::cpp2::ColumnDef> cols;
            for (auto i = 0; i < intFieldsNum; i++) {
                nebula::cpp2::ColumnDef column;
                column.name = folly::stringPrintf("col_%d", i);
                column.type.type = nebula::cpp2::SupportedType::INT;
                cols.emplace_back(std::move(column));
            }
            for (auto i = intFieldsNum; i < intFieldsNum + stringFieldsNum; i++) {
                nebula::cpp2::ColumnDef column;
                column.name = folly::stringPrintf("col_%d", i);
                column.type.type = nebula::cpp2::SupportedType::STRING;
                cols.emplace_back(std::move(column));
            }
            // indexId can be same with edgeType
            indexes.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                                 edgeType, edgeType, std::move(cols));
        }
    } else {
        for (auto tagId = 3001; tagId < 3010; tagId++) {
            std::vector<nebula::cpp2::ColumnDef> cols;
            for (auto i = 0; i < intFieldsNum; i++) {
                nebula::cpp2::ColumnDef column;
                column.name = folly::stringPrintf("tag_%d_col_%d", tagId, i);
                column.type.type = nebula::cpp2::SupportedType::INT;
                cols.emplace_back(std::move(column));
            }
            for (auto i = intFieldsNum; i < intFieldsNum + stringFieldsNum; i++) {
                nebula::cpp2::ColumnDef column;
                column.name = folly::stringPrintf("tag_%d_col_%d", tagId, i);
                column.type.type = nebula::cpp2::SupportedType::STRING;
                cols.emplace_back(std::move(column));
            }
            // indexId can be same with tagId
            indexes.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                                 tagId, tagId, std::move(cols));
        }
    }
    return indexes;
}

TEST(IndexTest, InsertVerticesTest) {
    fs::TempDir rootPath("/tmp/InsertVerticesTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());
    LOG(INFO) << "Prepare meta...";
    auto indexes = mockIndexes(false, 3, 3);
    auto schemaMan = mockSchemaMan(0, indexes, std::vector<nebula::cpp2::IndexItem>());
    cpp2::AddVerticesRequest req;
    req.space_id = 0;
    req.overwritable = true;
    for (auto partId = 0; partId < 3; partId++) {
        std::vector<cpp2::Vertex> vertices;
        for (auto vertexId = partId * 10; vertexId < 10 * (partId + 1); vertexId++) {
            std::vector<cpp2::Tag> tags;
            for (auto tagId = 3001; tagId < 3010; tagId++) {
                RowWriter writer;
                for (int64_t numInt = 0; numInt < 3; numInt++) {
                    writer << numInt;
                }
                for (auto numString = 3; numString < 6; numString++) {
                    writer << folly::stringPrintf("tag_string_col_%d", numString);
                }
                auto val = writer.encode();

                tags.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                                  tagId,
                                  std::move(val));
            }
            vertices.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                                  vertexId,
                                  std::move(tags));
        }
        req.parts.emplace(partId, std::move(vertices));
    }
    auto* processor = AddVerticesProcessor::instance(kv.get(), schemaMan.get(), nullptr);
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_codes.size());
    LOG(INFO) << "Check data...";
    for (auto partId = 0; partId < 3; partId++) {
        for (auto vertexId = 10 * partId; vertexId < 10 * (partId + 1); vertexId++) {
            auto prefix = NebulaKeyUtils::vertexPrefix(partId, vertexId);
            std::unique_ptr<kvstore::KVIterator> iter;
            EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, kv->prefix(0, partId, prefix, &iter));
            int32_t rowCount = 0;
            while (iter->valid()) {
                rowCount++;
                iter->next();
            }
            EXPECT_EQ(9, rowCount);
        }
    }
    LOG(INFO) << "Check index...";
    for (auto partId = 0; partId < 3; partId++) {
        for (auto indexId = 3001; indexId < 3010; indexId++) {
            auto prefix = NebulaKeyUtils::indexPrefix(partId, indexId);
            std::unique_ptr<kvstore::KVIterator> iter;
            EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, kv->prefix(0, partId, prefix, &iter));
            int32_t rowCount = 0;
            while (iter->valid()) {
                rowCount++;
                iter->next();
            }
            EXPECT_EQ(10, rowCount);
        }
    }
}

TEST(IndexTest, InsertEdgeTest) {
    fs::TempDir rootPath("/tmp/InsertEdgesTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());
    LOG(INFO) << "Prepare meta...";
    auto indexes = mockIndexes(true, 10, 10);
    auto schemaMan = mockSchemaMan(0, std::vector<nebula::cpp2::IndexItem>(), indexes);
    LOG(INFO) << "Build AddEdgesRequest...";
    auto* processor = AddEdgesProcessor::instance(kv.get(), schemaMan.get(), nullptr);
    cpp2::AddEdgesRequest req;
    req.space_id = 0;
    req.overwritable = true;
    // partId => List<Edge>
    // Edge => {EdgeKey, props}
    for (auto partId = 0; partId < 3; partId++) {
        std::vector<cpp2::Edge> edges;
        for (auto vertexId = partId * 10; vertexId < (partId + 1) * 10; vertexId++) {
            // Generate 7 out-edges for each edgeType.
            for (auto dstId = 10001; dstId <= 10007; dstId++) {
                VLOG(3) << "Write part " << partId << ", vertex " << vertexId << ", dst " << dstId;
                // Write multi versions,  we should get the latest version.
                for (auto version = 0; version < 3; version++) {
                    for (auto edgeType = 101; edgeType < 110; edgeType++) {
                        cpp2::EdgeKey key;
                        RowWriter writer(nullptr);
                        for (uint64_t numInt = 0; numInt < 10; numInt++) {
                            writer << (dstId + numInt);
                        }
                        for (auto numString = 10; numString < 20; numString++) {
                            writer << folly::stringPrintf("string_col_%d_%d", numString, version);
                        }
                        auto val = writer.encode();
                        key.set_src(vertexId);
                        key.set_edge_type(edgeType);
                        key.set_ranking(0);
                        key.set_dst(dstId);
                        edges.emplace_back();
                        edges.back().set_key(key);
                        edges.back().set_props(std::move(val));
                    }
                }
            }
        }
        req.parts.emplace(partId, std::move(edges));
    }
    LOG(INFO) << "Test AddEdgesProcessor...";
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_codes.size());

    LOG(INFO) << "Check data...";
    for (auto partId = 0; partId < 3; partId++) {
        for (auto vertexId = 10 * partId; vertexId < 10 * (partId + 1); vertexId++) {
            auto prefix = NebulaKeyUtils::edgePrefix(partId, vertexId);
            std::unique_ptr<kvstore::KVIterator> iter;
            EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, kv->prefix(0, partId, prefix, &iter));
            int32_t rowCount = 0;
            while (iter->valid()) {
                rowCount++;
                iter->next();
            }
            EXPECT_EQ(63, rowCount);
        }
    }
    LOG(INFO) << "Check index...";
    for (auto partId = 0; partId < 3; partId++) {
        for (auto indexId = 101; indexId < 110; indexId++) {
            auto prefix = NebulaKeyUtils::indexPrefix(partId, indexId);
            std::unique_ptr<kvstore::KVIterator> iter;
            EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, kv->prefix(0, partId, prefix, &iter));
            int32_t rowCount = 0;
            while (iter->valid()) {
                rowCount++;
                iter->next();
            }
            EXPECT_EQ(70, rowCount);
        }
    }
}

TEST(IndexTest, DeleteVertexTest) {
    fs::TempDir rootPath("/tmp/DeleteVertexTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());
    LOG(INFO) << "Prepare meta...";
    auto indexes = mockIndexes(false, 3, 3);
    auto schemaMan = mockSchemaMan(0, indexes, std::vector<nebula::cpp2::IndexItem>());
    {
        cpp2::AddVerticesRequest req;
        req.space_id = 0;
        req.overwritable = true;
        {
            std::vector<cpp2::Vertex> vertices;
            std::vector<cpp2::Tag> tags;
            for (auto tagId = 3001; tagId < 3010; tagId++) {
                RowWriter writer;
                for (int64_t numInt = 0; numInt < 3; numInt++) {
                    writer << numInt;
                }
                for (auto numString = 3; numString < 6; numString++) {
                    writer << folly::stringPrintf("tag_string_col_%d", numString);
                }
                auto val = writer.encode();
                tags.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                                  tagId,
                                  std::move(val));
            }
            vertices.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                                  10,
                                  std::move(tags));
            req.parts.emplace(1, std::move(vertices));
        }
        auto* processor = AddVerticesProcessor::instance(kv.get(), schemaMan.get(), nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_codes.size());
    }
    {
        auto* processor = DeleteVertexProcessor::instance(kv.get(), schemaMan.get(), nullptr);
        cpp2::DeleteVertexRequest req;
        req.set_space_id(0);
        req.set_part_id(1);
        req.set_vid(10);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_codes.size());
        for (auto indexId = 3001; indexId < 3010; indexId++) {
            auto prefix = NebulaKeyUtils::indexPrefix(1, indexId);
            std::unique_ptr<kvstore::KVIterator> iter;
            EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, kv->prefix(0, 1, prefix, &iter));
            int32_t rowCount = 0;
            while (iter->valid()) {
                rowCount++;
                iter->next();
            }
            EXPECT_EQ(0, rowCount);
        }
    }
}

TEST(IndexTest, DeleteEdgeTest) {
    fs::TempDir rootPath("/tmp/DeleteEdgeTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());
    LOG(INFO) << "Prepare meta...";
    auto indexes = mockIndexes(true, 10, 10);
    auto schemaMan = mockSchemaMan(0, std::vector<nebula::cpp2::IndexItem>(), indexes);
    LOG(INFO) << "Build AddEdgesRequest...";
    {
        auto* processor = AddEdgesProcessor::instance(kv.get(), schemaMan.get(), nullptr);
        cpp2::AddEdgesRequest req;
        req.space_id = 0;
        req.overwritable = true;
        {
            std::vector<cpp2::Edge> edges;
            for (auto edgeType = 101; edgeType < 110; edgeType++) {
                cpp2::EdgeKey key;
                RowWriter writer(nullptr);
                for (uint64_t numInt = 0; numInt < 10; numInt++) {
                    writer << (numInt);
                }
                for (auto numString = 10; numString < 20; numString++) {
                    writer << folly::stringPrintf("string_col_%d", numString);
                }
                auto val = writer.encode();
                key.set_src(10);
                key.set_edge_type(edgeType);
                key.set_ranking(0);
                key.set_dst(10001);
                edges.emplace_back();
                edges.back().set_key(key);
                edges.back().set_props(std::move(val));
            }
            req.parts.emplace(1, std::move(edges));
        }
        LOG(INFO) << "Test AddEdgesProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_codes.size());
    }
    {
        auto* processor = DeleteEdgesProcessor::instance(kv.get(), schemaMan.get());
        cpp2::DeleteEdgesRequest req;
        req.set_space_id(0);
        // partId => List<EdgeKey>
        std::vector<cpp2::EdgeKey> keys;
        for (auto edgeType = 101; edgeType < 110; edgeType++) {
            cpp2::EdgeKey key;
            RowWriter writer(nullptr);
            for (uint64_t numInt = 0; numInt < 10; numInt++) {
                writer << (numInt);
            }
            for (auto numString = 10; numString < 20; numString++) {
                writer << folly::stringPrintf("string_col_%d", numString);
            }
            auto val = writer.encode();
            key.set_src(10);
            key.set_edge_type(edgeType);
            key.set_ranking(0);
            key.set_dst(10001);
            keys.emplace_back(key);
        }
        req.parts.emplace(1, std::move(keys));
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_codes.size());
        for (auto indexId = 101; indexId < 110; indexId++) {
            auto prefix = NebulaKeyUtils::indexPrefix(1, indexId);
            std::unique_ptr<kvstore::KVIterator> iter;
            EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, kv->prefix(0, 1, prefix, &iter));
            int32_t rowCount = 0;
            while (iter->valid()) {
                rowCount++;
                iter->next();
            }
            EXPECT_EQ(0, rowCount);
        }
    }
}

TEST(IndexTest, UpdateVertexTest) {
    fs::TempDir rootPath("/tmp/DeleteVertexTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());
    LOG(INFO) << "Prepare meta...";
    auto indexes = mockIndexes(false, 3, 3);
    auto schemaMan = mockSchemaMan(0, indexes, std::vector<nebula::cpp2::IndexItem>());
    {
        cpp2::AddVerticesRequest req;
        req.space_id = 0;
        req.overwritable = true;
        {
            std::vector<cpp2::Vertex> vertices;
            std::vector<cpp2::Tag> tags;
            for (auto tagId = 3001; tagId < 3010; tagId++) {
                RowWriter writer;
                for (int64_t numInt = 0; numInt < 3; numInt++) {
                    writer << tagId + numInt;
                }
                for (auto numString = 3; numString < 6; numString++) {
                    writer << folly::stringPrintf("tag_string_col_%d", numString);
                }
                auto val = writer.encode();
                tags.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                                  tagId,
                                  std::move(val));
            }
            vertices.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                                  10,
                                  std::move(tags));
            req.parts.emplace(1, std::move(vertices));
        }

        auto* processor = AddVerticesProcessor::instance(kv.get(), schemaMan.get(), nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_codes.size());
    }
    {
        cpp2::UpdateVertexRequest req;
        req.set_space_id(0);
        req.set_vertex_id(10);
        req.set_part_id(1);
        LOG(INFO) << "Build filter...";
        // left int: $^.3001.tag_3001_col_2 >= 3001
        auto* tag1 = new std::string("3001");
        auto* prop1 = new std::string("tag_3001_col_2");
        auto* srcExp1 = new SourcePropertyExpression(tag1, prop1);
        auto* priExp1 = new PrimaryExpression(3001L);
        auto* left = new RelationalExpression(srcExp1,
                                              RelationalExpression::Operator::GE,
                                              priExp1);
        // right string: $^.3003.tag_3003_col_3 == tag_string_col_3_2;
        auto* tag2 = new std::string("3003");
        auto* prop2 = new std::string("tag_3003_col_3");
        auto* srcExp2 = new SourcePropertyExpression(tag2, prop2);
        std::string col3("tag_string_col_3");
        auto* priExp2 = new PrimaryExpression(col3);
        auto* right = new RelationalExpression(srcExp2,
                                               RelationalExpression::Operator::EQ,
                                               priExp2);
        // left AND right is ture
        auto logExp = std::make_unique<LogicalExpression>(left, LogicalExpression::AND, right);
        req.set_filter(Expression::encode(logExp.get()));
        LOG(INFO) << "Build update items...";
        std::vector<cpp2::UpdateItem> items;
        // int: 3001.tag_3001_col_0 = 1
        cpp2::UpdateItem item1;
        item1.set_name("3001");
        item1.set_prop("tag_3001_col_0");
        PrimaryExpression val1(1L);
        item1.set_value(Expression::encode(&val1));
        items.emplace_back(item1);
        // string: 3005.tag_3005_col_4 = tag_string_col_4_2_new
        cpp2::UpdateItem item2;
        item2.set_name("3005");
        item2.set_prop("tag_3005_col_4");
        std::string col4new("tag_string_col_4_2_new");
        PrimaryExpression val2(col4new);
        item2.set_value(Expression::encode(&val2));
        items.emplace_back(item2);
        req.set_update_items(std::move(items));
        LOG(INFO) << "Build yield...";
        // Return tag props: 3001.tag_3001_col_0, 3003.tag_3003_col_2, 3005.tag_3005_col_4
        decltype(req.return_columns) tmpColumns;
        for (int i = 0; i < 3; i++) {
            SourcePropertyExpression sourcePropExp(
                    new std::string(folly::to<std::string>(3001 + i * 2)),
                    new std::string(folly::stringPrintf("tag_%d_col_%d", 3001 + i * 2, i * 2)));
            tmpColumns.emplace_back(Expression::encode(&sourcePropExp));
        }
        req.set_return_columns(std::move(tmpColumns));
        req.set_insertable(false);

        LOG(INFO) << "Test UpdateVertexRequest...";
        auto* processor = UpdateVertexProcessor::instance(kv.get(), schemaMan.get(), nullptr);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        LOG(INFO) << "Verify index ...";
        std::vector<std::string> keys;
        for (auto indexId = 3001; indexId < 3010; indexId++) {
            auto prefix = NebulaKeyUtils::indexPrefix(1, indexId);
            std::unique_ptr<kvstore::KVIterator> iter;
            EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, kv->prefix(0, 1, prefix, &iter));

            while (iter->valid()) {
                keys.emplace_back(iter->key());
                iter->next();
            }
        }
        EXPECT_EQ(9, keys.size());
        int32_t oldHint = 0 , newHint = 0;
        for (auto& key : keys) {
            auto pos = key.find("tag_string_col_3tag_string_col_4tag_string_col");
            if (pos != std::string::npos) {
                oldHint++;
            }
            pos = key.find("tag_string_col_3tag_string_col_4_2_newtag_string_col");
            if (pos != std::string::npos) {
                newHint++;
            }
        }
        EXPECT_EQ(8, oldHint);
        EXPECT_EQ(1, newHint);
    }
}

TEST(IndexTest, UpdateEdgeTest) {
    fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());
    LOG(INFO) << "Prepare meta...";
    auto indexes = mockIndexes(true, 10, 10);
    auto schemaMan = mockSchemaMan(0, std::vector<nebula::cpp2::IndexItem>(), indexes);
    LOG(INFO) << "Build AddEdgesRequest...";
    {
        auto* processor = AddEdgesProcessor::instance(kv.get(), schemaMan.get(), nullptr);
        cpp2::AddEdgesRequest req;
        req.space_id = 0;
        req.overwritable = true;
        {
            std::vector<cpp2::Edge> edges;
            for (auto edgeType = 101; edgeType < 110; edgeType++) {
                cpp2::EdgeKey key;
                RowWriter writer(nullptr);
                for (uint64_t numInt = 0; numInt < 10; numInt++) {
                    writer << (numInt);
                }
                for (auto numString = 10; numString < 20; numString++) {
                    writer << folly::stringPrintf("string_col_%d", numString);
                }
                auto val = writer.encode();
                key.set_src(10);
                key.set_edge_type(edgeType);
                key.set_ranking(0);
                key.set_dst(10001);
                edges.emplace_back();
                edges.back().set_key(key);
                edges.back().set_props(std::move(val));
            }
            req.parts.emplace(1, std::move(edges));
        }
        LOG(INFO) << "Test AddEdgesProcessor...";
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_codes.size());
    }
    {
        LOG(INFO) << "Build UpdateEdgeRequest...";
        GraphSpaceID spaceId = 0;
        PartitionID partId = 1;
        VertexID srcId = 10;
        VertexID dstId = 10001;
        // src = 1, edge_type = 101, ranking = 0, dst = 10001
        storage::cpp2::EdgeKey edgeKey;
        edgeKey.set_src(srcId);
        edgeKey.set_edge_type(101);
        edgeKey.set_ranking(0);
        edgeKey.set_dst(dstId);
        cpp2::UpdateEdgeRequest req;
        req.set_space_id(spaceId);
        req.set_edge_key(edgeKey);
        req.set_part_id(partId);
        req.set_filter("");
        std::vector<cpp2::UpdateItem> items;
        // string: 101.col_10 = string_col_10_2_new
        cpp2::UpdateItem item;
        item.set_name("101");
        item.set_prop("col_10");
        std::string col10new("string_col_10_2_new");
        PrimaryExpression val2(col10new);
        item.set_value(Expression::encode(&val2));
        items.emplace_back(item);
        req.set_update_items(std::move(items));
        decltype(req.return_columns) tmpColumns;
        AliasPropertyExpression edgePropExp(
                new std::string(""), new std::string("101"), new std::string("col_10"));
        tmpColumns.emplace_back(Expression::encode(&edgePropExp));
        edgePropExp = AliasPropertyExpression(
                new std::string(""), new std::string("101"), new std::string("col_11"));
        tmpColumns.emplace_back(Expression::encode(&edgePropExp));
        req.set_return_columns(std::move(tmpColumns));
        req.set_insertable(false);
        auto* processor = UpdateEdgeProcessor::instance(kv.get(), schemaMan.get(), nullptr);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        EXPECT_EQ(0, resp.result.failed_codes.size());
        std::vector<std::string> keys;
        for (auto indexId = 101; indexId < 110; indexId++) {
            auto prefix = NebulaKeyUtils::indexPrefix(1, indexId);
            std::unique_ptr<kvstore::KVIterator> iter;
            EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, kv->prefix(0, 1, prefix, &iter));

            while (iter->valid()) {
                keys.emplace_back(iter->key());
                iter->next();
            }
        }
        EXPECT_EQ(9, keys.size());
        int32_t oldHint = 0 , newHint = 0;
        for (auto& key : keys) {
            auto pos = key.find("col_10string");
            if (pos != std::string::npos) {
                oldHint++;
            }
            pos = key.find("col_10_2_newstring");
            if (pos != std::string::npos) {
                newHint++;
            }
        }
        EXPECT_EQ(8, oldHint);
        EXPECT_EQ(1, newHint);
    }
}

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}


