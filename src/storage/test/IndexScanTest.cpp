/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "base/NebulaKeyUtils.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <limits>
#include "fs/TempDir.h"
#include "storage/test/TestUtils.h"
#include "storage/index/LookUpVertexIndexProcessor.h"
#include "storage/index/LookUpEdgeIndexProcessor.h"
#include "dataman/RowSetReader.h"
#include "dataman/RowReader.h"

namespace nebula {
namespace storage {
using IndexValues = std::vector<std::pair<nebula::cpp2::SupportedType, std::string>>;

std::string indexStr(RowReader* reader, const nebula::cpp2::ColumnDef& col) {
    auto res = RowReader::getPropByName(reader, col.get_name());
    if (!ok(res)) {
        LOG(ERROR) << "Skip bad column prop " << col.get_name();
        return "";
    }
    auto&& v = value(std::move(res));
    switch (col.get_type().get_type()) {
        case nebula::cpp2::SupportedType::BOOL: {
            auto val = boost::get<bool>(v);
            std::string raw;
            raw.reserve(sizeof(bool));
            raw.append(reinterpret_cast<const char*>(&val), sizeof(bool));
            return raw;
        }
        case nebula::cpp2::SupportedType::INT:
        case nebula::cpp2::SupportedType::TIMESTAMP: {
            return NebulaKeyUtils::encodeInt64(boost::get<int64_t>(v));
        }
        case nebula::cpp2::SupportedType::FLOAT:
        case nebula::cpp2::SupportedType::DOUBLE: {
            return NebulaKeyUtils::encodeDouble(boost::get<double>(v));
        }
        case nebula::cpp2::SupportedType::STRING: {
            return boost::get<std::string>(v);
        }
        default:
            LOG(ERROR) << "Unknown type: "
                       << static_cast<int32_t>(col.get_type().get_type());
    }
    return "";
}

IndexValues collectIndexValues(RowReader* reader,
                               const std::vector<nebula::cpp2::ColumnDef>& cols) {
    IndexValues values;
    if (reader == nullptr) {
        return values;
    }
    for (auto& col : cols) {
        auto val = indexStr(reader, col);
        values.emplace_back(col.get_type().get_type(), std::move(val));
    }
    return values;
}

static std::string genEdgeIndexKey(meta::SchemaManager* schemaMan,
                                   const std::string &prop,
                                   GraphSpaceID spaceId,
                                   PartitionID partId,
                                   EdgeType type,
                                   std::shared_ptr<nebula::cpp2::IndexItem>& index,
                                   VertexID src,
                                   VertexID dst) {
    auto reader = RowReader::getEdgePropReader(schemaMan,
                                               prop,
                                               spaceId,
                                               type);
    auto values = collectIndexValues(reader.get(),
                                     index->get_fields());
    auto indexKey = NebulaKeyUtils::edgeIndexKey(partId,
                                                 index->get_index_id(),
                                                 src,
                                                 0,
                                                 dst,
                                                 values);
    return indexKey;
}

static std::string genVertexIndexKey(meta::SchemaManager* schemaMan,
                                     const std::string &prop,
                                     GraphSpaceID spaceId,
                                     PartitionID partId,
                                     TagID tagId,
                                     std::shared_ptr<nebula::cpp2::IndexItem> &index,
                                     VertexID vId) {
    auto reader = RowReader::getTagPropReader(schemaMan,
                                              prop,
                                              spaceId,
                                              tagId);
    auto values = collectIndexValues(reader.get(),
                                     index->get_fields());
    auto indexKey = NebulaKeyUtils::vertexIndexKey(partId,
                                                   index->get_index_id(),
                                                   vId,
                                                   values);
    return indexKey;
}

static std::shared_ptr<meta::SchemaProviderIf> genEdgeSchemaProvider(
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


/**
 * It will generate tag SchemaProvider with some int fields and string fields
 * */
static std::shared_ptr<meta::SchemaProviderIf> genTagSchemaProvider(
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

static std::unique_ptr<meta::SchemaManager> mockSchemaMan(GraphSpaceID spaceId,
                                                          EdgeType edgeType,
                                                          TagID tagId) {
    auto* schemaMan = new AdHocSchemaManager();
    schemaMan->addEdgeSchema(spaceId /*space id*/, edgeType /*edge type*/,
                             genEdgeSchemaProvider(10, 10));
    schemaMan->addTagSchema(spaceId /*space id*/, tagId,
                             genTagSchemaProvider(tagId, 3, 3));
    std::unique_ptr<meta::SchemaManager> sm(schemaMan);
    return sm;
}

void mockData(kvstore::KVStore* kv ,
              meta::SchemaManager* schemaMan,
              meta::IndexManager* indexMan,
              EdgeType edgeType,
              TagID tagId,
              GraphSpaceID spaceId) {
    auto vindex = indexMan->getTagIndex(spaceId, tagId).value();
    auto eindex = indexMan->getEdgeIndex(spaceId, edgeType).value();
    for (auto partId = 0; partId < 3; partId++) {
        std::vector<kvstore::KV> data;
        for (auto vertexId = partId * 10; vertexId < (partId + 1) * 10; vertexId++) {
            auto key = NebulaKeyUtils::vertexKey(partId, vertexId, tagId, 0);
            RowWriter writer;
            for (uint64_t numInt = 0; numInt < 3; numInt++) {
                writer << (numInt + 1);
            }
            for (auto numString = 3; numString < 6; numString++) {
                writer << folly::stringPrintf("tag_string_col_%d", numString);
            }
            auto val = writer.encode();
            auto indexKey = genVertexIndexKey(schemaMan, val, spaceId, partId,
                                              tagId, vindex, vertexId);
            data.emplace_back(std::move(indexKey), "");
            data.emplace_back(std::move(key), std::move(val));

            for (auto dstId = 10001; dstId <= 10007; dstId++) {
                VLOG(3) << "Write part " << partId << ", vertex "
                        << vertexId << ", dst " << dstId;
                auto ekey =
                        NebulaKeyUtils::edgeKey(partId, vertexId, edgeType, 0, dstId,
                                                std::numeric_limits<int>::max() - 1);
                RowWriter ewriter(nullptr);
                for (uint64_t numInt = 0; numInt < 10; numInt++) {
                    ewriter << (numInt + 1);
                }
                for (auto numString = 10; numString < 20; numString++) {
                    ewriter << folly::stringPrintf("string_col_%d", numString);
                }
                auto eval = ewriter.encode();
                auto edgeIndex = genEdgeIndexKey(schemaMan, eval, spaceId, partId,
                                                 edgeType, eindex, vertexId, dstId);
                data.emplace_back(std::move(edgeIndex), "");
                data.emplace_back(std::move(ekey), std::move(eval));
            }
        }
        folly::Baton<true, std::atomic> baton;
        kv->asyncMultiPut(
                0, partId, std::move(data),
                [&](kvstore::ResultCode code) {
                    EXPECT_EQ(code, kvstore::ResultCode::SUCCEEDED);
                    baton.post();
                });
        baton.wait();
    }
}

static std::unique_ptr<meta::IndexManager> mockIndexMan(GraphSpaceID spaceId = 0,
                                                        TagID tagId = 3001,
                                                        EdgeType edgeType = 101) {
    auto* indexMan = new AdHocIndexManager();
    {
        std::vector<nebula::cpp2::ColumnDef> cols;
        for (auto i = 0; i < 3; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("tag_%d_col_%d", tagId, i);
            column.type.type = nebula::cpp2::SupportedType::INT;
            cols.emplace_back(std::move(column));
        }
        for (auto i = 3; i < 6; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("tag_%d_col_%d", tagId, i);
            column.type.type = nebula::cpp2::SupportedType::STRING;
            cols.emplace_back(std::move(column));
        }
        indexMan->addTagIndex(spaceId, tagId, tagId, std::move(cols));
    }
    {
        std::vector<nebula::cpp2::ColumnDef> cols;
        for (int32_t i = 0; i < 10; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("col_%d", i);
            column.type.type = nebula::cpp2::SupportedType::INT;
            cols.emplace_back(std::move(column));
        }
        for (int32_t i = 10; i < 20; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("col_%d", i);
            column.type.type = nebula::cpp2::SupportedType::STRING;
            cols.emplace_back(std::move(column));
        }
        indexMan->addEdgeIndex(spaceId, edgeType, edgeType, std::move(cols));
    }
    std::unique_ptr<meta::IndexManager> im(indexMan);
    return im;
}

static cpp2::LookUpVertexIndexResp execLookupVertices(const std::string& filter) {
    fs::TempDir rootPath("/tmp/execLookupVertices.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());
    GraphSpaceID spaceId = 0;
    TagID tagId = 3001;
    EdgeType type = 101;
    LOG(INFO) << "Prepare meta...";
    auto schemaMan = mockSchemaMan(spaceId, type, tagId);
    auto indexMan = mockIndexMan(spaceId, tagId, type);
    mockData(kv.get(), schemaMan.get(), indexMan.get(), type, tagId, spaceId);
    auto *processor = LookUpVertexIndexProcessor::instance(kv.get(),
                                                           schemaMan.get(),
                                                           indexMan.get(),
                                                           nullptr);
    cpp2::LookUpIndexRequest req;
    decltype(req.parts) parts;
    parts.emplace_back(0);
    parts.emplace_back(1);
    parts.emplace_back(2);
    decltype(req.return_columns) cols;
    cols.emplace_back("tag_3001_col_0");
    cols.emplace_back("tag_3001_col_1");
    cols.emplace_back("tag_3001_col_3");
    cols.emplace_back("tag_3001_col_4");
    req.set_space_id(spaceId);
    req.set_parts(std::move(parts));
    req.set_index_id(tagId);
    req.set_return_columns(cols);
    req.set_filter(filter);
    auto f = processor->getFuture();
    processor->process(req);
    return std::move(f).get();
}

static cpp2::LookUpEdgeIndexResp execLookupEdges(const std::string& filter) {
    fs::TempDir rootPath("/tmp/execLookupEdges.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());
    GraphSpaceID spaceId = 0;
    TagID tagId = 3001;
    EdgeType type = 101;
    LOG(INFO) << "Prepare meta...";
    auto schemaMan = mockSchemaMan(spaceId, type, tagId);
    auto indexMan = mockIndexMan(spaceId, tagId, type);
    mockData(kv.get(), schemaMan.get(), indexMan.get(), type, tagId, spaceId);
    auto *processor = LookUpEdgeIndexProcessor::instance(kv.get(),
                                                         schemaMan.get(),
                                                         indexMan.get(),
                                                         nullptr);
    cpp2::LookUpIndexRequest req;
    decltype(req.parts) parts;
    parts.emplace_back(0);
    parts.emplace_back(1);
    parts.emplace_back(2);
    decltype(req.return_columns) cols;
    cols.emplace_back("col_0");
    cols.emplace_back("col_1");
    cols.emplace_back("col_3");
    cols.emplace_back("col_4");
    cols.emplace_back("col_10");
    cols.emplace_back("col_11");
    cols.emplace_back("col_13");
    cols.emplace_back("col_14");
    req.set_space_id(spaceId);
    req.set_parts(std::move(parts));
    req.set_index_id(type);
    req.set_return_columns(cols);
    req.set_filter(filter);
    auto f = processor->getFuture();
    processor->process(req);
    return std::move(f).get();
}

TEST(IndexScanTest, SimpleScanTest) {
    {
        LOG(INFO) << "Build filter...";
        auto* prop = new std::string("tag_3001_col_0");
        auto* alias = new std::string("3001");
        auto* aliaExp = new AliasPropertyExpression(new std::string(""), alias, prop);
        auto* priExp = new PrimaryExpression(1L);
        auto relExp = std::make_unique<RelationalExpression>(aliaExp,
                                                             RelationalExpression::Operator::EQ,
                                                             priExp);
        auto resp = execLookupVertices(Expression::encode(relExp.get()));

        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(4, resp.get_schema()->get_columns().size());
        EXPECT_EQ(30, resp.rows.size());
    }
    {
        LOG(INFO) << "Build filter...";
        auto* prop = new std::string("col_0");
        auto* alias = new std::string("101");
        auto* aliaExp = new AliasPropertyExpression(new std::string(""), alias, prop);
        auto* priExp = new PrimaryExpression(1L);
        auto relExp = std::make_unique<RelationalExpression>(aliaExp,
                                                             RelationalExpression::Operator::EQ,
                                                             priExp);
        auto resp = execLookupEdges(Expression::encode(relExp.get()));
        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(8, resp.get_schema()->get_columns().size());
        EXPECT_EQ(210, resp.rows.size());
    }
}

TEST(IndexScanTest, AccurateScanTest) {
    {
        LOG(INFO) << "Build filter...";
        /**
         * where tag_3001_col_0 == 1 and
         *       tag_3001_col_1 == 2 and
         *       tag_3001_col_2 == 3
         */
        auto* col0 = new std::string("tag_3001_col_0");
        auto* alias0 = new std::string("3001");
        auto* ape0 = new AliasPropertyExpression(new std::string(""), alias0, col0);
        auto* pe0 = new PrimaryExpression(1L);
        auto* r1 =  new RelationalExpression(ape0,
                                              RelationalExpression::Operator::EQ,
                                              pe0);

        auto* col1 = new std::string("tag_3001_col_1");
        auto* alias1 = new std::string("3001");
        auto* ape1 = new AliasPropertyExpression(new std::string(""), alias1, col1);
        auto* pe1 = new PrimaryExpression(2L);
        auto* r2 =  new RelationalExpression(ape1,
                                             RelationalExpression::Operator::EQ,
                                             pe1);

        auto* col2 = new std::string("tag_3001_col_2");
        auto* alias2 = new std::string("3001");
        auto* ape2 = new AliasPropertyExpression(new std::string(""), alias2, col2);
        auto* pe2 = new PrimaryExpression(3L);
        auto* r3 =  new RelationalExpression(ape2,
                                             RelationalExpression::Operator::EQ,
                                             pe2);
        auto* le1 = new LogicalExpression(r1,
                                          LogicalExpression::AND,
                                          r2);

        auto logExp = std::make_unique<LogicalExpression>(le1,
                                                          LogicalExpression::AND,
                                                          r3);
        auto resp = execLookupVertices(Expression::encode(logExp.get()));
        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(4, resp.get_schema()->get_columns().size());
        EXPECT_EQ(30, resp.rows.size());
    }
    {
        LOG(INFO) << "Build filter...";
        /**
         * where col_0 == 1 and
         *       col_1 == 2 and
         *       col_2 == 3
         */
        auto* col0 = new std::string("col_0");
        auto* alias0 = new std::string("101");
        auto* ape0 = new AliasPropertyExpression(new std::string(""), alias0, col0);
        auto* pe0 = new PrimaryExpression(1L);
        auto* r1 =  new RelationalExpression(ape0,
                                             RelationalExpression::Operator::EQ,
                                             pe0);

        auto* col1 = new std::string("col_1");
        auto* alias1 = new std::string("101");
        auto* ape1 = new AliasPropertyExpression(new std::string(""), alias1, col1);
        auto* pe1 = new PrimaryExpression(2L);
        auto* r2 =  new RelationalExpression(ape1,
                                             RelationalExpression::Operator::EQ,
                                             pe1);

        auto* col2 = new std::string("col_2");
        auto* alias2 = new std::string("101");
        auto* ape2 = new AliasPropertyExpression(new std::string(""), alias2, col2);
        auto* pe2 = new PrimaryExpression(3L);
        auto* r3 =  new RelationalExpression(ape2,
                                             RelationalExpression::Operator::EQ,
                                             pe2);
        auto* le1 = new LogicalExpression(r1,
                                          LogicalExpression::AND,
                                          r2);

        auto logExp = std::make_unique<LogicalExpression>(le1,
                                                          LogicalExpression::AND,
                                                          r3);
        auto resp = execLookupEdges(Expression::encode(logExp.get()));

        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(8, resp.get_schema()->get_columns().size());
        EXPECT_EQ(210, resp.rows.size());
    }
}
TEST(IndexScanTest, SeekScanTest) {
    {
        LOG(INFO) << "Build filter...";
        /**
         * where tag_3001_col_0 > 0
         */
        auto* col0 = new std::string("tag_3001_col_0");
        auto* alias0 = new std::string("3001");
        auto* ape0 = new AliasPropertyExpression(new std::string(""), alias0, col0);
        auto* pe0 = new PrimaryExpression(0L);
        auto r1 =  std::make_unique<RelationalExpression>(ape0,
                                                          RelationalExpression::Operator::GT,
                                                          pe0);
        auto resp = execLookupVertices(Expression::encode(r1.get()));
        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(4, resp.get_schema()->get_columns().size());
        EXPECT_EQ(30, resp.rows.size());
    }
    {
        LOG(INFO) << "Build filter...";
        /**
         * where col_0 > 0
         */
        auto* col0 = new std::string("col_0");
        auto* alias0 = new std::string("101");
        auto* ape0 = new AliasPropertyExpression(new std::string(""), alias0, col0);
        auto* pe0 = new PrimaryExpression(0L);
        auto r1 =  std::make_unique<RelationalExpression>(ape0,
                                                          RelationalExpression::Operator::GT,
                                                          pe0);
        auto resp = execLookupEdges(Expression::encode(r1.get()));
        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(8, resp.get_schema()->get_columns().size());
        EXPECT_EQ(210, resp.rows.size());
    }
    {
        LOG(INFO) << "Build filter...";
        /**
         * where tag_3001_col_1 > 0
         */
        auto* col0 = new std::string("tag_3001_col_1");
        auto* alias0 = new std::string("3001");
        auto* ape0 = new AliasPropertyExpression(new std::string(""), alias0, col0);
        auto* pe0 = new PrimaryExpression(0L);
        auto r1 =  std::make_unique<RelationalExpression>(ape0,
                                                          RelationalExpression::Operator::GT,
                                                          pe0);
        auto resp = execLookupVertices(Expression::encode(r1.get()));
        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(4, resp.get_schema()->get_columns().size());
        EXPECT_EQ(30, resp.rows.size());
    }
    {
        LOG(INFO) << "Build filter...";
        /**
         * where col_1 > 0
         */
        auto* col0 = new std::string("col_1");
        auto* alias0 = new std::string("101");
        auto* ape0 = new AliasPropertyExpression(new std::string(""), alias0, col0);
        auto* pe0 = new PrimaryExpression(0L);
        auto r1 =  std::make_unique<RelationalExpression>(ape0,
                                                          RelationalExpression::Operator::GT,
                                                          pe0);
        auto resp = execLookupEdges(Expression::encode(r1.get()));
        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(8, resp.get_schema()->get_columns().size());
        EXPECT_EQ(210, resp.rows.size());
    }
}

TEST(IndexScanTest, PrefixScanTest) {
    {
        LOG(INFO) << "Build filter...";
        /**
         * where tag_3001_col_0 == 1 and
         *       tag_3001_col_1 > 1
         */
        auto* col0 = new std::string("tag_3001_col_0");
        auto* alias0 = new std::string("3001");
        auto* ape0 = new AliasPropertyExpression(new std::string(""), alias0, col0);
        auto* pe0 = new PrimaryExpression(1L);
        auto* r1 =  new RelationalExpression(ape0,
                                             RelationalExpression::Operator::EQ,
                                             pe0);

        auto* col1 = new std::string("tag_3001_col_1");
        auto* alias1 = new std::string("3001");
        auto* ape1 = new AliasPropertyExpression(new std::string(""), alias1, col1);
        auto* pe1 = new PrimaryExpression(1L);
        auto* r2 =  new RelationalExpression(ape1,
                                             RelationalExpression::Operator::GT,
                                             pe1);

        auto logExp = std::make_unique<LogicalExpression>(r1,
                                                          LogicalExpression::AND,
                                                          r2);
        auto resp = execLookupVertices(Expression::encode(logExp.get()));
        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(4, resp.get_schema()->get_columns().size());
        EXPECT_EQ(30, resp.rows.size());
    }
    {
        LOG(INFO) << "Build filter...";
        /**
         * where col_0 == 1 and
         *       col_1 > 1
         */
        auto* col0 = new std::string("col_0");
        auto* alias0 = new std::string("101");
        auto* ape0 = new AliasPropertyExpression(new std::string(""), alias0, col0);
        auto* pe0 = new PrimaryExpression(1L);
        auto* r1 =  new RelationalExpression(ape0,
                                             RelationalExpression::Operator::EQ,
                                             pe0);

        auto* col1 = new std::string("col_1");
        auto* alias1 = new std::string("101");
        auto* ape1 = new AliasPropertyExpression(new std::string(""), alias1, col1);
        auto* pe1 = new PrimaryExpression(1L);
        auto* r2 =  new RelationalExpression(ape1,
                                             RelationalExpression::Operator::GT,
                                             pe1);

        auto logExp = std::make_unique<LogicalExpression>(r1,
                                                          LogicalExpression::AND,
                                                          r2);
        auto resp = execLookupEdges(Expression::encode(logExp.get()));
        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(8, resp.get_schema()->get_columns().size());
        EXPECT_EQ(210, resp.rows.size());
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

