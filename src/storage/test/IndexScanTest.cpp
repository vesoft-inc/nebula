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
#include "storage/index/ScanVertexIndexProcessor.h"
#include "storage/index/ScanEdgeIndexProcessor.h"
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
                                   const nebula::cpp2::IndexItem &index,
                                   VertexID src,
                                   VertexID dst) {
    auto reader = RowReader::getEdgePropReader(schemaMan,
                                               prop,
                                               spaceId,
                                               type);
    auto values = collectIndexValues(reader.get(),
                                     index.get_cols());
    auto indexKey = NebulaKeyUtils::edgeIndexKey(partId,
                                                 index.index_id,
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
                                     const nebula::cpp2::IndexItem &index,
                                     VertexID vId) {
    auto reader = RowReader::getTagPropReader(schemaMan,
                                              prop,
                                              spaceId,
                                              tagId);
    auto values = collectIndexValues(reader.get(),
                                     index.get_cols());
    auto indexKey = NebulaKeyUtils::vertexIndexKey(partId,
                                                   index.index_id,
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
                                                          TagID tagId,
                                                          const nebula::cpp2::IndexItem& vindex,
                                                          const nebula::cpp2::IndexItem& eindex) {
    auto* schemaMan = new AdHocSchemaManager();
    schemaMan->addEdgeSchema(spaceId /*space id*/, edgeType /*edge type*/,
                             genEdgeSchemaProvider(10, 10));
    schemaMan->addTagSchema(spaceId /*space id*/, tagId,
                             genTagSchemaProvider(tagId, 3, 3));
    schemaMan->addTagIndex(spaceId, vindex);
    schemaMan->addEdgeIndex(spaceId, eindex);
    std::unique_ptr<meta::SchemaManager> sm(schemaMan);
    return sm;
}

void mockData(kvstore::KVStore* kv ,
              meta::SchemaManager* schemaMan,
              EdgeType edgeType,
              TagID tagId,
              GraphSpaceID spaceId,
              const nebula::cpp2::IndexItem &vindex,
              const nebula::cpp2::IndexItem &eindex) {
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

static nebula::cpp2::IndexItem mockEdgeIndex(int32_t intFieldsNum,
                                             int32_t stringFieldsNum,
                                             EdgeType edgeType) {
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
    nebula::cpp2::IndexItem indexItem;
    indexItem.set_index_id(edgeType);
    indexItem.set_tagOrEdge(edgeType);
    indexItem.set_cols(std::move(cols));
    return indexItem;
}

static nebula::cpp2::IndexItem mockVertexIndex(int32_t intFieldsNum,
                                               int32_t stringFieldsNum,
                                               TagID tagId) {
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
    nebula::cpp2::IndexItem indexItem;
    indexItem.set_index_id(tagId);
    indexItem.set_tagOrEdge(tagId);
    indexItem.set_cols(std::move(cols));
    return indexItem;
}

TEST(IndexScanTest, VertexScanTest) {
    fs::TempDir rootPath("/tmp/VertexScanTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());
    GraphSpaceID spaceId = 0;
    TagID tagId = 3001;
    EdgeType type = 101;
    LOG(INFO) << "Prepare meta...";
    auto vindex = mockVertexIndex(3, 3, tagId);
    auto eindex = mockEdgeIndex(10, 10, type);
    auto schemaMan = mockSchemaMan(spaceId, type, tagId, vindex, eindex);

    mockData(kv.get(), schemaMan.get(), type, tagId, spaceId, vindex, eindex);
    {
        auto executor = std::make_unique<folly::CPUThreadPoolExecutor>(3);
        auto* processor = ScanVertexIndexProcessor::instance(kv.get(), schemaMan.get(), nullptr,
                                                             executor.get(), nullptr);
        cpp2::IndexScanRequest req;

        decltype(req.parts) parts;
        parts.emplace_back(0);
        parts.emplace_back(1);
        parts.emplace_back(2);
        decltype(req.return_columns) cols;
        cols.emplace_back("tag_3001_col_0");
        cols.emplace_back("tag_3001_col_1");
        cols.emplace_back("tag_3001_col_3");
        cols.emplace_back("tag_3001_col_4");
        decltype(req.hint) hint;
        auto braw = NebulaKeyUtils::encodeInt64(boost::get<int64_t>(1));
        auto eraw = NebulaKeyUtils::encodeInt64(boost::get<int64_t>(2));
        hint.set_index_id(tagId);
        hint.set_is_range(false);
        decltype(hint.hint_items) items;
        nebula::cpp2::IndexHintItem hintItem;
        hintItem.set_first_str(std::move(braw));
        hintItem.set_second_str(std::move(eraw));
        hintItem.set_type(nebula::cpp2::SupportedType::INT);
        items.emplace_back(std::move(hintItem));
        hint.set_hint_items(items);

        req.set_space_id(spaceId);
        req.set_parts(std::move(parts));
        req.set_return_columns(cols);
        req.set_hint(hint);

        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(4, resp.get_schema()->get_columns().size());
        EXPECT_EQ(30, resp.rows.size());
    }

    {
        auto executor = std::make_unique<folly::CPUThreadPoolExecutor>(3);
        auto* processor = ScanEdgeIndexProcessor::instance(kv.get(), schemaMan.get(), nullptr,
                                                           executor.get(), nullptr);
        cpp2::IndexScanRequest req;

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
        decltype(req.hint) hint;
        auto braw = NebulaKeyUtils::encodeInt64(boost::get<int64_t>(1));
        auto eraw = NebulaKeyUtils::encodeInt64(boost::get<int64_t>(2));
        hint.set_is_range(false);
        hint.set_index_id(type);
        decltype(hint.hint_items) items;
        nebula::cpp2::IndexHintItem hintItem;
        hintItem.set_first_str(std::move(braw));
        hintItem.set_second_str(std::move(eraw));
        hintItem.set_type(nebula::cpp2::SupportedType::INT);
        items.emplace_back(std::move(hintItem));
        hint.set_hint_items(items);
        req.set_space_id(spaceId);
        req.set_parts(std::move(parts));
        req.set_return_columns(cols);
        req.set_hint(hint);

        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(8, resp.get_schema()->get_columns().size());
        EXPECT_EQ(210, resp.rows.size());
    }
}

TEST(IndexScanTest, VertexStringTypeTest) {
    fs::TempDir rootPath("/tmp/VertexStringTypeTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());
    GraphSpaceID spaceId = 0;
    PartitionID partId = 0;
    TagID tagId = 3001;

    nebula::cpp2::Schema schema;
    for (auto i = 0; i < 3; i++) {
        nebula::cpp2::ColumnDef column;
        column.name = folly::stringPrintf("tag_%d_col_%d", tagId, i);
        column.type.type = nebula::cpp2::SupportedType::STRING;
        schema.columns.emplace_back(std::move(column));
    }

    auto* schemaMan = new AdHocSchemaManager();
    schemaMan->addTagSchema(spaceId /*space id*/, tagId,
                            std::make_shared<ResultSchemaProvider>(std::move(schema)));


    std::vector<nebula::cpp2::ColumnDef> cols;
    for (auto i = 0; i < 3; i++) {
        nebula::cpp2::ColumnDef column;
        column.name = folly::stringPrintf("tag_%d_col_%d", tagId, i);
        column.type.type = nebula::cpp2::SupportedType::STRING;
        cols.emplace_back(std::move(column));
    }
    nebula::cpp2::IndexItem index;
    index.set_index_id(tagId);
    index.set_tagOrEdge(tagId);
    index.set_cols(std::move(cols));
    schemaMan->addTagIndex(spaceId, index);

    std::vector<kvstore::KV> data;
    auto key = NebulaKeyUtils::vertexKey(partId, 1, tagId, 0);
    RowWriter writer;
    writer << "AA" << "BBAA" << "BBAA";
    auto val = writer.encode();
    auto indexKey = genVertexIndexKey(schemaMan, val, spaceId, partId,
                                      tagId, index, 1);
    data.emplace_back(std::move(indexKey), "");
    data.emplace_back(std::move(key), std::move(val));
    key = NebulaKeyUtils::vertexKey(partId, 2, tagId, 0);
    RowWriter writer2;
    writer2 << "AABB" << "AABB" << "AABB";
    val = writer2.encode();
    indexKey = genVertexIndexKey(schemaMan, val, spaceId, partId,
                                 tagId, index, 1);
    data.emplace_back(std::move(indexKey), "");
    data.emplace_back(std::move(key), std::move(val));
    folly::Baton<true, std::atomic> baton;
    kv->asyncMultiPut(
            0, partId, std::move(data),
            [&](kvstore::ResultCode code) {
                EXPECT_EQ(code, kvstore::ResultCode::SUCCEEDED);
                baton.post();
            });
    baton.wait();

    {
        auto executor = std::make_unique<folly::CPUThreadPoolExecutor>(3);
        auto* processor = ScanVertexIndexProcessor::instance(kv.get(), schemaMan, nullptr,
                                                             executor.get(), nullptr);
        cpp2::IndexScanRequest req;
        decltype(req.parts) parts;
        parts.emplace_back(partId);
        decltype(req.return_columns) retCols;
        retCols.emplace_back("tag_3001_col_0");
        retCols.emplace_back("tag_3001_col_1");
        decltype(req.hint) hint;
        hint.set_index_id(tagId);
        hint.set_is_range(false);
        decltype(hint.hint_items) items;
        nebula::cpp2::IndexHintItem hintItem;
        hintItem.set_first_str("AABB");
        hintItem.set_second_str("");
        hintItem.set_type(nebula::cpp2::SupportedType::STRING);
        items.emplace_back(hintItem);
        hint.set_hint_items(items);
        req.set_space_id(spaceId);
        req.set_parts(std::move(parts));
        req.set_return_columns(retCols);
        req.set_hint(hint);

        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(2, resp.get_schema()->get_columns().size());
        EXPECT_EQ(1, resp.rows.size());
    }

    {
        auto executor = std::make_unique<folly::CPUThreadPoolExecutor>(3);
        auto* processor = ScanVertexIndexProcessor::instance(kv.get(), schemaMan, nullptr,
                                                             executor.get(), nullptr);
        cpp2::IndexScanRequest req;
        decltype(req.parts) parts;
        parts.emplace_back(partId);
        decltype(req.return_columns) retCols;
        retCols.emplace_back("tag_3001_col_0");
        retCols.emplace_back("tag_3001_col_1");
        decltype(req.hint) hint;
        hint.set_is_range(false);
        hint.set_index_id(tagId);
        decltype(hint.hint_items) items;
        nebula::cpp2::IndexHintItem hintItem;
        hintItem.set_first_str("AA");
        hintItem.set_second_str("");
        hintItem.set_type(nebula::cpp2::SupportedType::STRING);
        items.emplace_back(std::move(hintItem));
        hint.set_hint_items(items);
        req.set_space_id(spaceId);
        req.set_parts(std::move(parts));
        req.set_return_columns(retCols);
        req.set_hint(hint);

        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(2, resp.get_schema()->get_columns().size());
        EXPECT_EQ(2, resp.rows.size());
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

