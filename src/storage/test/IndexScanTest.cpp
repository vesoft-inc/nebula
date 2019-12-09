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
<<<<<<< HEAD
#include "storage/index/LookUpVertexIndexProcessor.h"
#include "storage/index/LookUpEdgeIndexProcessor.h"
#include "dataman/RowSetReader.h"
#include "dataman/RowReader.h"
=======
#include "storage/ScanVertexIndexProcessor.h"
#include "storage/ScanEdgeIndexProcessor.h"
#include "dataman/RowSetReader.h"
#include "dataman/RowReader.h"
DECLARE_int32(load_data_interval_secs);
>>>>>>> online index scan

DECLARE_uint32(raft_heartbeat_interval_secs);

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
<<<<<<< HEAD
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
=======
            return folly::to<std::string>(boost::get<bool >(v));
        }
        case nebula::cpp2::SupportedType::INT:
        case nebula::cpp2::SupportedType::TIMESTAMP: {
            std::string raw;
            raw.reserve(sizeof(int64_t));
            auto val = folly::Endian::big(boost::get<int64_t>(v));
            raw.append(reinterpret_cast<const char*>(&val), sizeof(int64_t));
            return raw;
        }
        case nebula::cpp2::SupportedType::FLOAT:
        case nebula::cpp2::SupportedType::DOUBLE: {
            return folly::to<std::string>(boost::get<double>(v));
>>>>>>> online index scan
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
<<<<<<< HEAD
                                   std::shared_ptr<nebula::cpp2::IndexItem>& index,
=======
                                   const cpp2::IndexItem &index,
>>>>>>> online index scan
                                   VertexID src,
                                   VertexID dst) {
    auto reader = RowReader::getEdgePropReader(schemaMan,
                                               prop,
                                               spaceId,
                                               type);
    auto values = collectIndexValues(reader.get(),
<<<<<<< HEAD
                                     index->get_fields());
    auto indexKey = NebulaKeyUtils::edgeIndexKey(partId,
                                                 index->get_index_id(),
=======
                                     index.get_cols());
    auto indexKey = NebulaKeyUtils::edgeIndexKey(partId,
                                                 index.index_id,
>>>>>>> online index scan
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
<<<<<<< HEAD
                                     std::shared_ptr<nebula::cpp2::IndexItem> &index,
=======
                                     const cpp2::IndexItem &index,
>>>>>>> online index scan
                                     VertexID vId) {
    auto reader = RowReader::getTagPropReader(schemaMan,
                                              prop,
                                              spaceId,
                                              tagId);
    auto values = collectIndexValues(reader.get(),
<<<<<<< HEAD
                                     index->get_fields());
    auto indexKey = NebulaKeyUtils::vertexIndexKey(partId,
                                                   index->get_index_id(),
=======
                                     index.get_cols());
    auto indexKey = NebulaKeyUtils::vertexIndexKey(partId,
                                                   index.index_id,
>>>>>>> online index scan
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
<<<<<<< HEAD
              meta::IndexManager* indexMan,
              EdgeType edgeType,
              TagID tagId,
              GraphSpaceID spaceId) {
    auto vindex = indexMan->getTagIndex(spaceId, tagId).value();
    auto eindex = indexMan->getEdgeIndex(spaceId, edgeType).value();
=======
              EdgeType edgeType,
              TagID tagId,
              GraphSpaceID spaceId,
              const cpp2::IndexItem &vindex,
              const cpp2::IndexItem &eindex) {
>>>>>>> online index scan
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

<<<<<<< HEAD
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

static cpp2::LookUpVertexIndexResp execLookupVertices(const std::string& filter,
                                                      bool hasReturnCols = true) {
    fs::TempDir rootPath("/tmp/execLookupVertices.XXXXXX");
=======
static cpp2::IndexItem mockEdgeIndex(int32_t intFieldsNum,
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
    // indexId can be same with edgeType
    return cpp2::IndexItem(apache::thrift::FragileConstructor::FRAGILE,
                            edgeType, edgeType, std::move(cols));
}

static cpp2::IndexItem mockVertexIndex(int32_t intFieldsNum,
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
    return cpp2::IndexItem(apache::thrift::FragileConstructor::FRAGILE,
                           tagId, tagId, std::move(cols));
}

TEST(IndexScanTest, VertexScanTest) {
    fs::TempDir rootPath("/tmp/VertexScanTest.XXXXXX");
>>>>>>> online index scan
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());
    GraphSpaceID spaceId = 0;
    TagID tagId = 3001;
    EdgeType type = 101;
    LOG(INFO) << "Prepare meta...";
    auto schemaMan = mockSchemaMan(spaceId, type, tagId);
<<<<<<< HEAD
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
    if (hasReturnCols) {
=======
    auto vindex = mockVertexIndex(3, 3, tagId);
    auto eindex = mockEdgeIndex(10, 10, type);
    mockData(kv.get(), schemaMan.get(), type, tagId, spaceId, vindex, eindex);
    sleep(FLAGS_load_data_interval_secs + 1);
    {
        auto executor = std::make_unique<folly::CPUThreadPoolExecutor>(3);
        auto* processor = ScanVertexIndexProcessor::instance(kv.get(), schemaMan.get(), nullptr,
                                                             executor.get(), nullptr);
        cpp2::IndexScanRequest req;

        decltype(req.parts) parts;
        parts.emplace_back(0);
        parts.emplace_back(1);
        parts.emplace_back(2);
>>>>>>> online index scan
        decltype(req.return_columns) cols;
        cols.emplace_back("tag_3001_col_0");
        cols.emplace_back("tag_3001_col_1");
        cols.emplace_back("tag_3001_col_3");
        cols.emplace_back("tag_3001_col_4");
<<<<<<< HEAD
        req.set_return_columns(cols);
    }
    req.set_space_id(spaceId);
    req.set_parts(std::move(parts));
    req.set_index_id(tagId);

    req.set_filter(filter);
    auto f = processor->getFuture();
    processor->process(req);
    return std::move(f).get();
}

static cpp2::LookUpEdgeIndexResp execLookupEdges(const std::string& filter,
                                                 bool hasReturnCols = true) {
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
    if (hasReturnCols) {
=======
        decltype(req.hints) hints;
        std::string braw, eraw;
        braw.reserve(sizeof(int64_t));
        eraw.reserve(sizeof(int64_t));
        auto begin = folly::Endian::big(boost::get<int64_t>(1));
        braw.append(reinterpret_cast<const char*>(&begin), sizeof(int64_t));
        auto end = folly::Endian::big(boost::get<int64_t>(2));
        eraw.append(reinterpret_cast<const char*>(&end), sizeof(int64_t));
        hints.emplace_back(cpp2::IndexHint(apache::thrift::FragileConstructor::FRAGILE,
                                           false, braw, eraw, nebula::cpp2::SupportedType::INT));

        req.set_space_id(spaceId);
        req.set_parts(std::move(parts));
        req.set_index(vindex);
        req.set_return_columns(cols);
        req.set_hints(hints);

        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(4, resp.get_schema().get_columns().size());
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
>>>>>>> online index scan
        decltype(req.return_columns) cols;
        cols.emplace_back("col_0");
        cols.emplace_back("col_1");
        cols.emplace_back("col_3");
        cols.emplace_back("col_4");
        cols.emplace_back("col_10");
        cols.emplace_back("col_11");
        cols.emplace_back("col_13");
        cols.emplace_back("col_14");
<<<<<<< HEAD
        req.set_return_columns(cols);
    }
    req.set_space_id(spaceId);
    req.set_parts(std::move(parts));
    req.set_index_id(type);

    req.set_filter(filter);
    auto f = processor->getFuture();
    processor->process(req);
    return std::move(f).get();
}

static cpp2::LookUpEdgeIndexResp checkLookupEdgesString(const std::string& filter) {
    fs::TempDir rootPath("/tmp/checkLookupEdgesString.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());
    GraphSpaceID spaceId = 0;
    EdgeType type = 101;

    nebula::cpp2::Schema schema;
    for (auto i = 0; i < 3; i++) {
        nebula::cpp2::ColumnDef column;
        column.name = folly::stringPrintf("col_%d", i);
        column.type.type = nebula::cpp2::SupportedType::STRING;
        schema.columns.emplace_back(std::move(column));
    }
    auto sp = std::make_shared<ResultSchemaProvider>(std::move(schema));
    auto* sm = new AdHocSchemaManager();
    sm->addEdgeSchema(spaceId /*space id*/, type /*edge type*/, sp);
    std::unique_ptr<meta::SchemaManager> schemaMan(sm);

    auto* im = new AdHocIndexManager();
    {
        std::vector<nebula::cpp2::ColumnDef> cols;
        for (int32_t i = 0; i < 3; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("col_%d", i);
            column.type.type = nebula::cpp2::SupportedType::STRING;
            cols.emplace_back(std::move(column));
        }
        im->addEdgeIndex(spaceId, type, type, std::move(cols));
    }
    std::unique_ptr<meta::IndexManager> indexMan(im);
    auto eindex = indexMan->getEdgeIndex(spaceId, type).value();
    sleep(FLAGS_raft_heartbeat_interval_secs);
    {
        for (auto partId = 0; partId < 3; partId++) {
            std::vector<kvstore::KV> data;
            auto version = std::numeric_limits<int>::max() - 1;
            {
                VertexID srcId = 1;
                VertexID dstId = 10;
                auto eKey = NebulaKeyUtils::edgeKey(partId, srcId, type, 0, dstId, version);
                RowWriter ewriter(nullptr);
                ewriter << "AB" << "CAB" << "CABC";
                auto eval = ewriter.encode();
                auto edgeIndex = genEdgeIndexKey(schemaMan.get(), eval, spaceId, partId,
                                                 type, eindex, srcId, dstId);
                data.emplace_back(std::move(edgeIndex), "");
                data.emplace_back(std::move(eKey), std::move(eval));
            }
            {
                VertexID srcId = 2;
                VertexID dstId = 20;
                auto eKey = NebulaKeyUtils::edgeKey(partId, srcId, type, 0, dstId, version);
                RowWriter ewriter(nullptr);
                ewriter << "ABC" << "ABC" << "ABC";
                auto eval = ewriter.encode();
                auto edgeIndex = genEdgeIndexKey(schemaMan.get(), eval, spaceId, partId,
                                                 type, eindex, srcId, dstId);
                data.emplace_back(std::move(edgeIndex), "");
                data.emplace_back(std::move(eKey), std::move(eval));
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
    cols.emplace_back("col_2");
    req.set_space_id(spaceId);
    req.set_parts(std::move(parts));
    req.set_index_id(type);
    req.set_return_columns(cols);
    req.set_filter(filter);
    auto f = processor->getFuture();
    processor->process(req);
    return std::move(f).get();
}

static cpp2::LookUpVertexIndexResp checkLookupVerticesString(const std::string& filter) {
    fs::TempDir rootPath("/tmp/checkLookupVerticesString.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());
    GraphSpaceID spaceId = 0;
    TagID tagId = 3001;
    nebula::cpp2::Schema schema;
    for (auto i = 0; i < 3; i++) {
        nebula::cpp2::ColumnDef column;
        column.name = folly::stringPrintf("col_%d", i);
        column.type.type = nebula::cpp2::SupportedType::STRING;
        schema.columns.emplace_back(std::move(column));
    }
    auto sp = std::make_shared<ResultSchemaProvider>(std::move(schema));
    auto* sm = new AdHocSchemaManager();
    sm->addTagSchema(spaceId /*space id*/, tagId /*Tag ID*/, sp);
    std::unique_ptr<meta::SchemaManager> schemaMan(sm);

    auto* im = new AdHocIndexManager();
    {
        std::vector<nebula::cpp2::ColumnDef> cols;
        for (int32_t i = 0; i < 3; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("col_%d", i);
            column.type.type = nebula::cpp2::SupportedType::STRING;
            cols.emplace_back(std::move(column));
        }
        im->addTagIndex(spaceId, tagId, tagId, std::move(cols));
    }
    std::unique_ptr<meta::IndexManager> indexMan(im);
    auto vindex = indexMan->getTagIndex(spaceId, tagId).value();
    sleep(FLAGS_raft_heartbeat_interval_secs);
    {
        for (auto partId = 0; partId < 3; partId++) {
            std::vector<kvstore::KV> data;
            auto version = std::numeric_limits<int>::max() - 1;
            {
                VertexID vertexId = 100;
                auto key = NebulaKeyUtils::vertexKey(partId, vertexId, tagId, version);
                RowWriter twriter(nullptr);
                twriter << "AB" << "CAB" << "CABC";
                auto tval = twriter.encode();
                auto vIndex = genVertexIndexKey(schemaMan.get(), tval, spaceId, partId,
                                                   tagId, vindex, vertexId);
                data.emplace_back(std::move(vIndex), "");
                data.emplace_back(std::move(key), std::move(tval));
            }
            {
                VertexID vertexId = 200;
                auto key = NebulaKeyUtils::vertexKey(partId, vertexId, tagId, version);
                RowWriter twriter(nullptr);
                twriter << "ABC" << "ABC" << "ABC";
                auto tval = twriter.encode();
                auto vIndex = genVertexIndexKey(schemaMan.get(), tval, spaceId, partId,
                                                tagId, vindex, vertexId);
                data.emplace_back(std::move(vIndex), "");
                data.emplace_back(std::move(key), std::move(tval));
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
    cols.emplace_back("col_0");
    cols.emplace_back("col_1");
    cols.emplace_back("col_2");
    req.set_space_id(spaceId);
    req.set_parts(std::move(parts));
    req.set_index_id(tagId);
    req.set_return_columns(cols);
    req.set_filter(filter);
    auto f = processor->getFuture();
    processor->process(req);
    return std::move(f).get();
}

static cpp2::LookUpEdgeIndexResp checkLookupEdgesDouble(const std::string& filter) {
    fs::TempDir rootPath("/tmp/checkLookupEdgesDouble.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());
    GraphSpaceID spaceId = 0;
    EdgeType type = 101;
=======
        decltype(req.hints) hints;
        std::string braw, eraw;
        braw.reserve(sizeof(int64_t));
        eraw.reserve(sizeof(int64_t));
        auto begin = folly::Endian::big(boost::get<int64_t>(1));
        braw.append(reinterpret_cast<const char*>(&begin), sizeof(int64_t));
        auto end = folly::Endian::big(boost::get<int64_t>(2));
        eraw.append(reinterpret_cast<const char*>(&end), sizeof(int64_t));
        hints.emplace_back(cpp2::IndexHint(apache::thrift::FragileConstructor::FRAGILE,
                                           false, braw, eraw, nebula::cpp2::SupportedType::INT));

        req.set_space_id(spaceId);
        req.set_parts(std::move(parts));
        req.set_index(eindex);
        req.set_return_columns(cols);
        req.set_hints(hints);

        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(8, resp.get_schema().get_columns().size());
        EXPECT_EQ(210, resp.rows.size());
    }
}

TEST(IndexScanTest, VertexStringTypeTest) {
    fs::TempDir rootPath("/tmp/VertexStringTypeTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());
    GraphSpaceID spaceId = 0;
    PartitionID partId = 0;
    TagID tagId = 3001;
>>>>>>> online index scan

    nebula::cpp2::Schema schema;
    for (auto i = 0; i < 3; i++) {
        nebula::cpp2::ColumnDef column;
<<<<<<< HEAD
        column.name = folly::stringPrintf("col_%d", i);
        column.type.type = nebula::cpp2::SupportedType::DOUBLE;
        schema.columns.emplace_back(std::move(column));
    }
    auto sp = std::make_shared<ResultSchemaProvider>(std::move(schema));
    auto* sm = new AdHocSchemaManager();
    sm->addEdgeSchema(spaceId /*space id*/, type /*edge type*/, sp);
    std::unique_ptr<meta::SchemaManager> schemaMan(sm);

    auto* im = new AdHocIndexManager();
    {
        std::vector<nebula::cpp2::ColumnDef> cols;
        for (int32_t i = 0; i < 3; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("col_%d", i);
            column.type.type = nebula::cpp2::SupportedType::DOUBLE;
            cols.emplace_back(std::move(column));
        }
        im->addEdgeIndex(spaceId, type, type, std::move(cols));
    }
    std::unique_ptr<meta::IndexManager> indexMan(im);
    auto eindex = indexMan->getEdgeIndex(spaceId, type).value();
    sleep(FLAGS_raft_heartbeat_interval_secs);
    {
        for (auto partId = 0; partId < 3; partId++) {
            std::vector<kvstore::KV> data;
            auto version = std::numeric_limits<int>::max() - 1;
            {
                VertexID srcId = 1;
                VertexID dstId = 10;
                auto eKey = NebulaKeyUtils::edgeKey(partId, srcId, type, 0, dstId, version);
                RowWriter ewriter(nullptr);
                ewriter << boost::get<double>(1.1)
                        << boost::get<double>(0.0)
                        << boost::get<double>(-1.1);
                auto eval = ewriter.encode();
                auto edgeIndex = genEdgeIndexKey(schemaMan.get(), eval, spaceId, partId,
                                                 type, eindex, srcId, dstId);
                data.emplace_back(std::move(edgeIndex), "");
                data.emplace_back(std::move(eKey), std::move(eval));
            }
            {
                VertexID srcId = 2;
                VertexID dstId = 20;
                auto eKey = NebulaKeyUtils::edgeKey(partId, srcId, type, 0, dstId, version);
                RowWriter ewriter(nullptr);
                ewriter << boost::get<double>(2.2)
                        << boost::get<double>(0.0)
                        << boost::get<double>(-2.2);
                auto eval = ewriter.encode();
                auto edgeIndex = genEdgeIndexKey(schemaMan.get(), eval, spaceId, partId,
                                                 type, eindex, srcId, dstId);
                data.emplace_back(std::move(edgeIndex), "");
                data.emplace_back(std::move(eKey), std::move(eval));
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
    cols.emplace_back("col_2");
    req.set_space_id(spaceId);
    req.set_parts(std::move(parts));
    req.set_index_id(type);
    req.set_return_columns(cols);
    req.set_filter(filter);
    auto f = processor->getFuture();
    processor->process(req);
    return std::move(f).get();
}

static cpp2::LookUpVertexIndexResp checkLookupVerticesDouble(const std::string& filter) {
    fs::TempDir rootPath("/tmp/checkLookupVerticesDouble.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());
    GraphSpaceID spaceId = 0;
    TagID tagId = 3001;
    nebula::cpp2::Schema schema;
    for (auto i = 0; i < 3; i++) {
        nebula::cpp2::ColumnDef column;
        column.name = folly::stringPrintf("col_%d", i);
        column.type.type = nebula::cpp2::SupportedType::DOUBLE;
        schema.columns.emplace_back(std::move(column));
    }
    auto sp = std::make_shared<ResultSchemaProvider>(std::move(schema));
    auto* sm = new AdHocSchemaManager();
    sm->addTagSchema(spaceId /*space id*/, tagId /*Tag ID*/, sp);
    std::unique_ptr<meta::SchemaManager> schemaMan(sm);

    auto* im = new AdHocIndexManager();
    {
        std::vector<nebula::cpp2::ColumnDef> cols;
        for (int32_t i = 0; i < 3; i++) {
            nebula::cpp2::ColumnDef column;
            column.name = folly::stringPrintf("col_%d", i);
            column.type.type = nebula::cpp2::SupportedType::DOUBLE;
            cols.emplace_back(std::move(column));
        }
        im->addTagIndex(spaceId, tagId, tagId, std::move(cols));
    }
    std::unique_ptr<meta::IndexManager> indexMan(im);
    auto vindex = indexMan->getTagIndex(spaceId, tagId).value();
    sleep(FLAGS_raft_heartbeat_interval_secs);
    {
        for (auto partId = 0; partId < 3; partId++) {
            std::vector<kvstore::KV> data;
            auto version = std::numeric_limits<int>::max() - 1;
            {
                VertexID vertexId = 100;
                auto key = NebulaKeyUtils::vertexKey(partId, vertexId, tagId, version);
                RowWriter twriter(nullptr);
                twriter << boost::get<double>(1.1)
                        << boost::get<double>(0.0)
                        << boost::get<double>(-1.1);
                auto tval = twriter.encode();
                auto vIndex = genVertexIndexKey(schemaMan.get(), tval, spaceId, partId,
                                                tagId, vindex, vertexId);
                data.emplace_back(std::move(vIndex), "");
                data.emplace_back(std::move(key), std::move(tval));
            }
            {
                VertexID vertexId = 200;
                auto key = NebulaKeyUtils::vertexKey(partId, vertexId, tagId, version);
                RowWriter twriter(nullptr);
                twriter << boost::get<double>(2.2)
                        << boost::get<double>(0.0)
                        << boost::get<double>(-2.2);
                auto tval = twriter.encode();
                auto vIndex = genVertexIndexKey(schemaMan.get(), tval, spaceId, partId,
                                                tagId, vindex, vertexId);
                data.emplace_back(std::move(vIndex), "");
                data.emplace_back(std::move(key), std::move(tval));
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
    cols.emplace_back("col_0");
    cols.emplace_back("col_1");
    cols.emplace_back("col_2");
    req.set_space_id(spaceId);
    req.set_parts(std::move(parts));
    req.set_index_id(tagId);
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

TEST(IndexScanTest, NoReturnColumnsTest) {
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
        auto resp = execLookupVertices(Expression::encode(logExp.get()), false);
        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(NULL, resp.get_schema());
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
        auto resp = execLookupEdges(Expression::encode(logExp.get()), false);
        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(NULL, resp.get_schema());
        EXPECT_EQ(210, resp.rows.size());
    }
}

TEST(IndexScanTest, EdgeStringTest) {
    {
        LOG(INFO) << "Build filter...";
        /**
         * Total rows :
         *     col_0   col_1     col_2
         *     "AB" << "CAB" << "CABC"
         *     "ABC" << "ABC" << "ABC"
         *
         * where col_0 == "ABC"
         */
        auto* col0 = new std::string("col_0");
        auto* alias0 = new std::string("101");
        auto* ape0 = new AliasPropertyExpression(new std::string(""), alias0, col0);
        std::string c0("ABC");
        auto* pe0 = new PrimaryExpression(c0);
        auto r1 =  std::make_unique<RelationalExpression>(ape0,
                                                          RelationalExpression::Operator::EQ,
                                                          pe0);
        auto resp = checkLookupEdgesString(Expression::encode(r1.get()));
        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(3, resp.get_schema()->get_columns().size());
        EXPECT_EQ(3, resp.rows.size());
        RowWriter ewriter(nullptr);
        ewriter << "ABC" << "ABC" << "ABC";
        auto eval = ewriter.encode();
        for (const auto& row : resp.rows) {
            EXPECT_EQ(eval, row.get_props());
            EXPECT_EQ(2, row.get_key().get_src());
            EXPECT_EQ(20, row.get_key().get_dst());
            EXPECT_EQ(101, row.get_key().get_edge_type());
            EXPECT_EQ(0, row.get_key().get_ranking());
        }
    }
    {
        LOG(INFO) << "Build filter...";
        /**
         * Total rows :
         *     col_0   col_1     col_2
         *     "AB" << "CAB" << "CABC"
         *     "ABC" << "ABC" << "ABC"
         *
         * where col_0 == "AB"
         */
        auto* col0 = new std::string("col_0");
        auto* alias0 = new std::string("101");
        auto* ape0 = new AliasPropertyExpression(new std::string(""), alias0, col0);
        std::string c0("AB");
        auto* pe0 = new PrimaryExpression(c0);
        auto r1 =  std::make_unique<RelationalExpression>(ape0,
                                                          RelationalExpression::Operator::EQ,
                                                          pe0);
        auto resp = checkLookupEdgesString(Expression::encode(r1.get()));
        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(3, resp.get_schema()->get_columns().size());
        EXPECT_EQ(3, resp.rows.size());
        RowWriter ewriter(nullptr);
        ewriter << "AB" << "CAB" << "CABC";
        auto eval = ewriter.encode();
        for (const auto& row : resp.rows) {
            EXPECT_EQ(eval, row.get_props());
            EXPECT_EQ(1, row.get_key().get_src());
            EXPECT_EQ(10, row.get_key().get_dst());
            EXPECT_EQ(101, row.get_key().get_edge_type());
            EXPECT_EQ(0, row.get_key().get_ranking());
        }
    }
    {
        LOG(INFO) << "Build filter...";
        /**
         * Total rows :
         *     col_0   col_1     col_2
         *     "AB" << "CAB" << "CABC"
         *     "ABC" << "ABC" << "ABC"
         *
         * where col_1 == "CAB"
         */
        auto* col0 = new std::string("col_1");
        auto* alias0 = new std::string("101");
        auto* ape0 = new AliasPropertyExpression(new std::string(""), alias0, col0);
        std::string c1("CAB");
        auto* pe0 = new PrimaryExpression(c1);
        auto r1 =  std::make_unique<RelationalExpression>(ape0,
                                                          RelationalExpression::Operator::EQ,
                                                          pe0);
        auto resp = checkLookupEdgesString(Expression::encode(r1.get()));
        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(3, resp.get_schema()->get_columns().size());
        EXPECT_EQ(3, resp.rows.size());
        RowWriter ewriter(nullptr);
        ewriter << "AB" << "CAB" << "CABC";
        auto eval = ewriter.encode();
        for (const auto& row : resp.rows) {
            EXPECT_EQ(eval, row.get_props());
            EXPECT_EQ(1, row.get_key().get_src());
            EXPECT_EQ(10, row.get_key().get_dst());
            EXPECT_EQ(101, row.get_key().get_edge_type());
            EXPECT_EQ(0, row.get_key().get_ranking());
        }
    }
}

TEST(IndexScanTest, VertexStringTest) {
    {
        LOG(INFO) << "Build filter...";
        /**
         * Total rows :
         *     col_0   col_1     col_2
         *     "AB" << "CAB" << "CABC"
         *     "ABC" << "ABC" << "ABC"
         *
         * where col_0 == "ABC"
         */
        auto* col0 = new std::string("col_0");
        auto* alias0 = new std::string("3001");
        auto* ape0 = new AliasPropertyExpression(new std::string(""), alias0, col0);
        std::string c0("ABC");
        auto* pe0 = new PrimaryExpression(c0);
        auto r1 =  std::make_unique<RelationalExpression>(ape0,
                                                          RelationalExpression::Operator::EQ,
                                                          pe0);
        auto resp = checkLookupVerticesString(Expression::encode(r1.get()));
        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(3, resp.get_schema()->get_columns().size());
        EXPECT_EQ(3, resp.rows.size());
        RowWriter ewriter(nullptr);
        ewriter << "ABC" << "ABC" << "ABC";
        auto eval = ewriter.encode();
        for (const auto& row : resp.rows) {
            EXPECT_EQ(eval, row.get_props());
            EXPECT_EQ(200, row.get_vertex_id());
        }
    }
    {
        LOG(INFO) << "Build filter...";
        /**
         * Total rows :
         *     col_0   col_1     col_2
         *     "AB" << "CAB" << "CABC"
         *     "ABC" << "ABC" << "ABC"
         *
         * where col_0 == "AB"
         */
        auto* col0 = new std::string("col_0");
        auto* alias0 = new std::string("3001");
        auto* ape0 = new AliasPropertyExpression(new std::string(""), alias0, col0);
        std::string c0("AB");
        auto* pe0 = new PrimaryExpression(c0);
        auto r1 =  std::make_unique<RelationalExpression>(ape0,
                                                          RelationalExpression::Operator::EQ,
                                                          pe0);
        auto resp = checkLookupVerticesString(Expression::encode(r1.get()));
        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(3, resp.get_schema()->get_columns().size());
        EXPECT_EQ(3, resp.rows.size());
        RowWriter ewriter(nullptr);
        ewriter << "AB" << "CAB" << "CABC";
        auto eval = ewriter.encode();
        for (const auto& row : resp.rows) {
            EXPECT_EQ(eval, row.get_props());
            EXPECT_EQ(100, row.get_vertex_id());
        }
    }
    {
        LOG(INFO) << "Build filter...";
        /**
         * Total rows :
         *     col_0   col_1     col_2
         *     "AB" << "CAB" << "CABC"
         *     "ABC" << "ABC" << "ABC"
         *
         * where col_1 == "CAB"
         */
        auto* col0 = new std::string("col_1");
        auto* alias0 = new std::string("3001");
        auto* ape0 = new AliasPropertyExpression(new std::string(""), alias0, col0);
        std::string c1("CAB");
        auto* pe0 = new PrimaryExpression(c1);
        auto r1 =  std::make_unique<RelationalExpression>(ape0,
                                                          RelationalExpression::Operator::EQ,
                                                          pe0);
        auto resp = checkLookupVerticesString(Expression::encode(r1.get()));
        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(3, resp.get_schema()->get_columns().size());
        EXPECT_EQ(3, resp.rows.size());
        RowWriter ewriter(nullptr);
        ewriter << "AB" << "CAB" << "CABC";
        auto eval = ewriter.encode();
        for (const auto& row : resp.rows) {
            EXPECT_EQ(eval, row.get_props());
            EXPECT_EQ(100, row.get_vertex_id());
        }
    }
}

TEST(IndexScanTest, EdgeDoubleTest) {
    {
        LOG(INFO) << "Build filter...";
        /**
         * Total rows :
         *     col_0   col_1     col_2
         *     "1.1" << "0.0" << "-1.1"
         *     "2.2" << "0.0" << "-2.2"
         *
         * where col_0 == "1.1"
         */
        auto* col0 = new std::string("col_0");
        auto* alias0 = new std::string("101");
        auto* ape0 = new AliasPropertyExpression(new std::string(""), alias0, col0);
        double c0(1.1);
        auto* pe0 = new PrimaryExpression(c0);
        auto r1 =  std::make_unique<RelationalExpression>(ape0,
                                                          RelationalExpression::Operator::EQ,
                                                          pe0);
        auto resp = checkLookupEdgesDouble(Expression::encode(r1.get()));
        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(3, resp.get_schema()->get_columns().size());
        EXPECT_EQ(3, resp.rows.size());
        RowWriter ewriter(nullptr);
        ewriter << boost::get<double>(1.1)
                << boost::get<double>(0.0)
                << boost::get<double>(-1.1);
        auto eval = ewriter.encode();
        for (const auto& row : resp.rows) {
            EXPECT_EQ(eval, row.get_props());
            EXPECT_EQ(1, row.get_key().get_src());
            EXPECT_EQ(10, row.get_key().get_dst());
            EXPECT_EQ(101, row.get_key().get_edge_type());
            EXPECT_EQ(0, row.get_key().get_ranking());
        }
    }
    {
        LOG(INFO) << "Build filter...";
        /**
         * Total rows :
         *     col_0   col_1     col_2
         *     "1.1" << "0.0" << "-1.1"
         *     "2.2" << "0.0" << "-2.2"
         *
         * where col_2 == "-1.1"
         */
        auto* col2 = new std::string("col_2");
        auto* alias0 = new std::string("101");
        auto* ape0 = new AliasPropertyExpression(new std::string(""), alias0, col2);
        double c2(-1.1);
        auto* pe0 = new PrimaryExpression(c2);
        auto r1 =  std::make_unique<RelationalExpression>(ape0,
                                                          RelationalExpression::Operator::EQ,
                                                          pe0);
        auto resp = checkLookupEdgesDouble(Expression::encode(r1.get()));
        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(3, resp.get_schema()->get_columns().size());
        EXPECT_EQ(3, resp.rows.size());
        RowWriter ewriter(nullptr);
        ewriter << boost::get<double>(1.1)
                << boost::get<double>(0.0)
                << boost::get<double>(-1.1);
        auto eval = ewriter.encode();
        for (const auto& row : resp.rows) {
            EXPECT_EQ(eval, row.get_props());
            EXPECT_EQ(1, row.get_key().get_src());
            EXPECT_EQ(10, row.get_key().get_dst());
            EXPECT_EQ(101, row.get_key().get_edge_type());
            EXPECT_EQ(0, row.get_key().get_ranking());
        }
    }
    {
        LOG(INFO) << "Build filter...";
        /**
         * Total rows :
         *     col_0   col_1     col_2
         *     "1.1" << "0.0" << "-1.1"
         *     "2.2" << "0.0" << "-2.2"
         *
         * where col_0 > "1.1"
         */
        auto* col0 = new std::string("col_0");
        auto* alias0 = new std::string("101");
        auto* ape0 = new AliasPropertyExpression(new std::string(""), alias0, col0);
        double c0(1.1);
        auto* pe0 = new PrimaryExpression(c0);
        auto r1 =  std::make_unique<RelationalExpression>(ape0,
                                                          RelationalExpression::Operator::GT,
                                                          pe0);
        auto resp = checkLookupEdgesDouble(Expression::encode(r1.get()));
        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(3, resp.get_schema()->get_columns().size());
        EXPECT_EQ(3, resp.rows.size());
        RowWriter ewriter(nullptr);
        ewriter << boost::get<double>(2.2)
                << boost::get<double>(0.0)
                << boost::get<double>(-2.2);
        auto eval = ewriter.encode();
        for (const auto& row : resp.rows) {
            EXPECT_EQ(eval, row.get_props());
            EXPECT_EQ(2, row.get_key().get_src());
            EXPECT_EQ(20, row.get_key().get_dst());
            EXPECT_EQ(101, row.get_key().get_edge_type());
            EXPECT_EQ(0, row.get_key().get_ranking());
        }
    }
    {
        LOG(INFO) << "Build filter...";
        /**
         * Total rows :
         *     col_0   col_1     col_2
         *     "1.1" << "0.0" << "-1.1"
         *     "2.2" << "0.0" << "-2.2"
         *
         * where col_1 == "0.0"
         */
        auto* col1 = new std::string("col_1");
        auto* alias0 = new std::string("101");
        auto* ape0 = new AliasPropertyExpression(new std::string(""), alias0, col1);
        double c1(0.0);
        auto* pe0 = new PrimaryExpression(c1);
        auto r1 =  std::make_unique<RelationalExpression>(ape0,
                                                          RelationalExpression::Operator::EQ,
                                                          pe0);
        auto resp = checkLookupEdgesDouble(Expression::encode(r1.get()));
        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(3, resp.get_schema()->get_columns().size());
        EXPECT_EQ(6, resp.rows.size());
    }
}

TEST(IndexScanTest, VertexDoubleTest) {
    {
        LOG(INFO) << "Build filter...";
        /**
         * Total rows :
         *     col_0   col_1     col_2
         *     "1.1" << "0.0" << "-1.1"
         *     "2.2" << "0.0" << "-2.2"
         *
         * where col_0 == "1.1"
         */
        auto* col0 = new std::string("col_0");
        auto* alias0 = new std::string("3001");
        auto* ape0 = new AliasPropertyExpression(new std::string(""), alias0, col0);
        double c0(1.1);
        auto* pe0 = new PrimaryExpression(c0);
        auto r1 =  std::make_unique<RelationalExpression>(ape0,
                                                          RelationalExpression::Operator::EQ,
                                                          pe0);
        auto resp = checkLookupVerticesDouble(Expression::encode(r1.get()));
        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(3, resp.get_schema()->get_columns().size());
        EXPECT_EQ(3, resp.rows.size());
        RowWriter ewriter(nullptr);
        ewriter << boost::get<double>(1.1)
                << boost::get<double>(0.0)
                << boost::get<double>(-1.1);
        auto eval = ewriter.encode();
        for (const auto& row : resp.rows) {
            EXPECT_EQ(eval, row.get_props());
            EXPECT_EQ(100, row.get_vertex_id());
        }
    }
    {
        LOG(INFO) << "Build filter...";
        /**
         * Total rows :
         *     col_0   col_1     col_2
         *     "1.1" << "0.0" << "-1.1"
         *     "2.2" << "0.0" << "-2.2"
         *
         * where col_2 == "-1.1"
         */
        auto* col2 = new std::string("col_2");
        auto* alias0 = new std::string("3001");
        auto* ape0 = new AliasPropertyExpression(new std::string(""), alias0, col2);
        double c2(-1.1);
        auto* pe0 = new PrimaryExpression(c2);
        auto r1 =  std::make_unique<RelationalExpression>(ape0,
                                                          RelationalExpression::Operator::EQ,
                                                          pe0);
        auto resp = checkLookupVerticesDouble(Expression::encode(r1.get()));
        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(3, resp.get_schema()->get_columns().size());
        EXPECT_EQ(3, resp.rows.size());
        RowWriter ewriter(nullptr);
        ewriter << boost::get<double>(1.1)
                << boost::get<double>(0.0)
                << boost::get<double>(-1.1);
        auto eval = ewriter.encode();
        for (const auto& row : resp.rows) {
            EXPECT_EQ(eval, row.get_props());
            EXPECT_EQ(100, row.get_vertex_id());
        }
    }
    {
        LOG(INFO) << "Build filter...";
        /**
         * Total rows :
         *     col_0   col_1     col_2
         *     "1.1" << "0.0" << "-1.1"
         *     "2.2" << "0.0" << "-2.2"
         *
         * where col_0 > "1.1"
         */
        auto* col0 = new std::string("col_0");
        auto* alias0 = new std::string("3001");
        auto* ape0 = new AliasPropertyExpression(new std::string(""), alias0, col0);
        double c0(1.1);
        auto* pe0 = new PrimaryExpression(c0);
        auto r1 =  std::make_unique<RelationalExpression>(ape0,
                                                          RelationalExpression::Operator::GT,
                                                          pe0);
        auto resp = checkLookupVerticesDouble(Expression::encode(r1.get()));
        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(3, resp.get_schema()->get_columns().size());
        EXPECT_EQ(3, resp.rows.size());
        RowWriter ewriter(nullptr);
        ewriter << boost::get<double>(2.2)
                << boost::get<double>(0.0)
                << boost::get<double>(-2.2);
        auto eval = ewriter.encode();
        for (const auto& row : resp.rows) {
            EXPECT_EQ(eval, row.get_props());
            EXPECT_EQ(200, row.get_vertex_id());
        }
    }
    {
        LOG(INFO) << "Build filter...";
        /**
         * Total rows :
         *     col_0   col_1     col_2
         *     "1.1" << "0.0" << "-1.1"
         *     "2.2" << "0.0" << "-2.2"
         *
         * where col_1 == "0.0"
         */
        auto* col1 = new std::string("col_1");
        auto* alias0 = new std::string("3001");
        auto* ape0 = new AliasPropertyExpression(new std::string(""), alias0, col1);
        double c0(0.0);
        auto* pe0 = new PrimaryExpression(c0);
        auto r1 =  std::make_unique<RelationalExpression>(ape0,
                                                          RelationalExpression::Operator::EQ,
                                                          pe0);
        auto resp = checkLookupVerticesDouble(Expression::encode(r1.get()));
        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(3, resp.get_schema()->get_columns().size());
        EXPECT_EQ(6, resp.rows.size());
    }
}

=======
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
    auto index = cpp2::IndexItem(apache::thrift::FragileConstructor::FRAGILE,
                                 tagId, tagId, std::move(cols));

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
        decltype(req.hints) hints;
        hints.emplace_back(cpp2::IndexHint(apache::thrift::FragileConstructor::FRAGILE,
                                           false, "AABB", "", nebula::cpp2::SupportedType::STRING));

        req.set_space_id(spaceId);
        req.set_parts(std::move(parts));
        req.set_index(index);
        req.set_return_columns(retCols);
        req.set_hints(hints);

        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(2, resp.get_schema().get_columns().size());
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
        decltype(req.hints) hints;
        hints.emplace_back(cpp2::IndexHint(apache::thrift::FragileConstructor::FRAGILE,
                                           false, "AA", "", nebula::cpp2::SupportedType::STRING));

        req.set_space_id(spaceId);
        req.set_parts(std::move(parts));
        req.set_index(index);
        req.set_return_columns(retCols);
        req.set_hints(hints);

        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(2, resp.get_schema().get_columns().size());
        EXPECT_EQ(2, resp.rows.size());
    }
}
>>>>>>> online index scan
}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}

