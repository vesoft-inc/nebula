/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "utils/NebulaKeyUtils.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "fs/TempDir.h"
#include "storage/test/TestUtils.h"
#include "storage/query/QueryEdgePropsProcessor.h"
#include "dataman/RowSetReader.h"
#include "dataman/RowReader.h"

namespace nebula {
namespace storage {

static void mockData(kvstore::KVStore* kv, meta::SchemaManager* schemaMng, EdgeVersion version) {
    auto edgeType = 101;
    auto spaceId = 0;
    auto schema = schemaMng->getEdgeSchema(spaceId, edgeType);
    for (PartitionID partId = 0; partId < 3; partId++) {
        std::vector<kvstore::KV> data;
        for (VertexID vertexId = partId * 10; vertexId < (partId + 1) * 10; vertexId++) {
            // Generate 7 edges for each source vertex id
            for (VertexID dstId = 10001; dstId <= 10007; dstId++) {
                VLOG(3) << "Write part " << partId << ", vertex " << vertexId << ", dst " << dstId;
                auto key = NebulaKeyUtils::edgeKey(
                    partId, vertexId, edgeType, dstId - 10001, dstId, version);
                auto val = TestUtils::setupEncode(10, 20);
                data.emplace_back(std::move(key), std::move(val));
            }
        }
        folly::Baton<true, std::atomic> baton;
        kv->asyncMultiPut(0, partId, std::move(data), [&](kvstore::ResultCode code) {
            EXPECT_EQ(code, kvstore::ResultCode::SUCCEEDED);
            baton.post();
        });
        baton.wait();
    }
}

static void buildRequest(cpp2::EdgePropRequest& req) {
    req.set_space_id(0);
    decltype(req.parts) tmpEdges;
    for (PartitionID partId = 0; partId < 3; partId++) {
        for (VertexID vertexId =  partId * 10; vertexId < (partId + 1) * 10; vertexId++) {
            for (VertexID dstId = 10001; dstId <= 10007; dstId++) {
                cpp2::EdgeKey edgeKey;
                edgeKey.set_src(vertexId);
                edgeKey.set_edge_type(101);
                edgeKey.set_ranking(dstId - 10001);
                edgeKey.set_dst(dstId);
                tmpEdges[partId].emplace_back(std::move(edgeKey));
            }
        }
    }
    req.set_parts(std::move(tmpEdges));
    req.set_edge_type(101);
    // Return edge props col_0, col_2, col_4 ... col_18
    decltype(req.return_columns) tmpColumns;
    for (int i = 0; i < 10; i++) {
        tmpColumns.emplace_back(TestUtils::edgePropDef(folly::stringPrintf("col_%d", i * 2), 101));
    }
    req.set_return_columns(std::move(tmpColumns));
}

static void checkResponse(cpp2::EdgePropResponse& resp, uint32_t expectedColSize) {
    EXPECT_EQ(0, resp.result.failed_codes.size());
    EXPECT_EQ(expectedColSize, resp.schema.columns.size());
    auto provider = std::make_shared<ResultSchemaProvider>(resp.schema);
    LOG(INFO) << "Check edge props...";
    RowSetReader rsReader(provider, resp.data);
    auto it = rsReader.begin();
    int32_t rowNum = 0;
    while (static_cast<bool>(it)) {
        EXPECT_EQ(expectedColSize, it->numFields());
       {
            // _src
            // We can't ensure the order, so just check the srcId range.
            int64_t v;
            EXPECT_EQ(ResultType::SUCCEEDED, it->getVid(0, v));
            CHECK_GE(30, v);
            CHECK_LE(0, v);
        }
        {
            // _rank
            int64_t v;
            EXPECT_EQ(ResultType::SUCCEEDED, it->getInt<int64_t>(1, v));
            CHECK_EQ(rowNum % 7, v);
        }
        {
            // _dst
            int64_t v;
            EXPECT_EQ(ResultType::SUCCEEDED, it->getVid(2, v));
            CHECK_EQ(10001 + rowNum % 7, v);
        }
        {
            // _type
            int64_t v;
            EXPECT_EQ(ResultType::SUCCEEDED, it->getVid(3, v));
            CHECK_EQ(101, v);
        }
        // col_0, col_2 ... col_8
        for (auto i = 4; i < 9; i++) {
            int64_t v;
            EXPECT_EQ(ResultType::SUCCEEDED, it->getInt<int64_t>(i, v));
            CHECK_EQ((i - 4) * 2, v);
        }
        // col_10, col_12 ... col_18
        for (auto i = 9; i < 14; i++) {
            folly::StringPiece v;
            EXPECT_EQ(ResultType::SUCCEEDED, it->getString(i, v));
            CHECK_EQ(folly::stringPrintf("string_col_%d", (i - 9 + 5) * 2), v);
        }

        ++it;
        rowNum++;
    }
    EXPECT_EQ(it, rsReader.end());
    EXPECT_EQ(210, rowNum);
}

void checkAlteredProp(cpp2::EdgePropResponse& resp,
                      const std::string& prop,
                      const VariantType& val) {
    LOG(INFO) << "Check: " << prop;
    auto provider = std::make_shared<ResultSchemaProvider>(resp.schema);
    RowSetReader rsReader(provider, resp.data);
    auto it = rsReader.begin();
    while (it) {
        switch (val.which()) {
            case VAR_INT64: {
                int64_t v;
                EXPECT_EQ(ResultType::SUCCEEDED, it->getInt(prop, v));
                EXPECT_EQ(boost::get<int64_t>(val), v);
                break;
            }
            case VAR_DOUBLE: {
                double v;
                EXPECT_EQ(ResultType::SUCCEEDED, it->getDouble(prop, v));
                EXPECT_EQ(boost::get<double>(val), v);
                break;
            }
           case VAR_BOOL: {
                bool v;
                EXPECT_EQ(ResultType::SUCCEEDED, it->getBool(prop, v));
                EXPECT_EQ(boost::get<bool>(val), v);
                break;
           }
           case VAR_STR: {
                folly::StringPiece v;
                EXPECT_EQ(ResultType::SUCCEEDED, it->getString(prop, v));
                EXPECT_EQ(boost::get<std::string>(val), v);
                break;
           }
           default:
                LOG(FATAL) << "No such type: " << val.which();
        }
        ++it;
    }
}


void checkTTLResponse(cpp2::EdgePropResponse& resp) {
    EXPECT_EQ(0, resp.result.failed_codes.size());
    EXPECT_EQ(14, resp.schema.columns.size());
    auto provider = std::make_shared<ResultSchemaProvider>(resp.schema);
    LOG(INFO) << "Check edge props...";
    RowSetReader rsReader(provider, resp.data);
    EXPECT_EQ(0, rsReader.getData().size());

    auto it = rsReader.begin();
    int32_t rowNum = 0;
    while (static_cast<bool>(it)) {
        ++it;
        rowNum++;
    }
    EXPECT_EQ(it, rsReader.end());
    EXPECT_EQ(0, rowNum);
}


TEST(QueryEdgePropsTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/QueryEdgePropsTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());

    LOG(INFO) << "Prepare meta...";
    auto schemaMng = TestUtils::mockSchemaMan();
    LOG(INFO) << "Prepare data...";
    mockData(kv.get(), schemaMng.get(), 0);
    LOG(INFO) << "Build EdgePropRequest...";
    cpp2::EdgePropRequest req;
    buildRequest(req);

    LOG(INFO) << "Test QueryEdgePropsRequest...";
    auto* processor = QueryEdgePropsProcessor::instance(kv.get(), schemaMng.get(), nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    checkResponse(resp, 14);
}

TEST(QueryEdgePropsTest, TTLTest) {
    fs::TempDir rootPath("/tmp/QueryEdgePropsTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv(TestUtils::initKV(rootPath.path()));

    LOG(INFO) << "Prepare meta...";
    auto schemaMng = TestUtils::mockSchemaWithTTLMan();
    LOG(INFO) << "Prepare data...";
    mockData(kv.get(), schemaMng.get(), 0);
    LOG(INFO) << "Build EdgePropRequest...";
    cpp2::EdgePropRequest req;
    buildRequest(req);

    LOG(INFO) << "Test QueryEdgePropsRequest...";
    auto* processor = QueryEdgePropsProcessor::instance(kv.get(), schemaMng.get(), nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    checkTTLResponse(resp);
}

TEST(QueryEdgePropsTest, QueryAfterEdgeAltered) {
    fs::TempDir rootPath("/tmp/QueryEdgePropsTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());

    LOG(INFO) << "Prepare meta...";
    auto schemaMng = TestUtils::mockSchemaMan();
    LOG(INFO) << "Prepare data...";
    auto spaceId = 0;
    EdgeVersion version = std::numeric_limits<int64_t>::max() - 0;
    mockData(kv.get(), schemaMng.get(), version);

    {
        LOG(INFO) << "Alter the edge schema with adding a prop.";
        auto schemaVer = 1;
        for (auto edgeType = 101; edgeType < 110; edgeType++) {
                auto schema = schemaMng->getEdgeSchema(0, edgeType);
                auto iter = schema->begin();
                auto schemaWriter = std::make_shared<SchemaWriter>(schemaVer);
                while (iter) {
                    auto* name = iter->getName();
                    auto type = iter->getType();
                    schemaWriter->appendCol(name, std::move(type));
                    ++iter;
                }
                nebula::cpp2::ValueType type;
                type.type = nebula::cpp2::SupportedType::STRING;
                schemaWriter->appendCol("AddedProp", std::move(type));
                schemaMng->addEdgeSchema(spaceId, edgeType, schemaWriter, schemaVer);
        }

        LOG(INFO) << "Build EdgePropRequest...";
        cpp2::EdgePropRequest req;
        buildRequest(req);
        req.return_columns.emplace_back(TestUtils::edgePropDef("AddedProp", 101));

        LOG(INFO) << "Test QueryEdgePropsRequest...";
        auto* processor = QueryEdgePropsProcessor::instance(kv.get(), schemaMng.get(), nullptr);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        LOG(INFO) << "Check the results...";
        checkResponse(resp, 15);

        std::string prop = "AddedProp";
        std::string val = "";
        checkAlteredProp(resp, prop, val);
    }

    version = std::numeric_limits<int64_t>::max() - 1;
    {
        LOG(INFO) << "Now update data with new edge prop";
        auto edgeType = 101;
        for (auto partId = 0; partId < 3; partId++) {
            std::vector<kvstore::KV> data;
            for (auto vertexId = partId * 10; vertexId < (partId + 1) * 10; vertexId++) {
                // Generate 7 edges for each source vertex id
                for (auto dstId = 10001; dstId <= 10007; dstId++) {
                    VLOG(3) << "Write part " << partId << ", vertex "
                            << vertexId << ", dst " << dstId;
                    auto key = NebulaKeyUtils::edgeKey(
                            partId, vertexId, edgeType, dstId - 10001, dstId, version);
                    auto schema = schemaMng->getEdgeSchema(spaceId, edgeType);
                    RowWriter writer(schema);
                    for (int64_t numInt = 0; numInt < 10; numInt++) {
                        writer << numInt;
                    }
                    for (auto numString = 10; numString < 20; numString++) {
                        writer << folly::stringPrintf("string_col_%d", numString);
                    }
                    writer << "AddedPropValue";
                    auto val = writer.encode();
                    data.emplace_back(std::move(key), std::move(val));
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

        LOG(INFO) << "Build EdgePropRequest...";
        cpp2::EdgePropRequest req;
        buildRequest(req);
        req.return_columns.emplace_back(TestUtils::edgePropDef("AddedProp", 101));

        LOG(INFO) << "Test QueryEdgePropsRequest...";
        auto* processor = QueryEdgePropsProcessor::instance(kv.get(), schemaMng.get(), nullptr);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        LOG(INFO) << "Check the results...";
        checkResponse(resp, 15);

        std::string prop = "AddedProp";
        std::string val = "AddedPropValue";
        checkAlteredProp(resp, prop, val);
    }
    {
        LOG(INFO) << "Alter the edge schema with altering the prop type.";
        auto schemaVer = 2;
        for (auto edgeType = 101; edgeType < 110; edgeType++) {
                auto schema = schemaMng->getEdgeSchema(0, edgeType, 0);
                auto iter = schema->begin();
                auto schemaWriter = std::make_shared<SchemaWriter>(schemaVer);
                while (iter) {
                    auto* name = iter->getName();
                    auto type = iter->getType();
                    schemaWriter->appendCol(name, std::move(type));
                    ++iter;
                }
                nebula::cpp2::ValueType type;
                type.type = nebula::cpp2::SupportedType::INT;
                schemaWriter->appendCol("AddedProp", std::move(type));
                schemaMng->addEdgeSchema(spaceId, edgeType, schemaWriter, schemaVer);
        }

        LOG(INFO) << "Build EdgePropRequest...";
        cpp2::EdgePropRequest req;
        buildRequest(req);
        req.return_columns.emplace_back(TestUtils::edgePropDef("AddedProp", 101));

        LOG(INFO) << "Test QueryEdgePropsRequest...";
        auto* processor = QueryEdgePropsProcessor::instance(kv.get(), schemaMng.get(), nullptr);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        LOG(INFO) << "Check the results...";
        EXPECT_EQ(0, resp.result.failed_codes.size());
        EXPECT_EQ(15, resp.schema.columns.size());
        auto provider = std::make_shared<ResultSchemaProvider>(resp.schema);
        LOG(INFO) << "Check edge props...";
        checkResponse(resp, 15);

        std::string prop = "AddedProp";
        int64_t val = 0;
        checkAlteredProp(resp, prop, val);
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
