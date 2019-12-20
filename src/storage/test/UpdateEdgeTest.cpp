/* Copyright (c) 2019 vesoft inc. All rights reserved.
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
#include "storage/mutate/UpdateEdgeProcessor.h"
#include "dataman/RowSetReader.h"
#include "dataman/RowReader.h"

namespace nebula {
namespace storage {

void mockData(kvstore::KVStore* kv) {
    LOG(INFO) << "Prepare data...";
    std::vector<kvstore::KV> data;
    for (auto partId = 0; partId < 3; partId++) {
        for (auto vertexId = partId * 10; vertexId < (partId + 1) * 10; vertexId++) {
            for (auto tagId = 3001; tagId < 3010; tagId++) {
                // Write multi versions, we should get/update the latest version
                for (auto version = 0; version < 3; version++) {
                    auto key = NebulaKeyUtils::vertexKey(partId, vertexId, tagId,
                            std::numeric_limits<int32_t>::max() - version);
                    RowWriter writer;
                    for (int64_t numInt = 0; numInt < 3; numInt++) {
                        writer << partId + tagId + version + numInt;
                    }
                    for (auto numString = 3; numString < 6; numString++) {
                        writer << folly::stringPrintf("tag_string_col_%d_%d", numString, version);
                    }
                    auto val = writer.encode();
                    data.emplace_back(std::move(key), std::move(val));
                }
            }
            // Generate 7 out-edges for each edgeType.
            for (auto dstId = 10001; dstId <= 10007; dstId++) {
                VLOG(3) << "Write part " << partId << ", vertex " << vertexId << ", dst " << dstId;
                // Write multi versions,  we should get the latest version.
                for (auto version = 0; version < 3; version++) {
                    auto key = NebulaKeyUtils::edgeKey(partId, vertexId, 101, 0, dstId,
                                                       std::numeric_limits<int>::max() - version);
                    RowWriter writer(nullptr);
                    for (uint64_t numInt = 0; numInt < 10; numInt++) {
                        writer << (dstId + numInt);
                    }
                    for (auto numString = 10; numString < 20; numString++) {
                        writer << folly::stringPrintf("string_col_%d_%d", numString, version);
                    }
                    auto val = writer.encode();
                    data.emplace_back(std::move(key), std::move(val));
                }
            }
            // Generate 5 in-edges for each edgeType, the edgeType is negative
            for (auto srcId = 10001; srcId <= 10005; srcId++) {
                VLOG(3) << "Write part " << partId << ", vertex " << vertexId << ", src " << srcId;
                for (auto version = 0; version < 3; version++) {
                    auto key = NebulaKeyUtils::edgeKey(partId, vertexId, -101, 0, srcId,
                                                       std::numeric_limits<int>::max() - version);
                    data.emplace_back(std::move(key), "");
                }
            }
        }
        folly::Baton<true, std::atomic> baton;
        kv->asyncMultiPut(0, partId, std::move(data),
            [&](kvstore::ResultCode code) {
                CHECK_EQ(code, kvstore::ResultCode::SUCCEEDED);
                baton.post();
            });
        baton.wait();
    }
}


TEST(UpdateEdgeTest, Set_Filter_Yield_Test) {
    fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());

    LOG(INFO) << "Prepare meta...";
    auto schemaMan = TestUtils::mockSchemaMan();
    mockData(kv.get());

    LOG(INFO) << "Build UpdateEdgeRequest...";
    GraphSpaceID spaceId = 0;
    PartitionID partId = 0;
    VertexID srcId = 1;
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
    LOG(INFO) << "Build filter...";
    // left int: $^.3001.tag_3001_col_0 >= 0
    auto* tag1 = new std::string("3001");
    auto* prop1 = new std::string("tag_3001_col_0");
    auto* srcExp1 = new SourcePropertyExpression(tag1, prop1);
    auto* priExp1 = new PrimaryExpression(0L);
    auto* left = new RelationalExpression(srcExp1,
                                          RelationalExpression::Operator::GE,
                                          priExp1);
    // right string: $^.3003.tag_3003_col_3 == tag_string_col_3_2;
    auto* tag2 = new std::string("3003");
    auto* prop2 = new std::string("tag_3003_col_3");
    auto* srcExp2 = new SourcePropertyExpression(tag2, prop2);
    std::string col3("tag_string_col_3_2");
    auto* priExp2 = new PrimaryExpression(col3);
    auto* right = new RelationalExpression(srcExp2,
                                           RelationalExpression::Operator::EQ,
                                           priExp2);
    // left AND right is ture
    auto logExp = std::make_unique<LogicalExpression>(left, LogicalExpression::AND, right);
    req.set_filter(Expression::encode(logExp.get()));
    LOG(INFO) << "Build update items...";
    std::vector<cpp2::UpdateItem> items;
    // int: 101.col_0 = 101.col_2 = 10001 + 2 = 10003
    cpp2::UpdateItem item1;
    item1.set_name("101");
    item1.set_prop("col_0");
    auto* edge101 = new std::string("101");
    auto* propCol2 = new std::string("col_2");
    AliasPropertyExpression val1(new std::string(""), edge101, propCol2);
    item1.set_value(Expression::encode(&val1));
    items.emplace_back(item1);
    // string: 101.col_10 = string_col_10_2_new
    cpp2::UpdateItem item2;
    item2.set_name("101");
    item2.set_prop("col_10");
    std::string col10new("string_col_10_2_new");
    PrimaryExpression val2(col10new);
    item2.set_value(Expression::encode(&val2));
    items.emplace_back(item2);
    req.set_update_items(std::move(items));
    decltype(req.return_columns) tmpColumns;
    tmpColumns.emplace_back(Expression::encode(&val1));
    tmpColumns.emplace_back(Expression::encode(&val2));
    SourcePropertyExpression sourcePropExp(new std::string(folly::to<std::string>(3002)),
                                           new std::string("tag_3002_col_2"));
    tmpColumns.emplace_back(Expression::encode(&sourcePropExp));

    req.set_return_columns(std::move(tmpColumns));
    req.set_insertable(false);
    LOG(INFO) << "Test UpdateEdgeRequest...";
    auto* processor = UpdateEdgeProcessor::instance(kv.get(), schemaMan.get(), nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(0, resp.result.failed_codes.size());
    EXPECT_FALSE(resp.get_upsert());
    ASSERT_TRUE(resp.__isset.schema);
    auto provider = std::make_shared<ResultSchemaProvider>(resp.schema);
    auto reader = RowReader::getRowReader(resp.data, provider);
    EXPECT_EQ(3, reader->numFields());
    for (int i = 0; i < 3; i++) {
        auto res = RowReader::getPropByIndex(reader.get(), i);
        if (ok(res)) {
            switch (i) {
                case 0: {
                    auto&& v0 = value(std::move(res));
                    EXPECT_EQ(10003, boost::get<int64_t>(v0));
                    break;
                }
                case 1: {
                    auto&& v1 = value(std::move(res));
                    EXPECT_STREQ("string_col_10_2_new", boost::get<std::string>(v1).c_str());
                    break;
                }
                case 2: {
                    auto&& v2 = value(std::move(res));
                    EXPECT_EQ(3006, boost::get<int64_t>(v2));
                    break;
                }
                default:
                    break;
            }
        }
    }
    // check the kvstore
    std::vector<std::string> keys;
    std::vector<std::string> values;
    auto lastVersion = std::numeric_limits<int32_t>::max() - 2;
    auto kvstoreEdgeKey = NebulaKeyUtils::edgeKey(partId, srcId, 101, 0, dstId, lastVersion);
    keys.emplace_back(kvstoreEdgeKey);
    kvstore::ResultCode code = kv->multiGet(spaceId, partId, std::move(keys), &values);
    CHECK_EQ(code, kvstore::ResultCode::SUCCEEDED);
    EXPECT_EQ(1, values.size());
    auto edgeSchema = schemaMan->getEdgeSchema(spaceId, 101);
    auto edgeReader = RowReader::getRowReader(values[0], edgeSchema);
    auto res = RowReader::getPropByName(edgeReader.get(), "col_0");
    CHECK(ok(res));
    auto&& v1 = value(std::move(res));
    EXPECT_EQ(10003, boost::get<int64_t>(v1));
    res = RowReader::getPropByName(edgeReader.get(), "col_10");
    CHECK(ok(res));
    auto&& v2 = value(std::move(res));
    EXPECT_STREQ("string_col_10_2_new", boost::get<std::string>(v2).c_str());
}


TEST(UpdateEdgeTest, Insertable_Test) {
    fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());

    LOG(INFO) << "Prepare meta...";
    auto schemaMan = TestUtils::mockSchemaMan();
    mockData(kv.get());

    LOG(INFO) << "Build UpdateEdgeRequest...";
    GraphSpaceID spaceId = 0;
    PartitionID partId = 0;
    VertexID srcId = 1;
    VertexID dstId = 10008;
    // src = 1, edge_type = 101, ranking = 0, dst = 10008
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
    LOG(INFO) << "Build update items...";
    std::vector<cpp2::UpdateItem> items;
    // int: 101.col_0 = $^.3002.tag_3002_col_2
    cpp2::UpdateItem item1;
    item1.set_name("101");
    item1.set_prop("col_0");
    auto* tag3002 = new std::string("3002");
    auto* propCol2 = new std::string("tag_3002_col_2");
    SourcePropertyExpression val1(tag3002, propCol2);
    item1.set_value(Expression::encode(&val1));
    items.emplace_back(item1);
    // string: 101.col_10 = string_col_10_2_new
    cpp2::UpdateItem item2;
    item2.set_name("101");
    item2.set_prop("col_10");
    std::string col10new("string_col_10_2_new");
    PrimaryExpression val2(col10new);
    item2.set_value(Expression::encode(&val2));
    items.emplace_back(item2);
    req.set_update_items(std::move(items));
    decltype(req.return_columns) tmpColumns;
    AliasPropertyExpression edgePropExp(
        new std::string(""), new std::string("101"), new std::string("col_0"));
    tmpColumns.emplace_back(Expression::encode(&edgePropExp));
    edgePropExp = AliasPropertyExpression(
        new std::string(""), new std::string("101"), new std::string("col_1"));
    tmpColumns.emplace_back(Expression::encode(&edgePropExp));
    edgePropExp = AliasPropertyExpression(
        new std::string(""), new std::string("101"), new std::string("col_10"));
    tmpColumns.emplace_back(Expression::encode(&edgePropExp));
    edgePropExp = AliasPropertyExpression(
        new std::string(""), new std::string("101"), new std::string("col_11"));
    tmpColumns.emplace_back(Expression::encode(&edgePropExp));
    tmpColumns.emplace_back(Expression::encode(&val1));
    req.set_return_columns(std::move(tmpColumns));
    req.set_insertable(true);

    LOG(INFO) << "Test UpdateEdgeRequest...";
    auto* processor = UpdateEdgeProcessor::instance(kv.get(), schemaMan.get(), nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(0, resp.result.failed_codes.size());
    EXPECT_TRUE(resp.get_upsert());
    ASSERT_TRUE(resp.__isset.schema);
    auto provider = std::make_shared<ResultSchemaProvider>(resp.schema);
    auto reader = RowReader::getRowReader(resp.data, provider);
    EXPECT_EQ(5, reader->numFields());
    for (int i = 0; i < 5; i++) {
        auto res = RowReader::getPropByIndex(reader.get(), i);
        if (ok(res)) {
            switch (i) {
                case 0: {
                    auto&& v0 = value(std::move(res));
                    EXPECT_EQ(3006, boost::get<int64_t>(v0));
                    break;
                }
                case 1: {
                    auto&& v1 = value(std::move(res));
                    EXPECT_EQ(0, boost::get<int64_t>(v1));
                    break;
                }
                case 2: {
                    auto&& v2 = value(std::move(res));
                    EXPECT_STREQ("string_col_10_2_new", boost::get<std::string>(v2).c_str());
                    break;
                }
                case 3: {
                    auto&& v3 = value(std::move(res));
                    EXPECT_STREQ("", boost::get<std::string>(v3).c_str());
                    break;
                }
                case 4: {
                    auto&& v4 = value(std::move(res));
                    EXPECT_EQ(3006, boost::get<int64_t>(v4));
                    break;
                }
                default:
                    break;
            }
        }
    }
    // get inserted edge from kvstore directly
    auto prefix = NebulaKeyUtils::prefix(partId, srcId, 101, 0, dstId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kv->prefix(spaceId, partId, prefix, &iter);
    EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());
    reader = RowReader::getEdgePropReader(schemaMan.get(), iter->val(), spaceId, 101);
    auto res = RowReader::getPropByName(reader.get(), "col_0");
    EXPECT_TRUE(ok(res));
    auto&& v0 = value(std::move(res));
    EXPECT_EQ(3006L, boost::get<int64_t>(v0));
    res = RowReader::getPropByName(reader.get(), "col_1");
    EXPECT_TRUE(ok(res));
    auto&& v1 = value(std::move(res));
    EXPECT_EQ(0L, boost::get<int64_t>(v1));
    res = RowReader::getPropByName(reader.get(), "col_10");
    EXPECT_TRUE(ok(res));
    auto&& v2 = value(std::move(res));
    EXPECT_STREQ("string_col_10_2_new", boost::get<std::string>(v2).c_str());
    res = RowReader::getPropByName(reader.get(), "col_11");
    EXPECT_TRUE(ok(res));
    auto&& v3 = value(std::move(res));
    EXPECT_STREQ("", boost::get<std::string>(v3).c_str());
}

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}

