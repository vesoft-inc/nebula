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
#include "storage/mutate/UpdateVertexProcessor.h"
#include "dataman/RowSetReader.h"
#include "dataman/RowReader.h"

namespace nebula {
namespace storage {

void mockData(kvstore::KVStore* kv) {
    LOG(INFO) << "Prepare data...";
    std::vector<kvstore::KV> data;
    for (auto partId = 0; partId < 3; partId++) {
        for (auto vertexId = partId * 10; vertexId < (partId + 1) * 10; vertexId++) {
            // NOTE: the range of tagId is [3001, 3008], excluding 3009(for insert test).
            for (auto tagId = 3001; tagId < 3010 - 1; tagId++) {
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


TEST(UpdateVertexTest, Set_Filter_Yield_Test) {
    fs::TempDir rootPath("/tmp/UpdateVertexTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());

    LOG(INFO) << "Prepare meta...";
    auto schemaMan = TestUtils::mockSchemaMan();
    mockData(kv.get());

    LOG(INFO) << "Build UpdateVertexRequest...";
    GraphSpaceID spaceId = 0;
    PartitionID partId = 0;
    VertexID vertexId = 1;
    cpp2::UpdateVertexRequest req;
    req.set_space_id(spaceId);
    req.set_vertex_id(vertexId);
    req.set_part_id(partId);
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
                    EXPECT_EQ(1L, boost::get<int64_t>(v0));
                    break;
                }
                case 1: {
                    auto&& v1 = value(std::move(res));
                    EXPECT_EQ(0 + 3003 + 2 + 2, boost::get<int64_t>(v1));
                    break;
                }
                case 2: {
                    auto&& v2 = value(std::move(res));
                    EXPECT_STREQ("tag_string_col_4_2_new", boost::get<std::string>(v2).c_str());
                    break;
                }
                default:
                    break;
            }
        }
    }
    // get tag3001, tag3003 and tag3005 from kvstore directly
    std::vector<std::string> keys;
    std::vector<std::string> values;
    auto lastVersion = std::numeric_limits<int32_t>::max() - 2;
    for (int i = 0; i < 3; i++) {
        // tagId = 3001 + i * 2
        auto vertexKey = NebulaKeyUtils::vertexKey(partId, vertexId, 3001 + i * 2, lastVersion);
        keys.emplace_back(vertexKey);
    }
    kvstore::ResultCode code = kv->multiGet(spaceId, partId, std::move(keys), &values);
    CHECK_EQ(code, kvstore::ResultCode::SUCCEEDED);
    EXPECT_EQ(3, values.size());
    for (int i = 0; i < 3; i++) {
        auto tagSchema = schemaMan->getTagSchema(spaceId, 3001 + i * 2);
        auto tagReader = RowReader::getRowReader(values[i], tagSchema);
        auto res = RowReader::getPropByName(tagReader.get(),
                                            folly::stringPrintf("tag_%d_col_%d", 3001 + i*2, i*2));
        CHECK(ok(res));
        switch (i) {
            case 0: {
                auto&& v0 = value(std::move(res));
                EXPECT_EQ(1L, boost::get<int64_t>(v0));
                break;
            }
            case 1: {
                auto&& v1 = value(std::move(res));
                EXPECT_EQ(0 + 3003 + 2 + 2, boost::get<int64_t>(v1));
                break;
            }
            case 2: {
                auto&& v2 = value(std::move(res));
                EXPECT_STREQ("tag_string_col_4_2_new", boost::get<std::string>(v2).c_str());
                break;
            }
            default:
                break;
        }
    }
}


TEST(UpdateVertexTest, Insertable_Test) {
    fs::TempDir rootPath("/tmp/UpdateVertexTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());

    LOG(INFO) << "Prepare meta...";
    auto schemaMan = TestUtils::mockSchemaMan();
    mockData(kv.get());

    LOG(INFO) << "Build UpdateVertexRequest...";
    GraphSpaceID spaceId = 0;
    PartitionID partId = 0;
    VertexID vertexId = 1;
    cpp2::UpdateVertexRequest req;
    req.set_space_id(spaceId);
    req.set_vertex_id(vertexId);
    req.set_part_id(partId);
    req.set_filter("");
    LOG(INFO) << "Build update items...";
    std::vector<cpp2::UpdateItem> items;
    // int: 3001.tag_3001_col_0 = 1
    cpp2::UpdateItem item1;
    item1.set_name("3001");
    item1.set_prop("tag_3001_col_0");
    PrimaryExpression val1(1L);
    item1.set_value(Expression::encode(&val1));
    items.emplace_back(item1);
    // string: 3009.tag_3009_col_4 = tag_string_col_4_2_new
    cpp2::UpdateItem item2;
    item2.set_name("3009");
    item2.set_prop("tag_3009_col_4");
    std::string col4new("tag_string_col_4_2_new");
    PrimaryExpression val2(col4new);
    item2.set_value(Expression::encode(&val2));
    items.emplace_back(item2);
    req.set_update_items(std::move(items));
    LOG(INFO) << "Build yield...";
    // Return tag props: tag_3001_col_0, tag_3003_col_2, tag_3005_col_4
    decltype(req.return_columns) tmpColumns;
    for (int i = 0; i < 3; i++) {
        SourcePropertyExpression sourcePropExp(
            new std::string(folly::to<std::string>(3001 + i * 2)),
            new std::string(folly::stringPrintf("tag_%d_col_%d", 3001 + i * 2, i * 2)));
        tmpColumns.emplace_back(Expression::encode(&sourcePropExp));
    }
    // tag_3009_col_0 ~ tag_3009_col_5
    for (int i = 0; i < 6; i++) {
        SourcePropertyExpression sourcePropExp(
            new std::string(folly::to<std::string>(3009)),
            new std::string(folly::stringPrintf("tag_3009_col_%d", i)));
        tmpColumns.emplace_back(Expression::encode(&sourcePropExp));
    }
    req.set_return_columns(std::move(tmpColumns));
    LOG(INFO) << "Make it insertable...";
    // insert tag: 3009.tag_3009_col_4
    req.set_insertable(true);

    LOG(INFO) << "Test UpdateVertexRequest...";
    auto* processor = UpdateVertexProcessor::instance(kv.get(), schemaMan.get(), nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(0, resp.result.failed_codes.size());
    EXPECT_TRUE(resp.get_upsert());
    ASSERT_TRUE(resp.__isset.schema);
    auto provider = std::make_shared<ResultSchemaProvider>(resp.schema);
    auto reader = RowReader::getRowReader(resp.data, provider);
    EXPECT_EQ(3 + 6, reader->numFields());
    // 3001.tag_3001_col_0
    auto res = RowReader::getPropByIndex(reader.get(), 0);
    if (ok(res)) {
        auto&& v = value(std::move(res));
        EXPECT_EQ(1L, boost::get<int64_t>(v));
    }
    // 3003.tag_3003_col_2
    res = RowReader::getPropByIndex(reader.get(), 1);
    if (ok(res)) {
        auto&& v = value(std::move(res));
        EXPECT_EQ(0 + 3003 + 2 + 2, boost::get<int64_t>(v));
    }
    // 3009.tag_3009_col_4
    res = RowReader::getPropByIndex(reader.get(), 7);
    if (ok(res)) {
        auto&& v = value(std::move(res));
        EXPECT_STREQ("tag_string_col_4_2_new", boost::get<std::string>(v).c_str());
    }
    // get tag3009 from kvstore directly
    auto prefix = NebulaKeyUtils::vertexPrefix(partId, vertexId, 3009);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kv->prefix(spaceId, partId, prefix, &iter);
    EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());
    reader = RowReader::getTagPropReader(schemaMan.get(), iter->val(), spaceId, 3009);
    res = RowReader::getPropByName(reader.get(), item2.get_prop());
    EXPECT_TRUE(ok(res));
    auto&& v = value(std::move(res));
    EXPECT_STREQ("tag_string_col_4_2_new", boost::get<std::string>(v).c_str());
}


TEST(UpdateVertexTest, Invalid_Set_Test) {
    fs::TempDir rootPath("/tmp/UpdateVertexTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());

    LOG(INFO) << "Prepare meta...";
    auto schemaMan = TestUtils::mockSchemaMan();
    mockData(kv.get());

    LOG(INFO) << "Build UpdateVertexRequest...";
    GraphSpaceID spaceId = 0;
    PartitionID partId = 0;
    VertexID vertexId = 1;
    cpp2::UpdateVertexRequest req;
    req.set_space_id(spaceId);
    req.set_vertex_id(vertexId);
    req.set_part_id(partId);
    req.set_filter("");
    LOG(INFO) << "Build invalid update items...";
    std::vector<cpp2::UpdateItem> items;
    // 3001.tag_3001_col_0 = e101.col_0
    cpp2::UpdateItem item;
    item.set_name("3001");
    item.set_prop("tag_3001_col_0");
    auto* alias = new std::string("e101");
    auto* edgeProp = new std::string("col_0");
    AliasPropertyExpression edgeExp(new std::string(""), alias, edgeProp);
    item.set_value(Expression::encode(&edgeExp));
    items.emplace_back(item);
    req.set_update_items(std::move(items));
    req.set_insertable(false);

    LOG(INFO) << "Test UpdateVertexRequest...";
    auto* processor = UpdateVertexProcessor::instance(kv.get(), schemaMan.get(), nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(1, resp.result.failed_codes.size());
    EXPECT_TRUE(nebula::storage::cpp2::ErrorCode::E_INVALID_UPDATER
                    == resp.result.failed_codes[0].code);
    EXPECT_FALSE(nebula::storage::cpp2::ErrorCode::E_INVALID_FILTER
                    == resp.result.failed_codes[0].code);
}


TEST(UpdateVertexTest, Invalid_Filter_Test) {
    fs::TempDir rootPath("/tmp/UpdateVertexTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());

    LOG(INFO) << "Prepare meta...";
    auto schemaMan = TestUtils::mockSchemaMan();
    mockData(kv.get());

    LOG(INFO) << "Build UpdateVertexRequest...";
    GraphSpaceID spaceId = 0;
    PartitionID partId = 0;
    VertexID vertexId = 1;
    cpp2::UpdateVertexRequest req;
    req.set_space_id(spaceId);
    req.set_vertex_id(vertexId);
    req.set_part_id(partId);
    req.set_filter("");
    LOG(INFO) << "Build invalid filter...";
    auto* prop = new std::string("tag_3001_col_0");
    auto inputExp = std::make_unique<InputPropertyExpression>(prop);
    req.set_filter(Expression::encode(inputExp.get()));
    LOG(INFO) << "Build update items...";
    std::vector<cpp2::UpdateItem> items;
    // int: 3001.tag_3001_col_0 = 1L
    cpp2::UpdateItem item;
    item.set_name("3001");
    item.set_prop("tag_3001_col_0");
    PrimaryExpression val(1L);
    item.set_value(Expression::encode(&val));
    items.emplace_back(item);
    req.set_update_items(std::move(items));
    req.set_insertable(false);

    LOG(INFO) << "Test UpdateVertexRequest...";
    auto* processor = UpdateVertexProcessor::instance(kv.get(), schemaMan.get(), nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(1, resp.result.failed_codes.size());
    EXPECT_TRUE(nebula::storage::cpp2::ErrorCode::E_INVALID_FILTER
                    == resp.result.failed_codes[0].code);
    EXPECT_FALSE(nebula::storage::cpp2::ErrorCode::E_INVALID_UPDATER
                    == resp.result.failed_codes[0].code);
}

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}

