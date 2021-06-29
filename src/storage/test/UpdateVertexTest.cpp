/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "storage/mutate/UpdateVertexProcessor.h"
#include "utils/NebulaKeyUtils.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "storage/test/TestUtils.h"
#include "codec/RowReader.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "common/expression/ConstantExpression.h"

DECLARE_bool(mock_ttl_col);
DECLARE_int32(mock_ttl_duration);

namespace nebula {
namespace storage {

ObjectPool objPool;
auto pool = &objPool;
static bool encode(const meta::NebulaSchemaProvider* schema,
                   const std::string& key,
                   const std::vector<Value>& props,
                   std::vector<kvstore::KV>& data) {
    RowWriterV2 writer(schema);
    for (size_t i = 0; i < props.size(); i++) {
        auto r = writer.setValue(i, props[i]);
        if (r != WriteResult::SUCCEEDED) {
            LOG(ERROR) << "Invalid prop " << i;
            return false;
        }
    }
    auto ret = writer.finish();
    if (ret != WriteResult::SUCCEEDED) {
        LOG(ERROR) << "Failed to write data";
        return false;
    }
    auto encode = std::move(writer).moveEncodedStr();
    data.emplace_back(std::move(key), std::move(encode));
    return true;
}

static bool mockVertexData(storage::StorageEnv* env, int32_t totalParts, int32_t spaceVidLen) {
    GraphSpaceID spaceId = 1;
    auto verticesPart = mock::MockData::mockVerticesofPart(totalParts);

    folly::Baton<true, std::atomic> baton;
    std::atomic<size_t> count(verticesPart.size());

    for (const auto& part : verticesPart) {
        std::vector<kvstore::KV> data;
        data.clear();
        for (const auto& vertex : part.second) {
            TagID tagId = vertex.tId_;
            auto key = NebulaKeyUtils::vertexKey(spaceVidLen, part.first, vertex.vId_, tagId);
            auto schema = env->schemaMan_->getTagSchema(spaceId, tagId);
            if (!schema) {
                LOG(ERROR) << "Invalid tagId " << tagId;
                return false;
            }

            auto ret = encode(schema.get(), key, vertex.props_, data);
            if (!ret) {
                LOG(ERROR) << "Write field failed";
                return false;
            }
        }

        env->kvstore_->asyncMultiPut(
            spaceId, part.first, std::move(data), [&](nebula::cpp2::ErrorCode code) {
                ASSERT_EQ(code, nebula::cpp2::ErrorCode::SUCCEEDED);
                count.fetch_sub(1);
                if (count.load() == 0) {
                    baton.post();
                }
            });
    }
    baton.wait();
    return true;
}

void addTagPropInKey(std::vector<std::string>& returnCols) {
    {
        const auto& exp = *SourcePropertyExpression::make(pool, "1", kVid);
        returnCols.emplace_back(Expression::encode(exp));
    }
    {
        const auto& exp = *SourcePropertyExpression::make(pool, "1", kTag);
        returnCols.emplace_back(Expression::encode(exp));
    }
}

// not filter, update success
TEST(UpdateVertexTest, No_Filter_Test) {
    fs::TempDir rootPath("/tmp/UpdateVertexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    TagID tagId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();

    EXPECT_TRUE(mockVertexData(env, parts, spaceVidLen));

    LOG(INFO) << "Build UpdateVertexRequest...";
    cpp2::UpdateVertexRequest req;

    req.set_space_id(spaceId);
    auto partId = std::hash<std::string>()("Tim Duncan") % parts + 1;
    VertexID vertexId("Tim Duncan");
    req.set_part_id(partId);
    req.set_vertex_id(vertexId);
    req.set_tag_id(tagId);

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedProp> updatedProps;
    // int: player.age = 45
    cpp2::UpdatedProp uProp1;
    uProp1.set_name("age");
    const auto& val1 = *ConstantExpression::make(pool, 45L);
    uProp1.set_value(Expression::encode(val1));
    updatedProps.emplace_back(uProp1);

    // string: player.country= China
    cpp2::UpdatedProp uProp2;
    uProp2.set_name("country");
    std::string col4new("China");
    const auto& val2 = *ConstantExpression::make(pool, col4new);
    uProp2.set_value(Expression::encode(val2));
    updatedProps.emplace_back(uProp2);
    req.set_updated_props(std::move(updatedProps));

    LOG(INFO) << "Build yield...";
    // Return player props: name, age, country
    std::vector<std::string> tmpProps;
    const auto& sourcePropExp1 = *SourcePropertyExpression::make(pool, "1", "name");
    tmpProps.emplace_back(Expression::encode(sourcePropExp1));

    const auto& sourcePropExp2 = *SourcePropertyExpression::make(pool, "1", "age");
    tmpProps.emplace_back(Expression::encode(sourcePropExp2));

    const auto& sourcePropExp3 = *SourcePropertyExpression::make(pool, "1", "country");
    tmpProps.emplace_back(Expression::encode(sourcePropExp3));

    addTagPropInKey(tmpProps);

    req.set_return_props(std::move(tmpProps));
    req.set_insertable(false);

    LOG(INFO) << "Test UpdateVertexRequest...";
    auto* processor = UpdateVertexProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(0, (*resp.result_ref()).failed_parts.size());
    EXPECT_EQ(6, (*resp.props_ref()).colNames.size());
    EXPECT_EQ("_inserted", (*resp.props_ref()).colNames[0]);
    EXPECT_EQ("1.name", (*resp.props_ref()).colNames[1]);
    EXPECT_EQ("1.age", (*resp.props_ref()).colNames[2]);
    EXPECT_EQ("1.country", (*resp.props_ref()).colNames[3]);
    EXPECT_EQ(std::string("1.").append(kVid), (*resp.props_ref()).colNames[4]);
    EXPECT_EQ(std::string("1.").append(kTag), (*resp.props_ref()).colNames[5]);

    EXPECT_EQ(1, (*resp.props_ref()).rows.size());
    EXPECT_EQ(6, (*resp.props_ref()).rows[0].values.size());

    EXPECT_EQ(false, (*resp.props_ref()).rows[0].values[0].getBool());
    EXPECT_EQ("Tim Duncan", (*resp.props_ref()).rows[0].values[1].getStr());
    EXPECT_EQ(45, (*resp.props_ref()).rows[0].values[2].getInt());
    EXPECT_EQ("China", (*resp.props_ref()).rows[0].values[3].getStr());
    EXPECT_EQ("Tim Duncan", (*resp.props_ref()).rows[0].values[4].getStr());
    EXPECT_EQ(1, (*resp.props_ref()).rows[0].values[5].getInt());

    // get player from kvstore directly
    auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen, partId, vertexId, tagId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());

    auto reader = RowReaderWrapper::getTagPropReader(env->schemaMan_, spaceId, tagId, iter->val());
    auto val = reader->getValueByName("name");
    EXPECT_EQ("Tim Duncan", val.getStr());
    val = reader->getValueByName("age");
    EXPECT_EQ(45, val.getInt());
    val = reader->getValueByName("country");
    EXPECT_EQ("China", val.getStr());
}

// filter, update failed
TEST(UpdateVertexTest, Filter_Yield_Test2) {
    fs::TempDir rootPath("/tmp/UpdateVertexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    TagID tagId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();
    EXPECT_TRUE(mockVertexData(env, parts, spaceVidLen));

    LOG(INFO) << "Build UpdateVertexRequest...";
    cpp2::UpdateVertexRequest req;

    req.set_space_id(spaceId);
    auto partId = std::hash<std::string>()("Tim Duncan") % parts + 1;
    VertexID vertexId("Tim Duncan");
    req.set_part_id(partId);
    req.set_vertex_id(vertexId);
    req.set_tag_id(tagId);

    LOG(INFO) << "Build filter...";
    // left int:  1.startYear = 1997
    auto* srcExp1 = SourcePropertyExpression::make(pool, "1", "startYear");
    auto* priExp1 = ConstantExpression::make(pool, 1997L);
    auto* left = RelationalExpression::makeEQ(pool, srcExp1, priExp1);

    // right int: 1.endYear = 2017
    auto* srcExp2 = SourcePropertyExpression::make(pool, "1", "endYear");
    auto* priExp2 = ConstantExpression::make(pool, 2017L);
    auto* right = RelationalExpression::makeEQ(pool, srcExp2, priExp2);
    // left AND right is ture
    auto logExp = LogicalExpression::makeAnd(pool, left, right);
    req.set_condition(Expression::encode(*logExp));

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedProp> updatedProps;
    // int: player.age = 46
    cpp2::UpdatedProp uProp1;
    uProp1.set_name("age");
    const auto& val1 = *ConstantExpression::make(pool, 46L);
    uProp1.set_value(Expression::encode(val1));
    updatedProps.emplace_back(uProp1);
    // string: player.country= China
    cpp2::UpdatedProp uProp2;
    uProp2.set_name("country");
    std::string col4new("China");
    const auto& val2 = *ConstantExpression::make(pool, col4new);
    uProp2.set_value(Expression::encode(val2));
    updatedProps.emplace_back(uProp2);
    req.set_updated_props(std::move(updatedProps));

    LOG(INFO) << "Build yield...";
    {
        // Return player props: name, age, country
        std::vector<std::string> tmpProps;
        const auto& sourcePropExp1 = *SourcePropertyExpression::make(pool, "1", "name");
        tmpProps.emplace_back(Expression::encode(sourcePropExp1));

        const auto& sourcePropExp2 = *SourcePropertyExpression::make(pool, "1", "age");
        tmpProps.emplace_back(Expression::encode(sourcePropExp2));

        const auto& sourcePropExp3 = *SourcePropertyExpression::make(pool, "1", "country");
        tmpProps.emplace_back(Expression::encode(sourcePropExp3));

        addTagPropInKey(tmpProps);

        req.set_return_props(std::move(tmpProps));
        req.set_insertable(false);
    }

    LOG(INFO) << "Test UpdateVertexRequest...";
    auto* processor = UpdateVertexProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(1, (*resp.result_ref()).failed_parts.size());

    // Note: If filtered out, the result is old
    EXPECT_EQ(6, (*resp.props_ref()).colNames.size());
    EXPECT_EQ("_inserted", (*resp.props_ref()).colNames[0]);
    EXPECT_EQ("1.name", (*resp.props_ref()).colNames[1]);
    EXPECT_EQ("1.age", (*resp.props_ref()).colNames[2]);
    EXPECT_EQ("1.country", (*resp.props_ref()).colNames[3]);
    EXPECT_EQ(std::string("1.").append(kVid), (*resp.props_ref()).colNames[4]);
    EXPECT_EQ(std::string("1.").append(kTag), (*resp.props_ref()).colNames[5]);

    EXPECT_EQ(1, (*resp.props_ref()).rows.size());
    EXPECT_EQ(6, (*resp.props_ref()).rows[0].values.size());
    EXPECT_EQ(false, (*resp.props_ref()).rows[0].values[0].getBool());
    EXPECT_EQ("Tim Duncan", (*resp.props_ref()).rows[0].values[1].getStr());
    EXPECT_EQ(44, (*resp.props_ref()).rows[0].values[2].getInt());
    EXPECT_EQ("America", (*resp.props_ref()).rows[0].values[3].getStr());
    EXPECT_EQ("Tim Duncan", (*resp.props_ref()).rows[0].values[4].getStr());
    EXPECT_EQ(1, (*resp.props_ref()).rows[0].values[5].getInt());

    // get player from kvstore directly
    // Because no update, the value is old
    auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen, partId, vertexId, tagId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());

    auto reader = RowReaderWrapper::getTagPropReader(env->schemaMan_, spaceId, tagId, iter->val());
    auto val = reader->getValueByName("name");
    EXPECT_EQ("Tim Duncan", val.getStr());
    val = reader->getValueByName("age");
    EXPECT_EQ(44, val.getInt());
    val = reader->getValueByName("country");
    EXPECT_EQ("America", val.getStr());
}

// upsert, insert success
TEST(UpdateVertexTest, Insertable_Test) {
    fs::TempDir rootPath("/tmp/UpdateVertexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    TagID tagId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();
    EXPECT_TRUE(mockVertexData(env, parts, spaceVidLen));

    LOG(INFO) << "Build UpdateVertexRequest...";
    cpp2::UpdateVertexRequest req;

    req.set_space_id(spaceId);
    auto partId = std::hash<std::string>()("Brandon Ingram") % parts + 1;
    VertexID vertexId("Brandon Ingram");
    req.set_part_id(partId);
    req.set_vertex_id(vertexId);
    req.set_tag_id(tagId);

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedProp> updatedProps;
    // string: player.name= "Brandon Ingram"
    cpp2::UpdatedProp uProp1;
    uProp1.set_name("name");
    std::string colnew("Brandon Ingram");
    const auto& val1 = *ConstantExpression::make(pool, colnew);
    uProp1.set_value(Expression::encode(val1));
    updatedProps.emplace_back(uProp1);

    // int: player.age = 20
    cpp2::UpdatedProp uProp2;
    uProp2.set_name("age");
    const auto& val2 = *ConstantExpression::make(pool, 20L);
    uProp2.set_value(Expression::encode(val2));
    updatedProps.emplace_back(uProp2);
    req.set_updated_props(std::move(updatedProps));

    LOG(INFO) << "Build yield...";
    // Return player props: name, age, country
    {
        std::vector<std::string> tmpProps;
        const auto& sourcePropExp1 = *SourcePropertyExpression::make(pool, "1", "name");
        tmpProps.emplace_back(Expression::encode(sourcePropExp1));

        const auto& sourcePropExp2 = *SourcePropertyExpression::make(pool, "1", "age");
        tmpProps.emplace_back(Expression::encode(sourcePropExp2));

        const auto& sourcePropExp3 = *SourcePropertyExpression::make(pool, "1", "country");
        tmpProps.emplace_back(Expression::encode(sourcePropExp3));

        addTagPropInKey(tmpProps);

        req.set_return_props(std::move(tmpProps));
        req.set_insertable(true);
    }

    LOG(INFO) << "Test UpdateVertexRequest...";
    auto* processor = UpdateVertexProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(0, (*resp.result_ref()).failed_parts.size());
    EXPECT_EQ(6, (*resp.props_ref()).colNames.size());
    EXPECT_EQ("_inserted", (*resp.props_ref()).colNames[0]);
    EXPECT_EQ("1.name", (*resp.props_ref()).colNames[1]);
    EXPECT_EQ("1.age", (*resp.props_ref()).colNames[2]);
    EXPECT_EQ("1.country", (*resp.props_ref()).colNames[3]);
    EXPECT_EQ(std::string("1.").append(kVid), (*resp.props_ref()).colNames[4]);
    EXPECT_EQ(std::string("1.").append(kTag), (*resp.props_ref()).colNames[5]);

    EXPECT_EQ(1, (*resp.props_ref()).rows.size());
    EXPECT_EQ(6, (*resp.props_ref()).rows[0].values.size());
    EXPECT_EQ(true, (*resp.props_ref()).rows[0].values[0].getBool());
    EXPECT_EQ("Brandon Ingram", (*resp.props_ref()).rows[0].values[1].getStr());
    EXPECT_EQ(20, (*resp.props_ref()).rows[0].values[2].getInt());
    EXPECT_EQ("America", (*resp.props_ref()).rows[0].values[3].getStr());
    EXPECT_EQ("Brandon Ingram", (*resp.props_ref()).rows[0].values[4].getStr());
    EXPECT_EQ(1, (*resp.props_ref()).rows[0].values[5].getInt());

    // get player from kvstore directly
    auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen, partId, vertexId, tagId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());

    auto reader = RowReaderWrapper::getTagPropReader(env->schemaMan_, spaceId, tagId, iter->val());
    auto val = reader->getValueByName("name");
    EXPECT_EQ("Brandon Ingram", val.getStr());
    val = reader->getValueByName("age");
    EXPECT_EQ(20, val.getInt());
    val = reader->getValueByName("country");
    EXPECT_EQ("America", val.getStr());
}

// invalid update prop, update failed
TEST(UpdateVertexTest, Invalid_Update_Prop_Test) {
    fs::TempDir rootPath("/tmp/UpdateVertexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    TagID tagId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();
    EXPECT_TRUE(mockVertexData(env, parts, spaceVidLen));

    LOG(INFO) << "Build UpdateVertexRequest...";
    cpp2::UpdateVertexRequest req;

    req.set_space_id(spaceId);
    auto partId = std::hash<std::string>()("Tim Duncan") % parts + 1;
    VertexID vertexId("Tim Duncan");
    req.set_part_id(partId);
    req.set_vertex_id(vertexId);
    req.set_tag_id(tagId);

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedProp> updatedProps;
    // int: player.age = 46
    cpp2::UpdatedProp uProp1;
    uProp1.set_name("age");
    const auto& uVal1 = *ConstantExpression::make(pool, 46L);
    uProp1.set_value(Expression::encode(uVal1));
    updatedProps.emplace_back(uProp1);
    // int: player.birth = 1997 invalid
    cpp2::UpdatedProp uProp2;
    uProp2.set_name("birth");
    const auto& uVal2 = *ConstantExpression::make(pool, 1997L);
    uProp2.set_value(Expression::encode(uVal2));
    updatedProps.emplace_back(uProp2);
    req.set_updated_props(std::move(updatedProps));

    LOG(INFO) << "Build yield...";
    // Return player props: name, age
    {
        std::vector<std::string> tmpProps;
        const auto& sourcePropExp1 = *SourcePropertyExpression::make(pool, "1", "name");
        tmpProps.emplace_back(Expression::encode(sourcePropExp1));

        const auto& sourcePropExp2 = *SourcePropertyExpression::make(pool, "1", "age");
        tmpProps.emplace_back(Expression::encode(sourcePropExp2));

        req.set_return_props(std::move(tmpProps));
    }
    req.set_insertable(false);

    LOG(INFO) << "Test UpdateVertexRequest...";
    auto* processor = UpdateVertexProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(1, (*resp.result_ref()).failed_parts.size());

    // get player from kvstore directly
    // Because no update, the value is old
    auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen, partId, vertexId, tagId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());

    auto reader = RowReaderWrapper::getTagPropReader(env->schemaMan_, spaceId, tagId, iter->val());
    auto val = reader->getValueByName("name");
    EXPECT_EQ("Tim Duncan", val.getStr());
    val = reader->getValueByName("age");
    EXPECT_EQ(44, val.getInt());
}

// invalid filter, update failed
TEST(UpdateVertexTest, Invalid_Filter_Test) {
    fs::TempDir rootPath("/tmp/UpdateVertexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    TagID tagId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();

    EXPECT_TRUE(mockVertexData(env, parts, spaceVidLen));

    LOG(INFO) << "Build UpdateVertexRequest...";
    cpp2::UpdateVertexRequest req;

    req.set_space_id(spaceId);
    auto partId = std::hash<std::string>()("Tim Duncan") % parts + 1;
    VertexID vertexId("Tim Duncan");
    req.set_part_id(partId);
    req.set_vertex_id(vertexId);
    req.set_tag_id(tagId);

    LOG(INFO) << "Build filter...";
    // left int:  1.startYear = 1997
    auto* srcExp1 = SourcePropertyExpression::make(pool, "1", "startYear");
    auto* priExp1 = ConstantExpression::make(pool, 1997L);
    auto* left = RelationalExpression::makeEQ(pool, srcExp1, priExp1);

    // invalid prop
    // right int: 1.birth = 1990
    auto* srcExp2 = SourcePropertyExpression::make(pool, "1", "birth");
    auto* priExp2 = ConstantExpression::make(pool, 1990L);
    auto* right = RelationalExpression::makeEQ(pool, srcExp2, priExp2);
    // left AND right is ture
    auto logExp = LogicalExpression::makeAnd(pool, left, right);
    req.set_condition(Expression::encode(*logExp));

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedProp> updatedProps;
    // int: player.age = 46
    cpp2::UpdatedProp uProp1;
    uProp1.set_name("age");
    const auto& uVal1 = *ConstantExpression::make(pool, 46L);
    uProp1.set_value(Expression::encode(uVal1));
    updatedProps.emplace_back(uProp1);

    // string: player.country= America
    cpp2::UpdatedProp uProp2;
    uProp2.set_name("country");
    std::string colnew("China");
    const auto& uVal2 = *ConstantExpression::make(pool, colnew);
    uProp2.set_value(Expression::encode(uVal2));
    updatedProps.emplace_back(uProp2);
    req.set_updated_props(std::move(updatedProps));

    LOG(INFO) << "Build yield...";
    // Return player props: name, age, country
    {
        std::vector<std::string> tmpProps;
        const auto& sourcePropExp1 = *SourcePropertyExpression::make(pool, "1", "name");
        tmpProps.emplace_back(Expression::encode(sourcePropExp1));

        const auto& sourcePropExp2 = *SourcePropertyExpression::make(pool, "1", "age");
        tmpProps.emplace_back(Expression::encode(sourcePropExp2));

        const auto& sourcePropExp3 = *SourcePropertyExpression::make(pool, "1", "country");
        tmpProps.emplace_back(Expression::encode(sourcePropExp3));

        req.set_return_props(std::move(tmpProps));
    }
    req.set_insertable(false);

    LOG(INFO) << "Test UpdateVertexRequest...";
    auto* processor = UpdateVertexProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(1, (*resp.result_ref()).failed_parts.size());

    // get player from kvstore directly
    // Because no update, the value is old
    auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen, partId, vertexId, tagId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());

    auto reader = RowReaderWrapper::getTagPropReader(env->schemaMan_, spaceId, tagId, iter->val());
    auto val = reader->getValueByName("name");
    EXPECT_EQ("Tim Duncan", val.getStr());
    val = reader->getValueByName("age");
    EXPECT_EQ(44, val.getInt());
    val = reader->getValueByName("country");
    EXPECT_EQ("America", val.getStr());
}

// upsert, insert success when filter is valid, but value is false
TEST(UpdateVertexTest, Insertable_Filter_Value_Test) {
    fs::TempDir rootPath("/tmp/UpdateVertexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    TagID tagId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();
    EXPECT_TRUE(mockVertexData(env, parts, spaceVidLen));

    LOG(INFO) << "Build UpdateVertexRequest...";
    cpp2::UpdateVertexRequest req;

    req.set_space_id(spaceId);
    auto partId = std::hash<std::string>()("Brandon Ingram") % parts + 1;
    VertexID vertexId("Brandon Ingram");
    req.set_part_id(partId);
    req.set_vertex_id(vertexId);
    req.set_tag_id(tagId);

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedProp> updatedProps;
    // string: player.name= "Brandon Ingram"
    cpp2::UpdatedProp uProp1;
    uProp1.set_name("name");
    std::string colnew("Brandon Ingram");
    const auto& val1 = *ConstantExpression::make(pool, colnew);
    uProp1.set_value(Expression::encode(val1));
    updatedProps.emplace_back(uProp1);

    // int: player.age = 20
    cpp2::UpdatedProp uProp2;
    uProp2.set_name("age");
    const auto& val2 = *ConstantExpression::make(pool, 20L);
    uProp2.set_value(Expression::encode(val2));
    updatedProps.emplace_back(uProp2);
    req.set_updated_props(std::move(updatedProps));

    LOG(INFO) << "Build filter...";
    // filter is valid, but filter value is false
    // left int:  1.startYear = 2020
    auto* srcExp1 = SourcePropertyExpression::make(pool, "1", "startYear");
    auto* priExp1 = ConstantExpression::make(pool, 2020L);
    auto* left = RelationalExpression::makeEQ(pool, srcExp1, priExp1);

    // right int: 1.age = 30
    auto* srcExp2 = SourcePropertyExpression::make(pool, "1", "age");
    auto* priExp2 = ConstantExpression::make(pool, 30L);
    auto* right = RelationalExpression::makeEQ(pool, srcExp2, priExp2);
    // left AND right is false
    auto logExp = LogicalExpression::makeAnd(pool, left, right);
    req.set_condition(Expression::encode(*logExp));

    LOG(INFO) << "Build yield...";
    // Return player props: name, age, country
    {
        std::vector<std::string> tmpProps;
        const auto& sourcePropExp1 = *SourcePropertyExpression::make(pool, "1", "name");
        tmpProps.emplace_back(Expression::encode(sourcePropExp1));

        const auto& sourcePropExp2 = *SourcePropertyExpression::make(pool, "1", "age");
        tmpProps.emplace_back(Expression::encode(sourcePropExp2));

        const auto& sourcePropExp3 = *SourcePropertyExpression::make(pool, "1", "country");
        tmpProps.emplace_back(Expression::encode(sourcePropExp3));

        addTagPropInKey(tmpProps);

        req.set_return_props(std::move(tmpProps));
        req.set_insertable(true);
    }

    LOG(INFO) << "Test UpdateVertexRequest...";
    auto* processor = UpdateVertexProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(0, (*resp.result_ref()).failed_parts.size());
    EXPECT_EQ(6, (*resp.props_ref()).colNames.size());
    EXPECT_EQ("_inserted", (*resp.props_ref()).colNames[0]);
    EXPECT_EQ("1.name", (*resp.props_ref()).colNames[1]);
    EXPECT_EQ("1.age", (*resp.props_ref()).colNames[2]);
    EXPECT_EQ("1.country", (*resp.props_ref()).colNames[3]);
    EXPECT_EQ(std::string("1.").append(kVid), (*resp.props_ref()).colNames[4]);
    EXPECT_EQ(std::string("1.").append(kTag), (*resp.props_ref()).colNames[5]);

    EXPECT_EQ(1, (*resp.props_ref()).rows.size());
    EXPECT_EQ(true, (*resp.props_ref()).rows[0].values[0].getBool());
    EXPECT_EQ("Brandon Ingram", (*resp.props_ref()).rows[0].values[1].getStr());
    EXPECT_EQ(20, (*resp.props_ref()).rows[0].values[2].getInt());
    EXPECT_EQ("America", (*resp.props_ref()).rows[0].values[3].getStr());
    EXPECT_EQ("Brandon Ingram", (*resp.props_ref()).rows[0].values[4].getStr());
    EXPECT_EQ(1, (*resp.props_ref()).rows[0].values[5].getInt());

    // get player from kvstore directly
    auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen, partId, vertexId, tagId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());

    auto reader = RowReaderWrapper::getTagPropReader(env->schemaMan_, spaceId, tagId, iter->val());
    auto val = reader->getValueByName("name");
    EXPECT_EQ("Brandon Ingram", val.getStr());
    val = reader->getValueByName("age");
    EXPECT_EQ(20, val.getInt());
    val = reader->getValueByName("country");
    EXPECT_EQ("America", val.getStr());
}

// empty value test
TEST(UpdateVertexTest, CorruptDataTest) {
    fs::TempDir rootPath("/tmp/UpdateVertexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    TagID tagId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();

    LOG(INFO) << "Write a vertex with empty value!";

    auto partId = std::hash<std::string>()("Lonzo Ball") % parts + 1;
    VertexID vertexId("Lonzo Ball");
    auto key = NebulaKeyUtils::vertexKey(spaceVidLen, partId, vertexId, 1);
    std::vector<kvstore::KV> data;
    data.emplace_back(std::make_pair(key, ""));
    folly::Baton<> baton;
    env->kvstore_->asyncMultiPut(
        spaceId, partId, std::move(data), [&](nebula::cpp2::ErrorCode code) {
            ASSERT_EQ(code, nebula::cpp2::ErrorCode::SUCCEEDED);
            baton.post();
        });
    baton.wait();

    LOG(INFO) << "Build UpdateVertexRequest...";
    cpp2::UpdateVertexRequest req;
    req.set_space_id(spaceId);
    req.set_part_id(partId);
    req.set_vertex_id(vertexId);
    req.set_tag_id(tagId);

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedProp> updatedProps;
    // int: player.age = 23
    cpp2::UpdatedProp uProp1;
    uProp1.set_name("age");
    const auto& uVal1 = *ConstantExpression::make(pool, 23L);
    uProp1.set_value(Expression::encode(uVal1));
    updatedProps.emplace_back(uProp1);
    req.set_updated_props(std::move(updatedProps));

    LOG(INFO) << "Build yield...";
    // Return player props: name, age, country
    std::vector<std::string> tmpProps;
    const auto& sourcePropExp1 = *SourcePropertyExpression::make(pool, "1", "country");
    tmpProps.emplace_back(Expression::encode(sourcePropExp1));

    req.set_return_props(std::move(tmpProps));
    req.set_insertable(false);

    LOG(INFO) << "Test UpdateVertexRequest...";
    auto* processor = UpdateVertexProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(1, (*resp.result_ref()).failed_parts.size());
}

// TTL test
// Data expired, Insertable is false, Empty when querying
TEST(UpdateVertexTest, TTL_NoInsert_Test) {
    FLAGS_mock_ttl_col = true;

    fs::TempDir rootPath("/tmp/UpdateVertexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    TagID tagId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();

    EXPECT_TRUE(mockVertexData(env, parts, spaceVidLen));

    LOG(INFO) << "Build UpdateVertexRequest...";
    cpp2::UpdateVertexRequest req;

    req.set_space_id(spaceId);
    auto partId = std::hash<std::string>()("Tim Duncan") % parts + 1;
    VertexID vertexId("Tim Duncan");
    req.set_part_id(partId);
    req.set_vertex_id(vertexId);
    req.set_tag_id(tagId);

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedProp> updatedProps;
    // int: player.age = 45
    cpp2::UpdatedProp uProp1;
    uProp1.set_name("age");
    const auto& uVal1 = *ConstantExpression::make(pool, 45L);
    uProp1.set_value(Expression::encode(uVal1));
    updatedProps.emplace_back(uProp1);
    // string: player.country= China
    cpp2::UpdatedProp uProp2;
    uProp2.set_name("country");
    std::string colnew("China");
    const auto& uVal2 = *ConstantExpression::make(pool, colnew);
    uProp2.set_value(Expression::encode(uVal2));
    updatedProps.emplace_back(uProp2);
    req.set_updated_props(std::move(updatedProps));

    LOG(INFO) << "Build yield...";
    // Return player props: name, age, country
    {
        std::vector<std::string> tmpProps;
        const auto& sourcePropExp1 = *SourcePropertyExpression::make(pool, "1", "name");
        tmpProps.emplace_back(Expression::encode(sourcePropExp1));

        const auto& sourcePropExp2 = *SourcePropertyExpression::make(pool, "1", "age");
        tmpProps.emplace_back(Expression::encode(sourcePropExp2));

        const auto& sourcePropExp3 = *SourcePropertyExpression::make(pool, "1", "country");
        tmpProps.emplace_back(Expression::encode(sourcePropExp3));

        req.set_return_props(std::move(tmpProps));
    }
    req.set_insertable(false);

    sleep(FLAGS_mock_ttl_duration + 1);

    LOG(INFO) << "Test UpdateVertexRequest...";
    auto* processor = UpdateVertexProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(1, (*resp.result_ref()).failed_parts.size());
}

// TTL test
// Data expired, Insertable is true, not empty when querying
TEST(UpdateVertexTest, TTL_Insert_No_Exist_Test) {
    FLAGS_mock_ttl_col = true;

    fs::TempDir rootPath("/tmp/UpdateVertexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    TagID tagId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();

    EXPECT_TRUE(mockVertexData(env, parts, spaceVidLen));

    LOG(INFO) << "Build UpdateVertexRequest...";
    cpp2::UpdateVertexRequest req;

    req.set_space_id(spaceId);
    auto partId = std::hash<std::string>()("Tim") % parts + 1;
    VertexID vertexId("Tim");
    req.set_part_id(partId);
    req.set_vertex_id(vertexId);
    req.set_tag_id(tagId);

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedProp> updatedProps;

    // string: 1.name = Tim
    cpp2::UpdatedProp uProp1;
    uProp1.set_name("name");
    std::string col1new("Tim");
    const auto& uVal1 = *ConstantExpression::make(pool, col1new);
    uProp1.set_value(Expression::encode(uVal1));
    updatedProps.emplace_back(uProp1);

    // int: player.age = 20
    cpp2::UpdatedProp uProp2;
    uProp2.set_name("age");
    const auto& uVal2 = *ConstantExpression::make(pool, 20L);
    uProp2.set_value(Expression::encode(uVal2));
    updatedProps.emplace_back(uProp2);
    req.set_updated_props(std::move(updatedProps));

    LOG(INFO) << "Build yield...";
    // Return player props: name, age, country
    {
        std::vector<std::string> tmpProps;
        const auto& sourcePropExp1 = *SourcePropertyExpression::make(pool, "1", "name");
        tmpProps.emplace_back(Expression::encode(sourcePropExp1));

        const auto& sourcePropExp2 = *SourcePropertyExpression::make(pool, "1", "age");
        tmpProps.emplace_back(Expression::encode(sourcePropExp2));

        const auto& sourcePropExp3 = *SourcePropertyExpression::make(pool, "1", "country");
        tmpProps.emplace_back(Expression::encode(sourcePropExp3));

        addTagPropInKey(tmpProps);

        req.set_return_props(std::move(tmpProps));
    }
    req.set_insertable(true);

    sleep(FLAGS_mock_ttl_duration + 1);

    LOG(INFO) << "Test UpdateVertexRequest...";
    auto* processor = UpdateVertexProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(0, (*resp.result_ref()).failed_parts.size());
    EXPECT_EQ(6, (*resp.props_ref()).colNames.size());
    EXPECT_EQ("_inserted", (*resp.props_ref()).colNames[0]);
    EXPECT_EQ("1.name", (*resp.props_ref()).colNames[1]);
    EXPECT_EQ("1.age", (*resp.props_ref()).colNames[2]);
    EXPECT_EQ("1.country", (*resp.props_ref()).colNames[3]);
    EXPECT_EQ(std::string("1.").append(kVid), (*resp.props_ref()).colNames[4]);
    EXPECT_EQ(std::string("1.").append(kTag), (*resp.props_ref()).colNames[5]);

    EXPECT_EQ(1, (*resp.props_ref()).rows.size());
    EXPECT_EQ(6, (*resp.props_ref()).rows[0].values.size());

    EXPECT_EQ(true, (*resp.props_ref()).rows[0].values[0].getBool());
    EXPECT_EQ("Tim", (*resp.props_ref()).rows[0].values[1].getStr());
    EXPECT_EQ(20, (*resp.props_ref()).rows[0].values[2].getInt());
    EXPECT_EQ("America", (*resp.props_ref()).rows[0].values[3].getStr());
    EXPECT_EQ("Tim", (*resp.props_ref()).rows[0].values[4].getStr());
    EXPECT_EQ(1, (*resp.props_ref()).rows[0].values[5].getInt());

    // get player from kvstore directly
    auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen, partId, vertexId, tagId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());

    auto reader = RowReaderWrapper::getTagPropReader(env->schemaMan_, spaceId, tagId, iter->val());
    auto val = reader->getValueByName("name");
    EXPECT_EQ("Tim", val.getStr());
    val = reader->getValueByName("age");
    EXPECT_EQ(20, val.getInt());
    val = reader->getValueByName("country");
    EXPECT_EQ("America", val.getStr());
}

// Data expired, Insertable is true, not empty when querying
TEST(UpdateVertexTest, TTL_Insert_Test) {
    FLAGS_mock_ttl_col = true;

    fs::TempDir rootPath("/tmp/UpdateVertexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    TagID tagId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();

    EXPECT_TRUE(mockVertexData(env, parts, spaceVidLen));

    LOG(INFO) << "Build UpdateVertexRequest...";
    cpp2::UpdateVertexRequest req;

    req.set_space_id(spaceId);
    auto partId = std::hash<std::string>()("Tim Duncan") % parts + 1;
    VertexID vertexId("Tim Duncan");
    req.set_part_id(partId);
    req.set_vertex_id(vertexId);
    req.set_tag_id(tagId);

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedProp> updatedProps;

    // string: 1.name = Tim Duncan
    cpp2::UpdatedProp uProp1;
    uProp1.set_name("name");
    std::string col1new("Tim Duncan");
    const auto& uVal1 = *ConstantExpression::make(pool, col1new);
    uProp1.set_value(Expression::encode(uVal1));
    updatedProps.emplace_back(uProp1);

    // int: 1.age = 50L
    cpp2::UpdatedProp uProp2;
    uProp2.set_name("age");
    const auto& uVal2 = *ConstantExpression::make(pool, 50L);
    uProp2.set_value(Expression::encode(uVal2));
    updatedProps.emplace_back(uProp2);

    // string: player.country= China
    cpp2::UpdatedProp uProp3;
    uProp3.set_name("country");
    std::string col3new("China");
    const auto& uVal3 = *ConstantExpression::make(pool, col3new);
    uProp3.set_value(Expression::encode(uVal3));
    updatedProps.emplace_back(uProp3);

    req.set_updated_props(std::move(updatedProps));

    LOG(INFO) << "Build yield...";
    // Return player props: name, age, country
    {
        std::vector<std::string> tmpProps;
        const auto& sourcePropExp1 = *SourcePropertyExpression::make(pool, "1", "name");
        tmpProps.emplace_back(Expression::encode(sourcePropExp1));

        const auto& sourcePropExp2 = *SourcePropertyExpression::make(pool, "1", "age");
        tmpProps.emplace_back(Expression::encode(sourcePropExp2));

        const auto& sourcePropExp3 = *SourcePropertyExpression::make(pool, "1", "country");
        tmpProps.emplace_back(Expression::encode(sourcePropExp3));

        addTagPropInKey(tmpProps);

        req.set_return_props(std::move(tmpProps));
    }
    req.set_insertable(true);

    sleep(FLAGS_mock_ttl_duration + 1);

    LOG(INFO) << "Test UpdateVertexRequest...";
    auto* processor = UpdateVertexProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(0, (*resp.result_ref()).failed_parts.size());
    EXPECT_EQ(6, (*resp.props_ref()).colNames.size());
    EXPECT_EQ("_inserted", (*resp.props_ref()).colNames[0]);
    EXPECT_EQ("1.name", (*resp.props_ref()).colNames[1]);
    EXPECT_EQ("1.age", (*resp.props_ref()).colNames[2]);
    EXPECT_EQ("1.country", (*resp.props_ref()).colNames[3]);
    EXPECT_EQ(std::string("1.").append(kVid), (*resp.props_ref()).colNames[4]);
    EXPECT_EQ(std::string("1.").append(kTag), (*resp.props_ref()).colNames[5]);

    EXPECT_EQ(1, (*resp.props_ref()).rows.size());
    EXPECT_EQ(6, (*resp.props_ref()).rows[0].values.size());

    EXPECT_EQ(true, (*resp.props_ref()).rows[0].values[0].getBool());
    EXPECT_EQ("Tim Duncan", (*resp.props_ref()).rows[0].values[1].getStr());
    EXPECT_EQ(50L, (*resp.props_ref()).rows[0].values[2].getInt());
    EXPECT_EQ("China", (*resp.props_ref()).rows[0].values[3].getStr());
    EXPECT_EQ("Tim Duncan", (*resp.props_ref()).rows[0].values[4].getStr());
    EXPECT_EQ(1, (*resp.props_ref()).rows[0].values[5].getInt());

    // Get player from kvstore directly, ttl expired data can be readed
    // First record is inserted record data
    // Second record is expired ttl data
    auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen, partId, vertexId, tagId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());

    auto schema = env->schemaMan_->getTagSchema(spaceId, tagId);
    EXPECT_TRUE(schema != NULL);

    int count = 0;
    while (iter && iter->valid()) {
        auto reader = RowReaderWrapper::getRowReader(schema.get(), iter->val());
        if (count == 1) {
            auto val = reader->getValueByName("name");
            EXPECT_EQ("Tim Duncan", val.getStr());

            val = reader->getValueByName("age");
            EXPECT_EQ(44L, val.getInt());

            val = reader->getValueByName("country");
            EXPECT_EQ("America", val.getStr());
        } else {
            auto val = reader->getValueByName("name");
            EXPECT_EQ("Tim Duncan", val.getStr());

            val = reader->getValueByName("age");
            EXPECT_EQ(50L, val.getInt());

            val = reader->getValueByName("country");
            EXPECT_EQ("China", val.getStr());
        }
        iter->next();
        count++;
    }
    EXPECT_EQ(1, count);
}

// upsert, insert faild
// age filed has not default value and not nullable, not in set clause
TEST(UpdateVertexTest, Insertable_No_Defalut_Test) {
    fs::TempDir rootPath("/tmp/UpdateVertexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    TagID tagId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();
    EXPECT_TRUE(mockVertexData(env, parts, spaceVidLen));

    LOG(INFO) << "Build UpdateVertexRequest...";
    cpp2::UpdateVertexRequest req;

    req.set_space_id(spaceId);
    auto partId = std::hash<std::string>()("Brandon Ingram") % parts + 1;
    VertexID vertexId("Brandon Ingram");
    req.set_part_id(partId);
    req.set_vertex_id(vertexId);
    req.set_tag_id(tagId);

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedProp> updatedProps;
    // string: player.name= "Brandon Ingram"
    cpp2::UpdatedProp uProp1;
    uProp1.set_name("name");
    std::string colnew("Brandon Ingram");
    const auto& val1 = *ConstantExpression::make(pool, colnew);
    uProp1.set_value(Expression::encode(val1));
    updatedProps.emplace_back(uProp1);

    LOG(INFO) << "Build yield...";
    // Return player props: name, age
    {
        std::vector<std::string> tmpProps;
        const auto& sourcePropExp1 = *SourcePropertyExpression::make(pool, "1", "name");
        tmpProps.emplace_back(Expression::encode(sourcePropExp1));

        const auto& sourcePropExp2 = *SourcePropertyExpression::make(pool, "1", "age");
        tmpProps.emplace_back(Expression::encode(sourcePropExp2));

        req.set_return_props(std::move(tmpProps));
        req.set_insertable(true);
    }

    LOG(INFO) << "Test UpdateVertexRequest...";
    auto* processor = UpdateVertexProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(1, (*resp.result_ref()).failed_parts.size());
}

// upsert, insert success
// age filed has not default value and not nullable, but in set clause
TEST(UpdateVertexTest, Insertable_In_Set_Test) {
    fs::TempDir rootPath("/tmp/UpdateVertexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    TagID tagId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();
    EXPECT_TRUE(mockVertexData(env, parts, spaceVidLen));

    LOG(INFO) << "Build UpdateVertexRequest...";
    cpp2::UpdateVertexRequest req;

    req.set_space_id(spaceId);
    auto partId = std::hash<std::string>()("Brandon Ingram") % parts + 1;
    VertexID vertexId("Brandon Ingram");
    req.set_part_id(partId);
    req.set_vertex_id(vertexId);
    req.set_tag_id(tagId);

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedProp> updatedProps;
    // string: player.name= "Brandon Ingram"
    cpp2::UpdatedProp uProp1;
    uProp1.set_name("name");
    std::string colnew("Brandon Ingram");
    const auto& val1 = *ConstantExpression::make(pool, colnew);
    uProp1.set_value(Expression::encode(val1));
    updatedProps.emplace_back(uProp1);

    // int: player.age = $^.player.career
    cpp2::UpdatedProp uProp2;
    uProp2.set_name("age");
    const auto& val2 = *SourcePropertyExpression::make(pool, "1", "career");
    uProp2.set_value(Expression::encode(val2));
    updatedProps.emplace_back(uProp2);
    req.set_updated_props(std::move(updatedProps));

    LOG(INFO) << "Build yield...";
    // Return player props: name, age
    {
        std::vector<std::string> tmpProps;
        const auto& sourcePropExp1 = *SourcePropertyExpression::make(pool, "1", "name");
        tmpProps.emplace_back(Expression::encode(sourcePropExp1));

        const auto& sourcePropExp2 = *SourcePropertyExpression::make(pool, "1", "age");
        tmpProps.emplace_back(Expression::encode(sourcePropExp2));

        addTagPropInKey(tmpProps);

        req.set_return_props(std::move(tmpProps));
        req.set_insertable(true);
    }

    LOG(INFO) << "Test UpdateVertexRequest...";
    auto* processor = UpdateVertexProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(0, (*resp.result_ref()).failed_parts.size());
    EXPECT_EQ(5, (*resp.props_ref()).colNames.size());
    EXPECT_EQ("_inserted", (*resp.props_ref()).colNames[0]);
    EXPECT_EQ("1.name", (*resp.props_ref()).colNames[1]);
    EXPECT_EQ("1.age", (*resp.props_ref()).colNames[2]);
    EXPECT_EQ(std::string("1.").append(kVid), (*resp.props_ref()).colNames[3]);
    EXPECT_EQ(std::string("1.").append(kTag), (*resp.props_ref()).colNames[4]);

    EXPECT_EQ(1, (*resp.props_ref()).rows.size());
    EXPECT_EQ(5, (*resp.props_ref()).rows[0].values.size());
    EXPECT_EQ(true, (*resp.props_ref()).rows[0].values[0].getBool());
    EXPECT_EQ("Brandon Ingram", (*resp.props_ref()).rows[0].values[1].getStr());
    EXPECT_EQ(10, (*resp.props_ref()).rows[0].values[2].getInt());
    EXPECT_EQ("Brandon Ingram", (*resp.props_ref()).rows[0].values[3].getStr());
    EXPECT_EQ(1, (*resp.props_ref()).rows[0].values[4].getInt());

    // get player from kvstore directly
    auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen, partId, vertexId, tagId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    EXPECT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());

    auto reader = RowReaderWrapper::getTagPropReader(env->schemaMan_, spaceId, tagId, iter->val());
    auto val = reader->getValueByName("name");
    EXPECT_EQ("Brandon Ingram", val.getStr());
    val = reader->getValueByName("age");
    EXPECT_EQ(10, val.getInt());
}

// update uses another tag test, failed
TEST(UpdateVertexTest, Update_Multi_tag_Test) {
    fs::TempDir rootPath("/tmp/UpdateVertexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    TagID tagId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();

    EXPECT_TRUE(mockVertexData(env, parts, spaceVidLen));

    LOG(INFO) << "Build UpdateVertexRequest...";
    cpp2::UpdateVertexRequest req;

    req.set_space_id(spaceId);
    auto partId = std::hash<std::string>()("Tim Duncan") % parts + 1;
    VertexID vertexId("Tim Duncan");
    req.set_part_id(partId);
    req.set_vertex_id(vertexId);
    req.set_tag_id(tagId);

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedProp> updatedProps;

    // int: player.age = $^.team.career
    cpp2::UpdatedProp uProp1;
    uProp1.set_name("name");
    // value is another tag expression
    const auto& val2 = *SourcePropertyExpression::make(pool, "2", "name");
    uProp1.set_value(Expression::encode(val2));
    updatedProps.emplace_back(uProp1);
    req.set_updated_props(std::move(updatedProps));

    LOG(INFO) << "Build yield...";
    // Return player props: name, age, country
    std::vector<std::string> tmpProps;
    const auto& sourcePropExp1 = *SourcePropertyExpression::make(pool, "1", "name");
    tmpProps.emplace_back(Expression::encode(sourcePropExp1));

    addTagPropInKey(tmpProps);

    req.set_return_props(std::move(tmpProps));
    req.set_insertable(false);

    LOG(INFO) << "Test UpdateVertexRequest...";
    auto* processor = UpdateVertexProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(1, (*resp.result_ref()).failed_parts.size());

    // get player from kvstore directly
    auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen, partId, vertexId, tagId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());

    auto reader = RowReaderWrapper::getTagPropReader(env->schemaMan_, spaceId, tagId, iter->val());
    auto val = reader->getValueByName("name");
    EXPECT_EQ("Tim Duncan", val.getStr());
}

// upsert uses another tag test, failed
TEST(UpdateVertexTest, Upsert_Multi_tag_Test) {
    fs::TempDir rootPath("/tmp/UpdateVertexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    TagID tagId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();

    EXPECT_TRUE(mockVertexData(env, parts, spaceVidLen));

    LOG(INFO) << "Build UpdateVertexRequest...";
    cpp2::UpdateVertexRequest req;

    req.set_space_id(spaceId);
    auto partId = std::hash<std::string>()("Tim Duncan") % parts + 1;
    VertexID vertexId("Tim Duncan");
    req.set_part_id(partId);
    req.set_vertex_id(vertexId);
    req.set_tag_id(tagId);

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedProp> updatedProps;

    // int: player.age = $^.team.career
    cpp2::UpdatedProp uProp1;
    uProp1.set_name("name");
    // value is another tag expression
    const auto& val2 = *SourcePropertyExpression::make(pool, "2", "name");
    uProp1.set_value(Expression::encode(val2));
    updatedProps.emplace_back(uProp1);
    req.set_updated_props(std::move(updatedProps));

    LOG(INFO) << "Build yield...";
    // Return player props: name, age, country
    std::vector<std::string> tmpProps;
    const auto& sourcePropExp1 = *SourcePropertyExpression::make(pool, "1", "name");
    tmpProps.emplace_back(Expression::encode(sourcePropExp1));

    addTagPropInKey(tmpProps);

    req.set_return_props(std::move(tmpProps));
    req.set_insertable(true);

    LOG(INFO) << "Test UpdateVertexRequest...";
    auto* processor = UpdateVertexProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(1, (*resp.result_ref()).failed_parts.size());

    // get player from kvstore directly
    auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen, partId, vertexId, tagId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());

    auto reader = RowReaderWrapper::getTagPropReader(env->schemaMan_, spaceId, tagId, iter->val());
    auto val = reader->getValueByName("name");
    EXPECT_EQ("Tim Duncan", val.getStr());
}

// upsert/update field type and value does not match test, failed
TEST(UpdateVertexTest, Upsert_Field_Type_And_Value_Match_Test) {
    fs::TempDir rootPath("/tmp/UpdateVertexTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    TagID tagId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();

    EXPECT_TRUE(mockVertexData(env, parts, spaceVidLen));

    LOG(INFO) << "Build UpdateVertexRequest...";
    cpp2::UpdateVertexRequest req;

    req.set_space_id(spaceId);
    auto partId = std::hash<std::string>()("Tim Duncan") % parts + 1;
    VertexID vertexId("Tim Duncan");
    req.set_part_id(partId);
    req.set_vertex_id(vertexId);
    req.set_tag_id(tagId);

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedProp> updatedProps;

    // string: player.country_ = 2011(value int)
    cpp2::UpdatedProp uProp1;
    uProp1.set_name("country");
    const auto& uVal1 = *ConstantExpression::make(pool, 2011L);
    uProp1.set_value(Expression::encode(uVal1));
    updatedProps.emplace_back(uProp1);
    req.set_updated_props(std::move(updatedProps));


    LOG(INFO) << "Build yield...";
    // Return player props: name, age, country
    std::vector<std::string> tmpProps;
    const auto& sourcePropExp1 = *SourcePropertyExpression::make(pool, "1", "name");
    tmpProps.emplace_back(Expression::encode(sourcePropExp1));

    addTagPropInKey(tmpProps);

    req.set_return_props(std::move(tmpProps));
    req.set_insertable(true);

    LOG(INFO) << "Test UpdateVertexRequest...";
    auto* processor = UpdateVertexProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(1, (*resp.result_ref()).failed_parts.size());


    // get player from kvstore directly
    auto prefix = NebulaKeyUtils::vertexPrefix(spaceVidLen, partId, vertexId, tagId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());

    auto reader = RowReaderWrapper::getTagPropReader(env->schemaMan_, spaceId, tagId, iter->val());
    auto val = reader->getValueByName("name");
    EXPECT_EQ("Tim Duncan", val.getStr());
    val = reader->getValueByName("country");
    EXPECT_EQ("America", val.getStr());
}


}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
