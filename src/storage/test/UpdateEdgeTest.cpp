/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "utils/NebulaKeyUtils.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "storage/test/TestUtils.h"
#include "storage/mutate/UpdateEdgeProcessor.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "common/expression/ConstantExpression.h"
#include "storage/test/QueryTestUtils.h"

DECLARE_bool(mock_ttl_col);
DECLARE_int32(mock_ttl_duration);

namespace nebula {
namespace storage {

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


static bool mockEdgeData(storage::StorageEnv* env, int32_t totalParts, int32_t spaceVidLen) {
    GraphSpaceID spaceId = 1;
    auto edgesPart = mock::MockData::mockEdgesofPart(totalParts);

    folly::Baton<true, std::atomic> baton;
    std::atomic<size_t> count(edgesPart.size());

    for (const auto& part : edgesPart) {
        std::vector<kvstore::KV> data;
        data.clear();

        for (const auto& edge : part.second) {
            // Switch version to big-endian, make sure the key is in ordered.
            auto version = std::numeric_limits<int64_t>::max() - 0L;
            version = folly::Endian::big(version);
            auto key = NebulaKeyUtils::edgeKey(spaceVidLen,
                                               part.first,
                                               edge.srcId_,
                                               edge.type_,
                                               edge.rank_,
                                               edge.dstId_,
                                               version);
            auto schema = env->schemaMan_->getEdgeSchema(spaceId, std::abs(edge.type_));
            if (!schema) {
                LOG(ERROR) << "Invalid edge " << edge.type_;
                return false;
            }

            auto ret = encode(schema.get(), key, edge.props_, data);
            if (!ret) {
                LOG(ERROR) << "Write field failed";
                return false;
            }
        }
        env->kvstore_->asyncMultiPut(spaceId, part.first, std::move(data),
                                     [&](kvstore::ResultCode code) {
                                         CHECK_EQ(code, kvstore::ResultCode::SUCCEEDED);
                                         count.fetch_sub(1);
                                         if (count.load() == 0) {
                                             baton.post();
                                         }
                                     });
    }
    baton.wait();
    return true;
}

void addEdgePropInKey(std::vector<std::string>& returnCols) {
    {
        auto* edge = new std::string("101");
        auto* prop = new std::string(kSrc);
        EdgePropertyExpression exp(edge, prop);
        returnCols.emplace_back(Expression::encode(exp));
    }
    {
        auto* edge = new std::string("101");
        auto* prop = new std::string(kType);
        EdgePropertyExpression exp(edge, prop);
        returnCols.emplace_back(Expression::encode(exp));
    }
    {
        auto* edge = new std::string("101");
        auto* prop = new std::string(kRank);
        EdgePropertyExpression exp(edge, prop);
        returnCols.emplace_back(Expression::encode(exp));
    }
    {
        auto* edge = new std::string("101");
        auto* prop = new std::string(kDst);
        EdgePropertyExpression exp(edge, prop);
        returnCols.emplace_back(Expression::encode(exp));
    }
}

// No filter, update success
TEST(UpdateEdgeTest, No_Filter_Test) {
    fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();

    EXPECT_TRUE(mockEdgeData(env, parts, spaceVidLen));
    {
        LOG(INFO) << "Build UpdateEdgeRequest...";
        cpp2::UpdateEdgeRequest req;
        auto partId = std::hash<std::string>()("Tim Duncan") % parts + 1;
        req.set_space_id(spaceId);
        req.set_part_id(partId);
        // src = Tim Duncan, edge_type = 101, ranking = 1997, dst = Spurs
        VertexID srcId = "Tim Duncan";
        VertexID dstId = "Spurs";
        EdgeRanking rank = 1997;
        EdgeType edgeType = 101;
        storage::cpp2::EdgeKey edgeKey;
        edgeKey.set_src(srcId);
        edgeKey.set_edge_type(edgeType);
        edgeKey.set_ranking(rank);
        edgeKey.set_dst(dstId);
        req.set_edge_key(edgeKey);

        LOG(INFO) << "Build updated props...";
        std::vector<cpp2::UpdatedProp> props;
        // int: 101.teamCareer = 20
        cpp2::UpdatedProp uProp1;
        uProp1.set_name("teamCareer");
        ConstantExpression val1(20);
        uProp1.set_value(Expression::encode(val1));
        props.emplace_back(uProp1);

        // bool: 101.type = trade
        cpp2::UpdatedProp uProp2;
        uProp2.set_name("type");
        std::string colnew("trade");
        ConstantExpression val2(colnew);
        uProp2.set_value(Expression::encode(val2));
        props.emplace_back(uProp2);
        req.set_updated_props(std::move(props));

        LOG(INFO) << "Build yield...";
        // Return serve props: playerName, teamName, teamCareer, type
        std::vector<std::string> tmpProps;
        auto* yEdge1 = new std::string("101");
        auto* yProp1 = new std::string("playerName");
        EdgePropertyExpression edgePropExp1(yEdge1, yProp1);
        tmpProps.emplace_back(Expression::encode(edgePropExp1));

        auto* yEdge2 = new std::string("101");
        auto* yProp2 = new std::string("teamName");
        EdgePropertyExpression edgePropExp2(yEdge2, yProp2);
        tmpProps.emplace_back(Expression::encode(edgePropExp2));

        auto* yEdge3 = new std::string("101");
        auto* yProp3 = new std::string("teamCareer");
        EdgePropertyExpression edgePropExp3(yEdge3, yProp3);
        tmpProps.emplace_back(Expression::encode(edgePropExp3));

        auto* yEdge4 = new std::string("101");
        auto* yProp4 = new std::string("type");
        EdgePropertyExpression edgePropExp4(yEdge4, yProp4);
        tmpProps.emplace_back(Expression::encode(edgePropExp4));

        addEdgePropInKey(tmpProps);

        req.set_return_props(std::move(tmpProps));
        req.set_insertable(false);

        LOG(INFO) << "Test UpdateEdgeRequest...";
        auto* processor = UpdateEdgeProcessor::instance(env, nullptr);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        LOG(INFO) << "Check the results...";
        EXPECT_EQ(0, (*resp.result_ref()).failed_parts.size());
        EXPECT_EQ(9, (*resp.props_ref()).colNames.size());
        EXPECT_EQ("_inserted", (*resp.props_ref()).colNames[0]);
        EXPECT_EQ("101.playerName", (*resp.props_ref()).colNames[1]);
        EXPECT_EQ("101.teamName", (*resp.props_ref()).colNames[2]);
        EXPECT_EQ("101.teamCareer", (*resp.props_ref()).colNames[3]);
        EXPECT_EQ("101.type", (*resp.props_ref()).colNames[4]);
        EXPECT_EQ(std::string("101.").append(kSrc), (*resp.props_ref()).colNames[5]);
        EXPECT_EQ(std::string("101.").append(kType), (*resp.props_ref()).colNames[6]);
        EXPECT_EQ(std::string("101.").append(kRank), (*resp.props_ref()).colNames[7]);
        EXPECT_EQ(std::string("101.").append(kDst), (*resp.props_ref()).colNames[8]);

        EXPECT_EQ(1, (*resp.props_ref()).rows.size());
        EXPECT_EQ(9, (*resp.props_ref()).rows[0].values.size());
        EXPECT_EQ(false, (*resp.props_ref()).rows[0].values[0].getBool());
        EXPECT_EQ("Tim Duncan", (*resp.props_ref()).rows[0].values[1].getStr());
        EXPECT_EQ("Spurs", (*resp.props_ref()).rows[0].values[2].getStr());
        EXPECT_EQ(20, (*resp.props_ref()).rows[0].values[3].getInt());
        EXPECT_EQ("trade", (*resp.props_ref()).rows[0].values[4].getStr());
        EXPECT_EQ("Tim Duncan", (*resp.props_ref()).rows[0].values[5].getStr());
        EXPECT_EQ(101, (*resp.props_ref()).rows[0].values[6].getInt());
        EXPECT_EQ(1997, (*resp.props_ref()).rows[0].values[7].getInt());
        EXPECT_EQ("Spurs", (*resp.props_ref()).rows[0].values[8].getStr());

        // get serve from kvstore directly
        auto prefix = NebulaKeyUtils::edgePrefix(spaceVidLen, partId, srcId, edgeType, rank, dstId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
        EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
        EXPECT_TRUE(iter && iter->valid());

        auto edgeReader = RowReaderWrapper::getEdgePropReader(env->schemaMan_,
                                                              spaceId,
                                                              std::abs(edgeType),
                                                              iter->val());
        auto val = edgeReader->getValueByName("playerName");
        EXPECT_EQ("Tim Duncan", val.getStr());

        val = edgeReader->getValueByName("teamName");
        EXPECT_EQ("Spurs", val.getStr());
        val = edgeReader->getValueByName("teamCareer");
        EXPECT_EQ(20, val.getInt());
        val = edgeReader->getValueByName("type");
        EXPECT_EQ("trade", val.getStr());
    }

    // reverse
    {
        LOG(INFO) << "Build UpdateEdgeRequest...";
        cpp2::UpdateEdgeRequest req;
        auto partId = std::hash<std::string>()("Spurs") % parts + 1;
        req.set_space_id(spaceId);
        req.set_part_id(partId);
        // src = Spurs, edge_type = -101, ranking = 1997, dst = Tim Duncan
        VertexID srcId = "Spurs";
        VertexID dstId = "Tim Duncan";
        EdgeRanking rank = 1997;
        EdgeType edgeType = -101;
        storage::cpp2::EdgeKey edgeKey;
        edgeKey.set_src(srcId);
        edgeKey.set_edge_type(edgeType);
        edgeKey.set_ranking(rank);
        edgeKey.set_dst(dstId);
        req.set_edge_key(edgeKey);

        LOG(INFO) << "Build updated props...";
        std::vector<cpp2::UpdatedProp> props;
        // int: 101.teamCareer = 20
        cpp2::UpdatedProp uProp1;
        uProp1.set_name("teamCareer");
        ConstantExpression val1(20);
        uProp1.set_value(Expression::encode(val1));
        props.emplace_back(uProp1);

        // bool: 101.type = trade
        cpp2::UpdatedProp uProp2;
        uProp2.set_name("type");
        std::string colnew("trade");
        ConstantExpression val2(colnew);
        uProp2.set_value(Expression::encode(val2));
        props.emplace_back(uProp2);
        req.set_updated_props(std::move(props));

        LOG(INFO) << "Build yield...";
        // Return serve props: playerName, teamName, teamCareer, type
        std::vector<std::string> tmpProps;
        auto* yEdge1 = new std::string("101");
        auto* yProp1 = new std::string("playerName");
        EdgePropertyExpression edgePropExp1(yEdge1, yProp1);
        tmpProps.emplace_back(Expression::encode(edgePropExp1));

        auto* yEdge2 = new std::string("101");
        auto* yProp2 = new std::string("teamName");
        EdgePropertyExpression edgePropExp2(yEdge2, yProp2);
        tmpProps.emplace_back(Expression::encode(edgePropExp2));

        auto* yEdge3 = new std::string("101");
        auto* yProp3 = new std::string("teamCareer");
        EdgePropertyExpression edgePropExp3(yEdge3, yProp3);
        tmpProps.emplace_back(Expression::encode(edgePropExp3));

        auto* yEdge4 = new std::string("101");
        auto* yProp4 = new std::string("type");
        EdgePropertyExpression edgePropExp4(yEdge4, yProp4);
        tmpProps.emplace_back(Expression::encode(edgePropExp4));

        addEdgePropInKey(tmpProps);

        req.set_return_props(std::move(tmpProps));
        req.set_insertable(false);

        LOG(INFO) << "Test UpdateEdgeRequest...";
        auto* processor = UpdateEdgeProcessor::instance(env, nullptr);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        LOG(INFO) << "Check the results...";
        EXPECT_EQ(0, (*resp.result_ref()).failed_parts.size());
        EXPECT_EQ(9, (*resp.props_ref()).colNames.size());
        EXPECT_EQ("_inserted", (*resp.props_ref()).colNames[0]);
        EXPECT_EQ("101.playerName", (*resp.props_ref()).colNames[1]);
        EXPECT_EQ("101.teamName", (*resp.props_ref()).colNames[2]);
        EXPECT_EQ("101.teamCareer", (*resp.props_ref()).colNames[3]);
        EXPECT_EQ("101.type", (*resp.props_ref()).colNames[4]);
        EXPECT_EQ(std::string("101.").append(kSrc), (*resp.props_ref()).colNames[5]);
        EXPECT_EQ(std::string("101.").append(kType), (*resp.props_ref()).colNames[6]);
        EXPECT_EQ(std::string("101.").append(kRank), (*resp.props_ref()).colNames[7]);
        EXPECT_EQ(std::string("101.").append(kDst), (*resp.props_ref()).colNames[8]);

        EXPECT_EQ(1, (*resp.props_ref()).rows.size());
        EXPECT_EQ(9, (*resp.props_ref()).rows[0].values.size());
        EXPECT_EQ(false, (*resp.props_ref()).rows[0].values[0].getBool());
        EXPECT_EQ("Tim Duncan", (*resp.props_ref()).rows[0].values[1].getStr());
        EXPECT_EQ("Spurs", (*resp.props_ref()).rows[0].values[2].getStr());
        EXPECT_EQ(20, (*resp.props_ref()).rows[0].values[3].getInt());
        EXPECT_EQ("trade", (*resp.props_ref()).rows[0].values[4].getStr());
        EXPECT_EQ("Spurs", (*resp.props_ref()).rows[0].values[5].getStr());
        EXPECT_EQ(-101, (*resp.props_ref()).rows[0].values[6].getInt());
        EXPECT_EQ(1997, (*resp.props_ref()).rows[0].values[7].getInt());
        EXPECT_EQ("Tim Duncan", (*resp.props_ref()).rows[0].values[8].getStr());

        // get serve from kvstore directly
        auto prefix = NebulaKeyUtils::edgePrefix(spaceVidLen, partId, srcId, edgeType, rank, dstId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
        EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
        EXPECT_TRUE(iter && iter->valid());

        auto edgeReader = RowReaderWrapper::getEdgePropReader(env->schemaMan_,
                                                              spaceId,
                                                              std::abs(edgeType),
                                                              iter->val());
        auto val = edgeReader->getValueByName("playerName");
        EXPECT_EQ("Tim Duncan", val.getStr());
        val = edgeReader->getValueByName("teamName");
        EXPECT_EQ("Spurs", val.getStr());
        val = edgeReader->getValueByName("teamCareer");
        EXPECT_EQ(20, val.getInt());
        val = edgeReader->getValueByName("type");
        EXPECT_EQ("trade", val.getStr());
    }
}

// Filter out, update failed
TEST(UpdateEdgeTest, Filter_Yield_Test) {
    fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();

    EXPECT_TRUE(mockEdgeData(env, parts, spaceVidLen));

    LOG(INFO) << "Build UpdateEdgeRequest...";
    cpp2::UpdateEdgeRequest req;
    auto partId = std::hash<std::string>()("Tim Duncan") % parts + 1;
    req.set_space_id(spaceId);
    req.set_part_id(partId);
    VertexID srcId = "Tim Duncan";
    VertexID dstId = "Spurs";
    EdgeRanking rank = 1997;
    EdgeType edgeType = 101;
    // src = Tim Duncan, edge_type = 101, ranking = 1997, dst = Spurs
    storage::cpp2::EdgeKey edgeKey;
    edgeKey.set_src(srcId);
    edgeKey.set_edge_type(edgeType);
    edgeKey.set_ranking(rank);
    edgeKey.set_dst(dstId);
    req.set_edge_key(edgeKey);

    LOG(INFO) << "Build filter...";
    // left int:  101.startYear = 1997
    auto* fEdge1 = new std::string("101");
    auto* fProp1 = new std::string("startYear");
    auto* srcExp1 = new EdgePropertyExpression(fEdge1, fProp1);
    auto* priExp1 = new ConstantExpression(1997L);
    auto* left = new RelationalExpression(Expression::Kind::kRelEQ,
                                          srcExp1,
                                          priExp1);

    // right int: 101.endYear = 2017
    auto* fEdge2 = new std::string("101");
    auto* fProp2 = new std::string("endYear");
    auto* srcExp2 = new EdgePropertyExpression(fEdge2, fProp2);
    auto* priExp2 = new ConstantExpression(2017L);
    auto* right = new RelationalExpression(Expression::Kind::kRelEQ,
                                           srcExp2,
                                           priExp2);
    // left AND right is ture
    auto logExp = std::make_unique<LogicalExpression>(Expression::Kind::kLogicalAnd,
                                                      left,
                                                      right);
    req.set_condition(Expression::encode(*logExp.get()));

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedProp> props;
    // int: 101.teamCareer = 20
    cpp2::UpdatedProp uProp1;
    uProp1.set_name("teamCareer");
    ConstantExpression val1(20);
    uProp1.set_value(Expression::encode(val1));
    props.emplace_back(uProp1);

    // bool: 101.type = trade
    cpp2::UpdatedProp uProp2;
    uProp2.set_name("type");
    std::string colnew("trade");
    ConstantExpression val2(colnew);
    uProp2.set_value(Expression::encode(val2));
    props.emplace_back(uProp2);
    req.set_updated_props(std::move(props));

    LOG(INFO) << "Build yield...";
    // Return serve props: playerName, teamName, teamCareer, type
    std::vector<std::string> tmpProps;
    auto* yEdge1 = new std::string("101");
    auto* yProp1 = new std::string("playerName");
    EdgePropertyExpression edgePropExp1(yEdge1, yProp1);
    tmpProps.emplace_back(Expression::encode(edgePropExp1));

    auto* yEdge2 = new std::string("101");
    auto* yProp2 = new std::string("teamName");
    EdgePropertyExpression edgePropExp2(yEdge2, yProp2);
    tmpProps.emplace_back(Expression::encode(edgePropExp2));

    auto* yEdge3 = new std::string("101");
    auto* yProp3 = new std::string("teamCareer");
    EdgePropertyExpression edgePropExp3(yEdge3, yProp3);
    tmpProps.emplace_back(Expression::encode(edgePropExp3));

    auto* yEdge4 = new std::string("101");
    auto* yProp4 = new std::string("type");
    EdgePropertyExpression edgePropExp4(yEdge4, yProp4);
    tmpProps.emplace_back(Expression::encode(edgePropExp4));

    addEdgePropInKey(tmpProps);

    req.set_return_props(std::move(tmpProps));
    req.set_insertable(false);

    LOG(INFO) << "Test UpdateEdgeRequest...";
    auto* processor = UpdateEdgeProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(1, (*resp.result_ref()).failed_parts.size());

    // Note: If filtered out, the result is old
    EXPECT_EQ(9, (*resp.props_ref()).colNames.size());
    EXPECT_EQ("_inserted", (*resp.props_ref()).colNames[0]);
    EXPECT_EQ("101.playerName", (*resp.props_ref()).colNames[1]);
    EXPECT_EQ("101.teamName", (*resp.props_ref()).colNames[2]);
    EXPECT_EQ("101.teamCareer", (*resp.props_ref()).colNames[3]);
    EXPECT_EQ("101.type", (*resp.props_ref()).colNames[4]);
    EXPECT_EQ(std::string("101.").append(kSrc), (*resp.props_ref()).colNames[5]);
    EXPECT_EQ(std::string("101.").append(kType), (*resp.props_ref()).colNames[6]);
    EXPECT_EQ(std::string("101.").append(kRank), (*resp.props_ref()).colNames[7]);
    EXPECT_EQ(std::string("101.").append(kDst), (*resp.props_ref()).colNames[8]);

    // filter, return old value
    EXPECT_EQ(1, (*resp.props_ref()).rows.size());
    EXPECT_EQ(9, (*resp.props_ref()).rows[0].values.size());

    EXPECT_EQ(false, (*resp.props_ref()).rows[0].values[0].getBool());
    EXPECT_EQ("Tim Duncan", (*resp.props_ref()).rows[0].values[1].getStr());
    EXPECT_EQ("Spurs", (*resp.props_ref()).rows[0].values[2].getStr());
    EXPECT_EQ(19, (*resp.props_ref()).rows[0].values[3].getInt());
    EXPECT_EQ("zzzzz", (*resp.props_ref()).rows[0].values[4].getStr());
    EXPECT_EQ("Tim Duncan", (*resp.props_ref()).rows[0].values[5].getStr());
    EXPECT_EQ(101, (*resp.props_ref()).rows[0].values[6].getInt());
    EXPECT_EQ(1997, (*resp.props_ref()).rows[0].values[7].getInt());
    EXPECT_EQ("Spurs", (*resp.props_ref()).rows[0].values[8].getStr());

    // get serve from kvstore directly
    // Because no update, the value is old
    auto prefix = NebulaKeyUtils::edgePrefix(spaceVidLen, partId, srcId, edgeType, rank, dstId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());

    auto reader = RowReaderWrapper::getEdgePropReader(env->schemaMan_,
                                                      spaceId,
                                                      std::abs(edgeType),
                                                      iter->val());
    auto val = reader->getValueByName("playerName");
    EXPECT_EQ("Tim Duncan", val.getStr());
    val = reader->getValueByName("teamName");
    EXPECT_EQ("Spurs", val.getStr());
    val = reader->getValueByName("teamCareer");
    EXPECT_EQ(19, val.getInt());
    val = reader->getValueByName("type");
    EXPECT_EQ("zzzzz", val.getStr());
}

// Upsert, insert success
TEST(UpdateEdgeTest, Insertable_Test) {
    fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();
    EXPECT_TRUE(mockEdgeData(env, parts, spaceVidLen));

    LOG(INFO) << "Build UpdateEdgeRequest...";
    cpp2::UpdateEdgeRequest req;

    auto partId = std::hash<std::string>()("Brandon Ingram") % parts + 1;
    req.set_space_id(spaceId);
    req.set_part_id(partId);
    VertexID srcId = "Brandon Ingram";
    VertexID dstId = "Lakers";
    EdgeRanking rank = 2016;
    EdgeType edgeType = 101;
    // src = Brandon Ingram, edge_type = 101, ranking = 2016, dst = Lakers
    storage::cpp2::EdgeKey edgeKey;
    edgeKey.set_src(srcId);
    edgeKey.set_edge_type(edgeType);
    edgeKey.set_ranking(rank);
    edgeKey.set_dst(dstId);
    req.set_edge_key(edgeKey);

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedProp> props;

    // string: 101.playerName = Brandon Ingram
    cpp2::UpdatedProp uProp1;
    uProp1.set_name("playerName");
    std::string col1new("Brandon Ingram");
    ConstantExpression val1(col1new);
    uProp1.set_value(Expression::encode(val1));
    props.emplace_back(uProp1);

    // string: 101.teamName = Lakers
    cpp2::UpdatedProp uProp2;
    uProp2.set_name("teamName");
    std::string col2new("Lakers");
    ConstantExpression val2(col2new);
    uProp2.set_value(Expression::encode(val2));
    props.emplace_back(uProp2);

    // int: 101.startYear = 2016
    cpp2::UpdatedProp uProp3;
    uProp3.set_name("startYear");
    ConstantExpression val3(2016L);
    uProp3.set_value(Expression::encode(val3));
    props.emplace_back(uProp3);

    // int: 101.teamCareer = 1
    cpp2::UpdatedProp uProp4;
    uProp4.set_name("teamCareer");
    ConstantExpression val4(1L);
    uProp4.set_value(Expression::encode(val4));
    props.emplace_back(uProp4);
    req.set_updated_props(std::move(props));

    LOG(INFO) << "Build yield...";
    // Return serve props: playerName, teamName, teamCareer, type
    std::vector<std::string> tmpProps;
    auto* yEdge1 = new std::string("101");
    auto* yProp1 = new std::string("playerName");
    EdgePropertyExpression edgePropExp1(yEdge1, yProp1);
    tmpProps.emplace_back(Expression::encode(edgePropExp1));

    auto* yEdge2 = new std::string("101");
    auto* yProp2 = new std::string("teamName");
    EdgePropertyExpression edgePropExp2(yEdge2, yProp2);
    tmpProps.emplace_back(Expression::encode(edgePropExp2));

    auto* yEdge3 = new std::string("101");
    auto* yProp3 = new std::string("teamCareer");
    EdgePropertyExpression edgePropExp3(yEdge3, yProp3);
    tmpProps.emplace_back(Expression::encode(edgePropExp3));

    auto* yEdge4 = new std::string("101");
    auto* yProp4 = new std::string("type");
    EdgePropertyExpression edgePropExp4(yEdge4, yProp4);
    tmpProps.emplace_back(Expression::encode(edgePropExp4));

    addEdgePropInKey(tmpProps);

    req.set_return_props(std::move(tmpProps));
    req.set_insertable(true);

    LOG(INFO) << "Test UpdateEdgeRequest...";
    auto* processor = UpdateEdgeProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(0, (*resp.result_ref()).failed_parts.size());
    EXPECT_EQ(9, (*resp.props_ref()).colNames.size());
    EXPECT_EQ("_inserted", (*resp.props_ref()).colNames[0]);
    EXPECT_EQ("101.playerName", (*resp.props_ref()).colNames[1]);
    EXPECT_EQ("101.teamName", (*resp.props_ref()).colNames[2]);
    EXPECT_EQ("101.teamCareer", (*resp.props_ref()).colNames[3]);
    EXPECT_EQ("101.type", (*resp.props_ref()).colNames[4]);
    EXPECT_EQ(std::string("101.").append(kSrc), (*resp.props_ref()).colNames[5]);
    EXPECT_EQ(std::string("101.").append(kType), (*resp.props_ref()).colNames[6]);
    EXPECT_EQ(std::string("101.").append(kRank), (*resp.props_ref()).colNames[7]);
    EXPECT_EQ(std::string("101.").append(kDst), (*resp.props_ref()).colNames[8]);

    EXPECT_EQ(1, (*resp.props_ref()).rows.size());
    EXPECT_EQ(9, (*resp.props_ref()).rows[0].values.size());
    EXPECT_EQ(true, (*resp.props_ref()).rows[0].values[0].getBool());
    EXPECT_EQ("Brandon Ingram", (*resp.props_ref()).rows[0].values[1].getStr());
    EXPECT_EQ("Lakers", (*resp.props_ref()).rows[0].values[2].getStr());
    EXPECT_EQ(1, (*resp.props_ref()).rows[0].values[3].getInt());
    EXPECT_EQ("trade", (*resp.props_ref()).rows[0].values[4].getStr());
    EXPECT_EQ("Brandon Ingram", (*resp.props_ref()).rows[0].values[5].getStr());
    EXPECT_EQ(101, (*resp.props_ref()).rows[0].values[6].getInt());
    EXPECT_EQ(2016, (*resp.props_ref()).rows[0].values[7].getInt());
    EXPECT_EQ("Lakers", (*resp.props_ref()).rows[0].values[8].getStr());

    // get serve from kvstore directly
    auto prefix = NebulaKeyUtils::edgePrefix(spaceVidLen, partId, srcId, edgeType, rank, dstId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());

    auto reader = RowReaderWrapper::getEdgePropReader(env->schemaMan_,
                                                      spaceId,
                                                      std::abs(edgeType),
                                                      iter->val());
    auto val = reader->getValueByName("playerName");
    EXPECT_EQ("Brandon Ingram", val.getStr());
    val = reader->getValueByName("teamName");
    EXPECT_EQ("Lakers", val.getStr());
    val = reader->getValueByName("teamCareer");
    EXPECT_EQ(1, val.getInt());
    val = reader->getValueByName("type");
    EXPECT_EQ("trade", val.getStr());
}

// Invalid update prop, update failed
TEST(UpdateEdgeTest, Invalid_Update_Prop_Test) {
    fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();
    EXPECT_TRUE(mockEdgeData(env, parts, spaceVidLen));

    LOG(INFO) << "Build UpdateEdgeRequest...";
    cpp2::UpdateEdgeRequest req;

    auto partId = std::hash<std::string>()("Tim Duncan") % parts + 1;
    req.set_space_id(spaceId);
    req.set_part_id(partId);
    VertexID srcId = "Tim Duncan";
    VertexID dstId = "Spurs";
    EdgeRanking rank = 1997;
    EdgeType edgeType = 101;
    // src = Tim Duncan, edge_type = 101, ranking = 1997, dst = Spurs
    storage::cpp2::EdgeKey edgeKey;
    edgeKey.set_src(srcId);
    edgeKey.set_edge_type(edgeType);
    edgeKey.set_ranking(rank);
    edgeKey.set_dst(dstId);
    req.set_edge_key(edgeKey);

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedProp> props;
    // int: 101.teamCareer = 20
    cpp2::UpdatedProp uProp1;
    uProp1.set_name("teamCareer");
    ConstantExpression val1(30);
    uProp1.set_value(Expression::encode(val1));
    props.emplace_back(uProp1);

    // bool: 101.birth = 1997
    cpp2::UpdatedProp uProp2;
    uProp2.set_name("birth");
    ConstantExpression val2(1997);
    uProp2.set_value(Expression::encode(val2));
    props.emplace_back(uProp2);
    req.set_updated_props(std::move(props));

    LOG(INFO) << "Build yield...";
    // Return serve props: playerName, teamName, teamCareer, type
    std::vector<std::string> tmpProps;
    auto* yEdge1 = new std::string("101");
    auto* yProp1 = new std::string("playerName");
    EdgePropertyExpression edgePropExp1(yEdge1, yProp1);
    tmpProps.emplace_back(Expression::encode(edgePropExp1));

    auto* yEdge2 = new std::string("101");
    auto* yProp2 = new std::string("teamName");
    EdgePropertyExpression edgePropExp2(yEdge2, yProp2);
    tmpProps.emplace_back(Expression::encode(edgePropExp2));

    auto* yEdge3 = new std::string("101");
    auto* yProp3 = new std::string("teamCareer");
    EdgePropertyExpression edgePropExp3(yEdge3, yProp3);
    tmpProps.emplace_back(Expression::encode(edgePropExp3));

    auto* yEdge4 = new std::string("101");
    auto* yProp4 = new std::string("type");
    EdgePropertyExpression edgePropExp4(yEdge4, yProp4);
    tmpProps.emplace_back(Expression::encode(edgePropExp4));

    req.set_return_props(std::move(tmpProps));
    req.set_insertable(false);

    LOG(INFO) << "Test UpdateEdgeRequest...";
    auto* processor = UpdateEdgeProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(1, (*resp.result_ref()).failed_parts.size());

    // get serve from kvstore directly
    // Because no update, the value is old
    auto prefix = NebulaKeyUtils::edgePrefix(spaceVidLen, partId, srcId, edgeType, rank, dstId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());

    auto reader = RowReaderWrapper::getEdgePropReader(env->schemaMan_,
                                                      spaceId,
                                                      std::abs(edgeType),
                                                      iter->val());
    auto val = reader->getValueByName("playerName");
    EXPECT_EQ("Tim Duncan", val.getStr());
    val = reader->getValueByName("teamName");
    EXPECT_EQ("Spurs", val.getStr());
    val = reader->getValueByName("teamCareer");
    EXPECT_EQ(19, val.getInt());
    val = reader->getValueByName("type");
    EXPECT_EQ("zzzzz", val.getStr());
}

// Invalid filter, update failed
TEST(UpdateEdgeTest, Invalid_Filter_Test) {
    fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();
    EXPECT_TRUE(mockEdgeData(env, parts, spaceVidLen));

    LOG(INFO) << "Build UpdateEdgeRequest...";
    cpp2::UpdateEdgeRequest req;

    auto partId = std::hash<std::string>()("Tim Duncan") % parts + 1;
    req.set_space_id(spaceId);
    req.set_part_id(partId);
    VertexID srcId = "Tim Duncan";
    VertexID dstId = "Spurs";
    EdgeRanking rank = 1997;
    EdgeType edgeType = 101;
    // src = Tim Duncan, edge_type = 101, ranking = 1997, dst = Spurs
    storage::cpp2::EdgeKey edgeKey;
    edgeKey.set_src(srcId);
    edgeKey.set_edge_type(edgeType);
    edgeKey.set_ranking(rank);
    edgeKey.set_dst(dstId);
    req.set_edge_key(edgeKey);

    LOG(INFO) << "Build filter...";
    // left int:  101.startYear = 1997
    auto* fEdge1 = new std::string("101");
    auto* fProp1 = new std::string("startYear");
    auto* srcExp1 = new EdgePropertyExpression(fEdge1, fProp1);
    auto* priExp1 = new ConstantExpression(1997L);
    auto* left = new RelationalExpression(Expression::Kind::kRelEQ,
                                          srcExp1,
                                          priExp1);

    // right int: 101.endYear = 2017
    auto* fEdge2 = new std::string("101");
    auto* fProp2 = new std::string("birth");
    auto* srcExp2 = new EdgePropertyExpression(fEdge2, fProp2);
    auto* priExp2 = new ConstantExpression(1990L);
    auto* right = new RelationalExpression(Expression::Kind::kRelEQ,
                                           srcExp2,
                                           priExp2);
    // left AND right is ture
    auto logExp = std::make_unique<LogicalExpression>(Expression::Kind::kLogicalAnd,
                                                      left,
                                                      right);
    req.set_condition(Expression::encode(*logExp.get()));

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedProp> props;
    // int: 101.teamCareer = 20
    cpp2::UpdatedProp uProp1;
    uProp1.set_name("teamCareer");
    ConstantExpression val1(30);
    uProp1.set_value(Expression::encode(val1));
    props.emplace_back(uProp1);

    // bool: 101.birth = 1997
    cpp2::UpdatedProp uProp2;
    uProp2.set_name("type");
    std::string colnew("trade");
    ConstantExpression val2(colnew);
    uProp2.set_value(Expression::encode(val2));
    props.emplace_back(uProp2);
    req.set_updated_props(std::move(props));

    LOG(INFO) << "Build yield...";
    // Return serve props: playerName, teamName, teamCareer, type
    std::vector<std::string> tmpProps;
    auto* yEdge1 = new std::string("101");
    auto* yProp1 = new std::string("playerName");
    EdgePropertyExpression edgePropExp1(yEdge1, yProp1);
    tmpProps.emplace_back(Expression::encode(edgePropExp1));

    auto* yEdge2 = new std::string("101");
    auto* yProp2 = new std::string("teamName");
    EdgePropertyExpression edgePropExp2(yEdge2, yProp2);
    tmpProps.emplace_back(Expression::encode(edgePropExp2));

    auto* yEdge3 = new std::string("101");
    auto* yProp3 = new std::string("teamCareer");
    EdgePropertyExpression edgePropExp3(yEdge3, yProp3);
    tmpProps.emplace_back(Expression::encode(edgePropExp3));

    auto* yEdge4 = new std::string("101");
    auto* yProp4 = new std::string("type");
    EdgePropertyExpression edgePropExp4(yEdge4, yProp4);
    tmpProps.emplace_back(Expression::encode(edgePropExp4));

    req.set_return_props(std::move(tmpProps));
    req.set_insertable(false);

    LOG(INFO) << "Test UpdateEdgeRequest...";
    auto* processor = UpdateEdgeProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(1, (*resp.result_ref()).failed_parts.size());

    // get serve from kvstore directly
    // Because no update, the value is old
    auto prefix = NebulaKeyUtils::edgePrefix(spaceVidLen, partId, srcId, edgeType, rank, dstId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());

    auto reader = RowReaderWrapper::getEdgePropReader(env->schemaMan_,
                                                      spaceId,
                                                      std::abs(edgeType),
                                                      iter->val());
    auto val = reader->getValueByName("playerName");
    EXPECT_EQ("Tim Duncan", val.getStr());
    val = reader->getValueByName("teamName");
    EXPECT_EQ("Spurs", val.getStr());
    val = reader->getValueByName("teamCareer");
    EXPECT_EQ(19, val.getInt());
    val = reader->getValueByName("type");
    EXPECT_EQ("zzzzz", val.getStr());
}

// Upsert, insert success when filter is valid, but value is false
TEST(UpdateEdgeTest, Insertable_Filter_value_Test) {
    fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();
    EXPECT_TRUE(mockEdgeData(env, parts, spaceVidLen));

    LOG(INFO) << "Build UpdateEdgeRequest...";
    cpp2::UpdateEdgeRequest req;

    auto partId = std::hash<std::string>()("Brandon Ingram") % parts + 1;
    req.set_space_id(spaceId);
    req.set_part_id(partId);
    VertexID srcId = "Brandon Ingram";
    VertexID dstId = "Lakers";
    EdgeRanking rank = 2016;
    EdgeType edgeType = 101;
    // src = Brandon Ingram, edge_type = 101, ranking = 2016, dst = Lakers
    storage::cpp2::EdgeKey edgeKey;
    edgeKey.set_src(srcId);
    edgeKey.set_edge_type(edgeType);
    edgeKey.set_ranking(rank);
    edgeKey.set_dst(dstId);
    req.set_edge_key(edgeKey);

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedProp> props;

    // string: 101.playerName = Brandon Ingram
    cpp2::UpdatedProp uProp1;
    uProp1.set_name("playerName");
    std::string col1new("Brandon Ingram");
    ConstantExpression val1(col1new);
    uProp1.set_value(Expression::encode(val1));
    props.emplace_back(uProp1);

    // string: 101.teamName = Lakers
    cpp2::UpdatedProp uProp2;
    uProp2.set_name("teamName");
    std::string col2new("Lakers");
    ConstantExpression val2(col2new);
    uProp2.set_value(Expression::encode(val2));
    props.emplace_back(uProp2);

    // int: 101.startYear = 2016
    cpp2::UpdatedProp uProp3;
    uProp3.set_name("startYear");
    ConstantExpression val3(2016L);
    uProp3.set_value(Expression::encode(val3));
    props.emplace_back(uProp3);

    // int: 101.teamCareer = 1
    cpp2::UpdatedProp uProp4;
    uProp4.set_name("teamCareer");
    ConstantExpression val4(1L);
    uProp4.set_value(Expression::encode(val4));
    props.emplace_back(uProp4);
    req.set_updated_props(std::move(props));

    LOG(INFO) << "Build filter...";
    // filter is valid, but filter value is false
    // left int:  101.startYear = 1997
    auto* fEdge1 = new std::string("101");
    auto* fProp1 = new std::string("startYear");
    auto* srcExp1 = new EdgePropertyExpression(fEdge1, fProp1);
    auto* priExp1 = new ConstantExpression(1997L);
    auto* left = new RelationalExpression(Expression::Kind::kRelEQ,
                                          srcExp1,
                                          priExp1);

    // right int: 101.endYear = 2017
    auto* fEdge2 = new std::string("101");
    auto* fProp2 = new std::string("endYear");
    auto* srcExp2 = new EdgePropertyExpression(fEdge2, fProp2);
    auto* priExp2 = new ConstantExpression(2017L);
    auto* right = new RelationalExpression(Expression::Kind::kRelEQ,
                                           srcExp2,
                                           priExp2);
    // left AND right is false
    auto logExp = std::make_unique<LogicalExpression>(Expression::Kind::kLogicalAnd,
                                                      left,
                                                      right);
    req.set_condition(Expression::encode(*logExp.get()));

    LOG(INFO) << "Build yield...";
    // Return serve props: playerName, teamName, teamCareer, type
    std::vector<std::string> tmpProps;
    auto* yEdge1 = new std::string("101");
    auto* yProp1 = new std::string("playerName");
    EdgePropertyExpression edgePropExp1(yEdge1, yProp1);
    tmpProps.emplace_back(Expression::encode(edgePropExp1));

    auto* yEdge2 = new std::string("101");
    auto* yProp2 = new std::string("teamName");
    EdgePropertyExpression edgePropExp2(yEdge2, yProp2);
    tmpProps.emplace_back(Expression::encode(edgePropExp2));

    auto* yEdge3 = new std::string("101");
    auto* yProp3 = new std::string("teamCareer");
    EdgePropertyExpression edgePropExp3(yEdge3, yProp3);
    tmpProps.emplace_back(Expression::encode(edgePropExp3));

    auto* yEdge4 = new std::string("101");
    auto* yProp4 = new std::string("type");
    EdgePropertyExpression edgePropExp4(yEdge4, yProp4);
    tmpProps.emplace_back(Expression::encode(edgePropExp4));

    addEdgePropInKey(tmpProps);

    req.set_return_props(std::move(tmpProps));
    req.set_insertable(true);

    LOG(INFO) << "Test UpdateEdgeRequest...";
    auto* processor = UpdateEdgeProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(0, (*resp.result_ref()).failed_parts.size());
    EXPECT_EQ(9, (*resp.props_ref()).colNames.size());
    EXPECT_EQ("_inserted", (*resp.props_ref()).colNames[0]);
    EXPECT_EQ("101.playerName", (*resp.props_ref()).colNames[1]);
    EXPECT_EQ("101.teamName", (*resp.props_ref()).colNames[2]);
    EXPECT_EQ("101.teamCareer", (*resp.props_ref()).colNames[3]);
    EXPECT_EQ("101.type", (*resp.props_ref()).colNames[4]);
    EXPECT_EQ(std::string("101.").append(kSrc), (*resp.props_ref()).colNames[5]);
    EXPECT_EQ(std::string("101.").append(kType), (*resp.props_ref()).colNames[6]);
    EXPECT_EQ(std::string("101.").append(kRank), (*resp.props_ref()).colNames[7]);
    EXPECT_EQ(std::string("101.").append(kDst), (*resp.props_ref()).colNames[8]);

    EXPECT_EQ(1, (*resp.props_ref()).rows.size());
    EXPECT_EQ(9, (*resp.props_ref()).rows[0].values.size());

    EXPECT_EQ(true, (*resp.props_ref()).rows[0].values[0].getBool());
    EXPECT_EQ("Brandon Ingram", (*resp.props_ref()).rows[0].values[1].getStr());
    EXPECT_EQ("Lakers", (*resp.props_ref()).rows[0].values[2].getStr());
    EXPECT_EQ(1, (*resp.props_ref()).rows[0].values[3].getInt());
    EXPECT_EQ("trade", (*resp.props_ref()).rows[0].values[4].getStr());
    EXPECT_EQ("Brandon Ingram", (*resp.props_ref()).rows[0].values[5].getStr());
    EXPECT_EQ(101, (*resp.props_ref()).rows[0].values[6].getInt());
    EXPECT_EQ(2016, (*resp.props_ref()).rows[0].values[7].getInt());
    EXPECT_EQ("Lakers", (*resp.props_ref()).rows[0].values[8].getStr());

    // get serve from kvstore directly
    auto prefix = NebulaKeyUtils::edgePrefix(spaceVidLen, partId, srcId, edgeType, rank, dstId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());

    auto reader = RowReaderWrapper::getEdgePropReader(env->schemaMan_,
                                                      spaceId,
                                                      std::abs(edgeType),
                                                      iter->val());
    auto val = reader->getValueByName("playerName");
    EXPECT_EQ("Brandon Ingram", val.getStr());
    val = reader->getValueByName("teamName");
    EXPECT_EQ("Lakers", val.getStr());
    val = reader->getValueByName("teamCareer");
    EXPECT_EQ(1, val.getInt());
    val = reader->getValueByName("type");
    EXPECT_EQ("trade", val.getStr());
}

// Empty value test
TEST(UpdateEdgeTest, CorruptDataTest) {
    fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();

    LOG(INFO) << "Write a vertex with empty value!";
    auto partId = std::hash<std::string>()("Lonzo Ball") % parts + 1;
    VertexID srcId = "Lonzo Ball";
    VertexID dstId = "Lakers";
    EdgeRanking rank = 2017;
    EdgeType edgeType = 101;
    auto key = NebulaKeyUtils::edgeKey(spaceVidLen,
                                       partId,
                                       srcId,
                                       edgeType,
                                       rank,
                                       dstId,
                                       0L);

    std::vector<kvstore::KV> data;
    data.emplace_back(std::make_pair(key, ""));
    folly::Baton<> baton;
    env->kvstore_->asyncMultiPut(spaceId, partId, std::move(data),
        [&](kvstore::ResultCode code) {
            CHECK_EQ(code, kvstore::ResultCode::SUCCEEDED);
            baton.post();
        });
    baton.wait();

    LOG(INFO) << "Build UpdateEdgeRequest...";
    cpp2::UpdateEdgeRequest req;
    req.set_space_id(spaceId);
    req.set_part_id(partId);
    // src = Lonzo Ball, edge_type = 101, ranking = 2017, dst = Lakers
    storage::cpp2::EdgeKey edgeKey;
    edgeKey.set_src(srcId);
    edgeKey.set_edge_type(edgeType);
    edgeKey.set_ranking(rank);
    edgeKey.set_dst(dstId);
    req.set_edge_key(edgeKey);

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedProp> props;
    // int: 101.teamCareer = 2
    cpp2::UpdatedProp uProp1;
    uProp1.set_name("teamCareer");
    ConstantExpression val1(2);
    uProp1.set_value(Expression::encode(val1));
    props.emplace_back(uProp1);

    LOG(INFO) << "Build yield...";
    // Return serve props: playerName, teamName, teamCareer, type
    std::vector<std::string> tmpProps;
    auto* yEdge1 = new std::string("101");
    auto* yProp1 = new std::string("playerName");
    EdgePropertyExpression edgePropExp1(yEdge1, yProp1);
    tmpProps.emplace_back(Expression::encode(edgePropExp1));

    auto* yEdge2 = new std::string("101");
    auto* yProp2 = new std::string("teamName");
    EdgePropertyExpression edgePropExp2(yEdge2, yProp2);
    tmpProps.emplace_back(Expression::encode(edgePropExp2));

    auto* yEdge3 = new std::string("101");
    auto* yProp3 = new std::string("teamCareer");
    EdgePropertyExpression edgePropExp3(yEdge3, yProp3);
    tmpProps.emplace_back(Expression::encode(edgePropExp3));


    req.set_return_props(std::move(tmpProps));
    req.set_insertable(true);

    LOG(INFO) << "Test UpdateEdgeRequest...";
    auto* processor = UpdateEdgeProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(1, (*resp.result_ref()).failed_parts.size());
}

// TTL test
// Data expired, Insertable is false, upsert failed
TEST(UpdateEdgeTest, TTL_NoInsert_Test) {
    FLAGS_mock_ttl_col = true;

    fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();

    EXPECT_TRUE(mockEdgeData(env, parts, spaceVidLen));

    LOG(INFO) << "Build UpdateEdgeRequest...";
    cpp2::UpdateEdgeRequest req;

    auto partId = std::hash<std::string>()("Tim Duncan") % parts + 1;
    req.set_space_id(spaceId);
    req.set_part_id(partId);
    VertexID srcId = "Tim Duncan";
    VertexID dstId = "Spurs";
    EdgeRanking rank = 1997;
    EdgeType edgeType = 101;
    // src = Brandon Ingram, edge_type = 101, ranking = 2016, dst = Lakers
    storage::cpp2::EdgeKey edgeKey;
    edgeKey.set_src(srcId);
    edgeKey.set_edge_type(edgeType);
    edgeKey.set_ranking(rank);
    edgeKey.set_dst(dstId);
    req.set_edge_key(edgeKey);

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedProp> props;
    // int: 101.teamCareer = 20
    cpp2::UpdatedProp uProp1;
    uProp1.set_name("teamCareer");
    ConstantExpression val1(30);
    uProp1.set_value(Expression::encode(val1));
    props.emplace_back(uProp1);

    // bool: 101.birth = 1997
    cpp2::UpdatedProp uProp2;
    uProp2.set_name("type");
    std::string colnew("trade");
    ConstantExpression val2(colnew);
    uProp2.set_value(Expression::encode(val2));
    props.emplace_back(uProp2);
    req.set_updated_props(std::move(props));

    LOG(INFO) << "Build yield...";
    // Return serve props: playerName, teamName, teamCareer, type
    std::vector<std::string> tmpProps;
    auto* yEdge3 = new std::string("101");
    auto* yProp3 = new std::string("teamCareer");
    EdgePropertyExpression edgePropExp3(yEdge3, yProp3);
    tmpProps.emplace_back(Expression::encode(edgePropExp3));

    auto* yEdge4 = new std::string("101");
    auto* yProp4 = new std::string("type");
    EdgePropertyExpression edgePropExp4(yEdge4, yProp4);
    tmpProps.emplace_back(Expression::encode(edgePropExp4));

    req.set_return_props(std::move(tmpProps));
    req.set_insertable(false);

    sleep(FLAGS_mock_ttl_duration + 1);

    LOG(INFO) << "Test UpdateEdgeRequest...";
    auto* processor = UpdateEdgeProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(1, (*resp.result_ref()).failed_parts.size());
}

// TTL test
// Data not exist, Insertable is true, insert data
TEST(UpdateEdgeTest, TTL_Insert_No_Exist_Test) {
    FLAGS_mock_ttl_col = true;

    fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();

    EXPECT_TRUE(mockEdgeData(env, parts, spaceVidLen));

    LOG(INFO) << "Build UpdateEdgeRequest...";
    cpp2::UpdateEdgeRequest req;

    auto partId = std::hash<std::string>()("Tim") % parts + 1;
    req.set_space_id(spaceId);
    req.set_part_id(partId);
    VertexID srcId = "Tim";
    VertexID dstId = "Spurs";
    EdgeRanking rank = 1997;
    EdgeType edgeType = 101;
    // src = Brandon Ingram, edge_type = 101, ranking = 2016, dst = Lakers
    storage::cpp2::EdgeKey edgeKey;
    edgeKey.set_src(srcId);
    edgeKey.set_edge_type(edgeType);
    edgeKey.set_ranking(rank);
    edgeKey.set_dst(dstId);
    req.set_edge_key(edgeKey);

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedProp> props;
    // int: 101.playerName = Tim
    cpp2::UpdatedProp uProp1;
    uProp1.set_name("playerName");
    std::string col1new("Tim");
    ConstantExpression val1(col1new);
    uProp1.set_value(Expression::encode(val1));
    props.emplace_back(uProp1);

    // bool: 101.teamName = 1997
    cpp2::UpdatedProp uProp2;
    uProp2.set_name("teamName");
    std::string col2new("Spurs");
    ConstantExpression val2(col2new);
    uProp2.set_value(Expression::encode(val2));
    props.emplace_back(uProp2);

    // int: 101.teamCareer = 1
    cpp2::UpdatedProp uProp3;
    uProp3.set_name("teamCareer");
    ConstantExpression val3(1);
    uProp3.set_value(Expression::encode(val3));
    props.emplace_back(uProp3);

    req.set_updated_props(std::move(props));

    LOG(INFO) << "Build yield...";
    // Return serve props: playerName, teamName, teamCareer, type
    std::vector<std::string> tmpProps;
    auto* yEdge1 = new std::string("101");
    auto* yProp1 = new std::string("playerName");
    EdgePropertyExpression edgePropExp1(yEdge1, yProp1);
    tmpProps.emplace_back(Expression::encode(edgePropExp1));

    auto* yEdge2 = new std::string("101");
    auto* yProp2 = new std::string("teamName");
    EdgePropertyExpression edgePropExp2(yEdge2, yProp2);
    tmpProps.emplace_back(Expression::encode(edgePropExp2));

    auto* yEdge3 = new std::string("101");
    auto* yProp3 = new std::string("teamCareer");
    EdgePropertyExpression edgePropExp3(yEdge3, yProp3);
    tmpProps.emplace_back(Expression::encode(edgePropExp3));

    auto* yEdge4 = new std::string("101");
    auto* yProp4 = new std::string("type");
    EdgePropertyExpression edgePropExp4(yEdge4, yProp4);
    tmpProps.emplace_back(Expression::encode(edgePropExp4));

    addEdgePropInKey(tmpProps);

    req.set_return_props(std::move(tmpProps));
    req.set_insertable(true);

    sleep(FLAGS_mock_ttl_duration + 1);

    LOG(INFO) << "Test UpdateEdgeRequest...";
    auto* processor = UpdateEdgeProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(0, (*resp.result_ref()).failed_parts.size());
    EXPECT_EQ(9, (*resp.props_ref()).colNames.size());
    EXPECT_EQ("_inserted", (*resp.props_ref()).colNames[0]);
    EXPECT_EQ("101.playerName", (*resp.props_ref()).colNames[1]);
    EXPECT_EQ("101.teamName", (*resp.props_ref()).colNames[2]);
    EXPECT_EQ("101.teamCareer", (*resp.props_ref()).colNames[3]);
    EXPECT_EQ("101.type", (*resp.props_ref()).colNames[4]);
    EXPECT_EQ(std::string("101.").append(kSrc), (*resp.props_ref()).colNames[5]);
    EXPECT_EQ(std::string("101.").append(kType), (*resp.props_ref()).colNames[6]);
    EXPECT_EQ(std::string("101.").append(kRank), (*resp.props_ref()).colNames[7]);
    EXPECT_EQ(std::string("101.").append(kDst), (*resp.props_ref()).colNames[8]);

    EXPECT_EQ(1, (*resp.props_ref()).rows.size());
    EXPECT_EQ(9, (*resp.props_ref()).rows[0].values.size());

    EXPECT_EQ(true, (*resp.props_ref()).rows[0].values[0].getBool());
    EXPECT_EQ("Tim", (*resp.props_ref()).rows[0].values[1].getStr());
    EXPECT_EQ("Spurs", (*resp.props_ref()).rows[0].values[2].getStr());
    EXPECT_EQ(1, (*resp.props_ref()).rows[0].values[3].getInt());
    EXPECT_EQ("trade", (*resp.props_ref()).rows[0].values[4].getStr());
    EXPECT_EQ("Tim", (*resp.props_ref()).rows[0].values[5].getStr());
    EXPECT_EQ(101, (*resp.props_ref()).rows[0].values[6].getInt());
    EXPECT_EQ(1997, (*resp.props_ref()).rows[0].values[7].getInt());
    EXPECT_EQ("Spurs", (*resp.props_ref()).rows[0].values[8].getStr());

    // get serve from kvstore directly
    auto prefix = NebulaKeyUtils::edgePrefix(spaceVidLen, partId, srcId, edgeType, rank, dstId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());

    auto reader = RowReaderWrapper::getEdgePropReader(env->schemaMan_,
                                                      spaceId,
                                                      std::abs(edgeType),
                                                      iter->val());
    auto val = reader->getValueByName("playerName");
    EXPECT_EQ("Tim", val.getStr());
    val = reader->getValueByName("teamName");
    EXPECT_EQ("Spurs", val.getStr());
    val = reader->getValueByName("teamCareer");
    EXPECT_EQ(1, val.getInt());
    val = reader->getValueByName("type");
    EXPECT_EQ("trade", val.getStr());
}

// Data expired, Insertable is true, insert data
TEST(UpdateEdgeTest, TTL_Insert_Test) {
    FLAGS_mock_ttl_col = true;

    fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();
    EXPECT_TRUE(mockEdgeData(env, parts, spaceVidLen));

    LOG(INFO) << "Build UpdateEdgeRequest...";
    cpp2::UpdateEdgeRequest req;

    auto partId = std::hash<std::string>()("Tim Duncan") % parts + 1;
    req.set_space_id(spaceId);
    req.set_part_id(partId);
    VertexID srcId = "Tim Duncan";
    VertexID dstId = "Spurs";
    EdgeRanking rank = 1997;
    EdgeType edgeType = 101;
    // src = Brandon Ingram, edge_type = 101, ranking = 2016, dst = Lakers
    storage::cpp2::EdgeKey edgeKey;
    edgeKey.set_src(srcId);
    edgeKey.set_edge_type(edgeType);
    edgeKey.set_ranking(rank);
    edgeKey.set_dst(dstId);
    req.set_edge_key(edgeKey);

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedProp> props;
    // int: 101.playerName = Tim
    cpp2::UpdatedProp uProp1;
    uProp1.set_name("playerName");
    std::string col1new("Tim Duncan");
    ConstantExpression val1(col1new);
    uProp1.set_value(Expression::encode(val1));
    props.emplace_back(uProp1);

    // bool: 101.teamName = 1997
    cpp2::UpdatedProp uProp2;
    uProp2.set_name("teamName");
    std::string col2new("Spurs");
    ConstantExpression val2(col2new);
    uProp2.set_value(Expression::encode(val2));
    props.emplace_back(uProp2);

    // bool: 101.teamCareer = 40
    cpp2::UpdatedProp uProp3;
    uProp3.set_name("teamCareer");
    ConstantExpression val3(40);
    uProp3.set_value(Expression::encode(val3));
    props.emplace_back(uProp3);

    req.set_updated_props(std::move(props));

    LOG(INFO) << "Build yield...";
    // Return serve props: playerName, teamName, teamCareer, type
    std::vector<std::string> tmpProps;
    auto* yEdge1 = new std::string("101");
    auto* yProp1 = new std::string("playerName");
    EdgePropertyExpression edgePropExp1(yEdge1, yProp1);
    tmpProps.emplace_back(Expression::encode(edgePropExp1));

    auto* yEdge2 = new std::string("101");
    auto* yProp2 = new std::string("teamName");
    EdgePropertyExpression edgePropExp2(yEdge2, yProp2);
    tmpProps.emplace_back(Expression::encode(edgePropExp2));

    auto* yEdge3 = new std::string("101");
    auto* yProp3 = new std::string("teamCareer");
    EdgePropertyExpression edgePropExp3(yEdge3, yProp3);
    tmpProps.emplace_back(Expression::encode(edgePropExp3));

    auto* yEdge4 = new std::string("101");
    auto* yProp4 = new std::string("type");
    EdgePropertyExpression edgePropExp4(yEdge4, yProp4);
    tmpProps.emplace_back(Expression::encode(edgePropExp4));

    addEdgePropInKey(tmpProps);

    req.set_return_props(std::move(tmpProps));
    req.set_insertable(true);

    sleep(FLAGS_mock_ttl_duration + 1);

    LOG(INFO) << "Test UpdateEdgeRequest...";
    auto* processor = UpdateEdgeProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(0, (*resp.result_ref()).failed_parts.size());
    EXPECT_EQ(9, (*resp.props_ref()).colNames.size());
    EXPECT_EQ("_inserted", (*resp.props_ref()).colNames[0]);
    EXPECT_EQ("101.playerName", (*resp.props_ref()).colNames[1]);
    EXPECT_EQ("101.teamName", (*resp.props_ref()).colNames[2]);
    EXPECT_EQ("101.teamCareer", (*resp.props_ref()).colNames[3]);
    EXPECT_EQ("101.type", (*resp.props_ref()).colNames[4]);
    EXPECT_EQ(std::string("101.").append(kSrc), (*resp.props_ref()).colNames[5]);
    EXPECT_EQ(std::string("101.").append(kType), (*resp.props_ref()).colNames[6]);
    EXPECT_EQ(std::string("101.").append(kRank), (*resp.props_ref()).colNames[7]);
    EXPECT_EQ(std::string("101.").append(kDst), (*resp.props_ref()).colNames[8]);

    EXPECT_EQ(1, (*resp.props_ref()).rows.size());
    EXPECT_EQ(9, (*resp.props_ref()).rows[0].values.size());

    EXPECT_EQ(true, (*resp.props_ref()).rows[0].values[0].getBool());
    EXPECT_EQ("Tim Duncan", (*resp.props_ref()).rows[0].values[1].getStr());
    EXPECT_EQ("Spurs", (*resp.props_ref()).rows[0].values[2].getStr());
    EXPECT_EQ(40, (*resp.props_ref()).rows[0].values[3].getInt());
    EXPECT_EQ("trade", (*resp.props_ref()).rows[0].values[4].getStr());
    EXPECT_EQ("Tim Duncan", (*resp.props_ref()).rows[0].values[5].getStr());
    EXPECT_EQ(101, (*resp.props_ref()).rows[0].values[6].getInt());
    EXPECT_EQ(1997, (*resp.props_ref()).rows[0].values[7].getInt());
    EXPECT_EQ("Spurs", (*resp.props_ref()).rows[0].values[8].getStr());

    // get serve from kvstore directly
    auto prefix = NebulaKeyUtils::edgePrefix(spaceVidLen, partId, srcId, edgeType, rank, dstId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());

    auto schema = env->schemaMan_->getEdgeSchema(spaceId, std::abs(edgeType));
    EXPECT_TRUE(schema != NULL);

    int count = 0;
    while (iter && iter->valid()) {
        auto edgeReader = RowReaderWrapper::getRowReader(schema.get(), iter->val());
        auto val = edgeReader->getValueByName("playerName");
        EXPECT_EQ("Tim Duncan", val.getStr());

        val = edgeReader->getValueByName("teamName");
        EXPECT_EQ("Spurs", val.getStr());

        if (count == 1) {
            val = edgeReader->getValueByName("teamCareer");
            EXPECT_EQ(19, val.getInt());
            val = edgeReader->getValueByName("type");
            EXPECT_EQ("zzzzz", val.getStr());
        } else {
            val = edgeReader->getValueByName("teamCareer");
            EXPECT_EQ(40, val.getInt());
            val = edgeReader->getValueByName("type");
            EXPECT_EQ("trade", val.getStr());
        }
        iter->next();
        count++;
    }
    EXPECT_EQ(2, count);
}

// Update success, yield _src, _dst, _rank, _type
TEST(UpdateEdgeTest, Yield_Key_Test) {
    fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();

    EXPECT_TRUE(mockEdgeData(env, parts, spaceVidLen));

    LOG(INFO) << "Build UpdateEdgeRequest...";
    cpp2::UpdateEdgeRequest req;
    auto partId = std::hash<std::string>()("Tim Duncan") % parts + 1;
    req.set_space_id(spaceId);
    req.set_part_id(partId);
    // src = Tim Duncan, edge_type = 101, ranking = 1997, dst = Spurs
    VertexID srcId = "Tim Duncan";
    VertexID dstId = "Spurs";
    EdgeRanking rank = 1997;
    EdgeType edgeType = 101;
    storage::cpp2::EdgeKey edgeKey;
    edgeKey.set_src(srcId);
    edgeKey.set_edge_type(edgeType);
    edgeKey.set_ranking(rank);
    edgeKey.set_dst(dstId);
    req.set_edge_key(edgeKey);

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedProp> props;
    // int: 101.teamCareer = 20
    cpp2::UpdatedProp uProp1;
    uProp1.set_name("teamCareer");
    ConstantExpression val1(20);
    uProp1.set_value(Expression::encode(val1));
    props.emplace_back(uProp1);

    // bool: 101.type = trade
    cpp2::UpdatedProp uProp2;
    uProp2.set_name("type");
    std::string colnew("trade");
    ConstantExpression val2(colnew);
    uProp2.set_value(Expression::encode(val2));
    props.emplace_back(uProp2);
    req.set_updated_props(std::move(props));

    LOG(INFO) << "Build yield...";
    // Return serve props: playerName, teamName, teamCareer, type
    std::vector<std::string> tmpProps;
    auto* yEdge1 = new std::string("101");
    auto* yProp1 = new std::string("playerName");
    EdgePropertyExpression edgePropExp1(yEdge1, yProp1);
    tmpProps.emplace_back(Expression::encode(edgePropExp1));

    auto* yEdge2 = new std::string("101");
    auto* yProp2 = new std::string("teamName");
    EdgePropertyExpression edgePropExp2(yEdge2, yProp2);
    tmpProps.emplace_back(Expression::encode(edgePropExp2));

    auto* yEdge3 = new std::string("101");
    auto* yProp3 = new std::string("teamCareer");
    EdgePropertyExpression edgePropExp3(yEdge3, yProp3);
    tmpProps.emplace_back(Expression::encode(edgePropExp3));

    auto* yEdge4 = new std::string("101");
    auto* yProp4 = new std::string("type");
    EdgePropertyExpression edgePropExp4(yEdge4, yProp4);
    tmpProps.emplace_back(Expression::encode(edgePropExp4));

    // same as pass by EdgePropertyExpression
    auto* yEdge5 = new std::string("101");
    EdgeSrcIdExpression edgeSrcIdExp5(yEdge5);
    tmpProps.emplace_back(Expression::encode(edgeSrcIdExp5));

    auto* yEdge6 = new std::string("101");
    EdgeDstIdExpression edgeDstIdExp6(yEdge6);
    tmpProps.emplace_back(Expression::encode(edgeDstIdExp6));

    auto* yEdge7 = new std::string("101");
    EdgeRankExpression edgeRankExp7(yEdge7);
    tmpProps.emplace_back(Expression::encode(edgeRankExp7));

    auto* yEdge8 = new std::string("101");
    EdgeTypeExpression edgeTypeExp8(yEdge8);
    tmpProps.emplace_back(Expression::encode(edgeTypeExp8));

    req.set_return_props(std::move(tmpProps));
    req.set_insertable(false);

    LOG(INFO) << "Test UpdateEdgeRequest...";
    auto* processor = UpdateEdgeProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(0, (*resp.result_ref()).failed_parts.size());
    EXPECT_EQ(9, (*resp.props_ref()).colNames.size());
    EXPECT_EQ("_inserted", (*resp.props_ref()).colNames[0]);
    EXPECT_EQ("101.playerName", (*resp.props_ref()).colNames[1]);
    EXPECT_EQ("101.teamName", (*resp.props_ref()).colNames[2]);
    EXPECT_EQ("101.teamCareer", (*resp.props_ref()).colNames[3]);
    EXPECT_EQ("101.type", (*resp.props_ref()).colNames[4]);
    EXPECT_EQ("101._src", (*resp.props_ref()).colNames[5]);
    EXPECT_EQ("101._dst", (*resp.props_ref()).colNames[6]);
    EXPECT_EQ("101._rank", (*resp.props_ref()).colNames[7]);
    EXPECT_EQ("101._type", (*resp.props_ref()).colNames[8]);

    EXPECT_EQ(1, (*resp.props_ref()).rows.size());
    EXPECT_EQ(9, (*resp.props_ref()).rows[0].values.size());

    EXPECT_EQ(false, (*resp.props_ref()).rows[0].values[0].getBool());
    EXPECT_EQ("Tim Duncan", (*resp.props_ref()).rows[0].values[1].getStr());
    EXPECT_EQ("Spurs", (*resp.props_ref()).rows[0].values[2].getStr());
    EXPECT_EQ(20, (*resp.props_ref()).rows[0].values[3].getInt());
    EXPECT_EQ("trade", (*resp.props_ref()).rows[0].values[4].getStr());
    EXPECT_EQ("Tim Duncan", (*resp.props_ref()).rows[0].values[5].getStr());
    EXPECT_EQ("Spurs", (*resp.props_ref()).rows[0].values[6].getStr());
    EXPECT_EQ(1997, (*resp.props_ref()).rows[0].values[7].getInt());
    EXPECT_EQ(101, (*resp.props_ref()).rows[0].values[8].getInt());

    // get serve from kvstore directly
    auto prefix = NebulaKeyUtils::edgePrefix(spaceVidLen, partId, srcId, edgeType, rank, dstId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());

    auto edgeReader = RowReaderWrapper::getEdgePropReader(env->schemaMan_,
                                                          spaceId,
                                                          std::abs(edgeType),
                                                          iter->val());
    auto val = edgeReader->getValueByName("playerName");
    EXPECT_EQ("Tim Duncan", val.getStr());
    val = edgeReader->getValueByName("teamName");
    EXPECT_EQ("Spurs", val.getStr());
    val = edgeReader->getValueByName("teamCareer");
    EXPECT_EQ(20, val.getInt());
    val = edgeReader->getValueByName("type");
    EXPECT_EQ("trade", val.getStr());
}

// Update faild, yield edge is illegal
TEST(UpdateEdgeTest, Yield_Illegal_Key_Test) {
    fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();
    EXPECT_TRUE(mockEdgeData(env, parts, spaceVidLen));

    LOG(INFO) << "Build UpdateEdgeRequest...";
    cpp2::UpdateEdgeRequest req;
    auto partId = std::hash<std::string>()("Tim Duncan") % parts + 1;
    req.set_space_id(spaceId);
    req.set_part_id(partId);
    // src = Tim Duncan, edge_type = 101, ranking = 1997, dst = Spurs
    VertexID srcId = "Tim Duncan";
    VertexID dstId = "Spurs";
    EdgeRanking rank = 1997;
    EdgeType edgeType = 101;
    storage::cpp2::EdgeKey edgeKey;
    edgeKey.set_src(srcId);
    edgeKey.set_edge_type(edgeType);
    edgeKey.set_ranking(rank);
    edgeKey.set_dst(dstId);
    req.set_edge_key(edgeKey);

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedProp> props;
    // int: 101.teamCareer = 20
    cpp2::UpdatedProp uProp1;
    uProp1.set_name("teamCareer");
    ConstantExpression val1(20);
    uProp1.set_value(Expression::encode(val1));
    props.emplace_back(uProp1);

    // bool: 101.type = trade
    cpp2::UpdatedProp uProp2;
    uProp2.set_name("type");
    std::string colnew("trade");
    ConstantExpression val2(colnew);
    uProp2.set_value(Expression::encode(val2));
    props.emplace_back(uProp2);
    req.set_updated_props(std::move(props));

    LOG(INFO) << "Build yield...";
    // Return serve props: playerName, teamName, teamCareer, type
    std::vector<std::string> tmpProps;
    auto* yEdge1 = new std::string("101");
    auto* yProp1 = new std::string("playerName");
    EdgePropertyExpression edgePropExp1(yEdge1, yProp1);
    tmpProps.emplace_back(Expression::encode(edgePropExp1));

    auto* yEdge2 = new std::string("101");
    auto* yProp2 = new std::string("teamName");
    EdgePropertyExpression edgePropExp2(yEdge2, yProp2);
    tmpProps.emplace_back(Expression::encode(edgePropExp2));

    auto* yEdge3 = new std::string("101");
    auto* yProp3 = new std::string("teamCareer");
    EdgePropertyExpression edgePropExp3(yEdge3, yProp3);
    tmpProps.emplace_back(Expression::encode(edgePropExp3));

    auto* yEdge4 = new std::string("101");
    auto* yProp4 = new std::string("type");
    EdgePropertyExpression edgePropExp4(yEdge4, yProp4);
    tmpProps.emplace_back(Expression::encode(edgePropExp4));

    auto* yEdge5 = new std::string("1010");
    EdgeSrcIdExpression edgeSrcIdExp5(yEdge5);
    tmpProps.emplace_back(Expression::encode(edgeSrcIdExp5));

    auto* yEdge6 = new std::string("1011");
    EdgeDstIdExpression edgeDstIdExp6(yEdge6);
    tmpProps.emplace_back(Expression::encode(edgeDstIdExp6));

    auto* yEdge7 = new std::string("1012");
    EdgeRankExpression edgeRankExp7(yEdge7);
    tmpProps.emplace_back(Expression::encode(edgeRankExp7));

    auto* yEdge8 = new std::string("1013");
    EdgeTypeExpression edgeTypeExp8(yEdge8);
    tmpProps.emplace_back(Expression::encode(edgeTypeExp8));

    req.set_return_props(std::move(tmpProps));
    req.set_insertable(false);

    LOG(INFO) << "Test UpdateEdgeRequest...";
    auto* processor = UpdateEdgeProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(1, (*resp.result_ref()).failed_parts.size());

    // get serve from kvstore directly
    auto prefix = NebulaKeyUtils::edgePrefix(spaceVidLen, partId, srcId, edgeType, rank, dstId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());

    auto edgeReader = RowReaderWrapper::getEdgePropReader(env->schemaMan_,
                                                          spaceId,
                                                          std::abs(edgeType),
                                                          iter->val());
    auto val = edgeReader->getValueByName("playerName");
    EXPECT_EQ("Tim Duncan", val.getStr());
    val = edgeReader->getValueByName("teamName");
    EXPECT_EQ("Spurs", val.getStr());
    val = edgeReader->getValueByName("teamCareer");
    EXPECT_EQ(19, val.getInt());
    val = edgeReader->getValueByName("type");
    EXPECT_EQ("zzzzz", val.getStr());
}

// Upsert, insert faild
// teamCareer filed has not default value and not nullable, not in set clause
TEST(UpdateEdgeTest, Insertable_No_Default_Test) {
    fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();
    EXPECT_TRUE(mockEdgeData(env, parts, spaceVidLen));

    LOG(INFO) << "Build UpdateEdgeRequest...";
    cpp2::UpdateEdgeRequest req;

    auto partId = std::hash<std::string>()("Brandon Ingram") % parts + 1;
    req.set_space_id(spaceId);
    req.set_part_id(partId);
    VertexID srcId = "Brandon Ingram";
    VertexID dstId = "Lakers";
    EdgeRanking rank = 2016;
    EdgeType edgeType = 101;
    // src = Brandon Ingram, edge_type = 101, ranking = 2016, dst = Lakers
    storage::cpp2::EdgeKey edgeKey;
    edgeKey.set_src(srcId);
    edgeKey.set_edge_type(edgeType);
    edgeKey.set_ranking(rank);
    edgeKey.set_dst(dstId);
    req.set_edge_key(edgeKey);

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedProp> props;

    // string: 101.playerName = Brandon Ingram
    cpp2::UpdatedProp uProp1;
    uProp1.set_name("playerName");
    std::string col1new("Brandon Ingram");
    ConstantExpression val1(col1new);
    uProp1.set_value(Expression::encode(val1));
    props.emplace_back(uProp1);

    // string: 101.teamName = Lakers
    cpp2::UpdatedProp uProp2;
    uProp2.set_name("teamName");
    std::string col2new("Lakers");
    ConstantExpression val2(col2new);
    uProp2.set_value(Expression::encode(val2));
    props.emplace_back(uProp2);

    // int: 101.startYear = 2016
    cpp2::UpdatedProp uProp3;
    uProp3.set_name("startYear");
    ConstantExpression val3(2016L);
    uProp3.set_value(Expression::encode(val3));
    props.emplace_back(uProp3);

    LOG(INFO) << "Build yield...";
    // Return serve props: playerName, teamName, teamCareer
    std::vector<std::string> tmpProps;
    auto* yEdge1 = new std::string("101");
    auto* yProp1 = new std::string("playerName");
    EdgePropertyExpression edgePropExp1(yEdge1, yProp1);
    tmpProps.emplace_back(Expression::encode(edgePropExp1));

    auto* yEdge2 = new std::string("101");
    auto* yProp2 = new std::string("teamName");
    EdgePropertyExpression edgePropExp2(yEdge2, yProp2);
    tmpProps.emplace_back(Expression::encode(edgePropExp2));

    auto* yEdge3 = new std::string("101");
    auto* yProp3 = new std::string("teamCareer");
    EdgePropertyExpression edgePropExp3(yEdge3, yProp3);
    tmpProps.emplace_back(Expression::encode(edgePropExp3));

    req.set_return_props(std::move(tmpProps));
    req.set_insertable(true);

    LOG(INFO) << "Test UpdateEdgeRequest...";
    auto* processor = UpdateEdgeProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(1, (*resp.result_ref()).failed_parts.size());
}

// Upsert, insert success
// teamCareer filed has not default value and not nullable, but in set clause
TEST(UpdateEdgeTest, Insertable_In_Set_Test) {
    fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();
    EXPECT_TRUE(mockEdgeData(env, parts, spaceVidLen));

    LOG(INFO) << "Build UpdateEdgeRequest...";
    cpp2::UpdateEdgeRequest req;

    auto partId = std::hash<std::string>()("Brandon Ingram") % parts + 1;
    req.set_space_id(spaceId);
    req.set_part_id(partId);
    VertexID srcId = "Brandon Ingram";
    VertexID dstId = "Lakers";
    EdgeRanking rank = 2016;
    EdgeType edgeType = 101;
    // src = Brandon Ingram, edge_type = 101, ranking = 2016, dst = Lakers
    storage::cpp2::EdgeKey edgeKey;
    edgeKey.set_src(srcId);
    edgeKey.set_edge_type(edgeType);
    edgeKey.set_ranking(rank);
    edgeKey.set_dst(dstId);
    req.set_edge_key(edgeKey);

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedProp> props;

    // string: 101.playerName = Brandon Ingram
    cpp2::UpdatedProp uProp1;
    uProp1.set_name("playerName");
    std::string col1new("Brandon Ingram");
    ConstantExpression val1(col1new);
    uProp1.set_value(Expression::encode(val1));
    props.emplace_back(uProp1);

    // string: 101.teamName = Lakers
    cpp2::UpdatedProp uProp2;
    uProp2.set_name("teamName");
    std::string col2new("Lakers");
    ConstantExpression val2(col2new);
    uProp2.set_value(Expression::encode(val2));
    props.emplace_back(uProp2);

    // int: 101.startYear = 2016
    cpp2::UpdatedProp uProp3;
    uProp3.set_name("startYear");
    ConstantExpression val3(2016L);
    uProp3.set_value(Expression::encode(val3));
    props.emplace_back(uProp3);

    // int: 101.teamCareer = 1
    cpp2::UpdatedProp uProp4;
    uProp4.set_name("teamCareer");
    ConstantExpression val4(1L);
    uProp4.set_value(Expression::encode(val4));
    props.emplace_back(uProp4);
    req.set_updated_props(std::move(props));

    LOG(INFO) << "Build yield...";
    // Return serve props: playerName, teamName, teamCareer
    std::vector<std::string> tmpProps;
    auto* yEdge1 = new std::string("101");
    auto* yProp1 = new std::string("playerName");
    EdgePropertyExpression edgePropExp1(yEdge1, yProp1);
    tmpProps.emplace_back(Expression::encode(edgePropExp1));

    auto* yEdge2 = new std::string("101");
    auto* yProp2 = new std::string("teamName");
    EdgePropertyExpression edgePropExp2(yEdge2, yProp2);
    tmpProps.emplace_back(Expression::encode(edgePropExp2));

    auto* yEdge3 = new std::string("101");
    auto* yProp3 = new std::string("teamCareer");
    EdgePropertyExpression edgePropExp3(yEdge3, yProp3);
    tmpProps.emplace_back(Expression::encode(edgePropExp3));

    addEdgePropInKey(tmpProps);

    req.set_return_props(std::move(tmpProps));
    req.set_insertable(true);

    LOG(INFO) << "Test UpdateEdgeRequest...";
    auto* processor = UpdateEdgeProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(0, (*resp.result_ref()).failed_parts.size());
    EXPECT_EQ(8, (*resp.props_ref()).colNames.size());
    EXPECT_EQ("_inserted", (*resp.props_ref()).colNames[0]);
    EXPECT_EQ("101.playerName", (*resp.props_ref()).colNames[1]);
    EXPECT_EQ("101.teamName", (*resp.props_ref()).colNames[2]);
    EXPECT_EQ("101.teamCareer", (*resp.props_ref()).colNames[3]);
    EXPECT_EQ(std::string("101.").append(kSrc), (*resp.props_ref()).colNames[4]);
    EXPECT_EQ(std::string("101.").append(kType), (*resp.props_ref()).colNames[5]);
    EXPECT_EQ(std::string("101.").append(kRank), (*resp.props_ref()).colNames[6]);
    EXPECT_EQ(std::string("101.").append(kDst), (*resp.props_ref()).colNames[7]);

    EXPECT_EQ(1, (*resp.props_ref()).rows.size());
    EXPECT_EQ(8, (*resp.props_ref()).rows[0].values.size());
    EXPECT_EQ(true, (*resp.props_ref()).rows[0].values[0].getBool());
    EXPECT_EQ("Brandon Ingram", (*resp.props_ref()).rows[0].values[1].getStr());
    EXPECT_EQ("Lakers", (*resp.props_ref()).rows[0].values[2].getStr());
    EXPECT_EQ(1, (*resp.props_ref()).rows[0].values[3].getInt());
    EXPECT_EQ("Brandon Ingram", (*resp.props_ref()).rows[0].values[4].getStr());
    EXPECT_EQ(101, (*resp.props_ref()).rows[0].values[5].getInt());
    EXPECT_EQ(2016, (*resp.props_ref()).rows[0].values[6].getInt());
    EXPECT_EQ("Lakers", (*resp.props_ref()).rows[0].values[7].getStr());

    // get serve from kvstore directly
    auto prefix = NebulaKeyUtils::edgePrefix(spaceVidLen, partId, srcId, edgeType, rank, dstId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());

    auto reader = RowReaderWrapper::getEdgePropReader(env->schemaMan_,
                                                      spaceId,
                                                      std::abs(edgeType),
                                                      iter->val());
    auto val = reader->getValueByName("playerName");
    EXPECT_EQ("Brandon Ingram", val.getStr());
    val = reader->getValueByName("teamName");
    EXPECT_EQ("Lakers", val.getStr());
    val = reader->getValueByName("teamCareer");
    EXPECT_EQ(1, val.getInt());
}

// update uses another edge test, failed
TEST(UpdateEdgeTest, Update_Multi_edge_Test) {
    fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();
    ASSERT_TRUE(QueryTestUtils::mockEdgeData(env, parts));

    LOG(INFO) << "Build UpdateEdgeRequest...";
    cpp2::UpdateEdgeRequest req;

    auto partId = std::hash<std::string>()("Tim Duncan") % parts + 1;
    req.set_space_id(spaceId);
    req.set_part_id(partId);
    VertexID srcId = "Tim Duncan";
    VertexID dstId = "Spurs";
    EdgeRanking rank = 1997;
    EdgeType edgeType = 101;

    storage::cpp2::EdgeKey edgeKey;
    edgeKey.set_src(srcId);
    edgeKey.set_edge_type(edgeType);
    edgeKey.set_ranking(rank);
    edgeKey.set_dst(dstId);
    req.set_edge_key(edgeKey);

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedProp> props;

    // string: Serve.teamCareer_ = Teammate.endYear_
    cpp2::UpdatedProp uProp1;
    uProp1.set_name("teamCareer");
    auto* uEdge = new std::string("102");
    auto* uProp = new std::string("endYear");
    EdgePropertyExpression val1(uEdge, uProp);
    uProp1.set_value(Expression::encode(val1));
    props.emplace_back(uProp1);
    req.set_updated_props(std::move(props));

    LOG(INFO) << "Build yield...";
    // Return serve props: playerName, teamCareer
    std::vector<std::string> tmpProps;
    auto* yEdge1 = new std::string("101");
    auto* yProp1 = new std::string("playerName");
    EdgePropertyExpression edgePropExp1(yEdge1, yProp1);
    tmpProps.emplace_back(Expression::encode(edgePropExp1));

    auto* yEdge2 = new std::string("101");
    auto* yProp2 = new std::string("teamName");
    EdgePropertyExpression edgePropExp2(yEdge2, yProp2);
    tmpProps.emplace_back(Expression::encode(edgePropExp2));

    auto* yEdge3 = new std::string("101");
    auto* yProp3 = new std::string("teamCareer");
    EdgePropertyExpression edgePropExp3(yEdge3, yProp3);
    tmpProps.emplace_back(Expression::encode(edgePropExp3));

    addEdgePropInKey(tmpProps);

    req.set_return_props(std::move(tmpProps));
    req.set_insertable(false);

    LOG(INFO) << "Test UpdateEdgeRequest...";
    auto* processor = UpdateEdgeProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(1, (*resp.result_ref()).failed_parts.size());

    // get serve from kvstore directly
    auto prefix = NebulaKeyUtils::edgePrefix(spaceVidLen, partId, srcId, edgeType, rank, dstId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());

    auto reader = RowReaderWrapper::getEdgePropReader(env->schemaMan_,
                                                      spaceId,
                                                      std::abs(edgeType),
                                                      iter->val());
    auto val = reader->getValueByName("playerName");
    EXPECT_EQ("Tim Duncan", val.getStr());
    val = reader->getValueByName("teamName");
    EXPECT_EQ("Spurs", val.getStr());
    val = reader->getValueByName("teamCareer");
    EXPECT_EQ(19, val.getInt());
}


// upsert uses another edge test, failed
TEST(UpdateEdgeTest, Upsert_Multi_edge_Test) {
    fs::TempDir rootPath("/tmp/UpdateEdgeTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto parts = cluster.getTotalParts();

    GraphSpaceID spaceId = 1;
    auto status = env->schemaMan_->getSpaceVidLen(spaceId);
    ASSERT_TRUE(status.ok());
    auto spaceVidLen = status.value();
    ASSERT_TRUE(QueryTestUtils::mockEdgeData(env, parts));

    LOG(INFO) << "Build UpdateEdgeRequest...";
    cpp2::UpdateEdgeRequest req;

    auto partId = std::hash<std::string>()("Tim Duncan") % parts + 1;
    req.set_space_id(spaceId);
    req.set_part_id(partId);
    VertexID srcId = "Tim Duncan";
    VertexID dstId = "Spurs";
    EdgeRanking rank = 1997;
    EdgeType edgeType = 101;
    // src = Brandon Ingram, edge_type = 101, ranking = 2016, dst = Lakers
    storage::cpp2::EdgeKey edgeKey;
    edgeKey.set_src(srcId);
    edgeKey.set_edge_type(edgeType);
    edgeKey.set_ranking(rank);
    edgeKey.set_dst(dstId);
    req.set_edge_key(edgeKey);

    LOG(INFO) << "Build updated props...";
    std::vector<cpp2::UpdatedProp> props;

    // string: Serve.teamCareer_ = Teammate.endYear_
    cpp2::UpdatedProp uProp1;
    uProp1.set_name("teamCareer");
    auto* uEdge = new std::string("102");
    auto* uProp = new std::string("endYear");
    EdgePropertyExpression val1(uEdge, uProp);
    uProp1.set_value(Expression::encode(val1));
    props.emplace_back(uProp1);
    req.set_updated_props(std::move(props));

    LOG(INFO) << "Build yield...";
    // Return serve props: playerName, teamName, teamCareer
    std::vector<std::string> tmpProps;
    auto* yEdge1 = new std::string("101");
    auto* yProp1 = new std::string("playerName");
    EdgePropertyExpression edgePropExp1(yEdge1, yProp1);
    tmpProps.emplace_back(Expression::encode(edgePropExp1));

    auto* yEdge2 = new std::string("101");
    auto* yProp2 = new std::string("teamName");
    EdgePropertyExpression edgePropExp2(yEdge2, yProp2);
    tmpProps.emplace_back(Expression::encode(edgePropExp2));

    auto* yEdge3 = new std::string("101");
    auto* yProp3 = new std::string("teamCareer");
    EdgePropertyExpression edgePropExp3(yEdge3, yProp3);
    tmpProps.emplace_back(Expression::encode(edgePropExp3));

    addEdgePropInKey(tmpProps);

    req.set_return_props(std::move(tmpProps));
    req.set_insertable(true);

    LOG(INFO) << "Test UpdateEdgeRequest...";
    auto* processor = UpdateEdgeProcessor::instance(env, nullptr);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(1, (*resp.result_ref()).failed_parts.size());

    // get serve from kvstore directly
    auto prefix = NebulaKeyUtils::edgePrefix(spaceVidLen, partId, srcId, edgeType, rank, dstId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
    EXPECT_EQ(kvstore::ResultCode::SUCCEEDED, ret);
    EXPECT_TRUE(iter && iter->valid());

    auto reader = RowReaderWrapper::getEdgePropReader(env->schemaMan_,
                                                      spaceId,
                                                      std::abs(edgeType),
                                                      iter->val());
    auto val = reader->getValueByName("playerName");
    EXPECT_EQ("Tim Duncan", val.getStr());
    val = reader->getValueByName("teamName");
    EXPECT_EQ("Spurs", val.getStr());
    val = reader->getValueByName("teamCareer");
    EXPECT_EQ(19, val.getInt());
}

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
