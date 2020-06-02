/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "common/base/Base.h"
#include <gtest/gtest.h>
#include "common/fs/TempDir.h"
#include "storage/query/GetPropProcessor.h"
#include "storage/test/QueryTestUtils.h"

namespace nebula {
namespace storage {

cpp2::GetPropRequest buildVertexRequest(
        int32_t totalParts,
        const std::vector<VertexID> vertices,
        const std::vector<std::pair<TagID, std::vector<std::string>>> tags,
        bool returnAllProps = true) {
    std::hash<std::string> hash;
    cpp2::GetPropRequest req;
    req.space_id = 1;
    req.column_names.emplace_back("_vid");
    for (const auto& vertex : vertices) {
        PartitionID partId = (hash(vertex) % totalParts) + 1;
        nebula::Row row;
        row.columns.emplace_back(vertex);
        req.parts[partId].emplace_back(std::move(row));
    }

    UNUSED(tags);
    std::vector<cpp2::PropExp> props;
    if (props.empty() && returnAllProps) {
        req.set_props(std::move(props));
    }
    return req;
}

cpp2::GetPropRequest buildEdgeRequest(
        int32_t totalParts,
        const std::vector<cpp2::EdgeKey> edgeKeys,
        const std::vector<std::pair<EdgeType, std::vector<std::string>>> edges,
        bool returnAllProps = true) {
    std::hash<std::string> hash;
    cpp2::GetPropRequest req;
    req.space_id = 1;
    req.column_names.emplace_back("_src");
    req.column_names.emplace_back("_type");
    req.column_names.emplace_back("_ranking");
    req.column_names.emplace_back("_dst");
    for (const auto& edge : edgeKeys) {
        PartitionID partId = (hash(edge.src) % totalParts) + 1;
        nebula::Row row;
        row.columns.emplace_back(edge.src);
        row.columns.emplace_back(edge.edge_type);
        row.columns.emplace_back(edge.ranking);
        row.columns.emplace_back(edge.dst);
        req.parts[partId].emplace_back(std::move(row));
    }

    UNUSED(edges);
    std::vector<cpp2::PropExp> props;
    if (props.empty() && returnAllProps) {
        req.set_props(std::move(props));
    }
    return req;
}

void verifyResult(const std::vector<nebula::Row>& expect,
                  const nebula::DataSet& dataSet) {
    ASSERT_EQ(expect.size(), dataSet.rows.size());
    for (size_t i = 0; i < expect.size(); i++) {
        const auto& expectRow = expect[i];
        const auto& actualRow = dataSet.rows[i];
        ASSERT_EQ(expectRow.columns.size(), actualRow.columns.size());
        ASSERT_EQ(expectRow, actualRow);
    }
}

TEST(GetPropTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

    {
        LOG(INFO) << "GetVertexProp";
        std::vector<VertexID> vertices = {"Tim Duncan"};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        auto req = buildVertexRequest(totalParts, vertices, tags);

        auto* processor = GetPropProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        {
            std::vector<nebula::Row> expected;
            nebula::Row row;
            std::vector<Value> values {  // player
                "Tim Duncan", 44, false, 19, 1997, 2016, 1392, 19.0, 1, "America", 5};
            for (size_t i = 0; i < 1 + 11; i++) {  // team and tag3
                values.emplace_back(NullType::__NULL__);
            }
            row.columns = std::move(values);
            expected.emplace_back(std::move(row));
            ASSERT_TRUE(resp.__isset.props);
            verifyResult(expected, resp.props);
        }
    }
    {
        LOG(INFO) << "GetEdgeProp";
        std::vector<cpp2::EdgeKey> edgeKeys;
        {
            cpp2::EdgeKey edgeKey;
            edgeKey.src = "Tim Duncan";
            edgeKey.edge_type = 101;
            edgeKey.ranking = 1997;
            edgeKey.dst = "Spurs";
            edgeKeys.emplace_back(std::move(edgeKey));
        }
        std::vector<std::pair<TagID, std::vector<std::string>>> edges;
        auto req = buildEdgeRequest(totalParts, edgeKeys, edges);

        auto* processor = GetPropProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        {
            std::vector<nebula::Row> expected;
            nebula::Row row;
            std::vector<Value> values;
            // -teammate
            for (size_t i = 0; i < 5; i++) {
                values.emplace_back(NullType::__NULL__);
            }
            // -serve
            for (size_t i = 0; i < 9; i++) {
                values.emplace_back(NullType::__NULL__);
            }
            // serve
            values.emplace_back("Tim Duncan");
            values.emplace_back("Spurs");
            values.emplace_back(1997);
            values.emplace_back(2016);
            values.emplace_back(19);
            values.emplace_back(1392);
            values.emplace_back(19.0);
            values.emplace_back("zzzzz");
            values.emplace_back(5);
            // teammate
            for (size_t i = 0; i < 5; i++) {
                values.emplace_back(NullType::__NULL__);
            }
            row.columns = std::move(values);
            expected.emplace_back(std::move(row));
            ASSERT_TRUE(resp.__isset.props);
            verifyResult(expected, resp.props);
        }
    }
    {
        LOG(INFO) << "GetNotExisted";
        std::vector<VertexID> vertices = {"Not existed"};
        std::vector<std::pair<TagID, std::vector<std::string>>> tags;
        auto req = buildVertexRequest(totalParts, vertices, tags);

        auto* processor = GetPropProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();

        ASSERT_EQ(0, resp.result.failed_parts.size());
        {
            std::vector<nebula::Row> expected;
            nebula::Row row;
            std::vector<Value> values;
            for (size_t i = 0; i < 1 + 11 + 11; i++) {
                values.emplace_back(NullType::__NULL__);
            }
            row.columns = std::move(values);
            expected.emplace_back(std::move(row));
            ASSERT_TRUE(resp.__isset.props);
            verifyResult(expected, resp.props);
        }
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
