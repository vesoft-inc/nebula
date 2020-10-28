/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include <gtest/gtest.h>
#include "storage/query/ScanEdgeProcessor.h"
#include "storage/test/QueryTestUtils.h"

namespace nebula {
namespace storage {

cpp2::ScanEdgeRequest buildRequest(
        PartitionID partId,
        const std::string& cursor,
        const std::vector<std::pair<EdgeType, std::vector<std::string>>>& edges,
        int32_t rowLimit = 100,
        bool returnNoColumns = false,
        int64_t startTime = 0,
        int64_t endTime = std::numeric_limits<int64_t>::max()) {
    cpp2::ScanEdgeRequest req;
    req.set_space_id(1);
    req.set_part_id(partId);
    req.set_cursor(cursor);
    std::vector<cpp2::EdgeProp> returnColumns;
    for (const auto& edge : edges) {
        EdgeType edgeType = edge.first;
        cpp2::EdgeProp edgeProp;
        edgeProp.type = edgeType;
        for (const auto& prop : edge.second) {
            edgeProp.props.emplace_back(std::move(prop));
        }
        returnColumns.emplace_back(std::move(edgeProp));
    }
    req.set_return_columns(std::move(returnColumns));
    req.set_no_columns(returnNoColumns);
    req.set_limit(rowLimit);
    req.set_start_time(startTime);
    req.set_end_time(endTime);
    return req;
}

void checkResponse(const nebula::DataSet& dataSet,
                   const std::vector<std::pair<EdgeType, std::vector<std::string>>>& edges,
                   std::unordered_map<TagID, size_t>& expectColumnCount,
                   size_t& totalRowCount) {
    totalRowCount += dataSet.rows.size();
    for (const auto& row : dataSet.rows) {
        auto srcId = row.values[0].getStr();
        auto edgeType = row.values[1].getInt();
        auto expectCol = expectColumnCount[edgeType];
        ASSERT_EQ(expectCol, row.values.size());
        auto props = QueryTestUtils::findExpectProps(edgeType, {}, edges);
        switch (edgeType) {
            case 101: {
                // out edge serve
                auto iter = mock::MockData::playerServes_.find(srcId);
                CHECK(iter != mock::MockData::playerServes_.end());
                std::vector<Value> values;
                if (expectCol == 4 + 9) {
                    // return all columns, collect all columns
                    for (size_t i = 0; i < row.values.size(); i++) {
                        values.emplace_back(std::move(row.values[i]));
                    }
                    QueryTestUtils::checkOutServe(edgeType, props, iter->second, values);
                } else if (expectCol > 4) {
                    // return some properties, collect properites starting from fifth column
                    for (size_t i = 4; i < row.values.size(); i++) {
                        values.emplace_back(std::move(row.values[i]));
                    }
                    QueryTestUtils::checkOutServe(edgeType, props, iter->second, values);
                }
                break;
            }
            case -101: {
                // in edge teammates
                auto iter = mock::MockData::teamServes_.find(srcId);
                CHECK(iter != mock::MockData::teamServes_.end());
                std::vector<Value> values;
                if (expectCol == 4 + 9) {
                    // return all columns, collect all columns
                    for (size_t i = 0; i < row.values.size(); i++) {
                        values.emplace_back(std::move(row.values[i]));
                    }
                    QueryTestUtils::checkInServe(edgeType, props, iter->second, values);
                } else if (expectCol > 4) {
                    // return some properties, collect properites starting from fifth column
                    for (size_t i = 4; i < row.values.size(); i++) {
                        values.emplace_back(std::move(row.values[i]));
                    }
                    QueryTestUtils::checkInServe(edgeType, props, iter->second, values);
                }
                break;
            }
            default:
                LOG(FATAL) << "Should not reach here";
        }
    }
}


TEST(ScanEdgeTest, PropertyTest) {
    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

    EdgeType serve = 101;

    {
        LOG(INFO) << "Scan one edge with some properties in one batch";
        size_t totalRowCount = 0;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        std::unordered_map<TagID, size_t> expectColumnCount;
        expectColumnCount.emplace(serve, 4 + 3);
        for (PartitionID partId = 1; partId <= totalParts; partId++) {
            auto req = buildRequest(partId, "", edges);
            auto* processor = ScanEdgeProcessor::instance(env, nullptr);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();

            ASSERT_EQ(0, resp.result.failed_parts.size());
            checkResponse(resp.edge_data, edges, expectColumnCount, totalRowCount);
        }
        CHECK_EQ(mock::MockData::serves_.size(), totalRowCount);
    }
    {
        LOG(INFO) << "Scan one edge with all properties in one batch";
        size_t totalRowCount = 0;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        edges.emplace_back(serve, std::vector<std::string>{});
        std::unordered_map<TagID, size_t> expectColumnCount;
        expectColumnCount.emplace(serve, 4 + 9);
        for (PartitionID partId = 1; partId <= totalParts; partId++) {
            auto req = buildRequest(partId, "", edges);
            auto* processor = ScanEdgeProcessor::instance(env, nullptr);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();

            ASSERT_EQ(0, resp.result.failed_parts.size());
            checkResponse(resp.edge_data, edges, expectColumnCount, totalRowCount);
        }
        CHECK_EQ(mock::MockData::serves_.size(), totalRowCount);
    }
    {
        LOG(INFO) << "Scan one edge with no properties in one batch";
        size_t totalRowCount = 0;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        edges.emplace_back(serve, std::vector<std::string>{});
        std::unordered_map<TagID, size_t> expectColumnCount;
        expectColumnCount.emplace(serve, 4 + 0);
        for (PartitionID partId = 1; partId <= totalParts; partId++) {
            auto req = buildRequest(partId, "", edges, 100, true);
            auto* processor = ScanEdgeProcessor::instance(env, nullptr);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();

            ASSERT_EQ(0, resp.result.failed_parts.size());
            checkResponse(resp.edge_data, edges, expectColumnCount, totalRowCount);
        }
        CHECK_EQ(mock::MockData::serves_.size(), totalRowCount);
    }
    {
        LOG(INFO) << "Scan multi edge with some properties in one batch";
        size_t totalRowCount = 0;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        edges.emplace_back(-serve, std::vector<std::string>{"playerName", "startYear", "endYear"});
        std::unordered_map<TagID, size_t> expectColumnCount;
        expectColumnCount.emplace(serve, 4 + 3);
        expectColumnCount.emplace(-serve, 4 + 3);
        for (PartitionID partId = 1; partId <= totalParts; partId++) {
            auto req = buildRequest(partId, "", edges);
            auto* processor = ScanEdgeProcessor::instance(env, nullptr);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();

            ASSERT_EQ(0, resp.result.failed_parts.size());
            checkResponse(resp.edge_data, edges, expectColumnCount, totalRowCount);
        }
        CHECK_EQ(mock::MockData::serves_.size() * 2, totalRowCount);
    }
    {
        LOG(INFO) << "Scan multi edge with all properties in one batch";
        size_t totalRowCount = 0;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        edges.emplace_back(serve, std::vector<std::string>{});
        edges.emplace_back(-serve, std::vector<std::string>{});
        std::unordered_map<TagID, size_t> expectColumnCount;
        expectColumnCount.emplace(serve, 4 + 9);
        expectColumnCount.emplace(-serve, 4 + 9);
        for (PartitionID partId = 1; partId <= totalParts; partId++) {
            auto req = buildRequest(partId, "", edges);
            auto* processor = ScanEdgeProcessor::instance(env, nullptr);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();

            ASSERT_EQ(0, resp.result.failed_parts.size());
            checkResponse(resp.edge_data, edges, expectColumnCount, totalRowCount);
        }
        CHECK_EQ(mock::MockData::serves_.size() * 2, totalRowCount);
    }
    {
        LOG(INFO) << "Scan multi edge with no properties in one batch";
        size_t totalRowCount = 0;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        edges.emplace_back(serve, std::vector<std::string>{});
        edges.emplace_back(-serve, std::vector<std::string>{});
        std::unordered_map<TagID, size_t> expectColumnCount;
        expectColumnCount.emplace(serve, 4 + 0);
        expectColumnCount.emplace(-serve, 4 + 0);
        for (PartitionID partId = 1; partId <= totalParts; partId++) {
            auto req = buildRequest(partId, "", edges, 100, true);
            auto* processor = ScanEdgeProcessor::instance(env, nullptr);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();

            ASSERT_EQ(0, resp.result.failed_parts.size());
            checkResponse(resp.edge_data, edges, expectColumnCount, totalRowCount);
        }
        CHECK_EQ(mock::MockData::serves_.size() * 2, totalRowCount);
    }
    {
        LOG(INFO) << "Scan multi edge misc properties in one batch";
        size_t totalRowCount = 0;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        edges.emplace_back(serve, std::vector<std::string>{});
        edges.emplace_back(-serve, std::vector<std::string>{"playerName", "startYear", "endYear"});
        std::unordered_map<TagID, size_t> expectColumnCount;
        expectColumnCount.emplace(serve, 4 + 9);
        expectColumnCount.emplace(-serve, 4 + 3);
        for (PartitionID partId = 1; partId <= totalParts; partId++) {
            auto req = buildRequest(partId, "", edges);
            auto* processor = ScanEdgeProcessor::instance(env, nullptr);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();

            ASSERT_EQ(0, resp.result.failed_parts.size());
            checkResponse(resp.edge_data, edges, expectColumnCount, totalRowCount);
        }
        CHECK_EQ(mock::MockData::serves_.size() * 2, totalRowCount);
    }
}

TEST(ScanEdgeTest, CursorTest) {
    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

    EdgeType serve = 101;

    {
        LOG(INFO) << "Scan one edge with some properties with limit = 5";
        size_t totalRowCount = 0;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        std::unordered_map<TagID, size_t> expectColumnCount;
        expectColumnCount.emplace(serve, 4 + 3);
        for (PartitionID partId = 1; partId <= totalParts; partId++) {
            bool hasNext = true;
            std::string cursor = "";
            while (hasNext) {
                auto req = buildRequest(partId, cursor, edges, 5);
                auto* processor = ScanEdgeProcessor::instance(env, nullptr);
                auto f = processor->getFuture();
                processor->process(req);
                auto resp = std::move(f).get();

                ASSERT_EQ(0, resp.result.failed_parts.size());
                checkResponse(resp.edge_data, edges, expectColumnCount, totalRowCount);
                hasNext = resp.get_has_next();
                if (hasNext) {
                    CHECK(resp.__isset.next_cursor);
                    cursor = *resp.get_next_cursor();
                }
            }
        }
        CHECK_EQ(mock::MockData::serves_.size(), totalRowCount);
    }
    {
        LOG(INFO) << "Scan one edge with some properties with limit = 1";
        size_t totalRowCount = 0;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        edges.emplace_back(serve, std::vector<std::string>{"teamName", "startYear", "endYear"});
        std::unordered_map<TagID, size_t> expectColumnCount;
        expectColumnCount.emplace(serve, 4 + 3);
        for (PartitionID partId = 1; partId <= totalParts; partId++) {
            bool hasNext = true;
            std::string cursor = "";
            while (hasNext) {
                auto req = buildRequest(partId, cursor, edges, 1);
                auto* processor = ScanEdgeProcessor::instance(env, nullptr);
                auto f = processor->getFuture();
                processor->process(req);
                auto resp = std::move(f).get();

                ASSERT_EQ(0, resp.result.failed_parts.size());
                checkResponse(resp.edge_data, edges, expectColumnCount, totalRowCount);
                hasNext = resp.get_has_next();
                if (hasNext) {
                    CHECK(resp.__isset.next_cursor);
                    cursor = *resp.get_next_cursor();
                }
            }
        }
        CHECK_EQ(mock::MockData::serves_.size(), totalRowCount);
    }
    {
        LOG(INFO) << "Scan multi edge misc properties with limit = 1";
        size_t totalRowCount = 0;
        std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
        edges.emplace_back(serve, std::vector<std::string>{});
        edges.emplace_back(-serve, std::vector<std::string>{"playerName", "startYear", "endYear"});
        std::unordered_map<TagID, size_t> expectColumnCount;
        expectColumnCount.emplace(serve, 4 + 9);
        expectColumnCount.emplace(-serve, 4 + 3);
        for (PartitionID partId = 1; partId <= totalParts; partId++) {
            bool hasNext = true;
            std::string cursor = "";
            while (hasNext) {
                auto req = buildRequest(partId, cursor, edges, 1);
                auto* processor = ScanEdgeProcessor::instance(env, nullptr);
                auto f = processor->getFuture();
                processor->process(req);
                auto resp = std::move(f).get();

                ASSERT_EQ(0, resp.result.failed_parts.size());
                checkResponse(resp.edge_data, edges, expectColumnCount, totalRowCount);
                hasNext = resp.get_has_next();
                if (hasNext) {
                    CHECK(resp.__isset.next_cursor);
                    cursor = *resp.get_next_cursor();
                }
            }
        }
        CHECK_EQ(mock::MockData::serves_.size() * 2, totalRowCount);
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
