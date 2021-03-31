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
        const std::pair<EdgeType, std::vector<std::string>>& edge,
        int64_t rowLimit = 100,
        int64_t startTime = 0,
        int64_t endTime = std::numeric_limits<int64_t>::max(),
        bool onlyLatestVer = false) {
    cpp2::ScanEdgeRequest req;
    req.set_space_id(1);
    req.set_part_id(partId);
    req.set_cursor(cursor);
    EdgeType edgeType = edge.first;
    cpp2::EdgeProp edgeProp;
    edgeProp.set_type(edgeType);
    for (const auto& prop : edge.second) {
        (*edgeProp.props_ref()).emplace_back(std::move(prop));
    }
    req.set_return_columns(std::move(edgeProp));
    req.set_limit(rowLimit);
    req.set_start_time(startTime);
    req.set_end_time(endTime);
    req.set_only_latest_version(onlyLatestVer);
    return req;
}

void checkResponse(const nebula::DataSet& dataSet,
                   const std::pair<EdgeType, std::vector<std::string>> edge,
                   size_t expectColumnCount,
                   size_t& totalRowCount) {
    ASSERT_EQ(dataSet.colNames.size(), expectColumnCount);
    if (!edge.second.empty()) {
        ASSERT_EQ(dataSet.colNames.size(), edge.second.size());
        for (size_t i = 0; i < dataSet.colNames.size(); i++) {
            ASSERT_EQ(dataSet.colNames[i], std::to_string(edge.first) + "." + edge.second[i]);
        }
    }
    totalRowCount += dataSet.rows.size();
    for (const auto& row : dataSet.rows) {
        // always pass name or kSrc at first
        auto srcId = row.values[0].getStr();
        auto edgeType = edge.first;
        ASSERT_EQ(expectColumnCount, row.values.size());
        auto props = edge.second;
        switch (edgeType) {
            case 101: {
                // out edge serve
                auto iter = mock::MockData::playerServes_.find(srcId);
                CHECK(iter != mock::MockData::playerServes_.end());
                QueryTestUtils::checkOutServe(edgeType, props, iter->second, row.values, 4, 5);
                break;
            }
            case -101: {
                // in edge teammates
                auto iter = mock::MockData::teamServes_.find(srcId);
                CHECK(iter != mock::MockData::teamServes_.end());
                std::vector<Value> values;
                QueryTestUtils::checkInServe(edgeType, props, iter->second, row.values);
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
        auto edge = std::make_pair(serve, std::vector<std::string>{
            kSrc, kType, kRank, kDst, "teamName", "startYear", "endYear"});
        for (PartitionID partId = 1; partId <= totalParts; partId++) {
            auto req = buildRequest(partId, "", edge);
            auto* processor = ScanEdgeProcessor::instance(env, nullptr);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();

            ASSERT_EQ(0, resp.result.failed_parts.size());
            checkResponse(*resp.edge_data_ref(), edge, edge.second.size(), totalRowCount);
        }
        CHECK_EQ(mock::MockData::serves_.size(), totalRowCount);
    }
    {
        LOG(INFO) << "Scan one edge with all properties in one batch";
        size_t totalRowCount = 0;
        auto edge = std::make_pair(serve, std::vector<std::string>{});
        for (PartitionID partId = 1; partId <= totalParts; partId++) {
            auto req = buildRequest(partId, "", edge);
            auto* processor = ScanEdgeProcessor::instance(env, nullptr);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();

            ASSERT_EQ(0, resp.result.failed_parts.size());
            // all 9 columns in value
            checkResponse(*resp.edge_data_ref(), edge, 9, totalRowCount);
        }
        CHECK_EQ(mock::MockData::serves_.size(), totalRowCount);
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
        auto edge = std::make_pair(serve, std::vector<std::string>{
            kSrc, kType, kRank, kDst, "teamName", "startYear", "endYear"});
        for (PartitionID partId = 1; partId <= totalParts; partId++) {
            bool hasNext = true;
            std::string cursor = "";
            while (hasNext) {
                auto req = buildRequest(partId, cursor, edge, 5);
                auto* processor = ScanEdgeProcessor::instance(env, nullptr);
                auto f = processor->getFuture();
                processor->process(req);
                auto resp = std::move(f).get();

                ASSERT_EQ(0, resp.result.failed_parts.size());
                checkResponse(*resp.edge_data_ref(), edge, edge.second.size(), totalRowCount);
                hasNext = resp.get_has_next();
                if (hasNext) {
                    CHECK(resp.next_cursor_ref().has_value());
                    cursor = *resp.next_cursor_ref();
                }
            }
        }
        CHECK_EQ(mock::MockData::serves_.size(), totalRowCount);
    }
    {
        LOG(INFO) << "Scan one edge with some properties with limit = 1";
        size_t totalRowCount = 0;
        auto edge = std::make_pair(serve, std::vector<std::string>{
            kSrc, kType, kRank, kDst, "teamName", "startYear", "endYear"});
        for (PartitionID partId = 1; partId <= totalParts; partId++) {
            bool hasNext = true;
            std::string cursor = "";
            while (hasNext) {
                auto req = buildRequest(partId, cursor, edge, 1);
                auto* processor = ScanEdgeProcessor::instance(env, nullptr);
                auto f = processor->getFuture();
                processor->process(req);
                auto resp = std::move(f).get();

                ASSERT_EQ(0, resp.result.failed_parts.size());
                checkResponse(*resp.edge_data_ref(), edge, edge.second.size(), totalRowCount);
                hasNext = resp.get_has_next();
                if (hasNext) {
                    CHECK(resp.next_cursor_ref().has_value());
                    cursor = *resp.next_cursor_ref();
                }
            }
        }
        CHECK_EQ(mock::MockData::serves_.size(), totalRowCount);
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
