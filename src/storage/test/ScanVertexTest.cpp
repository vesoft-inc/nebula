/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include <gtest/gtest.h>
#include "storage/query/ScanVertexProcessor.h"
#include "storage/test/QueryTestUtils.h"

namespace nebula {
namespace storage {

cpp2::ScanVertexRequest buildRequest(
        PartitionID partId,
        const std::string& cursor,
        const std::pair<TagID, std::vector<std::string>>& tag,
        int32_t rowLimit = 100,
        bool returnNoColumns = false,
        int64_t startTime = 0,
        int64_t endTime = std::numeric_limits<int64_t>::max(),
        bool onlyLatestVer = false) {
    cpp2::ScanVertexRequest req;
    req.set_space_id(1);
    req.set_part_id(partId);
    req.set_cursor(cursor);
    TagID tagId = tag.first;
    cpp2::VertexProp vertexProp;
    vertexProp.tag = tagId;
    for (const auto& prop : tag.second) {
        vertexProp.props.emplace_back(std::move(prop));
    }
    req.set_return_columns(std::move(vertexProp));
    req.set_no_columns(returnNoColumns);
    req.set_limit(rowLimit);
    req.set_start_time(startTime);
    req.set_end_time(endTime);
    req.set_only_latest_version(onlyLatestVer);
    return req;
}

void checkResponse(const nebula::DataSet& dataSet,
                   const std::pair<TagID, std::vector<std::string>>& tag,
                   std::unordered_map<TagID, size_t>& expectColumnCount,
                   size_t& totalRowCount) {
    totalRowCount += dataSet.rows.size();
    for (const auto& row : dataSet.rows) {
        auto vId = row.values[0].getStr();
        auto tagId = row.values[1].getInt();
        auto expectCol = expectColumnCount[tagId];
        ASSERT_EQ(expectCol, row.values.size());
        auto props = tag.second;
        switch (tagId) {
            case 1: {
                // tag player
                auto iter = std::find_if(mock::MockData::players_.begin(),
                                         mock::MockData::players_.end(),
                                         [&] (const auto& player) { return player.name_ == vId; });
                CHECK(iter != mock::MockData::players_.end());
                if (expectCol > 2) {
                    std::vector<Value> values;
                    // properties starts from the third column
                    for (size_t i = 2; i < row.values.size(); i++) {
                        values.emplace_back(std::move(row.values[i]));
                    }
                    QueryTestUtils::checkPlayer(props, *iter, values);
                }
                break;
            }
            case 2: {
                // tag team
                auto iter = std::find(mock::MockData::teams_.begin(),
                                      mock::MockData::teams_.end(),
                                      vId);
                CHECK(iter != mock::MockData::teams_.end());
                if (expectCol > 2) {
                    std::vector<Value> values;
                    // properties starts from the third column
                    for (size_t i = 2; i < row.values.size(); i++) {
                        values.emplace_back(std::move(row.values[i]));
                    }
                    ASSERT_EQ(1, values.size());
                    ASSERT_EQ(*iter, values[0].getStr());
                }
                break;
            }
            default:
                LOG(FATAL) << "Should not reach here";
        }
    }
}

TEST(ScanVertexTest, PropertyTest) {
    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

    TagID player = 1;

    {
        LOG(INFO) << "Scan one tag with some properties in one batch";
        size_t totalRowCount = 0;
        auto tag = std::make_pair(player, std::vector<std::string>{"name", "age", "avgScore"});
        std::unordered_map<TagID, size_t> expectColumnCount;
        expectColumnCount.emplace(player, 2 + 3);
        for (PartitionID partId = 1; partId <= totalParts; partId++) {
            auto req = buildRequest(partId, "", tag);
            auto* processor = ScanVertexProcessor::instance(env, nullptr);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();

            ASSERT_EQ(0, resp.result.failed_parts.size());
            checkResponse(resp.vertex_data, tag, expectColumnCount, totalRowCount);
        }
        CHECK_EQ(mock::MockData::players_.size(), totalRowCount);
    }
    {
        LOG(INFO) << "Scan one tag with all properties in one batch";
        size_t totalRowCount = 0;
        auto tag = std::make_pair(player, std::vector<std::string>{});
        std::unordered_map<TagID, size_t> expectColumnCount;
        expectColumnCount.emplace(player, 2 + 11);
        for (PartitionID partId = 1; partId <= totalParts; partId++) {
            auto req = buildRequest(partId, "", tag);
            auto* processor = ScanVertexProcessor::instance(env, nullptr);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();

            ASSERT_EQ(0, resp.result.failed_parts.size());
            checkResponse(resp.vertex_data, tag, expectColumnCount, totalRowCount);
        }
        CHECK_EQ(mock::MockData::players_.size(), totalRowCount);
    }
    {
        LOG(INFO) << "Scan one tag with no properties in one batch";
        size_t totalRowCount = 0;
        auto tag = std::make_pair(player, std::vector<std::string>{});
        std::unordered_map<TagID, size_t> expectColumnCount;
        expectColumnCount.emplace(player, 2 + 0);
        for (PartitionID partId = 1; partId <= totalParts; partId++) {
            auto req = buildRequest(partId, "", tag, 100, true);
            auto* processor = ScanVertexProcessor::instance(env, nullptr);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();

            ASSERT_EQ(0, resp.result.failed_parts.size());
            checkResponse(resp.vertex_data, tag, expectColumnCount, totalRowCount);
        }
        CHECK_EQ(mock::MockData::players_.size(), totalRowCount);
    }
}

TEST(ScanVertexTest, CursorTest) {
    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

    TagID player = 1;

    {
        LOG(INFO) << "Scan one tag with some properties with limit = 5";
        size_t totalRowCount = 0;
        auto tag = std::make_pair(player, std::vector<std::string>{"name", "age", "avgScore"});
        std::unordered_map<TagID, size_t> expectColumnCount;
        expectColumnCount.emplace(player, 2 + 3);
        for (PartitionID partId = 1; partId <= totalParts; partId++) {
            bool hasNext = true;
            std::string cursor = "";
            while (hasNext) {
                auto req = buildRequest(partId, cursor, tag, 5);
                auto* processor = ScanVertexProcessor::instance(env, nullptr);
                auto f = processor->getFuture();
                processor->process(req);
                auto resp = std::move(f).get();

                ASSERT_EQ(0, resp.result.failed_parts.size());
                checkResponse(resp.vertex_data, tag, expectColumnCount, totalRowCount);
                hasNext = resp.get_has_next();
                if (hasNext) {
                    CHECK(resp.__isset.next_cursor);
                    cursor = *resp.get_next_cursor();
                }
            }
        }
        CHECK_EQ(mock::MockData::players_.size(), totalRowCount);
    }
    {
        LOG(INFO) << "Scan one tag with some properties with limit = 1";
        size_t totalRowCount = 0;
        auto tag = std::make_pair(player, std::vector<std::string>{"name", "age", "avgScore"});
        std::unordered_map<TagID, size_t> expectColumnCount;
        expectColumnCount.emplace(player, 2 + 3);
        for (PartitionID partId = 1; partId <= totalParts; partId++) {
            bool hasNext = true;
            std::string cursor = "";
            while (hasNext) {
                auto req = buildRequest(partId, cursor, tag, 1);
                auto* processor = ScanVertexProcessor::instance(env, nullptr);
                auto f = processor->getFuture();
                processor->process(req);
                auto resp = std::move(f).get();

                ASSERT_EQ(0, resp.result.failed_parts.size());
                checkResponse(resp.vertex_data, tag, expectColumnCount, totalRowCount);
                hasNext = resp.get_has_next();
                if (hasNext) {
                    CHECK(resp.__isset.next_cursor);
                    cursor = *resp.get_next_cursor();
                }
            }
        }
        CHECK_EQ(mock::MockData::players_.size(), totalRowCount);
    }
}

TEST(ScanVertexTest, OnlyLatestVerTest) {
    FLAGS_enable_multi_versions = true;
    fs::TempDir rootPath("/tmp/GetNeighborsTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto* env = cluster.storageEnv_.get();
    auto totalParts = cluster.getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));
    sleep(1);
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockEdgeData(env, totalParts));

    TagID player = 1;

    {
        LOG(INFO) << "Scan one tag with some properties only latest version";
        size_t totalRowCount = 0;
        auto tag = std::make_pair(player, std::vector<std::string>{"name", "age", "avgScore"});
        std::unordered_map<TagID, size_t> expectColumnCount;
        expectColumnCount.emplace(player, 2 + 3);
        for (PartitionID partId = 1; partId <= totalParts; partId++) {
            auto req = buildRequest(
                partId, "", tag, 100, false, 0, std::numeric_limits<int64_t>::max(), true);
            auto* processor = ScanVertexProcessor::instance(env, nullptr);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();

            ASSERT_EQ(0, resp.result.failed_parts.size());
            checkResponse(resp.vertex_data, tag, expectColumnCount, totalRowCount);
        }
        CHECK_EQ(mock::MockData::players_.size(), totalRowCount);
    }
    FLAGS_enable_multi_versions = false;
    {
        LOG(INFO) << "Scan one tag with some properties all version";
        size_t totalRowCount = 0;
        auto tag = std::make_pair(player, std::vector<std::string>{"name", "age", "avgScore"});
        std::unordered_map<TagID, size_t> expectColumnCount;
        expectColumnCount.emplace(player, 2 + 3);
        for (PartitionID partId = 1; partId <= totalParts; partId++) {
            auto req = buildRequest(
                partId, "", tag, 100, false, 0, std::numeric_limits<int64_t>::max(), true);
            auto* processor = ScanVertexProcessor::instance(env, nullptr);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();

            ASSERT_EQ(0, resp.result.failed_parts.size());
            checkResponse(resp.vertex_data, tag, expectColumnCount, totalRowCount);
        }
        CHECK_EQ(mock::MockData::players_.size(), totalRowCount);
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
