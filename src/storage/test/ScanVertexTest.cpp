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
        int64_t rowLimit = 100,
        int64_t startTime = 0,
        int64_t endTime = std::numeric_limits<int64_t>::max(),
        bool onlyLatestVer = false) {
    cpp2::ScanVertexRequest req;
    req.set_space_id(1);
    req.set_part_id(partId);
    req.set_cursor(cursor);
    TagID tagId = tag.first;
    cpp2::VertexProp vertexProp;
    vertexProp.set_tag(tagId);
    for (const auto& prop : tag.second) {
        (*vertexProp.props_ref()).emplace_back(std::move(prop));
    }
    req.set_return_columns(std::move(vertexProp));
    req.set_limit(rowLimit);
    req.set_start_time(startTime);
    req.set_end_time(endTime);
    req.set_only_latest_version(onlyLatestVer);
    return req;
}

void checkResponse(const nebula::DataSet& dataSet,
                   const std::pair<TagID, std::vector<std::string>>& tag,
                   size_t expectColumnCount,
                   size_t& totalRowCount) {
    ASSERT_EQ(dataSet.colNames.size(), expectColumnCount);
    if (!tag.second.empty()) {
        ASSERT_EQ(dataSet.colNames.size(), tag.second.size());
        for (size_t i = 0; i < dataSet.colNames.size(); i++) {
            ASSERT_EQ(dataSet.colNames[i], std::to_string(tag.first) + "." + tag.second[i]);
        }
    }
    totalRowCount += dataSet.rows.size();
    for (const auto& row : dataSet.rows) {
        // always pass player name or kVid at first
        auto vId = row.values[0].getStr();
        auto tagId = tag.first;
        ASSERT_EQ(expectColumnCount, row.values.size());
        auto props = tag.second;
        switch (tagId) {
            case 1: {
                // tag player
                auto iter = std::find_if(mock::MockData::players_.begin(),
                                         mock::MockData::players_.end(),
                                         [&] (const auto& player) { return player.name_ == vId; });
                CHECK(iter != mock::MockData::players_.end());
                QueryTestUtils::checkPlayer(props, *iter, row.values);
                break;
            }
            case 2: {
                // tag team
                auto iter = std::find(mock::MockData::teams_.begin(),
                                      mock::MockData::teams_.end(),
                                      vId);
                QueryTestUtils::checkTeam(props, *iter, row.values);
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
        auto tag = std::make_pair(player, std::vector<std::string>{
            kVid, kTag, "name", "age", "avgScore"});
        for (PartitionID partId = 1; partId <= totalParts; partId++) {
            auto req = buildRequest(partId, "", tag);
            auto* processor = ScanVertexProcessor::instance(env, nullptr);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();

            ASSERT_EQ(0, resp.result.failed_parts.size());
            checkResponse(*resp.vertex_data_ref(), tag, tag.second.size(), totalRowCount);
        }
        CHECK_EQ(mock::MockData::players_.size(), totalRowCount);
    }
    {
        LOG(INFO) << "Scan one tag with all properties in one batch";
        size_t totalRowCount = 0;
        auto tag = std::make_pair(player, std::vector<std::string>{});
        for (PartitionID partId = 1; partId <= totalParts; partId++) {
            auto req = buildRequest(partId, "", tag);
            auto* processor = ScanVertexProcessor::instance(env, nullptr);
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();

            ASSERT_EQ(0, resp.result.failed_parts.size());
            // all 11 columns in value
            checkResponse(*resp.vertex_data_ref(), tag, 11, totalRowCount);
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
        auto tag = std::make_pair(player, std::vector<std::string>{
            kVid, kTag, "name", "age", "avgScore"});
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
                checkResponse(*resp.vertex_data_ref(), tag, tag.second.size(), totalRowCount);
                hasNext = resp.get_has_next();
                if (hasNext) {
                    CHECK(resp.next_cursor_ref());
                    cursor = *resp.next_cursor_ref();
                }
            }
        }
        CHECK_EQ(mock::MockData::players_.size(), totalRowCount);
    }
    {
        LOG(INFO) << "Scan one tag with some properties with limit = 1";
        size_t totalRowCount = 0;
        auto tag = std::make_pair(player, std::vector<std::string>{
            kVid, kTag, "name", "age", "avgScore"});
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
                checkResponse(*resp.vertex_data_ref(), tag, tag.second.size(), totalRowCount);
                hasNext = resp.get_has_next();
                if (hasNext) {
                    CHECK(resp.next_cursor_ref());
                    cursor = *resp.next_cursor_ref();
                }
            }
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
