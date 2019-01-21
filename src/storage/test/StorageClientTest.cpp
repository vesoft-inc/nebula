/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "fs/TempDir.h"
#include "storage/test/TestUtils.h"
#include "storage/client/StorageClient.h"
#include "meta/PartManager.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"

namespace nebula {
namespace storage {

TEST(StorageClientTest, VerticesInterfacesTest) {
    fs::TempDir rootPath("/tmp/StorageClientTest.XXXXXX");
    uint32_t localIp;
    uint32_t localPort = 10002;
    network::NetworkUtils::ipv4ToInt("127.0.0.1", localIp);
    auto sc = TestUtils::mockServer(localPort, rootPath.path());


    GraphSpaceID spaceId = 0;
    meta::AdHocPartManagersBuilder::add(spaceId,
                                        HostAddr(localIp, localPort),
                                        {0, 1, 2, 3, 4, 5});

    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    auto client = std::make_unique<StorageClient>(threadPool);
    {
        LOG(INFO) << "Prepare data...";
        std::vector<storage::cpp2::Vertex> vertices;
        for (int32_t vId = 0; vId < 10; vId++) {
            cpp2::Vertex v;
            v.set_id(vId);
            decltype(v.tags) tags;
            for (int32_t tagId = 3001; tagId < 3010; tagId++) {
                cpp2::Tag t;
                t.set_tag_id(tagId);
                // Generate some tag props.
                RowWriter writer;
                for (uint64_t numInt = 0; numInt < 3; numInt++) {
                    writer << numInt;
                }
                for (auto numString = 3; numString < 6; numString++) {
                    writer << folly::stringPrintf("tag_string_col_%d", numString);
                }
                t.set_props(writer.encode());
                tags.emplace_back(std::move(t));
            }
            v.set_tags(std::move(tags));
            vertices.emplace_back(std::move(v));
        }
        auto f = client->addVertices(spaceId, std::move(vertices), true);
        LOG(INFO) << "Waiting for the response...";
        auto resp = std::move(f).get();
        ASSERT_TRUE(resp.succeeded());
    }
    {
        std::vector<VertexID> vIds;
        std::vector<cpp2::PropDef> retCols;
        for (int32_t vId = 0; vId < 10; vId++) {
            vIds.emplace_back(vId);
        }
        for (int i = 0; i < 3; i++) {
            retCols.emplace_back(
                TestUtils::propDef(cpp2::PropOwner::SOURCE,
                                   folly::stringPrintf("tag_%d_col_%d", 3001 + i*2, i*2),
                                   3001 + i*2));
        }
        auto f = client->getVertexProps(spaceId, std::move(vIds), std::move(retCols));
        auto resp = std::move(f).get();
        ASSERT_TRUE(resp.succeeded());

        auto& results = resp.responses();
        ASSERT_EQ(1, results.size());
        EXPECT_EQ(0, results[0].result.failed_codes.size());

        EXPECT_EQ(3, results[0].vertex_schema.columns.size());
        auto tagProvider = std::make_shared<ResultSchemaProvider>(results[0].vertex_schema);
        EXPECT_EQ(10, results[0].vertices.size());
        for (auto& vp : results[0].vertices) {
            auto tagReader = RowReader::getRowReader(vp.vertex_data, tagProvider);
            EXPECT_EQ(3, tagReader->numFields());
            int64_t col1;
            EXPECT_EQ(ResultType::SUCCEEDED, tagReader->getInt("tag_3001_col_0", col1));
            EXPECT_EQ(col1, 0);

            int64_t col2;
            EXPECT_EQ(ResultType::SUCCEEDED, tagReader->getInt("tag_3003_col_2", col2));
            EXPECT_EQ(col2, 2);

            folly::StringPiece col3;
            EXPECT_EQ(ResultType::SUCCEEDED, tagReader->getString("tag_3005_col_4", col3));
            EXPECT_EQ(folly::stringPrintf("tag_string_col_4"), col3);
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


