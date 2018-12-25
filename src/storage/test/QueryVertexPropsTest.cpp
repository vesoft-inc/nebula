/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "fs/TempDir.h"
#include "storage/test/TestUtils.h"
#include "storage/QueryVertexPropsProcessor.h"
#include "storage/KeyUtils.h"
#include "dataman/RowSetReader.h"
#include "dataman/RowReader.h"

namespace nebula {
namespace storage {

TEST(QueryVertexPropsTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/QueryVertexPropsTest.XXXXXX");
    auto* kv = TestUtils::initKV(rootPath.path());
    LOG(INFO) << "Prepare meta...";
    auto* schemaMan = new meta::MemorySchemaManager();
    auto& tagSchema = schemaMan->tagSchema();
    for (auto tagId = 3001; tagId < 3010; tagId++) {
        tagSchema[0][tagId] = TestUtils::genTagSchemaProvider(tagId, 3, 3);
    }

    LOG(INFO) << "Prepare data...";
    for (auto partId = 0; partId < 3; partId++) {
        std::vector<kvstore::KV> data;
        for (auto vertexId = partId * 10; vertexId < (partId + 1) * 10; vertexId++) {
            for (auto tagId = 3001; tagId < 3010; tagId++) {
                auto key = KeyUtils::vertexKey(partId, vertexId, tagId, 0);
                RowWriter writer;
                for (auto numInt = 0; numInt < 3; numInt++) {
                    writer << numInt;
                }
                for (auto numString = 3; numString < 6; numString++) {
                    writer << folly::stringPrintf("tag_string_col_%d", numString);
                }
                auto val = writer.encode();
                data.emplace_back(std::move(key), std::move(val));
            }
        }
        kv->asyncMultiPut(0, partId, std::move(data), [&](kvstore::ResultCode code, HostAddr addr) {
            EXPECT_EQ(code, kvstore::ResultCode::SUCCESSED);
            UNUSED(addr);
        });
    }

    LOG(INFO) << "Build VertexPropsRequest...";
    cpp2::VertexPropRequest req;
    req.set_space_id(0);
    decltype(req.ids) tmpIds;
    for (auto partId = 0; partId < 3; partId++) {
        for (auto vertexId =  partId * 10; vertexId < (partId + 1) * 10; vertexId++) {
            tmpIds[partId].push_back(vertexId);
        }
    }
    req.set_ids(std::move(tmpIds));
    // Return tag props col_0, col_2, col_4
    decltype(req.return_columns) tmpColumns;
    for (int i = 0; i < 3; i++) {
       tmpColumns.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                               cpp2::PropOwner::SOURCE, 3001 + i*2,
                               folly::stringPrintf("tag_%d_col_%d", 3001 + i*2, i*2));
    }
    req.set_return_columns(std::move(tmpColumns));

    LOG(INFO) << "Test QueryVertexPropsRequest...";
    auto* processor = QueryVertexPropsProcessor::instance(kv, schemaMan);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(3, resp.codes.size());
    for (auto i = 0; i < 3; i++) {
        EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, resp.codes[i].code);
    }

    EXPECT_EQ(3, resp.vertex_schema.columns.size());
    auto tagProvider = std::make_unique<ResultSchemaProvider>(resp.vertex_schema);
    EXPECT_EQ(30, resp.vertices.size());
    for (auto& vp : resp.vertices) {
        RowReader tagReader(tagProvider.get(), vp.vertex_data);
        EXPECT_EQ(3, tagReader.numFields());
        int32_t col1;
        EXPECT_EQ(ResultType::SUCCEEDED, tagReader.getInt("tag_3001_col_0", col1));
        EXPECT_EQ(col1, 0);

        int32_t col2;
        EXPECT_EQ(ResultType::SUCCEEDED, tagReader.getInt("tag_3003_col_2", col2));
        EXPECT_EQ(col2, 2);

        folly::StringPiece col3;
        EXPECT_EQ(ResultType::SUCCEEDED, tagReader.getString("tag_3005_col_4", col3));
        EXPECT_EQ(folly::stringPrintf("tag_string_col_4"), col3);
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


