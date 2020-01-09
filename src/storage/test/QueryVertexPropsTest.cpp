/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "base/NebulaKeyUtils.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "fs/TempDir.h"
#include "storage/test/TestUtils.h"
#include "storage/query/QueryVertexPropsProcessor.h"
#include "dataman/RowSetReader.h"
#include "dataman/RowReader.h"

namespace nebula {
namespace storage {

void mockData(kvstore::KVStore* kv, TagVersion version) {
    LOG(INFO) << "Prepare data...";
    for (auto partId = 0; partId < 3; partId++) {
        std::vector<kvstore::KV> data;
        for (auto vertexId = partId * 10; vertexId < (partId + 1) * 10; vertexId++) {
            for (auto tagId = 3001; tagId < 3010; tagId++) {
                auto key = NebulaKeyUtils::vertexKey(partId, vertexId, tagId, version);
                RowWriter writer;
                for (int64_t numInt = 0; numInt < 3; numInt++) {
                    writer << (numInt + version);
                }
                for (auto numString = 3; numString < 6; numString++) {
                    writer << folly::stringPrintf("tag_string_col_%ld", numString + version);
                }
                auto val = writer.encode();
                data.emplace_back(std::move(key), std::move(val));
            }
        }
        folly::Baton<true, std::atomic> baton;
        kv->asyncMultiPut(
            0,
            partId,
            std::move(data),
            [&](kvstore::ResultCode code) {
                EXPECT_EQ(code, kvstore::ResultCode::SUCCEEDED);
                baton.post();
            });
        baton.wait();
    }
}

void testWithVersion(kvstore::KVStore* kv,
                     meta::SchemaManager* schemaMng,
                     folly::CPUThreadPoolExecutor* executor,
                     TagVersion version) {
    {
        LOG(INFO) << "Build VertexPropsRequest...";
        cpp2::VertexPropRequest req;
        req.set_space_id(0);
        decltype(req.parts) tmpIds;
        std::unordered_set<VertexID> vids;
        for (auto partId = 0; partId < 3; partId++) {
            for (auto vertexId =  partId * 10;
                vertexId < (partId + 1) * 10;
                vertexId++) {
                tmpIds[partId].emplace_back(vertexId);
                vids.emplace(vertexId);
            }
        }
        req.set_parts(std::move(tmpIds));
        // Return tag props col_0, col_2, col_4
        decltype(req.return_columns) tmpColumns;
        for (int i = 0; i < 3; i++) {
            tmpColumns.emplace_back(TestUtils::vertexPropDef(
                folly::stringPrintf("tag_%d_col_%d", 3001 + i * 2, i * 2), 3001 + i * 2));
        }
        req.set_return_columns(std::move(tmpColumns));

        LOG(INFO) << "Test QueryVertexPropsRequest...";
        auto* processor = QueryVertexPropsProcessor::instance(kv,
                                                            schemaMng,
                                                            nullptr,
                                                            executor);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        LOG(INFO) << "Check the results...";
        EXPECT_EQ(0, resp.result.failed_codes.size());

        EXPECT_EQ(30, resp.vertices.size());

        auto* vschema = resp.get_vertex_schema();
        DCHECK(vschema != nullptr);

        for (auto& vp : resp.vertices) {
            auto vidFound = vids.find(vp.vertex_id);
            ASSERT_TRUE(vidFound != vids.end()) << "Vid: " << vp.vertex_id;

            auto size = std::accumulate(vp.tag_data.cbegin(), vp.tag_data.cend(), 0,
                                        [vschema](int acc, auto& td) {
                                            auto it = vschema->find(td.tag_id);
                                            DCHECK(it != vschema->end());
                                            return acc + it->second.columns.size();
                                        });

            EXPECT_EQ(3, size);

            checkTagData<int64_t>(vp.tag_data, 3001, "tag_3001_col_0", vschema, 0 + version);
            checkTagData<int64_t>(vp.tag_data, 3003, "tag_3003_col_2", vschema, 2 + version);
            checkTagData<std::string>(vp.tag_data, 3005, "tag_3005_col_4", vschema,
                                    folly::stringPrintf("tag_string_col_%ld", 4 + version));
        }
    }
    {
        LOG(INFO) << "Build VertexPropsRequest...";
        cpp2::VertexPropRequest req;
        req.set_space_id(0);
        decltype(req.parts) tmpIds;
        std::unordered_set<VertexID> vids;
        for (auto partId = 0; partId < 3; partId++) {
            for (auto vertexId =  partId * 10;
                vertexId < (partId + 1) * 10;
                vertexId++) {
                tmpIds[partId].emplace_back(vertexId);
                vids.emplace(vertexId);
            }
        }
        req.set_parts(std::move(tmpIds));

        LOG(INFO) << "Test QueryVertexPropsRequest...";
        auto* processor = QueryVertexPropsProcessor::instance(kv,
                                                            schemaMng,
                                                            nullptr,
                                                            executor);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();

        LOG(INFO) << "Check the results...";
        EXPECT_EQ(0, resp.result.failed_codes.size());

        EXPECT_EQ(30, resp.vertices.size());

        for (auto& vp : resp.vertices) {
            auto vidFound = vids.find(vp.vertex_id);
            ASSERT_TRUE(vidFound != vids.end()) << "Vid: " << vp.vertex_id;

            auto schema = schemaMng->getTagSchema(0, 3001, 0);
            ASSERT_NE(nullptr, schema);
            checkTagData<int64_t>(vp.tag_data, 3001, "tag_3001_col_0", schema, 0 + version);

            schema = schemaMng->getTagSchema(0, 3003, 0);
            ASSERT_NE(nullptr, schema);
            checkTagData<int64_t>(vp.tag_data, 3003, "tag_3003_col_2", schema, 2 + version);

            schema = schemaMng->getTagSchema(0, 3005, 0);
            ASSERT_NE(nullptr, schema);
            checkTagData<std::string>(vp.tag_data, 3005, "tag_3005_col_4", schema,
                                    folly::stringPrintf("tag_string_col_%ld", 4 + version));
        }
    }
}

TEST(QueryVertexPropsTest, SimpleTest) {
    fs::TempDir rootPath("/tmp/QueryVertexPropsTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());

    LOG(INFO) << "Prepare meta...";
    auto schemaMan = TestUtils::mockSchemaMan();

    auto executor = std::make_unique<folly::CPUThreadPoolExecutor>(3);
    TagVersion version = 1;
    mockData(kv.get(), version);
    testWithVersion(kv.get(), schemaMan.get(), executor.get(), version);

    version = 0;
    mockData(kv.get(), version);
    testWithVersion(kv.get(), schemaMan.get(), executor.get(), version);
}
}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
