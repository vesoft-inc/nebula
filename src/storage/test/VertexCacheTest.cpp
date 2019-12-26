/* Copyright (c) 2019 vesoft inc. All rights reserved.
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
#include "storage/mutate/AddVerticesProcessor.h"
#include "storage/query/QueryVertexPropsProcessor.h"
#include "dataman/RowSetReader.h"
#include "dataman/RowReader.h"

DECLARE_int32(max_handlers_per_req);

namespace nebula {
namespace storage {

void addVertices(kvstore::KVStore* kv, VertexCache* cache, int nums) {
    LOG(INFO) << "Build AddVerticesRequest...";
    auto* processor = AddVerticesProcessor::instance(kv, nullptr, nullptr, cache);
    cpp2::AddVerticesRequest req;
    req.space_id = 0;
    req.overwritable = true;
    auto vertices = TestUtils::setupVertices(0, nums, 1, 3001);
    req.parts.emplace(0, std::move(vertices));

    LOG(INFO) << "Test AddVerticesProcessor...";
    auto fut = processor->getFuture();
    processor->process(req);
    auto resp = std::move(fut).get();
    EXPECT_EQ(0, resp.result.failed_codes.size());
}

void checkCache(VertexCache* cache, uint64_t evicts, uint64_t hits, uint64_t total) {
    EXPECT_EQ(evicts, cache->evicts());
    EXPECT_EQ(hits, cache->hits());
    EXPECT_EQ(total, cache->total());
}

void prepareData(kvstore::KVStore* kv) {
    LOG(INFO) << "Prepare data...";
    std::vector<kvstore::KV> data;
    TagID tagId = 3001;
    for (auto vertexId = 0; vertexId < 10000; vertexId++) {
        auto key = NebulaKeyUtils::vertexKey(0, vertexId, tagId, 0);
        RowWriter writer;
        for (int64_t numInt = 0; numInt < 3; numInt++) {
            writer << numInt;
        }
        for (auto numString = 3; numString < 6; numString++) {
            writer << folly::stringPrintf("tag_string_col_%d", numString);
        }
        auto val = writer.encode();
        data.emplace_back(std::move(key), std::move(val));
    }
    folly::Baton<true, std::atomic> baton;
    kv->asyncMultiPut(
        0,
        0,
        std::move(data),
        [&](kvstore::ResultCode code) {
            EXPECT_EQ(code, kvstore::ResultCode::SUCCEEDED);
            baton.post();
        });
    baton.wait();
}

void fetchVertices(kvstore::KVStore* kv,
                   meta::SchemaManager* schemaMan,
                   folly::Executor* executor,
                   VertexCache* cache,
                   VertexID start,
                   VertexID end) {
    cpp2::VertexPropRequest req;
    req.set_space_id(0);
    decltype(req.parts) tmpIds;
    for (auto vertexId = start; vertexId < end; vertexId++) {
        tmpIds[0].emplace_back(vertexId);
    }
    req.set_parts(std::move(tmpIds));
    // Return tag props col_0, col_2, col_4
    decltype(req.return_columns) tmpColumns;
    TagID tagId = 3001;
    for (int i = 0; i < 3; i++) {
        tmpColumns.emplace_back(TestUtils::vertexPropDef(
            folly::stringPrintf("tag_%d_col_%d", tagId, i * 2), tagId));
    }
    req.set_return_columns(std::move(tmpColumns));

    LOG(INFO) << "Test QueryVertexPropsRequest...";
    auto* processor = QueryVertexPropsProcessor::instance(kv,
                                                          schemaMan,
                                                          nullptr,
                                                          executor,
                                                          cache);
    auto f = processor->getFuture();
    processor->process(req);
    auto resp = std::move(f).get();

    LOG(INFO) << "Check the results...";
    EXPECT_EQ(0, resp.result.failed_codes.size());
    EXPECT_EQ(end - start, resp.vertices.size());
    auto* vschema = resp.get_vertex_schema();
    DCHECK(vschema != nullptr);
    for (auto& vp : resp.vertices) {
        auto size = std::accumulate(vp.tag_data.cbegin(), vp.tag_data.cend(), 0,
                                    [vschema](int acc, auto& td) {
                                        auto it = vschema->find(td.tag_id);
                                        DCHECK(it != vschema->end());
                                        return acc + it->second.columns.size();
                                    });
        EXPECT_EQ(3, size);
        checkTagData<int64_t>(vp.tag_data, 3001, "tag_3001_col_0", vschema, 0);
        checkTagData<int64_t>(vp.tag_data, 3001, "tag_3001_col_2", vschema, 2);
        checkTagData<std::string>(vp.tag_data, 3001, "tag_3001_col_4", vschema,
                                  folly::stringPrintf("tag_string_col_4"));
    }
}

TEST(VertexCacheTest, SimpleTest) {
    FLAGS_max_handlers_per_req = 1;
    fs::TempDir rootPath("/tmp/VertexCacheTest.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path());
    auto schemaMan = TestUtils::mockSchemaMan();
    auto executor = std::make_unique<folly::CPUThreadPoolExecutor>(1);
    prepareData(kv.get());
    VertexCache cache(1000, 0);

    LOG(INFO) << "Fetch some vertices...";
    fetchVertices(kv.get(), schemaMan.get(), executor.get(), &cache, 0, 1000);
    checkCache(&cache, 0, 0, 1000);

    fetchVertices(kv.get(), schemaMan.get(), executor.get(), &cache, 500, 1500);
    checkCache(&cache, 500, 500, 2000);

    LOG(INFO) << "Insert vertices from 0 to 1000";
    addVertices(kv.get(), &cache, 1000);
    checkCache(&cache, 1000, 500, 2000);
}

}  // namespace storage
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}


