/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <folly/Benchmark.h>
#include "fs/TempDir.h"
#include "utils/NebulaKeyUtils.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "storage/mutate/UpdateVertexProcessor.h"
#include "storage/mutate/UpdateEdgeProcessor.h"
#include "dataman/RowSetReader.h"
#include "dataman/RowReader.h"
#include "storage/test/TestUtils.h"
#include "meta/SchemaManager.h"


std::unique_ptr<nebula::kvstore::KVStore> gKV;
std::unique_ptr<nebula::storage::AdHocSchemaManager> schemaM;
std::unique_ptr<nebula::storage::AdHocIndexManager> indexM;

namespace nebula {
namespace storage {

void mockData(kvstore::KVStore* kv) {
    // Prepare data
    std::vector<kvstore::KV> data;
    for (int32_t pId = 0; pId < 3; pId++) {
        for (int32_t vId = pId * 10; vId < (pId + 1) * 10; vId++) {
            // NOTE: the range of tagId is [3001, 3008], excluding 3009(for insert test).
            for (int32_t tId = 3001; tId < 3010 - 1; tId++) {
                // Write multi versions, we should get/update the latest version
                for (int32_t v = 0; v < 3; v++) {
                    auto ver = std::numeric_limits<int32_t>::max() - v;
                    ver = folly::Endian::big(ver);
                    auto key = NebulaKeyUtils::vertexKey(pId, vId, tId, ver);

                    RowWriter writer;
                    for (int64_t numInt = 0; numInt < 3; numInt++) {
                        writer << pId + tId + v + numInt;
                    }
                    for (auto numString = 3; numString < 6; numString++) {
                        writer << folly::stringPrintf("tag_string_col_%d_%d", numString, v);
                    }
                    auto val = writer.encode();
                    data.emplace_back(std::move(key), std::move(val));
                }
            }

            // Generate 7 out-edges for each edgeType.
            for (int32_t dId = 10001; dId <= 10007; dId++) {
                // Write multi versions,  we should get the latest version.
                for (int32_t v = 0; v < 3; v++) {
                    auto ver = std::numeric_limits<int32_t>::max() - v;
                    ver = folly::Endian::big(ver);
                    auto key = NebulaKeyUtils::edgeKey(pId, vId, 101, 0, dId, ver);
                    RowWriter writer(nullptr);
                    for (uint64_t numInt = 0; numInt < 10; numInt++) {
                        writer << (dId + numInt);
                    }
                    for (auto numString = 10; numString < 20; numString++) {
                        writer << folly::stringPrintf("string_col_%d_%d", numString, v);
                    }
                    auto val = writer.encode();
                    data.emplace_back(std::move(key), std::move(val));
                }
            }
        }

        folly::Baton<true, std::atomic> baton;
        kv->asyncMultiPut(0, pId, std::move(data),
            [&](kvstore::ResultCode code) {
                CHECK_EQ(code, kvstore::ResultCode::SUCCEEDED);
                baton.post();
            });
        baton.wait();
    }
}


void setUp(const char* path) {
    GraphSpaceID spaceId = 0;
    gKV = TestUtils::initKV(path);
    schemaM.reset(new storage::AdHocSchemaManager());
    for (TagID tId = 3001; tId < 3010; tId++) {
        schemaM->addTagSchema(spaceId, tId, TestUtils::genTagSchemaProvider(tId, 3, 3));
    }
    for (EdgeType eType = 101; eType < 110; eType++) {
        schemaM->addEdgeSchema(spaceId, eType, TestUtils::genEdgeSchemaProvider(10, 10));
    }
    indexM.reset(new storage::AdHocIndexManager());
    for (TagID tId = 3001; tId < 3010; tId++) {
            std::vector<nebula::cpp2::ColumnDef> columns;
            for (int32_t i = 0; i < 3; i++) {
                nebula::cpp2::ColumnDef column;
                column.name = folly::stringPrintf("tag_%d_col_%d", tId, i);
                column.type.type = nebula::cpp2::SupportedType::INT;
                columns.emplace_back(std::move(column));
            }
            for (int32_t i = 3; i < 6; i++) {
                nebula::cpp2::ColumnDef column;
                column.name = folly::stringPrintf("tag_%d_col_%d", tId, i);
                column.type.type = nebula::cpp2::SupportedType::STRING;
                columns.emplace_back(std::move(column));
            }
            indexM->addTagIndex(spaceId, tId + 1000, tId, std::move(columns));
    }

    for (EdgeType eType = 101; eType < 110; eType++) {
            std::vector<nebula::cpp2::ColumnDef> columns;
            for (int32_t i = 0; i < 10; i++) {
                nebula::cpp2::ColumnDef column;
                column.name = folly::stringPrintf("col_%d", i);
                column.type.type = nebula::cpp2::SupportedType::INT;
                columns.emplace_back(std::move(column));
            }
            for (int32_t i = 10; i < 20; i++) {
                nebula::cpp2::ColumnDef column;
                column.name = folly::stringPrintf("col_%d", i);
                column.type.type = nebula::cpp2::SupportedType::STRING;
                columns.emplace_back(std::move(column));
            }
            indexM->addEdgeIndex(spaceId, eType + 100, eType, std::move(columns));
    }
    mockData(gKV.get());
}


cpp2::UpdateVertexRequest buildUpdateVertexReq(bool insertable) {
    cpp2::UpdateVertexRequest req;
    GraphSpaceID spaceId = 0;
    PartitionID partId = 0;
    VertexID vertexId = 1;

    req.set_space_id(spaceId);
    req.set_part_id(partId);
    req.set_vertex_id(vertexId);
    req.set_filter("");

    if (!insertable) {
        // Build updated props
        std::vector<cpp2::UpdateItem> items;
        // int: 3001.tag_3001_col_0 = 1
        cpp2::UpdateItem item1;
        item1.set_name("3001");
        item1.set_prop("tag_3001_col_0");
        PrimaryExpression val1(1L);
        item1.set_value(Expression::encode(&val1));
        items.emplace_back(item1);

        // string: 3001.tag_3001_col_4 = tag_string_col_4_2_new
        cpp2::UpdateItem item2;
        item2.set_name("3001");
        item2.set_prop("tag_3001_col_4");
        std::string col4new("tag_string_col_4_2_new");
        PrimaryExpression val2(col4new);
        item2.set_value(Expression::encode(&val2));
        items.emplace_back(item2);
        req.set_update_items(std::move(items));

        // Build yield
        // Return player props: name, age, country
        decltype(req.return_columns) tmpProps;
        auto* yTag1 =  new std::string(folly::to<std::string>(3001));
        auto* yProp1 = new std::string("tag_3001_col_0");
        SourcePropertyExpression sourcePropExp1(yTag1, yProp1);
        tmpProps.emplace_back(Expression::encode(&sourcePropExp1));

        auto* yTag2 =  new std::string(folly::to<std::string>(3001));
        auto* yProp2 = new std::string("tag_3001_col_2");
        SourcePropertyExpression sourcePropExp2(yTag2, yProp2);
        tmpProps.emplace_back(Expression::encode(&sourcePropExp2));

        auto* yTag3 =  new std::string(folly::to<std::string>(3001));
        auto* yProp3 = new std::string("tag_3001_col_4");
        SourcePropertyExpression sourcePropExp3(yTag3, yProp3);
        tmpProps.emplace_back(Expression::encode(&sourcePropExp3));

        req.set_return_columns(std::move(tmpProps));
        req.set_insertable(false);
    } else {
        std::vector<cpp2::UpdateItem> items;
        // int: 3009.tag_3009_col_0 = 1
        cpp2::UpdateItem item1;
        item1.set_name("3009");
        item1.set_prop("tag_3009_col_0");
        PrimaryExpression val1(1L);
        item1.set_value(Expression::encode(&val1));
        items.emplace_back(item1);

        // string: 3009.tag_3009_col_4 = tag_string_col_4_2_new
        cpp2::UpdateItem item2;
        item2.set_name("3009");
        item2.set_prop("tag_3009_col_4");
        std::string col4new("tag_string_col_4_2_new");
        PrimaryExpression val2(col4new);
        item2.set_value(Expression::encode(&val2));
        items.emplace_back(item2);
        req.set_update_items(std::move(items));

        // Build yield
        // Return player props: name, age, country
        decltype(req.return_columns) tmpProps;
        auto* yTag1 =  new std::string(folly::to<std::string>(3009));
        auto* yProp1 = new std::string("tag_3009_col_0");
        SourcePropertyExpression sourcePropExp1(yTag1, yProp1);
        tmpProps.emplace_back(Expression::encode(&sourcePropExp1));

        auto* yTag2 =  new std::string(folly::to<std::string>(3009));
        auto* yProp2 = new std::string("tag_3009_col_2");
        SourcePropertyExpression sourcePropExp2(yTag2, yProp2);
        tmpProps.emplace_back(Expression::encode(&sourcePropExp2));

        auto* yTag3 =  new std::string(folly::to<std::string>(3009));
        auto* yProp3 = new std::string("tag_3009_col_4");
        SourcePropertyExpression sourcePropExp3(yTag3, yProp3);
        tmpProps.emplace_back(Expression::encode(&sourcePropExp3));

        req.set_return_columns(std::move(tmpProps));
        req.set_insertable(true);
    }
    return req;
}

cpp2::UpdateEdgeRequest buildUpdateEdgeReq(bool insertable) {
    cpp2::UpdateEdgeRequest req;
    GraphSpaceID spaceId = 0;
    PartitionID partId = 0;
    req.set_space_id(spaceId);
    req.set_part_id(partId);

    if (!insertable) {
        // src = 1, edge_type = 101, ranking = 0, dst = 10001
        VertexID srcId = 1;
        VertexID dstId = 10001;
        storage::cpp2::EdgeKey edgeKey;
        edgeKey.set_src(srcId);
        edgeKey.set_edge_type(101);
        edgeKey.set_ranking(0);
        edgeKey.set_dst(dstId);
        req.set_edge_key(edgeKey);
        req.set_filter("");
        // Build updated props
        std::vector<cpp2::UpdateItem> items;
        // int: 101.col_0  = 10003
        cpp2::UpdateItem item1;
        item1.set_name("101");
        item1.set_prop("col_0");
        PrimaryExpression val1(100L);
        item1.set_value(Expression::encode(&val1));
        items.emplace_back(item1);

        // string: 101.col_10 = string_col_10_2_new
        cpp2::UpdateItem item2;
        item2.set_name("101");
        item2.set_prop("col_10");
        std::string col10new("string_col_10_2_new");
        PrimaryExpression val2(col10new);
        item2.set_value(Expression::encode(&val2));
        items.emplace_back(item2);
        req.set_update_items(std::move(items));


        decltype(req.return_columns) tmpColumns;
        tmpColumns.emplace_back(Expression::encode(&val1));
        tmpColumns.emplace_back(Expression::encode(&val2));
        req.set_return_columns(std::move(tmpColumns));
        req.set_insertable(false);
    } else {
        VertexID srcId = 1;
        VertexID dstId = 10008;
        // src = 1, edge_type = 101, ranking = 0, dst = 10008
        storage::cpp2::EdgeKey edgeKey;
        edgeKey.set_src(srcId);
        edgeKey.set_edge_type(101);
        edgeKey.set_ranking(0);
        edgeKey.set_dst(dstId);
        req.set_edge_key(edgeKey);
        req.set_filter("");

        // Build updated props
        std::vector<cpp2::UpdateItem> items;
        // build update props
        cpp2::UpdateItem item1;
        item1.set_name("101");
        item1.set_prop("col_0");
        PrimaryExpression val1(100L);
        item1.set_value(Expression::encode(&val1));
        items.emplace_back(item1);

        // string: 101.col_10 = string_col_10_2_new
        cpp2::UpdateItem item2;
        item2.set_name("101");
        item2.set_prop("col_10");
        std::string col10new("string_col_10_2_new");
        PrimaryExpression val2(col10new);
        item2.set_value(Expression::encode(&val2));
        items.emplace_back(item2);
        req.set_update_items(std::move(items));

        // Build yield
        // Return player props: name, age, country
        decltype(req.return_columns) tmpColumns;
        tmpColumns.emplace_back(Expression::encode(&val1));
        tmpColumns.emplace_back(Expression::encode(&val2));
        req.set_return_columns(std::move(tmpColumns));
        req.set_insertable(false);
        req.set_insertable(true);
    }
    return req;
}

}  // namespace storage
}  // namespace nebula

void updateVertex(int32_t iters, bool insertable) {
    nebula::storage::cpp2::UpdateVertexRequest req;
    BENCHMARK_SUSPEND {
        req = nebula::storage::buildUpdateVertexReq(insertable);
    }

    for (decltype(iters) i = 0; i < iters; i++) {
        // Test UpdateVertexRequest
        auto* processor
            = nebula::storage::UpdateVertexProcessor::instance(gKV.get(),
                                                               schemaM.get(),
                                                               indexM.get(),
                                                               nullptr);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        if (!resp.result.failed_codes.empty()) {
            LOG(ERROR) << "update faild";
            return;
        }
    }
}


void updateEdge(int32_t iters, bool insertable) {
    nebula::storage::cpp2::UpdateEdgeRequest req;
    BENCHMARK_SUSPEND {
        req = nebula::storage::buildUpdateEdgeReq(insertable);
    }

    for (decltype(iters) i = 0; i < iters; i++) {
        // Test UpdateEdgeRequest
        auto* processor
             = nebula::storage::UpdateEdgeProcessor::instance(gKV.get(),
                                                              schemaM.get(),
                                                              indexM.get(),
                                                              nullptr);
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        if (!resp.result.failed_codes.empty()) {
            LOG(ERROR) << "update faild";
            return;
        }
    }
}


BENCHMARK(update_vertex, iters) {
    updateVertex(iters, false);
}

BENCHMARK(upsert_vertex, iters) {
    updateVertex(iters, true);
}

BENCHMARK(update_edge, iters) {
    updateEdge(iters, false);
}

BENCHMARK(upsert_edge, iters) {
    updateEdge(iters, true);
}


int main(int argc, char** argv) {
    folly::init(&argc, &argv, true);
    nebula::fs::TempDir rootPath("/tmp/UpdateTest.XXXXXX");
    nebula::storage::setUp(rootPath.path());
    folly::runBenchmarks();
    gKV.reset();
    return 0;
}


/**

CPU : Intel(R) Xeon(R) CPU E5-2697 v3 @ 2.60GHz

update_vertex   : tag data exist and update

upsert_vertex   : tag data (first not exist) and upsert

update_edge     : edge data exist and update

upsert_edge     : edge data (first not exist)  and upsert

V1.0
==============================================================================
src/storage/test/UpdateVertexAndEdgeBenchmark.cpprelative  time/iter  iters/s
==============================================================================
update_vertex                                               322.47us    3.10K
upsert_vertex                                               324.23us    3.08K
update_edge                                                 387.79us    2.58K
upsert_edge                                                 369.61us    2.71K
==============================================================================

**/
