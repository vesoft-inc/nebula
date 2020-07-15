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
#include "storage/mutate/AddVerticesProcessor.h"
#include "storage/mutate/AddEdgesProcessor.h"
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

// only add two tag and two edge
void mockData(kvstore::KVStore* kv) {
    PartitionID partId = 0;
    VertexID vertexId = 1;
    VertexID dId = 10;
    // Prepare data
    std::vector<kvstore::KV> data;
    // Write multi versions, we should get/update the latest version
    auto ver = std::numeric_limits<int32_t>::max() - 0;
    ver = folly::Endian::big(ver);

    // add two tag
    {
        for (auto v = vertexId; v <= vertexId + 1; v++) {
            auto key = NebulaKeyUtils::vertexKey(partId, v, 1, ver);

            RowWriter writer;
            if (v == 1) {
                writer << "Tim Duncan";
            } else {
                writer << "Tim Duncanv1";
            }
            writer << 44;
            writer << false;
            writer << 19;
            writer << 1997;
            writer << 2016;
            writer << 1392;
            writer << 19.0;
            writer << 1;
            writer << "America";
            writer << 5;
            auto val = writer.encode();
            data.emplace_back(std::move(key), std::move(val));
        }
    }

    // add two edge
    {
        for (auto v = vertexId; v <= vertexId + 1; v++) {
            auto key = NebulaKeyUtils::edgeKey(partId, v, 101, 0, dId, ver);
            RowWriter writer(nullptr);
            if (v == 1) {
                writer << "Tim Duncan";
            } else {
                writer << "Tim Duncanv1";
            }
            writer << "Spurs";
            writer << 1997;
            writer << 2016;
            writer << 19;
            writer << 1392;
            writer << 19.0;
            writer << "zzzzz";
            writer << 5;
            auto val = writer.encode();
            data.emplace_back(std::move(key), std::move(val));
        }
    }

    folly::Baton<true, std::atomic> baton;
    kv->asyncMultiPut(0, partId, std::move(data),
        [&](kvstore::ResultCode code) {
            CHECK_EQ(code, kvstore::ResultCode::SUCCEEDED);
            baton.post();
        });
    baton.wait();
}


void setUp(const char* path) {
    GraphSpaceID spaceId = 0;
    gKV = TestUtils::initKV(path);
    schemaM.reset(new storage::AdHocSchemaManager());

    // add tag schema
    {
        std::shared_ptr<meta::NebulaSchemaProvider> schema(new meta::NebulaSchemaProvider(0));
        nebula::cpp2::ColumnDef column1;
        column1.type.type = nebula::cpp2::SupportedType::STRING;
        schema->addField("name", std::move(column1.type));

        nebula::cpp2::ColumnDef column2;
        column2.type.type = nebula::cpp2::SupportedType::INT;
        schema->addField("age", std::move(column2.type));

        nebula::cpp2::ColumnDef column3;
        column3.type.type = nebula::cpp2::SupportedType::BOOL;
        schema->addField("playing", std::move(column3.type));


        nebula::cpp2::ColumnDef column4;
        column4.type.type = nebula::cpp2::SupportedType::INT;
        schema->addField("career", std::move(column4.type));

        nebula::cpp2::ColumnDef column5;
        column5.type.type = nebula::cpp2::SupportedType::INT;
        schema->addField("startYear", std::move(column5.type));

        nebula::cpp2::ColumnDef column6;
        column6.type.type = nebula::cpp2::SupportedType::INT;
        schema->addField("endYear", std::move(column6.type));

        nebula::cpp2::ColumnDef column7;
        column7.type.type = nebula::cpp2::SupportedType::INT;
        schema->addField("games", std::move(column7.type));

        nebula::cpp2::ColumnDef column8;
        column8.type.type = nebula::cpp2::SupportedType::DOUBLE;
        schema->addField("avgScore", std::move(column8.type));

        nebula::cpp2::ColumnDef column9;
        column9.type.type = nebula::cpp2::SupportedType::INT;
        schema->addField("serveTeams", std::move(column9.type));

        nebula::cpp2::ColumnDef column10;
        column10.type.type = nebula::cpp2::SupportedType::STRING;
        schema->addField("country", std::move(column10.type));

        nebula::cpp2::ColumnDef column11;
        column11.type.type = nebula::cpp2::SupportedType::INT;
        schema->addField("champions", std::move(column11.type));

        schemaM->addTagSchema(spaceId, 1, schema);
    }

    // add edge schema
    {
        std::shared_ptr<meta::NebulaSchemaProvider> schema(new meta::NebulaSchemaProvider(0));
        nebula::cpp2::ColumnDef column1;
        column1.type.type = nebula::cpp2::SupportedType::STRING;
        schema->addField("playerName", std::move(column1.type));

        nebula::cpp2::ColumnDef column2;
        column2.type.type = nebula::cpp2::SupportedType::STRING;
        schema->addField("teamName", std::move(column2.type));

        nebula::cpp2::ColumnDef column3;
        column3.type.type = nebula::cpp2::SupportedType::INT;
        schema->addField("startYear", std::move(column3.type));

        nebula::cpp2::ColumnDef column4;
        column4.type.type = nebula::cpp2::SupportedType::INT;
        schema->addField("endYear", std::move(column4.type));

        nebula::cpp2::ColumnDef column5;
        column5.type.type = nebula::cpp2::SupportedType::INT;
        schema->addField("teamCareer", std::move(column5.type));

        nebula::cpp2::ColumnDef column6;
        column6.type.type = nebula::cpp2::SupportedType::INT;
        schema->addField("teamGames", std::move(column6.type));

        nebula::cpp2::ColumnDef column7;
        column7.type.type = nebula::cpp2::SupportedType::DOUBLE;
        schema->addField("teamAvgScore", std::move(column7.type));

        nebula::cpp2::ColumnDef column8;
        column8.type.type = nebula::cpp2::SupportedType::STRING;
        schema->addField("type", std::move(column8.type));

        nebula::cpp2::ColumnDef column9;
        column9.type.type = nebula::cpp2::SupportedType::INT;
        schema->addField("champions", std::move(column9.type));

        schemaM->addEdgeSchema(spaceId, 101, schema);
    }

    indexM.reset(new storage::AdHocIndexManager());
    mockData(gKV.get());
}

cpp2::AddVerticesRequest buildAddVertexReq() {
    cpp2::AddVerticesRequest req;
    GraphSpaceID spaceId = 0;
    PartitionID partId = 0;
    VertexID vertexId = 2;
    TagID tId = 1;

    req.set_space_id(spaceId);
    req.set_overwritable(true);
    std::vector<cpp2::Tag> tags;
    cpp2::Tag t;
    t.set_tag_id(tId);
    RowWriter writer;
    writer << "Tony Parker";
    writer << 38;
    writer << false;
    writer << 18;
    writer << 2001;
    writer << 2019;
    writer << 1254;
    writer << 15.5;
    writer << 2;
    writer << "France";
    writer << 4;

    auto val = writer.encode();
    t.set_props(std::move(val));
    tags.emplace_back(std::move(t));
    cpp2::Vertex v;
    v.set_id(vertexId);
    v.set_tags(std::move(tags));

    std::vector<cpp2::Vertex> vertices;
    vertices.emplace_back(std::move(v));
    req.parts.emplace(partId, std::move(vertices));
    return req;
}

cpp2::AddEdgesRequest buildAddEdgeReq() {
    cpp2::AddEdgesRequest req;
    GraphSpaceID spaceId = 0;
    PartitionID partId = 0;
    VertexID srcId = 1;
    VertexID dstId = 11;

    req.set_space_id(spaceId);
    req.set_overwritable(true);

    std::vector<cpp2::Edge> edges;
    cpp2::EdgeKey key;
    key.set_src(srcId);
    key.set_edge_type(101);
    key.set_ranking(0);
    key.set_dst(dstId);
    edges.emplace_back();
    edges.back().set_key(std::move(key));
    RowWriter writer;
    writer << "Tony Parker";
    writer << "Spurs";
    writer << 2001;
    writer << 2018;
    writer << 17;
    writer << 1198;
    writer << 16.6;
    writer << "trade";
    writer << 4;
    auto val = writer.encode();
    edges.back().set_props(std::move(val));

    // only insert one vertex one tag, 1.3009
    req.parts.emplace(partId, std::move(edges));
    return req;
}

cpp2::UpdateVertexRequest buildUpdateVertexReq() {
    cpp2::UpdateVertexRequest req;
    GraphSpaceID spaceId = 0;
    PartitionID partId = 0;
    VertexID vertexId = 1;

    req.set_space_id(spaceId);
    req.set_part_id(partId);
    req.set_vertex_id(vertexId);
    req.set_filter("");

    // Build updated props
    std::vector<cpp2::UpdateItem> items;
    // int: 1.age = 45
    cpp2::UpdateItem item1;
    item1.set_name("1");
    item1.set_prop("age");
    PrimaryExpression val1(45L);
    item1.set_value(Expression::encode(&val1));
    items.emplace_back(item1);

    // string: 1.country = China
    cpp2::UpdateItem item2;
    item2.set_name("1");
    item2.set_prop("country");
    std::string col4new("China");
    PrimaryExpression val2(col4new);
    item2.set_value(Expression::encode(&val2));
    items.emplace_back(item2);
    req.set_update_items(std::move(items));

    // Build yield
    // Return player props: name, age, country
    decltype(req.return_columns) tmpProps;
    auto* yTag1 =  new std::string("1");
    auto* yProp1 = new std::string("name");
    SourcePropertyExpression sourcePropExp1(yTag1, yProp1);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp1));

    auto* yTag2 =  new std::string("1");
    auto* yProp2 = new std::string("age");
    SourcePropertyExpression sourcePropExp2(yTag2, yProp2);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp2));

    auto* yTag3 =  new std::string("1");
    auto* yProp3 = new std::string("country");
    SourcePropertyExpression sourcePropExp3(yTag3, yProp3);
    tmpProps.emplace_back(Expression::encode(&sourcePropExp3));

    req.set_return_columns(std::move(tmpProps));
    req.set_insertable(false);
    return req;
}

cpp2::UpdateEdgeRequest buildUpdateEdgeReq() {
    cpp2::UpdateEdgeRequest req;
    GraphSpaceID spaceId = 0;
    PartitionID partId = 0;
    req.set_space_id(spaceId);
    req.set_part_id(partId);

    // src = 1, edge_type = 101, ranking = 0, dst = 10
    VertexID srcId = 1;
    VertexID dstId = 10;
    storage::cpp2::EdgeKey edgeKey;
    edgeKey.set_src(srcId);
    edgeKey.set_edge_type(101);
    edgeKey.set_ranking(0);
    edgeKey.set_dst(dstId);
    req.set_edge_key(edgeKey);
    req.set_filter("");
    // Build updated props
    std::vector<cpp2::UpdateItem> items;
    // int: 101.teamCareer  = 20
    cpp2::UpdateItem item1;
    item1.set_name("101");
    item1.set_prop("teamCareer");
    PrimaryExpression val1(20L);
    item1.set_value(Expression::encode(&val1));
    items.emplace_back(item1);

    // string: 101.type = trade
    cpp2::UpdateItem item2;
    item2.set_name("101");
    item2.set_prop("type");
    std::string col10new("trade");
    PrimaryExpression val2(col10new);
    item2.set_value(Expression::encode(&val2));
    items.emplace_back(item2);
    req.set_update_items(std::move(items));


    decltype(req.return_columns) tmpColumns;
    AliasPropertyExpression edgePropExp(
        new std::string(""), new std::string("101"), new std::string("playerName"));
    tmpColumns.emplace_back(Expression::encode(&edgePropExp));
    edgePropExp = AliasPropertyExpression(
        new std::string(""), new std::string("101"), new std::string("teamName"));
    tmpColumns.emplace_back(Expression::encode(&edgePropExp));
    edgePropExp = AliasPropertyExpression(
        new std::string(""), new std::string("101"), new std::string("teamCareer"));
    tmpColumns.emplace_back(Expression::encode(&edgePropExp));
    edgePropExp = AliasPropertyExpression(
        new std::string(""), new std::string("101"), new std::string("type"));
    tmpColumns.emplace_back(Expression::encode(&edgePropExp));

    req.set_return_columns(std::move(tmpColumns));
    req.set_insertable(false);
    return req;
}

}  // namespace storage
}  // namespace nebula

void insertVertex(int32_t iters) {
    nebula::storage::cpp2::AddVerticesRequest  req;
    BENCHMARK_SUSPEND {
        req = nebula::storage::buildAddVertexReq();
    }
    for (decltype(iters) i = 0; i < iters; i++) {
        // Test UpdateVertexRequest
        auto* processor
            = nebula::storage::AddVerticesProcessor::instance(gKV.get(),
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

void insertEdge(int32_t iters) {
    nebula::storage::cpp2::AddEdgesRequest  req;
    BENCHMARK_SUSPEND {
        req = nebula::storage::buildAddEdgeReq();
    }
    for (decltype(iters) i = 0; i < iters; i++) {
        // Test UpdateVertexRequest
        auto* processor
            = nebula::storage::AddEdgesProcessor::instance(gKV.get(),
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

void updateVertex(int32_t iters) {
    nebula::storage::cpp2::UpdateVertexRequest req;
    BENCHMARK_SUSPEND {
        req = nebula::storage::buildUpdateVertexReq();
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


void updateEdge(int32_t iters) {
    nebula::storage::cpp2::UpdateEdgeRequest req;
    BENCHMARK_SUSPEND {
        req = nebula::storage::buildUpdateEdgeReq();
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
    updateVertex(iters);
}

BENCHMARK(update_edge, iters) {
    updateEdge(iters);
}

BENCHMARK(insert_vertex, iters) {
    insertVertex(iters);
}

BENCHMARK(insert_edge, iters) {
    insertEdge(iters);
}

int main(int argc, char** argv) {
    FLAGS_enable_multi_versions = true;
    folly::init(&argc, &argv, true);
    nebula::fs::TempDir rootPath("/tmp/UpdateTest.XXXXXX");
    nebula::storage::setUp(rootPath.path());
    folly::runBenchmarks();
    gKV.reset();
    return 0;
}


/**

CPU : Intel(R) Xeon(R) CPU E5-2697 v3 @ 2.60GHz

release version

when update, there are four record(two tag record and two edge record)
before insert, there are four record(two tag record and two edge record)

update_vertex   : tag data exist and update

update_edge     : edge data exist and update

insert_vertex   : insert one record of one tag of one vertex

insert_edge     : insert one record of one edge


V1.0 in nebula 1.0
==============================================================================
src/storage/test/UpdateVertexAndEdgeBenchmark.cpprelative  time/iter  iters/s
==============================================================================
update_vertex                                               46.54us   21.49K
update_edge                                                 48.17us   20.76K
insert_vertex                                               17.07us   58.60K
insert_edge                                                 18.03us   55.48K
==============================================================================

**/
