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
#include "storage/query/QueryBoundProcessor.h"
#include "storage/test/TestUtils.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"
#include "meta/SchemaManager.h"
#include "common/filter/Expressions.h"

DEFINE_int64(max_rank, 1000, "max rank of each edge");
DEFINE_double(filter_ratio, 0.5, "ratio of data would pass filter");

std::unique_ptr<nebula::kvstore::KVStore> gKV;
std::unique_ptr<nebula::storage::AdHocSchemaManager> gSchemaMan;
std::unique_ptr<folly::CPUThreadPoolExecutor> gExecutor;

namespace nebula {
namespace storage {

// only add two tag and two edge
void mockData(kvstore::KVStore* kv) {
    GraphSpaceID spaceId = 0;
    PartitionID partId = 0;
    TagID tagId = 1;
    EdgeType edgeType = 101;
    std::vector<kvstore::KV> data;
    auto ver = std::numeric_limits<int32_t>::max() - 0;
    ver = folly::Endian::big(ver);

    // tags
    for (VertexID src = 1; src <= 10; src++) {
        VertexID dst = src + 100;
        {
            auto key = NebulaKeyUtils::vertexKey(partId, src, tagId, ver);

            RowWriter writer;
            writer << "Tim Duncan";
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
        // edges
        for (EdgeRanking rank = 0; rank < FLAGS_max_rank; rank++) {
            auto key = NebulaKeyUtils::edgeKey(partId, src, edgeType, rank, dst, ver);
            RowWriter writer(nullptr);
            writer << "Tim Duncan";
            writer << "Spurs";
            // use rank as startYear and endYear to add filter
            writer << rank;
            writer << rank;
            writer << 19;
            writer << 1392;
            writer << 19.0;
            writer << "zzzzz";
            writer << 5;
            auto val = writer.encode();
            data.emplace_back(std::move(key), std::move(val));
        }

        folly::Baton<true, std::atomic> baton;
        kv->asyncMultiPut(spaceId, partId, std::move(data),
            [&](kvstore::ResultCode code) {
                CHECK_EQ(code, kvstore::ResultCode::SUCCEEDED);
                baton.post();
            });
        baton.wait();
    }
}

void setUp(const char* path) {
    gExecutor = std::make_unique<folly::CPUThreadPoolExecutor>(1);
    GraphSpaceID spaceId = 0;
    gKV = TestUtils::initKV(path);
    gSchemaMan.reset(new storage::AdHocSchemaManager());

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

        gSchemaMan->addTagSchema(spaceId, 1, schema);
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

        gSchemaMan->addEdgeSchema(spaceId, 101, schema);
    }

    mockData(gKV.get());
}

cpp2::GetNeighborsRequest buildRequest(const std::vector<VertexID> vIds,
                                       const std::vector<std::string>& playerProps,
                                       const std::vector<std::string>& serveProps) {
    GraphSpaceID spaceId = 0;
    PartitionID partId = 0;
    TagID tagId = 1;
    EdgeType edgeType = 101;

    cpp2::GetNeighborsRequest req;
    req.set_space_id(spaceId);
    decltype(req.parts) parts;
    parts[partId] = vIds;
    req.set_parts(std::move(parts));
    req.set_edge_types({edgeType});

    for (const auto& prop : playerProps) {
        req.return_columns.emplace_back(TestUtils::vertexPropDef(prop, tagId));
    }
    for (const auto& prop : serveProps) {
        req.return_columns.emplace_back(TestUtils::edgePropDef(prop, edgeType));
    }
    return req;
}

}  // namespace storage
}  // namespace nebula

void go(int32_t iters,
        const std::vector<nebula::VertexID> vIds,
        const std::vector<std::string>& playerProps,
        const std::vector<std::string>& serveProps) {
    nebula::storage::cpp2::GetNeighborsRequest req;
    BENCHMARK_SUSPEND {
        req = nebula::storage::buildRequest(vIds, playerProps, serveProps);
    }
    for (decltype(iters) i = 0; i < iters; i++) {
        auto* processor = nebula::storage::QueryBoundProcessor::instance(
            gKV.get(), gSchemaMan.get(), nullptr, gExecutor.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
    }
}

void goFilter(int32_t iters,
              const std::vector<nebula::VertexID> vIds,
              const std::vector<std::string>& playerProps,
              const std::vector<std::string>& serveProps,
              int64_t value = FLAGS_max_rank * FLAGS_filter_ratio) {
    nebula::storage::cpp2::GetNeighborsRequest req;
    BENCHMARK_SUSPEND {
        req = nebula::storage::buildRequest(vIds, playerProps, serveProps);
        // where serve.startYear < value
        auto* edgeProp = new std::string("startYear");
        auto* alias = new std::string("101");
        auto* edgeExp = new nebula::AliasPropertyExpression(new std::string(""), alias, edgeProp);
        auto* priExp = new nebula::PrimaryExpression(value);
        auto relExp = std::make_unique<nebula::RelationalExpression>(
            edgeExp, nebula::RelationalExpression::Operator::GE, priExp);
        req.set_filter(nebula::Expression::encode(relExp.get()));
    }
    for (decltype(iters) i = 0; i < iters; i++) {
        auto* processor = nebula::storage::QueryBoundProcessor::instance(
            gKV.get(), gSchemaMan.get(), nullptr, gExecutor.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
    }
}

BENCHMARK(OneVertexOneProperty, iters) {
    go(iters, {1}, {"name"}, {"teamName"});
}
BENCHMARK_RELATIVE(OneVertexOnePropertyWithFilter, iters) {
    goFilter(iters, {1}, {"name"}, {"teamName"});
}

BENCHMARK(TenVertexOneProperty, iters) {
    go(iters, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, {"name"}, {"teamName"});
}
BENCHMARK_RELATIVE(TenVertexOnePropertyWithFilter, iters) {
    goFilter(iters, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, {"name"}, {"teamName"});
}

int main(int argc, char** argv) {
    folly::init(&argc, &argv, true);
    nebula::fs::TempDir rootPath("/tmp/QueryBoundBenchmarkTest.XXXXXX");
    nebula::storage::setUp(rootPath.path());
    folly::runBenchmarks();
    gKV.reset();
    return 0;
}


/*
40 processors, Intel(R) Xeon(R) CPU E5-2690 v2 @ 3.00GHz.
release

--max_rank=1000 --filter_ratio=0.1
============================================================================
GetNeighborsBenchmark.cpprelative                         time/iter  iters/s
============================================================================
OneVertexOneProperty                                       666.90us    1.50K
OneVertexOnePropertyWithFilter                    59.88%     1.11ms   897.93
TenVertexOneProperty                                         6.33ms   158.04
TenVertexOnePropertyWithFilter                    58.19%    10.87ms    91.96
============================================================================

--max_rank=1000 --filter_ratio=0.5
============================================================================
GetNeighborsBenchmark.cpprelative                         time/iter  iters/s
============================================================================
OneVertexOneProperty                                       665.77us    1.50K
OneVertexOnePropertyWithFilter                    78.97%   843.05us    1.19K
TenVertexOneProperty                                         6.30ms   158.70
TenVertexOnePropertyWithFilter                    78.05%     8.07ms   123.87
============================================================================

--max_rank=1000 --filter_ratio=1
============================================================================
GetNeighborsBenchmark.cpprelative                         time/iter  iters/s
============================================================================
OneVertexOneProperty                                       660.44us    1.51K
OneVertexOnePropertyWithFilter                   109.45%   603.40us    1.66K
TenVertexOneProperty                                         6.34ms   157.76
TenVertexOnePropertyWithFilter                   108.41%     5.85ms   171.03
============================================================================
*/
