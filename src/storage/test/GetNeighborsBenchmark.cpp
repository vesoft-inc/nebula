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
DEFINE_double(filter_ratio, 0.1, "ratio of data would pass filter");

std::unique_ptr<nebula::kvstore::KVStore> gKV;
std::unique_ptr<nebula::storage::AdHocSchemaManager> gSchemaMan;
std::unique_ptr<folly::CPUThreadPoolExecutor> gExecutor;

namespace nebula {
namespace storage {

// 10 tags and 10 * max_rank edges
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

std::string encode(const nebula::storage::cpp2::QueryResponse &resp) {
    std::string val;
    apache::thrift::CompactSerializer::serialize(resp, &val);
    return val;
}

void encodeBench(int32_t iters,
                 const std::vector<nebula::VertexID> vIds,
                 const std::vector<std::string>& playerProps,
                 const std::vector<std::string>& serveProps) {
    nebula::storage::cpp2::QueryResponse resp;
    BENCHMARK_SUSPEND {
        auto req = nebula::storage::buildRequest(vIds, playerProps, serveProps);
        auto* processor = nebula::storage::QueryBoundProcessor::instance(
            gKV.get(), gSchemaMan.get(), nullptr, gExecutor.get());
        auto f = processor->getFuture();
        processor->process(req);
        resp = std::move(f).get();
    }
    for (decltype(iters) i = 0; i < iters; i++) {
        auto encoded = encode(resp);
        folly::doNotOptimizeAway(encoded);
    }
}

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
        auto encoded = encode(resp);
        folly::doNotOptimizeAway(encoded);
    }
}

void goFilter(int32_t iters,
              const std::vector<nebula::VertexID> vIds,
              const std::vector<std::string>& playerProps,
              const std::vector<std::string>& serveProps,
              int64_t value = FLAGS_max_rank * FLAGS_filter_ratio,
              bool oneFilter = true) {
    nebula::storage::cpp2::GetNeighborsRequest req;
    BENCHMARK_SUSPEND {
        req = nebula::storage::buildRequest(vIds, playerProps, serveProps);
        if (oneFilter) {
            // where serve.startYear < value
            auto* edgeProp = new std::string("startYear");
            auto* alias = new std::string("101");
            auto* edgeExp = new nebula::AliasPropertyExpression(
                new std::string(""), alias, edgeProp);
            auto* priExp = new nebula::PrimaryExpression(value);
            auto relExp = std::make_unique<nebula::RelationalExpression>(
                edgeExp, nebula::RelationalExpression::Operator::LT, priExp);
            req.set_filter(nebula::Expression::encode(relExp.get()));
        } else {
            // where serve.startYear < value && serve.endYear < value
            // since startYear always equal to endYear, the data of which can pass filter is same,
            // just to test perf of multiple filter
            auto exp = std::make_unique<nebula::LogicalExpression>(
                new nebula::RelationalExpression(
                    new nebula::AliasPropertyExpression(
                        new std::string(""), new std::string("101"), new std::string("startYear")),
                    nebula::RelationalExpression::Operator::LT,
                    new nebula::PrimaryExpression(value)),
                nebula::LogicalExpression::AND,
                new nebula::RelationalExpression(
                    new nebula::AliasPropertyExpression(
                        new std::string(""), new std::string("101"), new std::string("endYear")),
                    nebula::RelationalExpression::Operator::LT,
                    new nebula::PrimaryExpression(value)));
            req.set_filter(nebula::Expression::encode(exp.get()));
        }
    }
    for (decltype(iters) i = 0; i < iters; i++) {
        auto* processor = nebula::storage::QueryBoundProcessor::instance(
            gKV.get(), gSchemaMan.get(), nullptr, gExecutor.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
        auto encoded = encode(resp);
        folly::doNotOptimizeAway(encoded);
    }
}

BENCHMARK(EncodeOneProperty, iters) {
    encodeBench(iters, {1}, {"name"}, {"playerName"});
}
BENCHMARK_RELATIVE(EncodeThreeProperty, iters) {
    encodeBench(iters, {1}, {"name"}, {"playerName", "teamName", "startYear"});
}
BENCHMARK_RELATIVE(EncodeFiveProperty, iters) {
    encodeBench(iters, {1}, {"name"},
                {"playerName", "teamName", "startYear", "endYear", "teamCareer"});
}
BENCHMARK_DRAW_LINE();

BENCHMARK(OneVertexOneProperty, iters) {
    go(iters, {1}, {"name"}, {"teamName"});
}
BENCHMARK_RELATIVE(OneVertexOnlyId, iters) {
    go(iters, {1}, {"name"}, {"_dst"});
}
BENCHMARK_RELATIVE(OneVertexThreeProperty, iters) {
    go(iters, {1}, {"name"}, {"playerName", "teamName", "startYear"});
}
BENCHMARK_RELATIVE(OneVertexFiveProperty, iters) {
    go(iters, {1}, {"name"}, {"playerName", "teamName", "startYear", "endYear", "teamCareer"});
}
BENCHMARK_DRAW_LINE();

BENCHMARK(NoFilter, iters) {
    go(iters, {1}, {"name"}, {"teamName"});
}
BENCHMARK_RELATIVE(OneFilterNonePass, iters) {
    goFilter(iters, {1}, {"name"}, {"teamName"}, FLAGS_max_rank * 0);
}
BENCHMARK_RELATIVE(OneFilterFewPass, iters) {
    goFilter(iters, {1}, {"name"}, {"teamName"}, FLAGS_max_rank * 0.1);
}
BENCHMARK_RELATIVE(OneFilterHalfPass, iters) {
    goFilter(iters, {1}, {"name"}, {"teamName"}, FLAGS_max_rank * 0.5);
}
BENCHMARK_RELATIVE(OneFilterAllPass, iters) {
    goFilter(iters, {1}, {"name"}, {"teamName"}, FLAGS_max_rank * 1);
}
BENCHMARK_RELATIVE(TwoFilterNonePass, iters) {
    goFilter(iters, {1}, {"name"}, {"teamName"}, FLAGS_max_rank * 0, false);
}
BENCHMARK_RELATIVE(TwoFilterFewPass, iters) {
    goFilter(iters, {1}, {"name"}, {"teamName"}, FLAGS_max_rank * 0.1, false);
}
BENCHMARK_RELATIVE(TwoFilterHalfPass, iters) {
    goFilter(iters, {1}, {"name"}, {"teamName"}, FLAGS_max_rank * 0.5, false);
}
BENCHMARK_RELATIVE(TwoFilterAllPass, iters) {
    goFilter(iters, {1}, {"name"}, {"teamName"}, FLAGS_max_rank * 1, false);
}
BENCHMARK_DRAW_LINE();

BENCHMARK(TenVertexOneProperty, iters) {
    go(iters, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, {"name"}, {"teamName"});
}
BENCHMARK_RELATIVE(TenVertexOnlyId, iters) {
    go(iters, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, {"name"}, {"_dst"});
}
BENCHMARK_RELATIVE(TenVertexThreeProperty, iters) {
    go(iters, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, {"name"}, {"playerName", "teamName", "startYear"});
}
BENCHMARK_RELATIVE(TenVertexFiveProperty, iters) {
    go(iters, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
       {"name"}, {"playerName", "teamName", "startYear", "endYear", "teamCareer"});
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

--max_rank=1000
============================================================================
GetNeighborsBenchmark.cpprelative                         time/iter  iters/s
============================================================================
EncodeOneProperty                                           29.64us   33.74K
EncodeThreeProperty                               96.18%    30.81us   32.45K
EncodeFiveProperty                                93.43%    31.72us   31.53K
----------------------------------------------------------------------------
OneVertexOneProperty                                       660.73us    1.51K
OneVertexOnlyId                                  348.60%   189.54us    5.28K
OneVertexThreeProperty                            59.34%     1.11ms   898.08
OneVertexFiveProperty                             42.89%     1.54ms   649.15
----------------------------------------------------------------------------
NoFilter                                                   656.47us    1.52K
OneFilterNonePass                                 92.77%   707.64us    1.41K
OneFilterFewPass                                  85.99%   763.42us    1.31K
OneFilterHalfPass                                 68.93%   952.34us    1.05K
OneFilterAllPass                                  56.61%     1.16ms   862.36
TwoFilterNonePass                                 62.03%     1.06ms   944.86
TwoFilterFewPass                                  58.74%     1.12ms   894.76
TwoFilterHalfPass                                 49.72%     1.32ms   757.37
TwoFilterAllPass                                  42.52%     1.54ms   647.76
----------------------------------------------------------------------------
TenVertexOneProperty                                         6.36ms   157.26
TenVertexOnlyId                                  372.16%     1.71ms   585.26
TenVertexThreeProperty                            58.76%    10.82ms    92.41
TenVertexFiveProperty                             41.77%    15.22ms    65.69
============================================================================
*/
