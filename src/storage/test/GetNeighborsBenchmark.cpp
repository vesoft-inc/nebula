/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include <folly/Benchmark.h>
#include "common/fs/TempDir.h"
#include "storage/query/GetNeighborsProcessor.h"
#include "storage/test/QueryTestUtils.h"

DEFINE_uint64(max_rank, 1000, "max rank of each edge");
DEFINE_double(filter_ratio, 0.5, "ratio of data would pass filter");

std::unique_ptr<nebula::mock::MockCluster> gCluster;

namespace nebula {
namespace storage {

cpp2::GetNeighborsRequest buildRequest(const std::vector<VertexID>& vertex,
                                       const std::vector<std::string>& playerProps,
                                       const std::vector<std::string>& serveProps) {
    TagID player = 1;
    EdgeType serve = 101;
    auto totalParts = gCluster->getTotalParts();
    std::vector<VertexID> vertices = vertex;
    std::vector<EdgeType> over = {serve};
    std::vector<std::pair<TagID, std::vector<std::string>>> tags;
    std::vector<std::pair<EdgeType, std::vector<std::string>>> edges;
    tags.emplace_back(player, playerProps);
    edges.emplace_back(serve, serveProps);
    auto req = QueryTestUtils::buildRequest(totalParts, vertices, over, tags, edges);
    return req;
}

void setUp(const char* path, EdgeRanking maxRank) {
    gCluster = std::make_unique<nebula::mock::MockCluster>();
    gCluster->initStorageKV(path);
    auto* env = gCluster->storageEnv_.get();
    auto totalParts = gCluster->getTotalParts();
    ASSERT_EQ(true, QueryTestUtils::mockVertexData(env, totalParts));
    ASSERT_EQ(true, QueryTestUtils::mockBenchEdgeData(env, totalParts, 1, maxRank));
}

}  // namespace storage
}  // namespace nebula

void go(int32_t iters,
        const std::vector<nebula::VertexID>& vertex,
        const std::vector<std::string>& playerProps,
        const std::vector<std::string>& serveProps) {
    nebula::storage::cpp2::GetNeighborsRequest req;
    BENCHMARK_SUSPEND {
        req = nebula::storage::buildRequest(vertex, playerProps, serveProps);
    }
    auto* env = gCluster->storageEnv_.get();
    for (decltype(iters) i = 0; i < iters; i++) {
        auto* processor = nebula::storage::GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
    }
}

void goFilter(int32_t iters,
              const std::vector<nebula::VertexID>& vertex,
              const std::vector<std::string>& playerProps,
              const std::vector<std::string>& serveProps,
              int64_t value = FLAGS_max_rank * FLAGS_filter_ratio) {
    nebula::storage::cpp2::GetNeighborsRequest req;
    BENCHMARK_SUSPEND {
        nebula::EdgeType serve = 101;
        req = nebula::storage::buildRequest(vertex, playerProps, serveProps);
        {
            // where serve.startYear < value
            nebula::RelationalExpression exp(
                nebula::Expression::Kind::kRelLT,
                new nebula::EdgePropertyExpression(
                    new std::string(folly::to<std::string>(serve)),
                    new std::string("startYear")),
                new nebula::ConstantExpression(nebula::Value(value)));
            req.traverse_spec.set_filter(nebula::Expression::encode(exp));
        }
    }
    auto* env = gCluster->storageEnv_.get();
    for (decltype(iters) i = 0; i < iters; i++) {
        auto* processor = nebula::storage::GetNeighborsProcessor::instance(env, nullptr, nullptr);
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
    }
}

// Players may serve more than one team, the total edges = teamCount * maxRank, which would effect
// the final result, so select some player only serve one team
BENCHMARK(OneVertexOneProperty, iters) {
    go(iters, {"Tim Duncan"}, {"name"}, {"teamName"});
}
BENCHMARK_RELATIVE(OneVertexOnePropertyWithFilter, iters) {
    goFilter(iters, {"Tim Duncan"}, {"name"}, {"teamName"});
}

BENCHMARK(OneVertexThreeProperty, iters) {
    go(iters, {"Tim Duncan"}, {"name", "age", "avgScore"}, {nebula::kDst, "startYear", "endYear"});
}
BENCHMARK_RELATIVE(OneVertexThreePropertyWithFilter, iters) {
    goFilter(iters,
             {"Tim Duncan"},
             {"name", "age", "avgScore"},
             {nebula::kDst, "startYear", "endYear"});
}

BENCHMARK(TenVertexOneProperty, iters) {
    go(iters,
       {"Tim Duncan", "Kobe Bryant", "Stephen Curry", "Manu Ginobili", "Joel Embiid",
        "Giannis Antetokounmpo", "Yao Ming", "Damian Lillard", "Dirk Nowitzki", "Klay Thompson"},
       {"name"},
       {"teamName"});
}
BENCHMARK_RELATIVE(TenVertexOnePropertyWithFilter, iters) {
    goFilter(
        iters,
        {"Tim Duncan", "Kobe Bryant", "Stephen Curry", "Manu Ginobili", "Joel Embiid",
        "Giannis Antetokounmpo", "Yao Ming", "Damian Lillard", "Dirk Nowitzki", "Klay Thompson"},
        {"name"},
        {"teamName"});
}

BENCHMARK(TenVertexThreeProperty, iters) {
    go(iters,
       {"Tim Duncan", "Kobe Bryant", "Stephen Curry", "Manu Ginobili", "Joel Embiid",
        "Giannis Antetokounmpo", "Yao Ming", "Damian Lillard", "Dirk Nowitzki", "Klay Thompson"},
       {"name", "age", "avgScore"},
       {nebula::kDst, "startYear", "endYear"});
}
BENCHMARK_RELATIVE(TenVertexThreePropertyWithFilter, iters) {
    goFilter(
        iters,
        {"Tim Duncan", "Kobe Bryant", "Stephen Curry", "Manu Ginobili", "Joel Embiid",
        "Giannis Antetokounmpo", "Yao Ming", "Damian Lillard", "Dirk Nowitzki", "Klay Thompson"},
        {"name", "age", "avgScore"},
        {nebula::kDst, "startYear", "endYear"});
}

int main(int argc, char** argv) {
    folly::init(&argc, &argv, true);
    nebula::fs::TempDir rootPath("/tmp/GetNeighborsBenchmark.XXXXXX");
    nebula::storage::setUp(rootPath.path(), FLAGS_max_rank);
    folly::runBenchmarks();
    gCluster.reset();
    return 0;
}


/*
Debug: No concurrency

--max_rank=100 --filter_ratio=0.1
============================================================================
GetNeighborsBenchmark.cpprelative                         time/iter  iters/s
============================================================================
OneVertexOneProperty                                       660.59us    1.51K
OneVertexOnePropertyWithFilter                   128.11%   515.66us    1.94K
OneVertexThreeProperty                                     879.71us    1.14K
OneVertexThreePropertyWithFilter                 162.34%   541.89us    1.85K
TenVertexOneProperty                                         5.79ms   172.80
TenVertexOnePropertyWithFilter                   138.86%     4.17ms   239.96
TenVertexThreeProperty                                       7.88ms   126.87
TenVertexThreePropertyWithFilter                 178.17%     4.42ms   226.04
============================================================================

--max_rank=1000 --filter_ratio=0.1
============================================================================
GetNeighborsBenchmark.cpprelative                         time/iter  iters/s
============================================================================
OneVertexOneProperty                                         5.39ms   185.55
OneVertexOnePropertyWithFilter                   142.80%     3.77ms   264.98
OneVertexThreeProperty                                       7.39ms   135.31
OneVertexThreePropertyWithFilter                 184.83%     4.00ms   250.10
TenVertexOneProperty                                        52.48ms    19.06
TenVertexOnePropertyWithFilter                   142.63%    36.79ms    27.18
TenVertexThreeProperty                                      72.96ms    13.71
TenVertexThreePropertyWithFilter                 186.05%    39.22ms    25.50
============================================================================

--max_rank=10000 --filter_ratio=0.1
============================================================================
GetNeighborsBenchmark.cpprelative                         time/iter  iters/s
============================================================================
OneVertexOneProperty                                        58.37ms    17.13
OneVertexOnePropertyWithFilter                   142.90%    40.85ms    24.48
OneVertexThreeProperty                                      81.73ms    12.24
OneVertexThreePropertyWithFilter                 187.62%    43.56ms    22.96
TenVertexOneProperty                                       587.09ms     1.70
TenVertexOnePropertyWithFilter                   142.45%   412.15ms     2.43
TenVertexThreeProperty                                     833.53ms     1.20
TenVertexThreePropertyWithFilter                 190.63%   437.25ms     2.29
============================================================================

--max_rank=100 --filter_ratio=0.5
============================================================================
GetNeighborsBenchmark.cpprelative                         time/iter  iters/s
============================================================================
OneVertexOneProperty                                       661.60us    1.51K
OneVertexOnePropertyWithFilter                    96.12%   688.34us    1.45K
OneVertexThreeProperty                                     879.63us    1.14K
OneVertexThreePropertyWithFilter                 110.58%   795.49us    1.26K
TenVertexOneProperty                                         5.82ms   171.69
TenVertexOnePropertyWithFilter                    98.96%     5.89ms   169.91
TenVertexThreeProperty                                       7.92ms   126.26
TenVertexThreePropertyWithFilter                 114.11%     6.94ms   144.08
============================================================================

--max_rank=1000 --filter_ratio=0.5
============================================================================
GetNeighborsBenchmark.cpprelative                         time/iter  iters/s
============================================================================
OneVertexOneProperty                                         5.38ms   185.88
OneVertexOnePropertyWithFilter                    99.68%     5.40ms   185.30
OneVertexThreeProperty                                       7.47ms   133.79
OneVertexThreePropertyWithFilter                 114.63%     6.52ms   153.36
TenVertexOneProperty                                        52.43ms    19.07
TenVertexOnePropertyWithFilter                    98.43%    53.27ms    18.77
TenVertexThreeProperty                                      74.08ms    13.50
TenVertexThreePropertyWithFilter                 114.83%    64.51ms    15.50
============================================================================

--max_rank=10000 --filter_ratio=0.5
============================================================================
GetNeighborsBenchmark.cpprelative                         time/iter  iters/s
============================================================================
OneVertexOneProperty                                        58.58ms    17.07
OneVertexOnePropertyWithFilter                   103.72%    56.48ms    17.71
OneVertexThreeProperty                                      80.95ms    12.35
OneVertexThreePropertyWithFilter                 119.57%    67.70ms    14.77
TenVertexOneProperty                                       589.36ms     1.70
TenVertexOnePropertyWithFilter                   102.98%   572.28ms     1.75
TenVertexThreeProperty                                     810.38ms     1.23
TenVertexThreePropertyWithFilter                 119.14%   680.17ms     1.47
============================================================================

--max_rank=100 --filter_ratio=1
============================================================================
GetNeighborsBenchmark.cpprelative                         time/iter  iters/s
============================================================================
OneVertexOneProperty                                       657.72us    1.52K
OneVertexOnePropertyWithFilter                    75.40%   872.30us    1.15K
OneVertexThreeProperty                                     869.05us    1.15K
OneVertexThreePropertyWithFilter                  79.84%     1.09ms   918.68
TenVertexOneProperty                                         5.78ms   172.95
TenVertexOnePropertyWithFilter                    74.37%     7.77ms   128.63
TenVertexThreeProperty                                       7.85ms   127.45
TenVertexThreePropertyWithFilter                  79.25%     9.90ms   101.00
============================================================================

--max_rank=1000 --filter_ratio=1
============================================================================
GetNeighborsBenchmark.cpprelative                         time/iter  iters/s
============================================================================
OneVertexOneProperty                                         4.75ms   210.74
OneVertexOnePropertyWithFilter                    73.43%     6.46ms   154.74
OneVertexThreeProperty                                       6.64ms   150.62
OneVertexThreePropertyWithFilter                  78.81%     8.42ms   118.70
TenVertexOneProperty                                        47.59ms    21.01
TenVertexOnePropertyWithFilter                    72.98%    65.21ms    15.33
TenVertexThreeProperty                                      66.23ms    15.10
TenVertexThreePropertyWithFilter                  78.48%    84.39ms    11.85
============================================================================

--max_rank=10000 --filter_ratio=1
============================================================================
GetNeighborsBenchmark.cpprelative                         time/iter  iters/s
============================================================================
OneVertexOneProperty                                        51.88ms    19.27
OneVertexOnePropertyWithFilter                    76.75%    67.60ms    14.79
OneVertexThreeProperty                                      71.34ms    14.02
OneVertexThreePropertyWithFilter                  81.37%    87.67ms    11.41
TenVertexOneProperty                                       519.31ms     1.93
TenVertexOnePropertyWithFilter                    75.84%   684.75ms     1.46
TenVertexThreeProperty                                     714.19ms     1.40
TenVertexThreePropertyWithFilter                  80.26%   889.86ms     1.12
============================================================================
*/
