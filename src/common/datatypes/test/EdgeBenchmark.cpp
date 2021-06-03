/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <string>
#include <unordered_set>
#include <vector>

#include <folly/Benchmark.h>

#include "common/base/Base.h"
#include "common/datatypes/Edge.h"
#include "common/datatypes/Value.h"

using nebula::Edge;
using nebula::Value;
using nebula::EdgeType;
using nebula::EdgeRanking;

static const int seed = folly::randomNumberSeed();
using RandomT = std::mt19937;
static RandomT rng(seed);

template <class Integral1, class Integral2>
Integral2 random(Integral1 low, Integral2 up) {
    std::uniform_int_distribution<> range(low, up);
    return range(rng);
}

std::string randomString(size_t size = 1000) {
    std::string str(size, ' ');
    for (size_t p = 0; p < size; p++) {
        str[p] = random('a', 'z');
    }
    return str;
}

BENCHMARK(HashStringEdgeKey, n) {
    std::vector<Edge> edges;
    BENCHMARK_SUSPEND {
        edges.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            edges.emplace_back(
                Edge(randomString(10), randomString(10), random(-200, 200), "like", 0, {}));
        }
    }
    std::unordered_set<std::string> uniqueEdge;
    for (const auto& edge : edges) {
        const auto& src = edge.src.toString();
        const auto& dst = edge.dst.toString();
        auto edgeKey = folly::stringPrintf("%s%ld%s%ld%d%ld",
                                           src.c_str(),
                                           src.size(),
                                           dst.c_str(),
                                           dst.size(),
                                           edge.type,
                                           edge.ranking);
        folly::hash::fnv64(edgeKey);
    }
}

BENCHMARK_RELATIVE(HashStringEdgeKeyOut, n) {
    std::vector<std::string> edges;
    BENCHMARK_SUSPEND {
        edges.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            auto edgeKey = folly::stringPrintf("%s%ld%s%ld%d%ld",
                                               randomString(10).c_str(),
                                               static_cast<int64_t>(10),
                                               randomString(10).c_str(),
                                               static_cast<int64_t>(10),
                                               random(-200, 200),
                                               static_cast<int64_t>(0));
            edges.emplace_back(edgeKey);
        }
    }
    std::unordered_set<std::string> uniqueEdge;
    for (const auto& edge : edges) {
        folly::hash::fnv64(edge);
    }
}

BENCHMARK_RELATIVE(HashEdge, n) {
    std::vector<Edge> edges;
    BENCHMARK_SUSPEND {
        edges.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            edges.emplace_back(
                Edge(randomString(10), randomString(10), random(-200, 200), "like", 0, {}));
        }
    }
    std::unordered_set<Edge> uniqueEdge;
    uniqueEdge.reserve(edges.size());
    for (auto& edge : edges) {
        uniqueEdge.emplace(edge);
    }
}

BENCHMARK_RELATIVE(HashEdgeCopy, n) {
    std::vector<Edge> edges;
    BENCHMARK_SUSPEND {
        edges.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            edges.emplace_back(
                Edge(randomString(10), randomString(10), random(-200, 200), "like", 0, {}));
        }
    }
    std::unordered_set<Edge> uniqueEdge;
    for (const auto& edge : edges) {
        uniqueEdge.emplace(edge);
    }
}

BENCHMARK_RELATIVE(HashMoveEdge, n) {
    std::vector<Edge> edges;
    BENCHMARK_SUSPEND {
        edges.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            edges.emplace_back(
                Edge(randomString(10), randomString(10), random(-200, 200), "like", 0, {}));
        }
    }
    std::unordered_set<Edge> uniqueEdge;
    uniqueEdge.reserve(edges.size());
    for (auto& edge : edges) {
        uniqueEdge.emplace(std::move(edge));
    }
}

BENCHMARK_RELATIVE(HashMoveStringEdgeKey, n) {
    std::vector<Edge> edges;
    BENCHMARK_SUSPEND {
        edges.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            edges.emplace_back(
                Edge(randomString(10), randomString(10), random(-200, 200), "like", 0, {}));
        }
    }
    std::unordered_set<std::string> uniqueEdge;
    uniqueEdge.reserve(edges.size());
    for (const auto& edge : edges) {
        const auto& src = edge.src.toString();
        const auto& dst = edge.dst.toString();
        auto edgeKey = folly::stringPrintf("%s%ld%s%ld%d%ld",
                                           src.c_str(),
                                           src.size(),
                                           dst.c_str(),
                                           dst.size(),
                                           edge.type,
                                           edge.ranking);
        uniqueEdge.emplace(std::move(edgeKey));
    }
}

BENCHMARK_RELATIVE(HashTupleEdge, n) {
    std::vector<Edge> edges;
    BENCHMARK_SUSPEND {
        edges.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            edges.emplace_back(
                Edge(randomString(10), randomString(10), random(-200, 200), "like", 0, {}));
        }
    }
    std::unordered_set<std::tuple<Value, EdgeType, EdgeRanking, Value>> uniqueEdge;
    uniqueEdge.reserve(edges.size());
    for (const auto& edge : edges) {
        auto edgeKey = std::make_tuple(edge.src, edge.type, edge.ranking, edge.dst);
        uniqueEdge.emplace(std::move(edgeKey));
    }
}

int main() {
    folly::runBenchmarks();
    return 0;
}

// ============================================================================
// /nebula-common/src/common/datatypes/test/EdgeBenchmark.cpp time/iter  iters/s
// ============================================================================
// HashStringEdgeKey                                          712.78ns    1.40M
// HashStringEdgeKeyOut                             315.52%   225.90ns    4.43M
// HashEdge                                          65.86%     1.08us  923.97K
// HashEdgeCopy                                      45.50%     1.57us  638.36K
// HashMoveEdge                                      65.77%     1.08us  922.67K
// HashMoveStringEdgeKey                            107.19%   664.96ns    1.50M
// HashTupleEdge                                    113.85%   626.04ns    1.60M
// ============================================================================
