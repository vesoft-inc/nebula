/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <chrono>
#include <folly/Benchmark.h>
#include <folly/Format.h>
#include <folly/container/Enumerate.h>
#include <gtest/gtest.h>

#include "TossEnvironment.h"

#define oneReqOneEdge         1
#define oneReqTenEdges        1
#define oneReqHundredEdges    1
#define oneReqThousandEdges   1


namespace nebula {
namespace storage {

using StorageClient = storage::GraphStorageClient;

bool useToss = true;
bool notToss = false;

int32_t gRank = 0;
std::map<int32_t, int32_t> gAddedEdges;

std::vector<meta::cpp2::PropertyType> gTypes;

std::vector<meta::cpp2::ColumnDef>
genColDefs(const std::vector<meta::cpp2::PropertyType>& types) {
    auto N = types.size();
    auto colNames = TossEnvironment::makeColNames(N);
    std::vector<meta::cpp2::ColumnDef> ret(N);
    for (auto i = 0U; i != N; ++i) {
        ret[i].set_name(colNames[i]);
        meta::cpp2::ColumnTypeDef tpDef;
        tpDef.set_type(types[i]);
        ret[i].set_type(tpDef);
        ret[i].set_nullable(true);
    }
    return ret;
}

void setupEnvironment() {
    auto env = TossEnvironment::getInstance(kMetaName, kMetaPort);
    std::vector<meta::cpp2::PropertyType> types;
    gTypes.emplace_back(meta::cpp2::PropertyType::INT64);
    gTypes.emplace_back(meta::cpp2::PropertyType::STRING);

    auto colDefs = genColDefs(gTypes);
    env->init(kSpaceName, kPart, kReplica, colDefs);
}

#if oneReqOneEdge
BENCHMARK(bmOneReqOneEdgeTossNo) {
    int32_t srcId = 10000;
    ++gAddedEdges[srcId];
    auto dstId = gAddedEdges[srcId];
    std::vector<nebula::Value> vals(gTypes.size());
    vals[0].setInt(srcId);
    vals[1].setStr(folly::sformat("bmoneReqOneEdgeTossNo, src={}, dst={}", srcId, dstId));

    auto env = TossEnvironment::getInstance(kMetaName, kMetaPort);
    auto edge = env->generateEdge(srcId, gRank, vals, dstId);

    env->syncAddEdge(edge, notToss);
}

BENCHMARK_RELATIVE(bmOneReqOneEdgeTossYes) {
    int32_t srcId = 20000;
    ++gAddedEdges[srcId];
    auto dstId = gAddedEdges[srcId];

    std::vector<nebula::Value> vals(gTypes.size());
    vals[0].setInt(srcId);
    vals[1].setStr(folly::sformat("bmoneReqOneEdgeTossYes, src={}, dst={}", srcId, dstId));

    auto env = TossEnvironment::getInstance(kMetaName, kMetaPort);
    auto edge = env->generateEdge(srcId, gRank, vals, dstId);

    env->syncAddEdge(edge, useToss);
}


BENCHMARK_DRAW_LINE();
#endif

#if oneReqTenEdges
BENCHMARK(bmOneReqTenEdges) {
    size_t cnt = 10;
    int32_t srcId = 30000;
    std::vector<nebula::Value> vals(gTypes.size());
    vals[0].setInt(srcId);
    vals[1].setStr(folly::sformat("{}", __func__));

    auto env = TossEnvironment::getInstance(kMetaName, kMetaPort);
    std::vector<cpp2::NewEdge> edges;
    edges.reserve(cnt);
    for (auto i = 0U; i < cnt; ++i) {
        ++gAddedEdges[srcId];
        auto dstId = gAddedEdges[srcId];
        edges.emplace_back(env->generateEdge(srcId, gRank, vals, dstId));
    }
    env->addEdgesAsync(edges, notToss).wait();
}

BENCHMARK_RELATIVE(bmOneReqTenEdgesToss) {
    size_t cnt = 10;
    int32_t srcId = 40000;
    std::vector<nebula::Value> vals(gTypes.size());
    vals[0].setInt(srcId);
    vals[1].setStr(folly::sformat("{}", __func__));

    auto env = TossEnvironment::getInstance(kMetaName, kMetaPort);
    std::vector<cpp2::NewEdge> edges;
    edges.reserve(cnt);
    for (auto i = 0U; i < cnt; ++i) {
        ++gAddedEdges[srcId];
        auto dstId = gAddedEdges[srcId];
        edges.emplace_back(env->generateEdge(srcId, gRank, vals, dstId));
    }
    env->addEdgesAsync(edges, useToss).wait();
}
#endif

#if oneReqHundredEdges
BENCHMARK_DRAW_LINE();
BENCHMARK(bmOneReqHundredEdges) {
    size_t cnt = 100;
    int32_t srcId = 50000;
    std::vector<nebula::Value> vals(gTypes.size());
    vals[0].setInt(srcId);
    vals[1].setStr(folly::sformat("{}", __func__));

    auto env = TossEnvironment::getInstance(kMetaName, kMetaPort);
    std::vector<cpp2::NewEdge> edges;
    edges.reserve(cnt);
    for (auto i = 0U; i < cnt; ++i) {
        ++gAddedEdges[srcId];
        auto dstId = gAddedEdges[srcId];
        edges.emplace_back(env->generateEdge(srcId, gRank, vals, dstId));
    }
    env->addEdgesAsync(edges, notToss).wait();
}

BENCHMARK_RELATIVE(bmOneReqHundredEdgesToss) {
    size_t cnt = 100;
    int32_t srcId = 60000;
    std::vector<nebula::Value> vals(gTypes.size());
    vals[0].setInt(srcId);
    vals[1].setStr(folly::sformat("{}", __func__));

    auto env = TossEnvironment::getInstance(kMetaName, kMetaPort);
    std::vector<cpp2::NewEdge> edges;
    edges.reserve(cnt);
    for (auto i = 0U; i < cnt; ++i) {
        ++gAddedEdges[srcId];
        auto dstId = gAddedEdges[srcId];
        edges.emplace_back(env->generateEdge(srcId, gRank, vals, dstId));
    }
    env->addEdgesAsync(edges, useToss).wait();
}
#endif

#if oneReqThousandEdges
BENCHMARK_DRAW_LINE();
BENCHMARK(bmOneReqThousandEdges) {
    size_t cnt = 1000;
    int32_t srcId = 70000;
    std::vector<nebula::Value> vals(gTypes.size());
    vals[0].setInt(srcId);
    vals[1].setStr(folly::sformat("{}", __func__));

    auto env = TossEnvironment::getInstance(kMetaName, kMetaPort);
    std::vector<cpp2::NewEdge> edges;
    edges.reserve(cnt);
    for (auto i = 0U; i < cnt; ++i) {
        ++gAddedEdges[srcId];
        auto dstId = gAddedEdges[srcId];
        edges.emplace_back(env->generateEdge(srcId, gRank, vals, dstId));
    }
    env->addEdgesAsync(edges, notToss).wait();
}

BENCHMARK_RELATIVE(bmOneReqThousandEdgesToss) {
    size_t cnt = 1000;
    int32_t srcId = 80000;
    std::vector<nebula::Value> vals(gTypes.size());
    vals[0].setInt(srcId);
    vals[1].setStr(folly::sformat("{}", __func__));

    auto env = TossEnvironment::getInstance(kMetaName, kMetaPort);
    std::vector<cpp2::NewEdge> edges;
    edges.reserve(cnt);
    for (auto i = 0U; i < cnt; ++i) {
        ++gAddedEdges[srcId];
        auto dstId = gAddedEdges[srcId];
        edges.emplace_back(env->generateEdge(srcId, gRank, vals, dstId));
    }
    env->addEdgesAsync(edges, useToss).wait();
}
#endif

}  // namespace storage
}  // namespace nebula

using namespace nebula::storage;  // NOLINT

int main(int argc, char** argv) {
    FLAGS_heartbeat_interval_secs = 1;
    FLAGS_logtostderr = 1;

    folly::init(&argc, &argv, false);
    setupEnvironment();
    folly::runBenchmarks();
    LOG(INFO) << "exit main";

    auto env = TossEnvironment::getInstance(kMetaName, kMetaPort);
    for (auto& item : gAddedEdges) {
        std::vector<nebula::storage::cpp2::NewEdge> edges;
        std::vector<nebula::Value> vals(gTypes.size());
        edges.emplace_back(env->generateEdge(item.first, gRank, vals));
        auto strNei = env->getNeiProps(edges);
        auto cnt = env->countSquareBrackets(strNei);
        LOG(INFO) << folly::sformat("testId={}, testCalled={}, cnt={}",
                                    item.first, item.second, cnt);
    }
}

// ============================================================================
// TossBenchmark.cpprelative                       (fcb6a91) time/iter  iters/s
// ============================================================================
// bmOneReqOneEdgeTossNo                                        1.82ms   549.76
// bmOneReqOneEdgeTossYes                            42.83%     4.25ms   235.46
// ----------------------------------------------------------------------------
// bmOneReqTenEdges                                             1.96ms   511.07
// bmOneReqTenEdgesToss                              20.05%     9.76ms   102.49
// ----------------------------------------------------------------------------
// bmOneReqHundredEdges                                         6.93ms   144.31
// bmOneReqHundredEdgesToss                          49.95%    13.87ms    72.08
// ----------------------------------------------------------------------------
// bmOneReqThousandEdges                                       33.76ms    29.62
// bmOneReqThousandEdgesToss                         69.75%    48.40ms    20.66
// ============================================================================
