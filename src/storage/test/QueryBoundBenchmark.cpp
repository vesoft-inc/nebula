/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#include "base/Base.h"
#include <folly/Benchmark.h>
#include <rocksdb/db.h>
#include <limits>
#include "fs/TempDir.h"
#include "storage/test/TestUtils.h"
#include "storage/query/QueryBoundProcessor.h"
#include "base/NebulaKeyUtils.h"
#include "dataman/RowSetReader.h"
#include "dataman/RowReader.h"
#include "meta/SchemaManager.h"
#include "storage/test/AdHocSchemaManager.h"
#include <folly/executors/CPUThreadPoolExecutor.h>

DEFINE_int32(req_parts, 3, "parts requested");
DEFINE_int32(vrpp, 100, "vertices requested per part");
DEFINE_int32(handler_num, 10, "The Executor's handler number");
DECLARE_int32(max_handlers_per_req);

std::unique_ptr<nebula::kvstore::KVStore> gKV;
std::unique_ptr<nebula::storage::AdHocSchemaManager> schema;

namespace nebula {
namespace storage {

void mockData(kvstore::KVStore* kv) {
    for (auto partId = 0; partId < 6; partId++) {
        std::vector<kvstore::KV> data;
        for (auto vertexId = 1; vertexId < 1000; vertexId++) {
            for (auto tagId = 3001; tagId < 3010; tagId++) {
                auto key = NebulaKeyUtils::vertexKey(partId, vertexId, tagId, 0);
                RowWriter writer;
                for (uint64_t numInt = 0; numInt < 3; numInt++) {
                    writer << numInt;
                }
                for (auto numString = 3; numString < 6; numString++) {
                    writer << folly::stringPrintf("tag_string_col_%d", numString);
                }
                auto val = writer.encode();
                data.emplace_back(std::move(key), std::move(val));
            }
            // Generate 7 out-edges for each edgeType.
            for (auto dstId = 10001; dstId <= 10007; dstId++) {
                VLOG(3) << "Write part " << partId << ", vertex " << vertexId << ", dst " << dstId;
                // Write multi versions,  we should get the latest version.
                for (auto version = 0; version < 3; version++) {
                    auto key = NebulaKeyUtils::edgeKey(partId, vertexId, 101,
                                                 dstId - 10001, dstId,
                                                 std::numeric_limits<int>::max() - version);
                    RowWriter writer(nullptr);
                    for (uint64_t numInt = 0; numInt < 10; numInt++) {
                        writer << numInt;
                    }
                    for (auto numString = 10; numString < 20; numString++) {
                        writer << folly::stringPrintf("string_col_%d_%d", numString, version);
                    }
                    auto val = writer.encode();
                    data.emplace_back(std::move(key), std::move(val));
                }
            }
            // Generate 5 in-edges for each edgeType, the edgeType is negative
            for (auto srcId = 20001; srcId <= 20005; srcId++) {
                VLOG(3) << "Write part " << partId << ", vertex " << vertexId << ", src " << srcId;
                for (auto version = 0; version < 3; version++) {
                    auto key = NebulaKeyUtils::edgeKey(partId, vertexId, -101,
                                                 srcId - 20001, srcId,
                                                 std::numeric_limits<int>::max() - version);
                    data.emplace_back(std::move(key), "");
                }
            }
        }
        kv->asyncMultiPut(
            0, partId, std::move(data),
            [&](kvstore::ResultCode code) {
                CHECK_EQ(code, kvstore::ResultCode::SUCCEEDED);
            });
    }
}

void setUp(const char* path) {
    gKV = TestUtils::initKV(path);
    schema.reset(new storage::AdHocSchemaManager());
    schema->addEdgeSchema(
        0 /*space id*/, 101 /*edge type*/, TestUtils::genEdgeSchemaProvider(10, 10));
    for (auto tagId = 3001; tagId < 3010; tagId++) {
        schema->addTagSchema(
            0 /*space id*/, tagId, TestUtils::genTagSchemaProvider(tagId, 3, 3));
    }
    mockData(gKV.get());
}

cpp2::GetNeighborsRequest buildRequest(bool outBound = true) {
    cpp2::GetNeighborsRequest req;
    req.set_space_id(0);
    decltype(req.parts) tmpIds;
    for (auto partId = 0; partId < FLAGS_req_parts; partId++) {
        for (auto vertexId = 1; vertexId <= FLAGS_vrpp; vertexId++) {
            tmpIds[partId].push_back(vertexId);
        }
    }
    req.set_parts(std::move(tmpIds));
    auto edge_type = outBound ? 101 : -101;
    std::vector<EdgeType> et = {edge_type};
    req.set_edge_types(et);
    // Return tag props col_0, col_2, col_4
    decltype(req.return_columns) tmpColumns;
    for (int i = 0; i < 3; i++) {
        tmpColumns.emplace_back(TestUtils::vertexPropDef(
            folly::stringPrintf("tag_%d_col_%d", 3001 + i * 2, i * 2), 3001 + i * 2));
    }
    tmpColumns.emplace_back(
        TestUtils::edgePropDef(folly::stringPrintf("_dst"), PropContext::PropInKeyType::DST));
    tmpColumns.emplace_back(
        TestUtils::edgePropDef(folly::stringPrintf("_rank"), PropContext::PropInKeyType::RANK));
    // Return edge props col_0, col_2, col_4 ... col_18
    for (int i = 0; i < 10; i++) {
        tmpColumns.emplace_back(
            TestUtils::edgePropDef(folly::stringPrintf("col_%d", i * 2), edge_type));
    }
    req.set_return_columns(std::move(tmpColumns));
    return req;
}

}  // namespace storage
}  // namespace nebula

void run(int32_t iters, int32_t handlerNum) {
    FLAGS_max_handlers_per_req = handlerNum;
    nebula::storage::cpp2::GetNeighborsRequest req;
    BENCHMARK_SUSPEND {
        req = nebula::storage::buildRequest();
    }
    auto executor = std::make_unique<folly::CPUThreadPoolExecutor>(FLAGS_handler_num);
    for (decltype(iters) i = 0; i < iters; i++) {
        auto* processor
                = nebula::storage::QueryBoundProcessor::instance(
                                                            gKV.get(),
                                                            schema.get(),
                                                            nullptr,
                                                            executor.get());
        auto f = processor->getFuture();
        processor->process(req);
        auto resp = std::move(f).get();
    }
}

BENCHMARK(query_bound_1, iters) {
    run(iters, 1);
}

BENCHMARK(query_bound_3, iters) {
    run(iters, 3);
}

BENCHMARK(query_bound_10, iters) {
    run(iters, 10);
}
/*************************
 * End of benchmarks
 ************************/


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
req_parts=1, vrpp=100, handler_num=10
Notes: all requests hit the cache.
TODO(heng): We need perf number under heavy load

============================================================================
QueryBoundBenchmark.cpprelative                           time/iter  iters/s
============================================================================
query_bound_1                                              122.30ms     8.18
query_bound_3                                               41.18ms    24.29
query_bound_10                                              13.69ms    73.04
============================================================================

*/
