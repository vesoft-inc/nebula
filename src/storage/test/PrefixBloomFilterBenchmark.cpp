/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <rocksdb/db.h>
#include <folly/Benchmark.h>
#include "fs/TempDir.h"
#include "kvstore/RocksEngineConfig.h"
#include "storage/test/TestUtils.h"
#include "utils/NebulaKeyUtils.h"

DEFINE_int64(part_number, 20, "partition numbers");
DEFINE_int64(vertex_per_part, 100, "vertex count with each partition");

namespace nebula {
namespace storage {

void mockData(kvstore::KVStore *kv) {
    LOG(INFO) << "Prepare data...";
    for (PartitionID partId = 0; partId < FLAGS_part_number; partId++) {
        std::vector<kvstore::KV> data;
        for (int32_t vertexId = partId * FLAGS_vertex_per_part;
                vertexId < (partId + 1) * FLAGS_vertex_per_part; vertexId++) {
            for (TagID tagId = 3001; tagId < 3010; tagId++) {
                auto key = NebulaKeyUtils::vertexKey(partId, vertexId, tagId, 1);
                auto val = folly::stringPrintf("%d_%d", vertexId, tagId);
                data.emplace_back(std::move(key), std::move(val));
            }
        }
        folly::Baton<true, std::atomic> baton;
        kv->asyncMultiPut(0, partId, std::move(data),
                [&](kvstore::ResultCode code) {
                    baton.post();
                    folly::doNotOptimizeAway(code);
                });
        baton.wait();
        kv->flush(0);
    }
}

void testPrefixSeek(kvstore::KVStore *kv, int32_t iters) {
    for (decltype(iters) i = 0; i < iters; i++) {
        for (PartitionID partId = 0; partId < FLAGS_part_number; partId++) {
            for (int32_t vertexId = partId * FLAGS_vertex_per_part;
                    vertexId < (partId + 1) * FLAGS_vertex_per_part; vertexId++) {
                auto prefix = NebulaKeyUtils::vertexPrefix(partId, vertexId);
                std::unique_ptr<kvstore::KVIterator> iter;
                kv->prefix(0, partId, prefix, &iter);
                iter->next();
            }
        }
    }
}

BENCHMARK(PrefixWithFilterOff, n) {
    folly::BenchmarkSuspender braces;
    FLAGS_rocksdb_column_family_options = R"({
        "level0_file_num_compaction_trigger":"100"
    })";
    FLAGS_rocksdb_block_cache = 0;
    fs::TempDir rootPath("/tmp/PrefixBloomFilterBenchmark.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path(),
            FLAGS_part_number);
    mockData(kv.get());
    braces.dismiss();
    testPrefixSeek(kv.get(), n);
}

BENCHMARK_RELATIVE(PrefixWithFilterOn, n) {
    folly::BenchmarkSuspender braces;
    FLAGS_rocksdb_column_family_options = R"({
        "level0_file_num_compaction_trigger":"100"
    })";
    FLAGS_enable_prefix_filtering = true;
    FLAGS_rocksdb_block_cache = 0;
    fs::TempDir rootPath("/tmp/PrefixBloomFilterBenchmark.XXXXXX");
    std::unique_ptr<kvstore::KVStore> kv = TestUtils::initKV(rootPath.path(),
            FLAGS_part_number);
    mockData(kv.get());
    braces.dismiss();
    testPrefixSeek(kv.get(), n);
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char **argv) {
    folly::init(&argc, &argv, true);
    folly::runBenchmarks();
    return 0;
}

/*
40 processors, Intel(R) Xeon(R) CPU E5-2650 v4 @ 2.20GHz

--part_number=20 --vertex_per_part=100
============================================================================
PrefixBloomFilterBenchmark.cpprelative                    time/iter  iters/s
PrefixWithFilterOff                                           4.07s  245.88m
PrefixWithFilterOn                               121.40%      3.35s  298.51m
============================================================================

--part_number=40 --vertex_per_part=100
============================================================================
PrefixBloomFilterBenchmark.cpprelative                    time/iter  iters/s
PrefixWithFilterOff                                          15.37s   65.07m
PrefixWithFilterOn                               137.75%     11.16s   89.63m
============================================================================

--part_number=40 --vertex_per_part=200
============================================================================
PrefixBloomFilterBenchmark.cpprelative                    time/iter  iters/s
PrefixWithFilterOff                                          35.29s   28.34m
PrefixWithFilterOn                               148.22%     23.81s   42.01m
============================================================================
*/
