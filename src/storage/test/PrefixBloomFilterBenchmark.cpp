/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <rocksdb/db.h>
#include <folly/Benchmark.h>
#include "common/fs/TempDir.h"
#include "kvstore/RocksEngineConfig.h"
#include "mock/MockCluster.h"
#include "utils/NebulaKeyUtils.h"
#include <gtest/gtest.h>

DEFINE_int64(vertex_per_part, 100, "vertex count with each partition");

namespace nebula {
namespace storage {

void mockData(StorageEnv* env, int32_t partCount) {
    LOG(INFO) << "Prepare data...";
    size_t vIdLen = 16;
    GraphSpaceID spaceId = 1;
    for (PartitionID partId = 1; partId <= partCount; partId++) {
        std::vector<kvstore::KV> data;
        for (int32_t vertexId = partId * FLAGS_vertex_per_part;
             vertexId < (partId + 1) * FLAGS_vertex_per_part;
             vertexId++) {
            for (TagID tagId = 3001; tagId < 3010; tagId++) {
                auto key = NebulaKeyUtils::vertexKey(
                    vIdLen, partId, std::to_string(vertexId), tagId);
                auto val = folly::stringPrintf("%d_%d", vertexId, tagId);
                data.emplace_back(std::move(key), std::move(val));
            }
        }
        folly::Baton<true, std::atomic> baton;
        env->kvstore_->asyncMultiPut(spaceId, partId, std::move(data),
                                     [&](nebula::cpp2::ErrorCode code) {
            ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, code);
            baton.post();
            folly::doNotOptimizeAway(code);
        });
        baton.wait();
        ASSERT_EQ(nebula::cpp2::ErrorCode::SUCCEEDED, env->kvstore_->flush(spaceId));
    }
}

void testPrefixSeek(StorageEnv* env, int32_t partCount, int32_t iters) {
    size_t vIdLen = 16;
    GraphSpaceID spaceId = 1;
    for (decltype(iters) i = 0; i < iters; i++) {
        for (PartitionID partId = 1; partId <= partCount; partId++) {
            for (int32_t vertexId = partId * FLAGS_vertex_per_part;
                 vertexId < (partId + 1) * FLAGS_vertex_per_part;
                 vertexId++) {
                auto prefix =
                    NebulaKeyUtils::vertexPrefix(vIdLen, partId, std::to_string(vertexId));
                std::unique_ptr<kvstore::KVIterator> iter;
                auto code = env->kvstore_->prefix(spaceId, partId, prefix, &iter);
                ASSERT_EQ(code, nebula::cpp2::ErrorCode::SUCCEEDED);
                CHECK(iter->valid());
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
    fs::TempDir rootPath("/tmp/GetPropTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto partCount = cluster.getTotalParts();
    auto* env = cluster.storageEnv_.get();
    mockData(env, partCount);
    braces.dismiss();
    testPrefixSeek(env, partCount, n);
}

BENCHMARK_RELATIVE(PrefixWithFilterOn, n) {
    folly::BenchmarkSuspender braces;
    FLAGS_rocksdb_column_family_options = R"({
        "level0_file_num_compaction_trigger":"100"
    })";
    FLAGS_enable_rocksdb_prefix_filtering = true;
    FLAGS_rocksdb_block_cache = 0;
    fs::TempDir rootPath("/tmp/GetPropTest.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(rootPath.path());
    auto partCount = cluster.getTotalParts();
    auto* env = cluster.storageEnv_.get();
    mockData(env, partCount);
    braces.dismiss();
    testPrefixSeek(env, partCount, n);
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
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
