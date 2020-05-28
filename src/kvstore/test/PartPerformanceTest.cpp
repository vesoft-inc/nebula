/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "kvstore/Part.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <rocksdb/cache.h>
#include <rocksdb/table.h>
#include <rocksdb/convenience.h>
#include <rocksdb/utilities/options_util.h>
#include <rocksdb/slice_transform.h>
#include <folly/Benchmark.h>

DEFINE_int64(part_performance_test_partnum, 10, "Total partitions");
DEFINE_int64(part_performance_test_rownum, 100000, "Total rows");

namespace nebula {
namespace kvstore {

void genData(rocksdb::DB* db) {
    rocksdb::WriteBatch updates;
    rocksdb::WriteOptions woptions;
    std::string str = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
                      "!@#$%^&*()_+-={}[]-={}[]:<>,./?~";
    for (auto i = 1; i <= FLAGS_part_performance_test_rownum; i++) {
        const int64_t rowId = i;
        auto partId = rowId % FLAGS_part_performance_test_partnum;
        std::string key, val;
        key.reserve(sizeof(partId) + sizeof(uint64_t) * 3);
        key.append(reinterpret_cast<const char*>(&partId), sizeof(partId))
           .append(reinterpret_cast<const char*>(&rowId), sizeof(uint64_t))
           .append(reinterpret_cast<const char*>(&rowId), sizeof(uint64_t))
           .append(reinterpret_cast<const char*>(&rowId), sizeof(uint64_t));

        val.reserve(4 + sizeof(uint64_t) * 3 + str.size());
        val.append("VAL_")
           .append(reinterpret_cast<const char*>(&rowId), sizeof(uint64_t))
           .append(reinterpret_cast<const char*>(&rowId), sizeof(uint64_t))
           .append(reinterpret_cast<const char*>(&rowId), sizeof(uint64_t))
           .append(str);
        updates.Put(std::move(key), std::move(val));
        if (i%1000 == 0) {
            rocksdb::Status status = db->Write(woptions, &updates);
            updates.Clear();
        }
    }
    rocksdb::Status status = db->Write(woptions, &updates);
    CHECK(status.ok());
    // flush memTable to sst file
    status = db->Flush(rocksdb::FlushOptions());
    CHECK(status.ok());

    // free system pages cache, prepare run `echo 3 > /proc/sys/vm/drop_caches` via root user.
    if (system("sync") < 0) {
        LOG(ERROR) << "Clean system cache error";
    }

    LOG(INFO) << "Data generation completed...";
}

void range(int32_t partId, rocksdb::DB* db) {
    auto start = static_cast<uint64_t>(FLAGS_part_performance_test_rownum * 0.5);
    auto end = static_cast<uint64_t>(FLAGS_part_performance_test_rownum * 0.8);
    std::string s, e;
    s.reserve(sizeof(partId) + sizeof(partId));
    s.append(reinterpret_cast<const char*>(&partId), sizeof(partId))
            .append(reinterpret_cast<const char*>(&start), sizeof(uint64_t))
            .append(reinterpret_cast<const char*>(&start), sizeof(uint64_t))
            .append(reinterpret_cast<const char*>(&start), sizeof(uint64_t));

    e.reserve(sizeof(partId) + sizeof(partId));
    e.append(reinterpret_cast<const char*>(&partId), sizeof(partId))
            .append(reinterpret_cast<const char*>(&end), sizeof(uint64_t))
            .append(reinterpret_cast<const char*>(&end), sizeof(uint64_t))
            .append(reinterpret_cast<const char*>(&end), sizeof(uint64_t));

    rocksdb::Slice sliceStart(s);
    rocksdb::Slice sliceEnd(e);
    rocksdb::ReadOptions roptions;
    rocksdb::Iterator* iter = db->NewIterator(roptions);
    if (iter) {
        iter->Seek(sliceStart);
    }

    while (iter != nullptr && iter->Valid() && (iter->key().compare(sliceEnd) < 0)) {
        iter->Next();
    }
    delete iter;
}

void multiThreadTest(bool useCache) {
    fs::TempDir dataPath("/tmp/part_multi_thread_test.XXXXXX");
    rocksdb::BlockBasedTableOptions table_options;
    rocksdb::Options options;
    rocksdb::DB* db = nullptr;
    std::vector<std::thread> threads;

    BENCHMARK_SUSPEND {
        if (useCache) {
            table_options.block_cache = rocksdb::NewLRUCache(100 * 1024 * 1024);
            options.table_factory.reset(NewBlockBasedTableFactory(table_options));
        } else {
            table_options.no_block_cache = true;
            options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
        }
        options.create_if_missing = true;

        rocksdb::Status status = rocksdb::DB::Open(options, dataPath.path(), &db);
        CHECK(status.ok());
        genData(db);
    }

    FOR_EACH_RANGE(i, 0, FLAGS_part_performance_test_partnum) {
        threads.emplace_back([db, i]() {
            range(i, db);
        });
    }

    FOR_EACH(t, threads) {
        t->join();
    }

    BENCHMARK_SUSPEND {
        db->Close();
        delete db;
    }
}

void singleThreadTest(bool useCache, int32_t partnum) {
    fs::TempDir dataPath("/tmp/part_single_thread_test.XXXXXX");
    rocksdb::BlockBasedTableOptions table_options;
    rocksdb::Options options;
    rocksdb::DB* db = nullptr;

    BENCHMARK_SUSPEND {
        if (useCache) {
            table_options.block_cache = rocksdb::NewLRUCache(100 * 1024 * 1024);
            options.table_factory.reset(NewBlockBasedTableFactory(table_options));
        } else {
            table_options.no_block_cache = true;
            options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
        }

        options.create_if_missing = true;
        rocksdb::Status status = rocksdb::DB::Open(options, dataPath.path(), &db);
        CHECK(status.ok());
        genData(db);
    }

    FOR_EACH_RANGE(i, 0, partnum) {
        range(i, db);
    }

    BENCHMARK_SUSPEND {
        db->Close();
        delete db;
    }
}

BENCHMARK(ParallelMultiplePartNoCache) {
    multiThreadTest(true);
}

BENCHMARK(SerialMultiplePartNoCache) {
    singleThreadTest(true, FLAGS_part_performance_test_partnum);
}

BENCHMARK(SerialSinglePartNocache) {
    singleThreadTest(true, 1);
}

BENCHMARK(ParallelMultiplePartCache) {
    multiThreadTest(false);
}

BENCHMARK(SerialMultiplePartCache) {
    singleThreadTest(false, FLAGS_part_performance_test_partnum);
}

BENCHMARK(SerialSinglePartCache) {
    singleThreadTest(false, 1);
}

}  // namespace kvstore
}  // namespace nebula


int main(int argc, char** argv) {
    folly::init(&argc, &argv, true);
    folly::runBenchmarks();
    return 0;
}

/**
 * The cache must be cleared as each test begin.
 * Using command `echo 3 > /proc/sys/vm/drop_caches` and sync by root user.
 *
 * ParallelMultiplePartNoCache : Multiple threads scan part in parallel . close block cache.
 * SerialMultiplePartNoCache : One thread scan part one by one. close block cache.
 * SerialSinglePartNocache : One thread scan part, only one part. close block cache.
 * ParallelMultiplePartCache : Multiple threads scan part in parallel . open block cache.
 * SerialMultiplePartCache : One thread scan part one by one. open block cache.
 * SerialSinglePartCache : One thread scan part, only one part. open block cache.
 */

/**
Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz

============================================================================
PartPerformanceTest.cpprelative                           time/iter  iters/s
============================================================================
1000 parts
============================================================================
ParallelMultiplePartNoCache                                967.66ms     1.03
SerialMultiplePartNoCache                                  834.85ms     1.20
SerialSinglePartNocache                                     53.25ms    18.78
ParallelMultiplePartCache                                  733.58ms     1.36
SerialMultiplePartCache                                    653.07ms     1.53
SerialSinglePartCache                                       54.96ms    18.19
============================================================================
100 parts
============================================================================
ParallelMultiplePartNoCache                                258.48ms     3.87
SerialMultiplePartNoCache                                  450.24ms     2.22
SerialSinglePartNocache                                     40.82ms    24.50
ParallelMultiplePartCache                                  154.87ms     6.46
SerialMultiplePartCache                                    318.12ms     3.14
SerialSinglePartCache                                       38.93ms    25.69
============================================================================
10 parts
============================================================================
ParallelMultiplePartNoCache                                235.84ms     4.24
SerialMultiplePartNoCache                                  406.69ms     2.46
SerialSinglePartNocache                                     92.42ms    10.82
ParallelMultiplePartCache                                  121.69ms     8.22
SerialMultiplePartCache                                    297.08ms     3.37
SerialSinglePartCache                                       60.43ms    16.55
============================================================================
**/
