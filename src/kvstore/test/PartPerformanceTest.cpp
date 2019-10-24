/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "rocksdb/cache.h"
#include "rocksdb/table.h"
#include "rocksdb/convenience.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/slice_transform.h"
#include "fs/TempDir.h"
#include "kvstore/Part.h"
#include <folly/Benchmark.h>

DEFINE_int64(part_performance_test_rownum, 100000, "Total rows");

namespace nebula {
namespace kvstore {

std::string randomStr(int32_t repeatNum) {
    std::string str("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
                    "!@#$%^&*()_+-={}[]-={}[]:<>,./?~");
    for (auto i = 0; i < repeatNum; i++) {
        str += str;
    }
    return str;
}

bool cleanSystemCache() {
    if (system("sync") < 0) {
        LOG(ERROR) << "Clean system cache error : sync";
        return false;
    }

    if (system("echo 1 > /proc/sys/vm/drop_caches") < 0) {
        LOG(ERROR) << "Clean system cache error : drop_caches";
        return false;
    }
    return true;
}

void genData(rocksdb::DB* db, int32_t partnum) {
    rocksdb::WriteBatch updates;
    rocksdb::WriteOptions woptions;

    for (auto i = 1; i <= FLAGS_part_performance_test_rownum; i++) {
        auto partId = i%partnum;
        std::string key, val;
        auto random = randomStr(10);
        key.reserve(sizeof(partId) + sizeof(uint64_t) * 3);
        key.append(reinterpret_cast<const char*>(&partId), sizeof(partId))
           .append(reinterpret_cast<const char*>(&i), sizeof(uint64_t))
           .append(reinterpret_cast<const char*>(&i), sizeof(uint64_t))
           .append(reinterpret_cast<const char*>(&i), sizeof(uint64_t));

        val.reserve(4 + sizeof(uint64_t) * 3 + 1000);
        val.append("VAL_")
           .append(reinterpret_cast<const char*>(&i), sizeof(uint64_t))
           .append(reinterpret_cast<const char*>(&i), sizeof(uint64_t))
           .append(reinterpret_cast<const char*>(&i), sizeof(uint64_t))
           .append(random);
        updates.Put(rocksdb::Slice(key), rocksdb::Slice(val));
        if (i%1000 == 0) {
            rocksdb::Status status = db->Write(woptions, &updates);
            status = db->Flush(rocksdb::FlushOptions());
            CHECK(status.ok());
            updates.Clear();
        }
    }
    LOG(INFO) << "Data generation completed...";
}

void range(int32_t partId, rocksdb::DB* db) {
    auto start = static_cast<uint64_t>(FLAGS_part_performance_test_rownum * 0.95);
    auto end = static_cast<uint64_t>(FLAGS_part_performance_test_rownum * 0.96);
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

void singleThreadTest(int32_t partnum) {
    fs::TempDir dataPath("/tmp/part_single_thread_test.XXXXXX");
    rocksdb::BlockBasedTableOptions table_options;
    rocksdb::Options options;
    rocksdb::DB* db = nullptr;

    BENCHMARK_SUSPEND {
        table_options.no_block_cache = true;
        options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
        options.create_if_missing = true;
        rocksdb::Status status = rocksdb::DB::Open(options, dataPath.path(), &db);
        CHECK(status.ok());
        genData(db, partnum);
    }

    for (auto i = 0; i < partnum; i++) {
        BENCHMARK_SUSPEND {
            if (!cleanSystemCache()) {
                LOG(ERROR) << "Data generation completed...";
                exit(1);
            }
        };
        range(i, db);
    }

    BENCHMARK_SUSPEND {
        db->Close();
        delete db;
    };
}

BENCHMARK(Part2000_SingleThread) {
    singleThreadTest(2000);
}

BENCHMARK(Part1000_SingleThread) {
    singleThreadTest(1000);
}

BENCHMARK(Part500_SingleThread) {
    singleThreadTest(500);
}

BENCHMARK(Part100_SingleThread) {
    singleThreadTest(100);
}

BENCHMARK(Part50_SingleThread) {
    singleThreadTest(50);
}

BENCHMARK(Part1_SingleThread) {
    singleThreadTest(1);
}

}  // namespace kvstore
}  // namespace nebula


int main(int argc, char** argv) {
    folly::init(&argc, &argv, true);
    folly::runBenchmarks();
    return 0;
}

/**
 * Through real simulations could be drop system cache by each partition before scan.
 *
 * The test data count is 100,000 rows, query result set totals 1,000 rows.
 *
 * This test case must use the root user,Because the system cache cleanup operation was called.
 */

/**
Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
============================================================================
PartPerformanceTest.cpprelative                           time/iter  iters/s
============================================================================
Part2000_SingleThread                                      545.08ms     1.83
Part1000_SingleThread                                      342.73ms     2.92
Part500_SingleThread                                       194.87ms     5.13
Part100_SingleThread                                        71.83ms    13.92
Part50_SingleThread                                         57.61ms    17.36
Part1_SingleThread                                          32.77ms    30.52
============================================================================ 
 
**/
