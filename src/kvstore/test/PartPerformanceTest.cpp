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
//    Not random for now, because it's too slow
//    std::random_device rd;
//    std::mt19937 generator(rd());
//    std::shuffle(str.begin(), str.end(), generator);
    for (auto i = 0; i < repeatNum; i++) {
        str += str;
    }
    return str;
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
           .append(std::move(random));
        updates.Put(rocksdb::Slice(key), rocksdb::Slice(val));
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

void multiThreadTest(int32_t partnum) {
    fs::TempDir dataPath("/tmp/part_multi_thread_test.XXXXXX");
    rocksdb::BlockBasedTableOptions table_options;
    rocksdb::Options options;
    rocksdb::DB* db = nullptr;
    std::vector<std::thread> threads;

    BENCHMARK_SUSPEND {
        table_options.no_block_cache = true;
        options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
        options.create_if_missing = true;

        rocksdb::Status status = rocksdb::DB::Open(options, dataPath.path(), &db);
        CHECK(status.ok());
        genData(db, partnum);
    }

    FOR_EACH_RANGE(i, 0, partnum) {
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

    FOR_EACH_RANGE(i, 0, partnum) {
        range(i, db);
    }

    BENCHMARK_SUSPEND {
        db->Close();
        delete db;
    };
}

BENCHMARK(Part1000_MultThread) {
    multiThreadTest(1000);
}

BENCHMARK(Part1000_SingleThread) {
    singleThreadTest(1000);
}

BENCHMARK(Part500_MultThread) {
    multiThreadTest(500);
}

BENCHMARK(Part500_SingleThread) {
    singleThreadTest(500);
}

BENCHMARK(Part100_MultThread) {
    multiThreadTest(100);
}

BENCHMARK(Part100_SingleThread) {
    singleThreadTest(100);
}

BENCHMARK(Part50_MultThread) {
    multiThreadTest(50);
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
Intel(R) Xeon(R) CPU E5-2690 v2 @ 3.00GHz
============================================================================
PartPerformanceTest.cpprelative                           time/iter  iters/s
============================================================================
Part1000_MultThread                                        177.65ms     5.63
Part1000_SingleThread                                      515.40ms     1.94
Part500_MultThread                                         140.08ms     7.14
Part500_SingleThread                                       597.76ms     1.67
Part100_MultThread                                          99.45ms    10.05
Part100_SingleThread                                       420.03ms     2.38
Part50_MultThread                                           96.06ms    10.41
Part50_SingleThread                                        400.83ms     2.49
Part1_SingleThread                                         371.62ms     2.69
============================================================================ 
 
**/
