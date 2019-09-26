/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include "fs/TempDir.h"
#include "kvstore/Part.h"
#include <folly/Benchmark.h>

DEFINE_int32(part_performance_test_partnum, 20, "Total versions");
DEFINE_int64(part_performance_test_rownum, 1000000, "Total versions");

namespace nebula {
namespace kvstore {

void genData(rocksdb::DB* db) {
    rocksdb::WriteBatch updates;
    rocksdb::WriteOptions woptions;

    for (auto i = 1; i <= FLAGS_part_performance_test_rownum; i++) {
        auto partId = i%FLAGS_part_performance_test_partnum;
        std::string key, val;
        key.reserve(sizeof(partId) + sizeof(uint64_t));
        key.append(reinterpret_cast<const char*>(&partId), sizeof(partId))
           .append(reinterpret_cast<const char*>(&i), sizeof(uint64_t));

        val.reserve(4 + sizeof(uint64_t));
        val.append("VAL_")
           .append(reinterpret_cast<const char*>(&i), sizeof(uint64_t));
        updates.Put(rocksdb::Slice(key), rocksdb::Slice(val));
        if (i%1000 == 0) {
            rocksdb::Status status = db->Write(woptions, &updates);
            CHECK(status.ok());
            updates.Clear();
        }
    }
    rocksdb::Status status = db->Write(woptions, &updates);
    CHECK(status.ok());
    LOG(INFO) << "Data generation completed...";
}

void range(int32_t partId, rocksdb::DB* db) {
    uint64_t start = static_cast<uint64_t>(FLAGS_part_performance_test_rownum * 0.5);
    uint64_t end = static_cast<uint64_t>(FLAGS_part_performance_test_rownum * 0.8);
    std::string s, e;
    s.reserve(sizeof(partId) + sizeof(partId));
    s.append(reinterpret_cast<const char*>(&partId), sizeof(partId))
            .append(reinterpret_cast<const char*>(&start), sizeof(uint64_t));

    e.reserve(sizeof(partId) + sizeof(partId));
    e.append(reinterpret_cast<const char*>(&partId), sizeof(partId))
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

void multiThreadTest() {
    fs::TempDir dataPath("/tmp/part_multi_thread_test.XXXXXX");
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB* db = nullptr;
    rocksdb::Status status = rocksdb::DB::Open(options, dataPath.path(), &db);
    CHECK(status.ok());
    BENCHMARK_SUSPEND {
        genData(db);
    }

    std::vector<std::thread> threads;
    for (auto i = 1; i <= FLAGS_part_performance_test_partnum; i++) {
        threads.emplace_back([db, i]() {
            range(i, db);
        });
    }

    for (auto &t : threads) {
        t.join();
    }

    BENCHMARK_SUSPEND {
        db->Close();
        delete db;
    }
}

void singleThreadTest() {
    fs::TempDir dataPath("/tmp/part_single_thread_test.XXXXXX");
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB* db = nullptr;
    rocksdb::Status status = rocksdb::DB::Open(options, dataPath.path(), &db);
    CHECK(status.ok());
    FLAGS_part_performance_test_partnum = 1;
    BENCHMARK_SUSPEND {
        genData(db);
        sleep(2);
    }
    range(1, db);
    BENCHMARK_SUSPEND {
        db->Close();
        delete db;
    };
}

BENCHMARK(MultiThreadTest) {
    multiThreadTest();
}

BENCHMARK(SingleThreadTest) {
    singleThreadTest();
}

}  // namespace kvstore
}  // namespace nebula


int main(int argc, char** argv) {
    folly::init(&argc, &argv, true);
    folly::runBenchmarks();
    return 0;
}

/**
Intel(R) Core(TM) i7-4770HQ CPU @ 2.20GHz

============================================================================
PartPerformanceTest.cpprelative                           time/iter  iters/s
============================================================================
MultiThreadTest                                              9.95ms   100.46
SingleThreadTest                                             7.41ms   134.97
============================================================================
**/
