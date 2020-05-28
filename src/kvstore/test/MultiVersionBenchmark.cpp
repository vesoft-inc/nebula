/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <folly/Benchmark.h>

DEFINE_bool(do_compact, false, "Do compaction after puts");
DEFINE_int32(versions, 100, "Total versions");

namespace nebula {
namespace kvstore {

std::string genKey(int prefix, int version) {
    std::string key;
    key.reserve(8);
    key.append(reinterpret_cast<const char*>(&prefix), sizeof(prefix));
    key.append(reinterpret_cast<const char*>(&version), sizeof(version));
    return key;
}

void testFn(bool withVersion) {
    rocksdb::DB* db = nullptr;
    BENCHMARK_SUSPEND {
        fs::TempDir rootPath("/tmp/multi_versions_test.XXXXXX");
        rocksdb::Options options;
        options.create_if_missing = true;
        options.disable_auto_compactions = true;
        auto status = rocksdb::DB::Open(options, rootPath.path(), &db);
        CHECK(status.ok());
        rocksdb::WriteOptions woptions;
        for (int i = 0; i < 1000; i++) {
            for (int v = 0; v < FLAGS_versions; v++) {
                std::string key;
                if (withVersion) {
                    key = genKey(i, v);
                } else {
                    key = genKey(i, 0);
                }
                auto val = folly::stringPrintf("val_%d_%d", i, v);
                db->Put(woptions,
                        rocksdb::Slice(key.data(), key.size()),
                        rocksdb::Slice(val.data(), val.size()));
            }
        }
        if (FLAGS_do_compact) {
            rocksdb::CompactRangeOptions croptions;
            db->CompactRange(croptions, nullptr, nullptr);
        }
    }
    auto start = genKey(0, 0);
    rocksdb::ReadOptions roptions;
    rocksdb::Iterator* iter = db->NewIterator(roptions);
    if (iter) {
        iter->Seek(rocksdb::Slice(start));
    }

    while (iter->Valid()) {
        iter->Next();
    }
    BENCHMARK_SUSPEND {
        delete iter;
        db->Close();
        delete db;
    }
}

BENCHMARK(WithVersionTest) {
    testFn(true);
}

BENCHMARK(WithOutVersionTest) {
    testFn(false);
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

do_compact = false;
============================================================================
MultiVersionBenchmark.cpprelative                         time/iter  iters/s
============================================================================
WithVersionTest                                              5.87ms   170.41
WithOutVersionTest                                           1.67ms   598.23
============================================================================


do_compact = true
============================================================================
MultiVersionBenchmark.cpprelative                         time/iter  iters/s
============================================================================
WithVersionTest                                             25.44ms    39.30
WithOutVersionTest                                         329.21us    3.04K
============================================================================
 *
 *
 * */
