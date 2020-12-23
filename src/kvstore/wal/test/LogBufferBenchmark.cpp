/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <folly/Benchmark.h>
#include "kvstore/wal/AtomicLogBuffer.h"
#include "kvstore/wal/test/InMemoryLogBufferList.h"

DEFINE_bool(only_seek, false, "Only seek in read test");

#define TEST_WRTIE       1
#define TEST_READ        1
#define TEST_RW_MIXED    1

using nebula::wal::AtomicLogBuffer;
using nebula::wal::Record;
using nebula::wal::InMemoryBufferList;
using nebula::LogID;

void prepareData(std::shared_ptr<InMemoryBufferList> inMemoryLogBuffer,
                 int32_t len,
                 size_t total) {
    for (size_t i = 0; i < total; i++) {
        inMemoryLogBuffer->push(i, 0, 0, std::string(len, 'A'));
    }
}

void prepareData(std::shared_ptr<AtomicLogBuffer> logBuffer,
                 int32_t len,
                 size_t total) {
    for (size_t i = 0; i < total; i++) {
        logBuffer->push(i, Record(0, 0, std::string(len, 'A')));
    }
}

/*************************
 * Begining of benchmarks
 ************************/

void runInMemoryLogBufferWriteTest(size_t iters, int32_t len) {
    std::shared_ptr<InMemoryBufferList> inMemoryLogBuffer;
    std::vector<std::string> recs;
    BENCHMARK_SUSPEND {
        recs.reserve(iters);
        for (size_t i = 0; i < iters; i++) {
            recs.emplace_back(len, 'A');
        }
        inMemoryLogBuffer = InMemoryBufferList::instance();
    }
    for (size_t i = 0; i < iters; i++) {
        inMemoryLogBuffer->push(i, 0, 0, std::move(recs[i]));
    }
    BENCHMARK_SUSPEND {
        recs.clear();
        inMemoryLogBuffer.reset();
    }
}

void runAtomicLogBufferWriteTest(size_t iters, int32_t len) {
    std::shared_ptr<AtomicLogBuffer> logBuffer;
    std::vector<Record> recs;
    BENCHMARK_SUSPEND {
        recs.reserve(iters);
        for (size_t i = 0; i < iters; i++) {
            recs.emplace_back(0, 0, std::string(len, 'A'));
        }
        logBuffer = AtomicLogBuffer::instance();
    }
    for (size_t i = 0; i < iters; i++) {
        logBuffer->push(i, std::move(recs[i]));
    }
    BENCHMARK_SUSPEND {
        recs.clear();
        logBuffer.reset();
    }
}

void runAtomicLogBufferWriteTestPush2(size_t iters, int32_t len) {
    std::shared_ptr<AtomicLogBuffer> logBuffer;
    std::vector<std::string> recs;
    BENCHMARK_SUSPEND {
        recs.reserve(iters);
        for (size_t i = 0; i < iters; i++) {
            recs.emplace_back(len, 'A');
        }
        logBuffer = AtomicLogBuffer::instance();
    }
    for (size_t i = 0; i < iters; i++) {
        logBuffer->push(i, 0, 0, std::move(recs[i]));
    }
    BENCHMARK_SUSPEND {
        recs.clear();
        logBuffer.reset();
    }
}

#if TEST_WRTIE

BENCHMARK(InMemoryLogBufferWriteShort, iters) {
    runInMemoryLogBufferWriteTest(iters, 16);
}

BENCHMARK_RELATIVE(AtomicLogBufferWriteShort, iters) {
    runAtomicLogBufferWriteTest(iters, 16);
}

BENCHMARK_RELATIVE(AtomicLogBufferWritePush2Short, iters) {
    runAtomicLogBufferWriteTestPush2(iters, 16);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(InMemoryLogBufferWriteMiddle, iters) {
    runInMemoryLogBufferWriteTest(iters, 128);
}

BENCHMARK_RELATIVE(AtomicLogBufferWriteMiddle, iters) {
    runAtomicLogBufferWriteTest(iters, 128);
}

BENCHMARK_RELATIVE(AtomicLogBufferWritePush2Middle, iters) {
    runAtomicLogBufferWriteTestPush2(iters, 128);
}
BENCHMARK_DRAW_LINE();

BENCHMARK(InMemoryLogBufferWriteLong, iters) {
    runInMemoryLogBufferWriteTest(iters, 1024);
}

BENCHMARK_RELATIVE(AtomicLogBufferWriteLong, iters) {
    runAtomicLogBufferWriteTest(iters, 1024);
}

BENCHMARK_RELATIVE(AtomicLogBufferWritePush2Long, iters) {
    runAtomicLogBufferWriteTestPush2(iters, 1024);
}
BENCHMARK_DRAW_LINE();

BENCHMARK(InMemoryLogBufferWriteVeryLong, iters) {
    runInMemoryLogBufferWriteTest(iters, 4096);
}

BENCHMARK_RELATIVE(AtomicLogBufferWriteVeryLong, iters) {
    runAtomicLogBufferWriteTest(iters, 4096);
}

BENCHMARK_RELATIVE(AtomicLogBufferWritePush2VeryLong, iters) {
    runAtomicLogBufferWriteTestPush2(iters, 4096);
}
BENCHMARK_DRAW_LINE();

#endif
/*============== Begin test for scan ===================== */
void runInMemoryLogBufferReadLatestN(int32_t total,
                                     int32_t N) {
    std::shared_ptr<InMemoryBufferList> inMemoryLogBuffer;
    BENCHMARK_SUSPEND {
        inMemoryLogBuffer = InMemoryBufferList::instance();
        prepareData(inMemoryLogBuffer, 1024, total);
    }
    auto start = total - N;
    auto end = total - 1;
    int32_t loopTimes = 1000000;
    while (loopTimes-- > 0) {
        auto iter = inMemoryLogBuffer->iterator(start, end);
        if (!FLAGS_only_seek) {
            for (; iter->valid(); ++(*iter)) {
                auto log = iter->logMsg();
                folly::doNotOptimizeAway(log);
            }
        }
    }
    BENCHMARK_SUSPEND {
        inMemoryLogBuffer.reset();
    }
}

void runAtomicLogBufferReadLatestN(int32_t total,
                                   int32_t N) {
    std::shared_ptr<AtomicLogBuffer> logBuffer;
    BENCHMARK_SUSPEND {
        logBuffer = AtomicLogBuffer::instance();
        prepareData(logBuffer, 1024, total);
    }
    auto start = total - N;
    auto end = total - 1;
    int32_t loopTimes = 1000000;
    while (loopTimes-- > 0) {
        auto iter = logBuffer->iterator(start, end);
        if (!FLAGS_only_seek) {
            for (; iter->valid(); ++(*iter)) {
                auto log = iter->logMsg();
                folly::doNotOptimizeAway(log);
            }
        }
    }
    BENCHMARK_SUSPEND {
        logBuffer.reset();
    }
}

#if TEST_READ
constexpr int32_t totalLogs = 20000;

BENCHMARK(InMemoryLogBufferReadLatest8) {
    runInMemoryLogBufferReadLatestN(totalLogs, 8);
}

BENCHMARK_RELATIVE(AtomicLogBufferReadLatest8) {
    runAtomicLogBufferReadLatestN(totalLogs, 8);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(InMemoryLogBufferReadLatest32) {
    runInMemoryLogBufferReadLatestN(totalLogs, 32);
}

BENCHMARK_RELATIVE(AtomicLogBufferReadLatest32) {
    runAtomicLogBufferReadLatestN(totalLogs, 32);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(InMemoryLogBufferReadLatest128) {
    runInMemoryLogBufferReadLatestN(totalLogs, 128);
}

BENCHMARK_RELATIVE(AtomicLogBufferReadLatest128) {
    runAtomicLogBufferReadLatestN(totalLogs, 128);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(InMemoryLogBufferReadLatest1024) {
    runInMemoryLogBufferReadLatestN(totalLogs, 1024);
}

BENCHMARK_RELATIVE(AtomicLogBufferReadLatest1024) {
    runAtomicLogBufferReadLatestN(totalLogs, 1024);
}

BENCHMARK_DRAW_LINE();

BENCHMARK(InMemoryLogBufferReadLatest6000) {
    runInMemoryLogBufferReadLatestN(totalLogs, 6000);
}

BENCHMARK_RELATIVE(AtomicLogBufferReadLatest6000) {
    runAtomicLogBufferReadLatestN(totalLogs, 6000);
}

BENCHMARK_DRAW_LINE();

#endif


#if TEST_RW_MIXED
/**
 *  Read write mix test
 *
 * */
template<class LogBufferPtr>
std::vector<std::unique_ptr<std::thread>>
runSingleWriterMultiReadersInBackground(LogBufferPtr logBuffer,
                                        bool startWriter,
                                        int32_t readersNum,
                                        std::atomic<LogID>& writePoint,
                                        std::atomic<bool>& stop) {
    std::vector<std::unique_ptr<std::thread>> threads;
    if (startWriter) {
        auto writer = std::make_unique<std::thread>([logBuffer, &writePoint, &stop] {
            LogID logId = 1000;  // start from 1000
            while (!stop) {
                logBuffer->push(logId, 0, 0, folly::stringPrintf("str_%ld", logId));
                writePoint.store(logId, std::memory_order_release);
                logId++;
                // The background writer tps about 10K
                usleep(100);
            }
        });
        threads.emplace_back(std::move(writer));
    }

    for (int i = 0; i < readersNum; i++) {
        threads.emplace_back(
                std::make_unique<std::thread>([logBuffer, &writePoint, &stop] {
                    while (!stop) {
                        auto wp = writePoint.load(std::memory_order_acquire);
                        auto start = wp - 32;
                        auto end = wp;
                        auto iter = logBuffer->iterator(start, end);
                        if (!iter->valid()) {
                            continue;
                        }
                        for (; iter->valid(); ++(*iter)) {
                            auto logId = iter->logId();
                            auto log = iter->logMsg();
                            folly::doNotOptimizeAway(logId);
                            folly::doNotOptimizeAway(log);
                        }
                        // The background reader qps about 10K
                        usleep(100);
                    }
                }));
    }
    return threads;
}

template<class LogBufferPtr>
void runRWMixedTestRead(LogBufferPtr logBuffer) {
    std::atomic<LogID> writePoint{1000};
    std::atomic<bool>  stop{false};
    std::vector<std::unique_ptr<std::thread>> threads;
    BENCHMARK_SUSPEND {
        threads = runSingleWriterMultiReadersInBackground(logBuffer, true, 1, writePoint, stop);
        // wait for all threads started.
        usleep(10);
    }
    auto start = writePoint - 32;
    auto end = writePoint - 1;
    int32_t loopTimes = 1000000;
    while (loopTimes-- > 0) {
        auto iter = logBuffer->iterator(start, end);
        for (; iter->valid(); ++(*iter)) {
            auto log = iter->logMsg();
            folly::doNotOptimizeAway(log);
        }
    }

    BENCHMARK_SUSPEND {
        stop = true;
        for (auto& t : threads) {
            t->join();
        }
    }
}

BENCHMARK(InMemoryListBufferRWMixedReadTest) {
    std::shared_ptr<InMemoryBufferList> logBuffer;
    BENCHMARK_SUSPEND {
        logBuffer = InMemoryBufferList::instance();
        prepareData(logBuffer, 1024, 1000);
    }
    runRWMixedTestRead(logBuffer);
    BENCHMARK_SUSPEND {
        logBuffer.reset();
    }
}

BENCHMARK_RELATIVE(AtomicLogBufferRWMixedReadTest) {
    std::shared_ptr<AtomicLogBuffer> logBuffer;
    BENCHMARK_SUSPEND {
        logBuffer = AtomicLogBuffer::instance();
        prepareData(logBuffer, 1024, 1000);
    }
    runRWMixedTestRead(logBuffer);
    BENCHMARK_SUSPEND {
        logBuffer.reset();
    }
}

BENCHMARK_DRAW_LINE();

template<class LogBufferPtr>
void runRWMixedTestWrite(LogBufferPtr logBuffer) {
    std::atomic<bool>  stop{false};
    std::vector<std::unique_ptr<std::thread>> threads;
    BENCHMARK_SUSPEND {
        std::atomic<LogID> writePoint{1000};
        threads = runSingleWriterMultiReadersInBackground(logBuffer, false, 1, writePoint, stop);
        // wait for all threads started.
        usleep(10);
    }
    LogID logId = 1000;
    int32_t loopTimes = 1024 * 1024;
    while (loopTimes-- > 0) {
        logBuffer->push(logId, 0, 0, std::string(1024, 'A'));
        logId++;
    }
    BENCHMARK_SUSPEND {
        stop = true;
        for (auto& t : threads) {
            t->join();
        }
    }
}

BENCHMARK(InMemoryListBufferRWMixedWriteTest) {
    std::shared_ptr<InMemoryBufferList> logBuffer;
    BENCHMARK_SUSPEND {
        logBuffer = InMemoryBufferList::instance();
        // Make sure the buffer has been full.
        prepareData(logBuffer, 1024, 1000 * 10);
    }
    runRWMixedTestWrite(logBuffer);
    BENCHMARK_SUSPEND {
        logBuffer.reset();
    }
}

BENCHMARK_RELATIVE(AtomicLogBufferRWMixedWriteTest) {
    std::shared_ptr<AtomicLogBuffer> logBuffer;
    BENCHMARK_SUSPEND {
        logBuffer = AtomicLogBuffer::instance();
        // Make sure the buffer has been full.
        prepareData(logBuffer, 1024, 1000 * 10);
    }
    runRWMixedTestWrite(logBuffer);
    BENCHMARK_SUSPEND {
        logBuffer.reset();
    }
}

#endif

/*************************
 * End of benchmarks
 ************************/


int main(int argc, char** argv) {
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    folly::runBenchmarks();
    return 0;
}
/*
Intel(R) Xeon(R) CPU E5-2690 v2 @ 3.00GHz
-O2  kMaxLenght=64    write test
============================================================================
LogBufferBenchmark.cpprelative                            time/iter  iters/s
============================================================================
InMemoryLogBufferWriteShort                                 98.04ns   10.20M
AtomicLogBufferWriteShort                        356.89%    27.47ns   36.40M
AtomicLogBufferWritePush2Short                   309.25%    31.70ns   31.54M
----------------------------------------------------------------------------
InMemoryLogBufferWriteMiddle                                97.39ns   10.27M
AtomicLogBufferWriteMiddle                       354.47%    27.48ns   36.40M
AtomicLogBufferWritePush2Middle                  306.73%    31.75ns   31.49M
----------------------------------------------------------------------------
InMemoryLogBufferWriteLong                                  97.32ns   10.27M
AtomicLogBufferWriteLong                         350.59%    27.76ns   36.02M
AtomicLogBufferWritePush2Long                    305.71%    31.84ns   31.41M
----------------------------------------------------------------------------
InMemoryLogBufferWriteVeryLong                              97.43ns   10.26M
AtomicLogBufferWriteVeryLong                     342.62%    28.44ns   35.17M
AtomicLogBufferWritePush2VeryLong                302.24%    32.24ns   31.02M
----------------------------------------------------------------------------
InMemoryLogBufferReadLatest8                               374.97ms     2.67
AtomicLogBufferReadLatest8                       290.72%   128.98ms     7.75
----------------------------------------------------------------------------
InMemoryLogBufferReadLatest32                              873.47ms     1.14
AtomicLogBufferReadLatest32                      273.17%   319.75ms     3.13
----------------------------------------------------------------------------
InMemoryLogBufferReadLatest128                                2.87s  348.32m
AtomicLogBufferReadLatest128                     267.61%      1.07s  932.14m
----------------------------------------------------------------------------
InMemoryLogBufferReadLatest1024                              21.54s   46.43m
AtomicLogBufferReadLatest1024                    264.77%      8.14s  122.92m
----------------------------------------------------------------------------
InMemoryLogBufferReadLatest6000                             2.02min    8.27m
AtomicLogBufferReadLatest6000                    281.61%     42.94s   23.29m
----------------------------------------------------------------------------
InMemoryListBufferRWMixedReadTest                          801.97ms     1.25
AtomicLogBufferRWMixedReadTest                   262.12%   305.96ms     3.27
----------------------------------------------------------------------------
InMemoryListBufferRWMixedWriteTest                         422.98ms     2.36
AtomicLogBufferRWMixedWriteTest                  241.31%   175.28ms     5.71
============================================================================
*/
