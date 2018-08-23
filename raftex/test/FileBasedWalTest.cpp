/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "raftex/FileBasedWal.h"
#include "raftex/BufferFlusher.h"
#include "fs/TempDir.h"

namespace vesoft {
namespace raftex {

using namespace vesoft::fs;

// Make a message which length > 1KB
static const char* kLongMsg =
    "abcdefghijklmnopqrstuvwxyz123456789abcdefghijklmnopqrstuvwxyz123456789"
    "abcdefghijklmnopqrstuvwxyz123456789abcdefghijklmnopqrstuvwxyz123456789"
    "abcdefghijklmnopqrstuvwxyz123456789abcdefghijklmnopqrstuvwxyz123456789"
    "abcdefghijklmnopqrstuvwxyz123456789abcdefghijklmnopqrstuvwxyz123456789"
    "abcdefghijklmnopqrstuvwxyz123456789abcdefghijklmnopqrstuvwxyz123456789"
    "abcdefghijklmnopqrstuvwxyz123456789abcdefghijklmnopqrstuvwxyz123456789"
    "abcdefghijklmnopqrstuvwxyz123456789abcdefghijklmnopqrstuvwxyz123456789"
    "abcdefghijklmnopqrstuvwxyz123456789abcdefghijklmnopqrstuvwxyz123456789"
    "abcdefghijklmnopqrstuvwxyz123456789abcdefghijklmnopqrstuvwxyz123456789"
    "abcdefghijklmnopqrstuvwxyz123456789abcdefghijklmnopqrstuvwxyz123456789"
    "abcdefghijklmnopqrstuvwxyz123456789abcdefghijklmnopqrstuvwxyz123456789"
    "abcdefghijklmnopqrstuvwxyz123456789abcdefghijklmnopqrstuvwxyz123456789"
    "abcdefghijklmnopqrstuvwxyz123456789abcdefghijklmnopqrstuvwxyz123456789"
    "abcdefghijklmnopqrstuvwxyz123456789abcdefghijklmnopqrstuvwxyz123456789"
    "abcdefghijklmnopqrstuvwxyz123456789abcdefghijklmnopqrstuvwxyz-%06d";

std::unique_ptr<BufferFlusher> flusher;


TEST(FileBasedWal, AppendLogs) {
    FileBasedWalPolicy policy;
    TempDir walDir("/tmp/testWal.XXXXXX");

    // Create a new WAL, add one log and close it
    auto wal = FileBasedWal::getWal(walDir.path(), policy, flusher.get());
    EXPECT_EQ(-1, wal->lastLogId());

    for (int i = 0; i < 10; i++) {
        EXPECT_TRUE(
            wal->appendLog(i /*id*/, 1 /*term*/, 0 /*cluster*/,
                           folly::stringPrintf("Test string %02d", i)));
    }
    EXPECT_EQ(9, wal->lastLogId());
    // Close the wal
    wal.reset();

    // Now let's open it to read
    wal = FileBasedWal::getWal(walDir.path(), policy, flusher.get());
    EXPECT_EQ(9, wal->lastLogId());

    auto it = wal->iterator(0, 9);
    LogID id = 0;
    while (it->valid()) {
        EXPECT_EQ(id, it->logId());
        EXPECT_EQ(folly::stringPrintf("Test string %02ld", id),
                  it->logMsg());
        ++(*it);
        ++id;
    }
    EXPECT_EQ(10, id);
}


TEST(FileBasedWal, CacheOverflow) {
    // Force to make each file 1MB, each buffer is 1MB, and there are two
    // buffers at most
    FileBasedWalPolicy policy;
    policy.fileSize = 1;
    policy.bufferSize = 1;
    policy.numBuffers = 2;

    TempDir walDir("/tmp/testWal.XXXXXX");

    // Create a new WAL, add one log and close it
    auto wal = FileBasedWal::getWal(walDir.path(), policy, flusher.get());
    EXPECT_EQ(-1, wal->lastLogId());

    // Append > 10MB logs in total
    for (int i = 0; i < 10000; i++) {
        ASSERT_TRUE(wal->appendLog(i /*id*/, 1 /*term*/, 0 /*cluster*/,
                                   folly::stringPrintf(kLongMsg, i)));
    }
    ASSERT_EQ(9999, wal->lastLogId());

    // Wait one second to make sure all buffers have been flushed
    sleep(1);

    // Close the wal
    ASSERT_EQ(1, wal.use_count());
    wal.reset();

    // Check the number of files
    auto files = FileUtils::listAllFilesInDir(walDir.path());
    ASSERT_EQ(11, files.size());

    // Now let's open it to read
    wal = FileBasedWal::getWal(walDir.path(), policy, flusher.get());
    EXPECT_EQ(9999, wal->lastLogId());

    auto it = wal->iterator(0, 9999);
    LogID id = 0;
    while (it->valid()) {
        ASSERT_EQ(id, it->logId());
        ASSERT_EQ(folly::stringPrintf(kLongMsg, id), it->logMsg());
        ++(*it);
        ++id;
    }
    EXPECT_EQ(10000, id);
}


TEST(FileBasedWal, Rollback) {
    // Force to make each file 1MB, each buffer is 1MB, and there are two
    // buffers at most
    FileBasedWalPolicy policy;
    policy.fileSize = 1;
    policy.bufferSize = 1;
    policy.numBuffers = 2;

    TempDir walDir("/tmp/testWal.XXXXXX");

    // Create a new WAL, add one log and close it
    auto wal = FileBasedWal::getWal(walDir.path(), policy, flusher.get());
    EXPECT_EQ(-1, wal->lastLogId());

    // Append > 10MB logs in total
    for (int i = 0; i < 10000; i++) {
        ASSERT_TRUE(wal->appendLog(i /*id*/, 1 /*term*/, 0 /*cluster*/,
                                   folly::stringPrintf(kLongMsg, i)));
    }
    ASSERT_EQ(9999, wal->lastLogId());

    // Rollback 100 logs
    wal->rollbackToLog(9899);
    ASSERT_EQ(9899, wal->lastLogId());

    // Try to read the last 10 logs
    auto it = wal->iterator(9890, 9899);
    LogID id = 9890;
    while (it->valid()) {
        ASSERT_EQ(id, it->logId());
        ASSERT_EQ(folly::stringPrintf(kLongMsg, id), it->logMsg());
        ++(*it);
        ++id;
    }
    EXPECT_EQ(9900, id);

    // Now let's rollback even more
    wal->rollbackToLog(4999);
    ASSERT_EQ(4999, wal->lastLogId());

    // Let's verify the last 10 logs
    it = wal->iterator(4990, 4999);
    id = 4990;
    while (it->valid()) {
        ASSERT_EQ(id, it->logId());
        ASSERT_EQ(folly::stringPrintf(kLongMsg, id), it->logMsg());
        ++(*it);
        ++id;
    }
    EXPECT_EQ(5000, id);

    // Now let's append 1000 more logs
    for (int i = 5000; i < 6000; i++) {
        ASSERT_TRUE(
            wal->appendLog(i /*id*/, 1 /*term*/, 0 /*cluster*/,
                           folly::stringPrintf(kLongMsg, i + 1000)));
    }
    ASSERT_EQ(5999, wal->lastLogId());

    // Let's verify the last 10 logs
    it = wal->iterator(5990, 5999);
    id = 5990;
    while (it->valid()) {
        ASSERT_EQ(id, it->logId());
        ASSERT_EQ(folly::stringPrintf(kLongMsg, id + 1000), it->logMsg());
        ++(*it);
        ++id;
    }
    EXPECT_EQ(6000, id);

    // Wait one second to make sure all buffers being flushed
    sleep(1);
}

}  // namespace raftex
}  // namespace vesoft


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    using namespace vesoft::raftex;
    flusher.reset(new BufferFlusher());

    return RUN_ALL_TESTS();
}


