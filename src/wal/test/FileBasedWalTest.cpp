/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "wal/FileBasedWal.h"
#include "wal/BufferFlusher.h"
#include "fs/TempDir.h"

namespace nebula {
namespace wal {

using nebula::fs::FileUtils;
using nebula::fs::TempDir;

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
    EXPECT_EQ(0, wal->lastLogId());

    for (int i = 1; i <= 10; i++) {
        EXPECT_TRUE(
            wal->appendLog(i /*id*/, 1 /*term*/, 0 /*cluster*/,
                           folly::stringPrintf("Test string %02d", i)));
    }
    EXPECT_EQ(10, wal->lastLogId());
    // Close the wal
    wal.reset();

    // Now let's open it to read
    wal = FileBasedWal::getWal(walDir.path(), policy, flusher.get());
    EXPECT_EQ(10, wal->lastLogId());

    auto it = wal->iterator(1, 10);
    LogID id = 1;
    while (it->valid()) {
        EXPECT_EQ(id, it->logId());
        EXPECT_EQ(folly::stringPrintf("Test string %02ld", id),
                  it->logMsg());
        ++(*it);
        ++id;
    }
    EXPECT_EQ(11, id);
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
    EXPECT_EQ(0, wal->lastLogId());

    // Append > 10MB logs in total
    for (int i = 1; i <= 10000; i++) {
        ASSERT_TRUE(wal->appendLog(i /*id*/, 1 /*term*/, 0 /*cluster*/,
                                   folly::stringPrintf(kLongMsg, i)));
    }
    ASSERT_EQ(10000, wal->lastLogId());

    ASSERT_EQ(2, wal.use_count());
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
    EXPECT_EQ(10000, wal->lastLogId());

    auto it = wal->iterator(1, 10000);
    LogID id = 1;
    while (it->valid()) {
        ASSERT_EQ(id, it->logId());
        ASSERT_EQ(folly::stringPrintf(kLongMsg, id), it->logMsg());
        ++(*it);
        ++id;
    }
    EXPECT_EQ(10001, id);
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
    EXPECT_EQ(0, wal->lastLogId());

    // Append > 10MB logs in total
    for (int i = 1; i <= 10000; i++) {
        ASSERT_TRUE(wal->appendLog(i /*id*/, 1 /*term*/, 0 /*cluster*/,
                                   folly::stringPrintf(kLongMsg, i)));
    }
    ASSERT_EQ(10000, wal->lastLogId());

    // Rollback 100 logs
    wal->rollbackToLog(9900);
    ASSERT_EQ(9900, wal->lastLogId());

    // Try to read the last 10 logs
    auto it = wal->iterator(9891, 9900);
    LogID id = 9891;
    while (it->valid()) {
        ASSERT_EQ(id, it->logId());
        ASSERT_EQ(folly::stringPrintf(kLongMsg, id), it->logMsg());
        ++(*it);
        ++id;
    }
    EXPECT_EQ(9901, id);

    // Now let's rollback even more
    wal->rollbackToLog(5000);
    ASSERT_EQ(5000, wal->lastLogId());

    // Let's verify the last 10 logs
    it = wal->iterator(4991, 5000);
    id = 4991;
    while (it->valid()) {
        ASSERT_EQ(id, it->logId());
        ASSERT_EQ(folly::stringPrintf(kLongMsg, id), it->logMsg());
        ++(*it);
        ++id;
    }
    EXPECT_EQ(5001, id);

    // Now let's append 1000 more logs
    for (int i = 5001; i <= 6000; i++) {
        ASSERT_TRUE(
            wal->appendLog(i /*id*/, 1 /*term*/, 0 /*cluster*/,
                           folly::stringPrintf(kLongMsg, i + 1000)));
    }
    ASSERT_EQ(6000, wal->lastLogId());

    // Let's verify the last 10 logs
    it = wal->iterator(5991, 6000);
    id = 5991;
    while (it->valid()) {
        ASSERT_EQ(id, it->logId());
        ASSERT_EQ(folly::stringPrintf(kLongMsg, id + 1000), it->logMsg());
        ++(*it);
        ++id;
    }
    EXPECT_EQ(6001, id);

    // Wait one second to make sure all buffers being flushed
    sleep(1);
}

TEST(FileBasedWal, RollbackToFile) {
    // Force to make each file 1MB, each buffer is 1MB, and there are two
    // buffers at most
    FileBasedWalPolicy policy;
    policy.fileSize = 1;
    policy.bufferSize = 1;
    policy.numBuffers = 2;

    TempDir walDir("/tmp/testWal.XXXXXX");

    // Create a new WAL, add one log and close it
    auto wal = FileBasedWal::getWal(walDir.path(), policy, flusher.get());
    EXPECT_EQ(0, wal->lastLogId());
    // Append > 1MB logs in total
    for (int i = 1; i <= 1000; i++) {
       ASSERT_TRUE(wal->appendLog(i /*id*/, 1 /*term*/, 0 /*cluster*/,
           folly::stringPrintf(kLongMsg, i)));
    }
    ASSERT_EQ(1000, wal->lastLogId());

    // Wait a few seconds to make sure all buffers being flushed
    sleep(2);

    // Close the wal
    wal.reset();

    // Check the number of files
    auto files = FileUtils::listAllFilesInDir(walDir.path());
    ASSERT_EQ(2, files.size());

    // Now let's open it to read
    wal = FileBasedWal::getWal(walDir.path(), policy, flusher.get());
    EXPECT_EQ(1000, wal->lastLogId());

    // Let's verify the logs
    auto it = wal->iterator(1, 1000);
    LogID id = 1;
    while (it->valid()) {
        ASSERT_EQ(id, it->logId());
        ASSERT_EQ(folly::stringPrintf(kLongMsg, id), it->logMsg());
        ++(*it);
        ++id;
    }
    EXPECT_EQ(1001, id);

    // Appending >1M logs make sure the first buffer will be sent to
    // flusher thread
    for (int i = 1001; i <= 2000; i++) {
        ASSERT_TRUE(
            wal->appendLog(i /*id*/, 1 /*term*/, 0 /*cluster*/,
                folly::stringPrintf(kLongMsg, i + 1000)));
    }
    ASSERT_EQ(2000, wal->lastLogId());

    // Rollbacking to 900 will remove all buffer in memory
    wal->rollbackToLog(900);
    ASSERT_EQ(900, wal->lastLogId());

    // Wait a few seconds to make sure all buffers being flushed
    sleep(2);
}

TEST(FileBasedWal, RollbackToMemory) {
    // Force to make each file 1MB, each buffer is 1MB, and there are two
    // buffers at most
    FileBasedWalPolicy policy;
    policy.fileSize = 1;
    policy.bufferSize = 1;
    policy.numBuffers = 2;

    TempDir walDir("/tmp/testWal.XXXXXX");

    // Create a new WAL, add one log and close it
    auto wal = FileBasedWal::getWal(walDir.path(), policy, flusher.get());
    EXPECT_EQ(0, wal->lastLogId());

    // Append < 1MB logs in total
    for (int i = 1; i <= 100; i++) {
        ASSERT_TRUE(wal->appendLog(i /*id*/, 1 /*term*/, 0 /*cluster*/,
            folly::stringPrintf(kLongMsg, i)));
    }
    ASSERT_EQ(100, wal->lastLogId());

    // Rollback 10 logs
    wal->rollbackToLog(90);
    ASSERT_EQ(90, wal->lastLogId());

    // Now let's append >1M more logs
    for (int i = 91; i <= 2100; i++) {
        ASSERT_TRUE(
            wal->appendLog(i /*id*/, 1 /*term*/, 0 /*cluster*/,
                folly::stringPrintf(kLongMsg, i + 1000)));
    }
    ASSERT_EQ(2100, wal->lastLogId());

    // Let's verify the logs
    auto it = wal->iterator(1, 2100);
    LogID id = 1;
    while (it->valid()) {
        ASSERT_EQ(id, it->logId());
        if (id < 91) {
            ASSERT_EQ(folly::stringPrintf(kLongMsg, id), it->logMsg());
        } else {
            ASSERT_EQ(folly::stringPrintf(kLongMsg, id + 1000), it->logMsg());
        }
        ++(*it);
        ++id;
    }
    EXPECT_EQ(2101, id);

    // Wait a few seconds to make sure all buffers being flushed
    sleep(2);
}

TEST(FileBasedWal, RollbackToZero) {
    // Force to make each file 1MB, each buffer is 1MB, and there are two
    // buffers at most
    FileBasedWalPolicy policy;
    policy.fileSize = 1;
    policy.bufferSize = 1;
    policy.numBuffers = 2;

    TempDir walDir("/tmp/testWal.XXXXXX");

    // Create a new WAL, add one log and close it
    auto wal = FileBasedWal::getWal(walDir.path(), policy, flusher.get());
    ASSERT_EQ(0, wal->lastLogId());
    // Rollback
    ASSERT_TRUE(wal->rollbackToLog(0));
    ASSERT_EQ(0, wal->lastLogId());

    // Append < 1MB logs in total
    for (int i = 1; i <= 100; i++) {
        ASSERT_TRUE(wal->appendLog(i /*id*/, 1 /*term*/, 0 /*cluster*/,
            folly::stringPrintf(kLongMsg, i)));
    }
    ASSERT_EQ(100, wal->lastLogId());
    // Rollback
    ASSERT_TRUE(wal->rollbackToLog(0));
    ASSERT_EQ(0, wal->lastLogId());

    // Append > 1MB logs in total
    for (int i = 1; i <= 1000; i++) {
        ASSERT_TRUE(wal->appendLog(i /*id*/, 1 /*term*/, 0 /*cluster*/,
            folly::stringPrintf(kLongMsg, i)));
    }
    ASSERT_EQ(1000, wal->lastLogId());
    // Rollback
    ASSERT_TRUE(wal->rollbackToLog(0));
    ASSERT_EQ(0, wal->lastLogId());
}

}  // namespace wal
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    using nebula::wal::flusher;
    flusher.reset(new nebula::wal::BufferFlusher());

    return RUN_ALL_TESTS();
}


