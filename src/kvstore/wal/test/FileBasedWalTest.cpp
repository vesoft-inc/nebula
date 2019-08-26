/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "kvstore/wal/FileBasedWal.h"
#include "kvstore/wal/BufferFlusher.h"
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

    auto wal = FileBasedWal::getWal(walDir.path(),
                                    policy,
                                    flusher.get(),
                                    [](LogID, TermID, ClusterID, const std::string&) {
                                        return true;
                                    });
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
    wal = FileBasedWal::getWal(walDir.path(),
                               policy,
                               flusher.get(),
                               [](LogID, TermID, ClusterID, const std::string&) {
                                   return true;
                               });
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
    policy.fileSize = 1024L * 1024L;
    policy.bufferSize = 1024L * 1024L;
    policy.numBuffers = 2;

    TempDir walDir("/tmp/testWal.XXXXXX");
    auto wal = FileBasedWal::getWal(walDir.path(),
                                    policy,
                                    flusher.get(),
                                    [](LogID, TermID, ClusterID, const std::string&) {
                                        return true;
                                    });
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
    wal = FileBasedWal::getWal(walDir.path(),
                               policy,
                               flusher.get(),
                               [](LogID, TermID, ClusterID, const std::string&) {
                                   return true;
                               });

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
    policy.fileSize = 1024L * 1024L;
    policy.bufferSize = 1024L * 1024L;
    policy.numBuffers = 2;

    TempDir walDir("/tmp/testWal.XXXXXX");
    auto wal = FileBasedWal::getWal(walDir.path(),
                                    policy,
                                    flusher.get(),
                                    [](LogID, TermID, ClusterID, const std::string&) {
                                        return true;
                                    });
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
    policy.fileSize = 1024L * 1024L;
    policy.bufferSize = 1024L * 1024L;
    policy.numBuffers = 2;

    TempDir walDir("/tmp/testWal.XXXXXX");
    auto wal = FileBasedWal::getWal(walDir.path(),
                                    policy,
                                    flusher.get(),
                                    [](LogID, TermID, ClusterID, const std::string&) {
                                        return true;
                                    });
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
    wal = FileBasedWal::getWal(walDir.path(),
                               policy,
                               flusher.get(),
                               [](LogID, TermID, ClusterID, const std::string&) {
                                   return true;
                               });
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
    policy.fileSize = 1024L * 1024L;
    policy.bufferSize = 1024L * 1024L;
    policy.numBuffers = 2;

    TempDir walDir("/tmp/testWal.XXXXXX");
    auto wal = FileBasedWal::getWal(walDir.path(),
                                    policy,
                                    flusher.get(),
                                    [](LogID, TermID, ClusterID, const std::string&) {
                                        return true;
                                    });
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
    policy.fileSize = 1024L * 1024L;
    policy.bufferSize = 1024L * 1024L;
    policy.numBuffers = 2;

    TempDir walDir("/tmp/testWal.XXXXXX");
    auto wal = FileBasedWal::getWal(walDir.path(),
                                    policy,
                                    flusher.get(),
                                    [](LogID, TermID, ClusterID, const std::string&) {
                                        return true;
                                    });
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

TEST(FileBasedWal, BackAndForth) {
    // Force to make each file 1MB, each buffer is 1MB, and there are two
    // buffers at most
    FileBasedWalPolicy policy;
    policy.fileSize = 1024L * 1024L;
    policy.bufferSize = 1024L * 1024L;
    policy.numBuffers = 2;

    TempDir walDir("/tmp/testWal.XXXXXX");
    auto wal = FileBasedWal::getWal(walDir.path(),
                                    policy,
                                    flusher.get(),
                                    [](LogID, TermID, ClusterID, const std::string&) {
                                        return true;
                                    });
    ASSERT_EQ(0, wal->lastLogId());

    // Go back and forth several times, each time append 200 and rollback 100 wal
    for (int count = 0; count < 10; count++) {
        for (int i = count * 100 + 1; i <= (count + 1) * 200; i++) {
            ASSERT_TRUE(wal->appendLog(i /*id*/, 1 /*term*/, 0 /*cluster*/,
                folly::stringPrintf(kLongMsg, i)));
        }
        ASSERT_TRUE(wal->rollbackToLog(100 * (count + 1)));
        ASSERT_EQ(100 * (count + 1), wal->lastLogId());
    }

    // We don't delete the overlaping wals, there must be 10 wal files
    ASSERT_EQ(10, FileUtils::listAllFilesInDir(walDir.path(), false, "*.wal").size());

    // Rollback
    for (int i = 999; i >= 0; i--) {
        ASSERT_TRUE(wal->rollbackToLog(i));
        ASSERT_EQ(i, wal->lastLogId());
    }
    ASSERT_EQ(0, FileUtils::listAllFilesInDir(walDir.path(), false, "*.wal").size());
}

TEST(FileBasedWal, TTLTest) {
    FileBasedWalPolicy policy;
    TempDir walDir("/tmp/testWal.XXXXXX");
    auto flu = std::make_unique<nebula::wal::BufferFlusher>();
    policy.ttl = 3;
    policy.bufferSize = 128;
    policy.fileSize = 1024;
    policy.numBuffers = 30;
    auto wal = FileBasedWal::getWal(walDir.path(),
                                    policy,
                                    flu.get(),
                                    [](LogID, TermID, ClusterID, const std::string&) {
                                        return true;
                                    });
    EXPECT_EQ(0, wal->lastLogId());

    for (int i = 1; i <= 100; i++) {
        EXPECT_TRUE(
            wal->appendLog(i /*id*/, 1 /*term*/, 0 /*cluster*/,
                           folly::stringPrintf("Test string %02d", i)));
    }
    // Wait for the buffer flushed.
    sleep(1);
    auto startIdAfterGC
        = static_cast<FileBasedWal*>(wal.get())->walFiles_.rbegin()->second->firstId();
    auto expiredFilesNum = static_cast<FileBasedWal*>(wal.get())->walFiles_.size() - 1;
    sleep(policy.ttl);
    for (int i = 101; i <= 200; i++) {
        EXPECT_TRUE(
            wal->appendLog(i /*id*/, 1 /*term*/, 0 /*cluster*/,
                           folly::stringPrintf("Test string %02d", i)));
    }
    EXPECT_EQ(200, wal->lastLogId());
    // Wait for the flusher finished!
    sleep(1);
    // Close the wal
    wal.reset();

    {
        // Now let's open it to read
        wal = FileBasedWal::getWal(walDir.path(),
                                   policy,
                                   flu.get(),
                                   [](LogID, TermID, ClusterID, const std::string&) {
                                       return true;
                                   });
        EXPECT_EQ(200, wal->lastLogId());

        auto it = wal->iterator(1, 200);
        LogID id = 1;
        while (it->valid()) {
            EXPECT_EQ(id, it->logId());
            EXPECT_EQ(folly::stringPrintf("Test string %02ld", id),
                      it->logMsg());
            ++(*it);
            ++id;
        }
        EXPECT_EQ(201, id);
    }
    auto totalFilesNum = static_cast<FileBasedWal*>(wal.get())->walFiles_.size();
    wal->cleanWAL();
    auto numFilesAfterGC = static_cast<FileBasedWal*>(wal.get())->walFiles_.size();
    // We will hold the last expired file.
    ASSERT_EQ(numFilesAfterGC, totalFilesNum - expiredFilesNum)
            << "total " << totalFilesNum
            << ", numFilesAfterGC " << numFilesAfterGC
            << ", expiredFilesNum " << expiredFilesNum;

    wal.reset();
    {
        // Now let's open it to read
        wal = FileBasedWal::getWal(walDir.path(),
                                   policy,
                                   flu.get(),
                                   [](LogID, TermID, ClusterID, const std::string&) {
                                       return true;
                                   });
        EXPECT_EQ(200, wal->lastLogId());
        EXPECT_EQ(startIdAfterGC, wal->firstLogId());
        auto it = wal->iterator(startIdAfterGC, 200);
        LogID id = startIdAfterGC;
        while (it->valid()) {
            EXPECT_EQ(id, it->logId());
            EXPECT_EQ(folly::stringPrintf("Test string %02ld", id),
                      it->logMsg());
            ++(*it);
            ++id;
        }
        EXPECT_EQ(201, id);
    }
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


