/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include <gtest/gtest.h>
#include "kvstore/wal/FileBasedWal.h"

DECLARE_int32(wal_ttl);

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


TEST(FileBasedWal, AppendLogs) {
    FileBasedWalInfo info;
    FileBasedWalPolicy policy;
    TempDir walDir("/tmp/testWal.XXXXXX");

    auto wal = FileBasedWal::getWal(walDir.path(),
                                    info,
                                    policy,
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
                               info,
                               policy,
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
    FileBasedWalInfo info;
    FileBasedWalPolicy policy;
    policy.fileSize = 1024L * 1024L;
    policy.bufferSize = 1024L * 1024L;

    TempDir walDir("/tmp/testWal.XXXXXX");
    auto wal = FileBasedWal::getWal(walDir.path(),
                                    info,
                                    policy,
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

    // Close the wal
    wal.reset();

    // Check the number of files
    auto files = FileUtils::listAllFilesInDir(walDir.path());
    ASSERT_EQ(11, files.size());

    // Now let's open it to read
    wal = FileBasedWal::getWal(walDir.path(),
                               info,
                               policy,
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
    FileBasedWalInfo info;
    FileBasedWalPolicy policy;
    policy.fileSize = 1024L * 1024L;
    policy.bufferSize = 1024L * 1024L;

    TempDir walDir("/tmp/testWal.XXXXXX");
    auto wal = FileBasedWal::getWal(walDir.path(),
                                    info,
                                    policy,
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

    {
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
    }

    // Now let's rollback even more
    wal->rollbackToLog(5000);
    ASSERT_EQ(5000, wal->lastLogId());

    {
        // Let's verify the last 10 logs
        auto it = wal->iterator(4991, 5000);
        LogID id = 4991;
        while (it->valid()) {
            ASSERT_EQ(id, it->logId());
            ASSERT_EQ(folly::stringPrintf(kLongMsg, id), it->logMsg());
            ++(*it);
            ++id;
        }
        EXPECT_EQ(5001, id);
    }
    // Now let's append 1000 more logs
    for (int i = 5001; i <= 6000; i++) {
        ASSERT_TRUE(
            wal->appendLog(i /*id*/, 1 /*term*/, 0 /*cluster*/,
                           folly::stringPrintf(kLongMsg, i + 1000)));
    }
    ASSERT_EQ(6000, wal->lastLogId());

    // Let's verify the last 10 logs
    auto it = wal->iterator(5991, 6000);
    LogID id = 5991;
    while (it->valid()) {
        ASSERT_EQ(id, it->logId());
        ASSERT_EQ(folly::stringPrintf(kLongMsg, id + 1000), it->logMsg());
        ++(*it);
        ++id;
    }
    EXPECT_EQ(6001, id);
}


TEST(FileBasedWal, RollbackThenReopen) {
    // Force to make each file 1MB, each buffer is 1MB, and there are two
    // buffers at most
    FileBasedWalInfo info;
    FileBasedWalPolicy policy;
    policy.fileSize = 1024L * 1024L;
    policy.bufferSize = 1024L * 1024L;

    TempDir walDir("/tmp/testWal.XXXXXX");
    auto wal = FileBasedWal::getWal(walDir.path(),
                                    info,
                                    policy,
                                    [](LogID, TermID, ClusterID, const std::string&) {
                                        return true;
                                    });
    EXPECT_EQ(0, wal->lastLogId());
    // Append > 1MB logs in total
    for (int i = 1; i <= 1500; i++) {
       ASSERT_TRUE(wal->appendLog(i /*id*/, 1 /*term*/, 0 /*cluster*/,
           folly::stringPrintf(kLongMsg, i)));
    }
    ASSERT_EQ(1500, wal->lastLogId());

    // Rollbacking to 800
    wal->rollbackToLog(800);
    ASSERT_EQ(800, wal->lastLogId());

    // Close the wal
    wal.reset();

    // Check the number of files
    auto files = FileUtils::listAllFilesInDir(walDir.path());
    ASSERT_EQ(1, files.size());

    // Now let's open it to read
    wal = FileBasedWal::getWal(walDir.path(),
                               info,
                               policy,
                               [](LogID, TermID, ClusterID, const std::string&) {
                                   return true;
                               });
    EXPECT_EQ(800, wal->lastLogId());

    // Let's verify the logs
    auto it = wal->iterator(1, 800);
    LogID id = 1;
    while (it->valid()) {
        ASSERT_EQ(id, it->logId());
        ASSERT_EQ(folly::stringPrintf(kLongMsg, id), it->logMsg());
        ++(*it);
        ++id;
    }
    EXPECT_EQ(801, id);
}

TEST(FileBasedWal, RollbackToZero) {
    // Force to make each file 1MB, each buffer is 1MB, and there are two
    // buffers at most
    FileBasedWalInfo info;
    FileBasedWalPolicy policy;
    policy.fileSize = 1024L * 1024L;
    policy.bufferSize = 1024L * 1024L;

    TempDir walDir("/tmp/testWal.XXXXXX");
    auto wal = FileBasedWal::getWal(walDir.path(),
                                    info,
                                    policy,
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
    FileBasedWalInfo info;
    FileBasedWalPolicy policy;
    policy.fileSize = 1024L * 1024L;
    policy.bufferSize = 1024L * 1024L;

    TempDir walDir("/tmp/testWal.XXXXXX");
    auto wal = FileBasedWal::getWal(walDir.path(),
                                    info,
                                    policy,
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

    ASSERT_EQ(10, FileUtils::listAllFilesInDir(walDir.path(), false, "*.wal").size());

    // Rollback
    for (int i = 999; i >= 0; i--) {
        ASSERT_TRUE(wal->rollbackToLog(i));
        ASSERT_EQ(i, wal->lastLogId());
    }
    ASSERT_EQ(0, FileUtils::listAllFilesInDir(walDir.path(), false, "*.wal").size());

    for (int i = 1; i <= 10; i++) {
        EXPECT_TRUE(
            wal->appendLog(i /*id*/, 1 /*term*/, 0 /*cluster*/,
                           folly::stringPrintf("Test string %02d", i)));
    }
    ASSERT_EQ(1, FileUtils::listAllFilesInDir(walDir.path(), false, "*.wal").size());
}


TEST(FileBasedWal, TTLTest) {
    FLAGS_wal_ttl = 3;
    TempDir walDir("/tmp/testWal.XXXXXX");
    FileBasedWalInfo info;
    FileBasedWalPolicy policy;
    policy.bufferSize = 128;
    policy.fileSize = 1024;
    auto wal = FileBasedWal::getWal(walDir.path(),
                                    info,
                                    policy,
                                    [](LogID, TermID, ClusterID, const std::string&) {
                                        return true;
                                    });
    EXPECT_EQ(0, wal->lastLogId());

    for (int i = 1; i <= 100; i++) {
        EXPECT_TRUE(
            wal->appendLog(i /*id*/, 1 /*term*/, 0 /*cluster*/,
                           folly::stringPrintf("Test string %02d", i)));
    }
    auto startIdAfterGC
        = static_cast<FileBasedWal*>(wal.get())->walFiles_.rbegin()->second->firstId();
    auto expiredFilesNum = static_cast<FileBasedWal*>(wal.get())->walFiles_.size() - 1;
    sleep(FLAGS_wal_ttl + 1);
    for (int i = 101; i <= 200; i++) {
        EXPECT_TRUE(
            wal->appendLog(i /*id*/, 1 /*term*/, 0 /*cluster*/,
                           folly::stringPrintf("Test string %02d", i)));
    }
    EXPECT_EQ(200, wal->lastLogId());
    // Close the wal
    wal.reset();

    {
        // Now let's open it to read
        wal = FileBasedWal::getWal(walDir.path(),
                                   info,
                                   policy,
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
                                   info,
                                   policy,
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

TEST(FileBasedWal, CheckLastWalTest) {
    FileBasedWalInfo info;
    FileBasedWalPolicy policy;
    policy.fileSize = 1024L * 1024L;
    TempDir walDir("/tmp/testWal.XXXXXX");

    auto wal = FileBasedWal::getWal(walDir.path(),
                                    info,
                                    policy,
                                    [](LogID, TermID, ClusterID, const std::string&) {
                                        return true;
                                    });
    {
        EXPECT_EQ(0, wal->lastLogId());

        for (int i = 1; i <= 1000; i++) {
            EXPECT_TRUE(
                wal->appendLog(i /*id*/, 1 /*term*/, 0 /*cluster*/,
                               folly::stringPrintf(kLongMsg, i)));
        }
        EXPECT_EQ(1000, wal->lastLogId());
        wal.reset();
    }
    {
        // Modify the wal file, make last wal invalid
        std::vector<std::string> files = FileUtils::listAllFilesInDir(walDir.path(), true, "*.wal");
        std::sort(files.begin(), files.end());
        size_t size = FileUtils::fileSize(files.back().c_str());
        auto fd = open(files.back().c_str(), O_WRONLY | O_APPEND);
        if (ftruncate(fd, size - sizeof(int32_t)) < 0) {
            LOG(ERROR) << "truncate failed";
            close(fd);
            return;
        }
        close(fd);

        // Now let's open it to read
        wal = FileBasedWal::getWal(walDir.path(),
                                   info,
                                   policy,
                                   [](LogID, TermID, ClusterID, const std::string&) {
                                       return true;
                                   });
        EXPECT_EQ(999, wal->lastLogId());
    }
    {
        // get lastId in previous wal, make last wal invalid
        std::vector<std::string> files = FileUtils::listAllFilesInDir(walDir.path(), true, "*.wal");
        std::sort(files.begin(), files.end());
        auto lastWalPath = files.back();
        auto it = wal->walFiles_.rbegin();
        it++;
        auto expected = it->second->lastId();
        wal.reset();

        auto fd = open(lastWalPath.c_str(), O_WRONLY | O_APPEND);
        if (ftruncate(fd, sizeof(LogID) + sizeof(TermID)) < 0) {
            LOG(ERROR) << "truncate failed";
            close(fd);
            return;
        }
        close(fd);

        // Now let's open it to read
        wal = FileBasedWal::getWal(walDir.path(),
                                   info,
                                   policy,
                                   [](LogID, TermID, ClusterID, const std::string&) {
                                       return true;
                                   });
        EXPECT_EQ(expected, wal->lastLogId());
        wal.reset();

        // truncate last wal
        fd = open(lastWalPath.c_str(), O_WRONLY | O_APPEND);
        if (ftruncate(fd, 0) < 0) {
            LOG(ERROR) << "truncate failed";
            close(fd);
            return;
        }
        close(fd);

        // Now let's open it to read
        wal = FileBasedWal::getWal(walDir.path(),
                                   info,
                                   policy,
                                   [](LogID, TermID, ClusterID, const std::string&) {
                                       return true;
                                   });
        EXPECT_EQ(expected, wal->lastLogId());

        // Append more log, and reset to 1000, the last wal should be an empty file
        for (int i = expected + 1; i <= expected + 1000; i++) {
           EXPECT_TRUE(wal->appendLog(i /*id*/, 1 /*term*/, 0 /*cluster*/,
               folly::stringPrintf(kLongMsg, i)));
        }
        EXPECT_EQ(expected + 1000, wal->lastLogId());

        wal->rollbackToLog(1000);
        ASSERT_EQ(1000, wal->lastLogId());
        wal.reset();

        // Now let's open it to read
        wal = FileBasedWal::getWal(walDir.path(),
                                   info,
                                   policy,
                                   [](LogID, TermID, ClusterID, const std::string&) {
                                       return true;
                                   });
        EXPECT_EQ(1000, wal->lastLogId());
    }
}

TEST(FileBasedWal, LinkTest) {
    TempDir walDir("/tmp/testWal.XXXXXX");
    FileBasedWalInfo info;
    FileBasedWalPolicy policy;
    policy.fileSize = 1024 * 512;
    auto wal = FileBasedWal::getWal(walDir.path(),
                                    info,
                                    policy,
                                    [](LogID, TermID, ClusterID, const std::string&) {
                                        return true;
                                    });
    EXPECT_EQ(0, wal->lastLogId());
    for (int i = 1; i <= 1000; i++) {
        EXPECT_TRUE(
            wal->appendLog(i /*id*/, 1 /*term*/, 0 /*cluster*/,
                           folly::stringPrintf(kLongMsg, i)));
    }
    auto snapshotFile = folly::stringPrintf("%s/snapshot", walDir.path());
    CHECK(wal->linkCurrentWAL(snapshotFile.c_str()));
    auto files = fs::FileUtils::listAllFilesInDir(snapshotFile.data(), true);
    ASSERT_EQ(3, files.size());
    std::sort(files.begin(), files.end());
    int i = 0;
    for (auto it = wal->walFiles_.begin(); it != wal->walFiles_.end(); ++it) {
        EXPECT_EQ(FileUtils::fileSize(it->second->path()), FileUtils::fileSize(files[i].c_str()));
        i++;
    }

    auto num = wal->walFiles_.size();
    EXPECT_TRUE(
            wal->appendLog(1001 /*id*/, 1 /*term*/, 0 /*cluster*/,
                           folly::stringPrintf(kLongMsg, 1001)));
    EXPECT_EQ(num + 1, wal->walFiles_.size());
}

TEST(FileBasedWal, CleanWalBeforeIdTest) {
    TempDir walDir("/tmp/testWal.XXXXXX");
    FileBasedWalInfo info;
    FileBasedWalPolicy policy;
    policy.fileSize = 1024 * 10;
    auto wal = FileBasedWal::getWal(walDir.path(),
                                    info,
                                    policy,
                                    [](LogID, TermID, ClusterID, const std::string&) {
                                        return true;
                                    });
    for (LogID i = 1; i <= 1000; i++) {
        EXPECT_TRUE(wal->appendLog(i, 1, 0, folly::stringPrintf(kLongMsg, i)));
    }

    // try to clean not existed log
    wal->cleanWAL(1001L);
    CHECK_EQ(1, wal->firstLogId());
    CHECK_EQ(1000, wal->lastLogId());

    LogID logToKeep = 1;
    while (logToKeep < 1000) {
        wal->cleanWAL(logToKeep);
        CHECK_LE(wal->firstLogId(), logToKeep);
        CHECK(!wal->walFiles_.empty());
        CHECK_LE(wal->walFiles_.begin()->second->firstId(), logToKeep);
        CHECK_GE(wal->walFiles_.begin()->second->lastId(), logToKeep);
        logToKeep += folly::Random::rand64(10);
    }
    CHECK_EQ(1000, wal->lastLogId());

    // try to clean not existed log
    wal->cleanWAL(1L);
    CHECK_EQ(1000, wal->lastLogId());
}

}  // namespace wal
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}


