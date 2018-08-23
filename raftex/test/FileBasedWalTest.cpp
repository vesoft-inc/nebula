/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "raftex/FileBasedWal.h"
#include "fs/TempDir.h"

namespace vesoft {
namespace vgraph {
namespace raftex {

using namespace vesoft::fs;

TEST(FileBasedWal, AppendLogs) {
    FileBasedWalPolicy policy;
    TempDir walDir("/tmp/testWal.XXXXXX");

    // Create a new WAL, add one log and close it
    auto wal = FileBasedWal::getWal(walDir.path(), policy);
    EXPECT_EQ(-1, wal->lastLogId());

    for (int i = 0; i < 10; i++) {
        EXPECT_TRUE(
            wal->appendLog(i, folly::stringPrintf("Test string %02d", i)));
    }
    EXPECT_EQ(9, wal->lastLogId());
    // Close the wal
    wal.reset();

    // Now let's open it to read
    wal = FileBasedWal::getWal(walDir.path(), policy);
    EXPECT_EQ(9, wal->lastLogId());

    auto it = wal->iterator(0);
    LogID id = 0;
    while (it->valid()) {
        EXPECT_EQ(id, it->logId());
        EXPECT_EQ(folly::stringPrintf("Test string %02d", id),
                  it->logMsg());
        ++(*it);
        ++id;
    }
    EXPECT_EQ(10, id);
}


TEST(FileBasedWal, CacheOverflow) {
    // Make a message which length > 1KB
    static const char* kMsg =
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

    // Force to make each file 1MB, each buffer is 1MB, and there are two
    // buffers at most
    FileBasedWalPolicy policy;
    policy.fileSize = 1;
    policy.bufferSize = 1;
    policy.numBuffers = 2;

    TempDir walDir("/tmp/testWal.XXXXXX");

    // Create a new WAL, add one log and close it
    auto wal = FileBasedWal::getWal(walDir.path(), policy);
    EXPECT_EQ(-1, wal->lastLogId());

    // Append > 10MB logs in total
    for (int i = 0; i < 10000; i++) {
        ASSERT_TRUE(wal->appendLog(i, folly::stringPrintf(kMsg, i)));
    }
    ASSERT_EQ(9999, wal->lastLogId());
    // Close the wal
    wal.reset();

    // Check the number of files
    auto files = FileUtils::listAllFilesInDir(walDir.path());
    ASSERT_EQ(11, files.size());

    // Now let's open it to read
    wal = FileBasedWal::getWal(walDir.path(), policy);
    EXPECT_EQ(9999, wal->lastLogId());

    auto it = wal->iterator(0);
    LogID id = 0;
    while (it->valid()) {
        ASSERT_EQ(id, it->logId());
        ASSERT_EQ(folly::stringPrintf(kMsg, id), it->logMsg());
        ++(*it);
        ++id;
    }
    EXPECT_EQ(10000, id);
}

}  // namespace consensus
}  // namespace vgraph
}  // namespace vesoft


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}


