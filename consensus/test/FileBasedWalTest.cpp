/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "consensus/FileBasedWal.h"
#include "fs/TempDir.h"

namespace vesoft {
namespace vgraph {
namespace consensus {

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


