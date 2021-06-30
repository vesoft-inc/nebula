
/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <gtest/gtest.h>
#include "kvstore/DiskManager.h"
#include "kvstore/wal/FileBasedWal.h"
#include "common/fs/FileUtils.h"
#include "common/fs/TempDir.h"

DECLARE_uint64(minimum_reserved_bytes);

namespace nebula {
namespace kvstore {

TEST(DiskManagerTest, PathTest) {
    std::string relative = "./";
    auto status = fs::FileUtils::realPath(relative.c_str());
    CHECK(status.ok());
    auto realPath = status.value();

    auto absolute = boost::filesystem::absolute(relative);
    EXPECT_NE(realPath, absolute.string());
    boost::filesystem::equivalent(absolute, realPath);

    auto canonical1 = boost::filesystem::canonical(relative);
    auto canonical2 = boost::filesystem::canonical(absolute);
    EXPECT_EQ(realPath, canonical1.string());
    EXPECT_EQ(realPath, canonical2.string());
}

TEST(DiskManagerTest, SimpleTest) {
    GraphSpaceID spaceId = 1;
    fs::TempDir disk1("/tmp/disk_man_test.XXXXXX");
    auto path1 = folly::stringPrintf("%s/nebula/%d", disk1.path(), spaceId);
    CHECK(fs::FileUtils::makeDir(path1));
    fs::TempDir disk2("/tmp/disk_man_test.XXXXXX");
    auto path2 = folly::stringPrintf("%s/nebula/%d", disk2.path(), spaceId);
    CHECK(fs::FileUtils::makeDir(path2));

    std::vector<std::string> dataPaths = {disk1.path(), disk2.path()};
    auto bgThread = std::make_shared<thread::GenericWorker>();
    CHECK(bgThread->start());
    DiskManager diskMan(dataPaths, bgThread);
    for (PartitionID partId = 1; partId <= 10; partId++) {
        diskMan.addPartToPath(spaceId, partId, path1);
    }
    for (PartitionID partId = 11; partId <= 20; partId++) {
        diskMan.addPartToPath(spaceId, partId, path2);
    }

    auto status = diskMan.path(spaceId);
    EXPECT_TRUE(status.ok());
    auto spacePath = status.value();
    std::sort(spacePath.begin(), spacePath.end());
    std::vector<std::string> expected = {path1, path2};
    std::sort(expected.begin(), expected.end());
    EXPECT_EQ(expected, spacePath);

    for (PartitionID partId = 1; partId <= 20; partId++) {
        auto partPath = diskMan.path(spaceId, partId);
        EXPECT_TRUE(partPath.ok());
        if (partId <= 10) {
            EXPECT_EQ(path1, partPath.value());
        } else {
            EXPECT_EQ(path2, partPath.value());
        }
    }
}

TEST(DiskManagerTest, AvailableTest) {
    GraphSpaceID spaceId = 1;
    fs::TempDir disk1("/tmp/disk_man_test.XXXXXX");
    auto path1 = folly::stringPrintf("%s/nebula/%d", disk1.path(), spaceId);
    boost::filesystem::create_directories(path1);
    fs::TempDir disk2("/tmp/disk_man_test.XXXXXX");
    auto path2 = folly::stringPrintf("%s/nebula/%d", disk2.path(), spaceId);
    boost::filesystem::create_directories(path2);

    std::vector<std::string> dataPaths = {disk1.path(), disk2.path()};
    DiskManager diskMan(dataPaths);
    for (PartitionID partId = 1; partId <= 10; partId++) {
        diskMan.addPartToPath(spaceId, partId, path1);
    }
    for (PartitionID partId = 11; partId <= 20; partId++) {
        diskMan.addPartToPath(spaceId, partId, path2);
    }

    // mock disk1 is full while disk2 is still vacant
    diskMan.freeBytes_[0] = 0;
    diskMan.freeBytes_[1] = FLAGS_minimum_reserved_bytes;
    for (PartitionID partId = 1; partId <= 10; partId++) {
        EXPECT_FALSE(diskMan.hasEnoughSpace(spaceId, partId));
    }
    for (PartitionID partId = 11; partId <= 20; partId++) {
        EXPECT_TRUE(diskMan.hasEnoughSpace(spaceId, partId));
    }

    // invalid path will always be rejected
    GraphSpaceID unknownSpace = 2;
    PartitionID unknownPart = 1;
    EXPECT_FALSE(diskMan.hasEnoughSpace(unknownSpace, unknownPart));
}

TEST(DiskManagerTest, WalNoSpaceTest) {
    GraphSpaceID spaceId = 1;
    PartitionID partId = 1;
    fs::TempDir root("/tmp/testWal.XXXXXX");
    std::string spacePath = folly::stringPrintf("%s/nebula/%d", root.path(), spaceId);
    boost::filesystem::create_directories(spacePath);
    auto walPath = folly::stringPrintf("%s/wal/%d", spacePath.c_str(), partId);

    wal::FileBasedWalInfo info;
    info.spaceId_ = spaceId;
    info.partId_ = partId;
    wal::FileBasedWalPolicy policy;
    policy.fileSize = 1024 * 10;
    std::vector<std::string> dataPaths = {root.path()};
    auto diskMan = std::make_shared<DiskManager>(dataPaths);
    diskMan->addPartToPath(spaceId, partId, spacePath);
    auto wal = wal::FileBasedWal::getWal(
        walPath,
        info,
        policy,
        [](LogID, TermID, ClusterID, const std::string&) { return true; },
        diskMan);

    diskMan->freeBytes_[0] = FLAGS_minimum_reserved_bytes + 10000;

    // remaining 10K space, we can only write 10 more logs
    int logSize = 1000;
    for (int i = 1; i <= 20; i++) {
        std::string log(logSize, 'a');
        bool ok = wal->appendLog(i, 1, 0, log);
        if (ok) {
            // mock disk available space decrease
            diskMan->freeBytes_[1] -= logSize;
        } else {
            EXPECT_GT(i, 10);
        }
    }
}


}  // namespace kvstore
}  // namespace nebula


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
