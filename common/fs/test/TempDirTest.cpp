/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "fs/TempDir.h"
#include "fs/FileUtils.h"

using namespace vesoft::fs;

TEST(TempDir, AutoRemoval) {
    std::string dirpath;

    {
        TempDir td("/tmp/TempDirAutoRemovalTest1.XXXXXX");
        ASSERT_FALSE(!td.path());
        dirpath = td.path();
        EXPECT_EQ(FileType::DIRECTORY, FileUtils::fileType(dirpath.c_str()));
    }
    ASSERT_EQ(FileType::NOTEXIST, FileUtils::fileType(dirpath.c_str()));

    {
        TempDir td("/tmp/", "TempDirAutoRemovalTest2.XXXXXX");
        ASSERT_FALSE(!td.path());
        dirpath = td.path();
        ASSERT_EQ(FileType::DIRECTORY, FileUtils::fileType(dirpath.c_str()));

        // Create a left-over sub-dir
        TempDir subDir(dirpath.c_str(), "sub_dir.XXXXXX", false);
    }
    ASSERT_EQ(FileType::NOTEXIST, FileUtils::fileType(dirpath.c_str()));
}


TEST(TempDir, LeftBehind) {
    std::string dirpath;

    {
        TempDir td("/tmp/", "TempDirLeftBehindTest1.XXXXXX", false);
        ASSERT_FALSE(!td.path());
        dirpath = td.path();
        ASSERT_EQ(FileType::DIRECTORY, FileUtils::fileType(dirpath.c_str()));
    }
    EXPECT_EQ(FileType::DIRECTORY, FileUtils::fileType(dirpath.c_str()));
    EXPECT_EQ(rmdir(dirpath.c_str()), 0);
    EXPECT_EQ(FileType::NOTEXIST, FileUtils::fileType(dirpath.c_str()));
}


TEST(TempDir, CreateFiles) {
    std::string dirpath;
    {
        TempDir td("/tmp/", "TempDirTest.XXXXXX");
        ASSERT_FALSE(!td.path());
        dirpath = td.path();
    
        int fd = open(FileUtils::joinPath(dirpath, "testfile.txt").c_str(),
                      O_CREAT | O_RDWR | O_EXCL,
                      0644);
        EXPECT_LE(0, fd);
        EXPECT_EQ(0, close(fd));
    }
    EXPECT_EQ(FileType::NOTEXIST, FileUtils::fileType(dirpath.c_str()));
}


TEST(TempDir, NegativeTest) {
    std::string dirpath;

    TempDir td("/", "TempDirNegativeTest1.XXXXXX", false);
    EXPECT_TRUE(!td.path());
}


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}

