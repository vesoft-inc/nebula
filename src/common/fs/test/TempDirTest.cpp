/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <fcntl.h>            // for open, O_CREAT, O_EXCL, O_RDWR
#include <folly/init/Init.h>  // for init
#include <glog/logging.h>     // for INFO
#include <gtest/gtest.h>      // for Message
#include <gtest/gtest.h>      // for TestPartResult
#include <gtest/gtest.h>      // for Message
#include <gtest/gtest.h>      // for TestPartResult
#include <unistd.h>           // for close, rmdir

#include <string>  // for string, allocator

#include "common/base/Logging.h"  // for SetStderrLogging
#include "common/fs/FileUtils.h"  // for FileUtils, FileType, FileType::DI...
#include "common/fs/TempDir.h"    // for TempDir

namespace nebula {
namespace fs {

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

    int fd =
        open(FileUtils::joinPath(dirpath, "testfile.txt").c_str(), O_CREAT | O_RDWR | O_EXCL, 0644);
    EXPECT_LE(0, fd);
    EXPECT_EQ(0, close(fd));
  }
  EXPECT_EQ(FileType::NOTEXIST, FileUtils::fileType(dirpath.c_str()));
}

}  // namespace fs
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
