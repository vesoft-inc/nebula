/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "fs/TempFile.h"

namespace nebula {
namespace fs {

TEST(TempFile, Basic) {
    // auto deletiong
    {
        const char *path = "/tmp/tmp.XXXXXX";
        std::string actual_path;
        {
            TempFile tmp(path);
            // same length
            ASSERT_EQ(::strlen(path), ::strlen(tmp.path()));
            // not equal to the template
            ASSERT_STRNE(path, tmp.path());
            // match to the pattern
            const std::regex pattern(R"(/tmp/tmp\.[0-9a-zA-Z]{6})");
            ASSERT_TRUE(std::regex_match(tmp.path(), pattern)) << tmp.path();
            // accessible
            ASSERT_EQ(0, ::access(tmp.path(), F_OK));
            // save the path
            actual_path = tmp.path();
        }
        // now not accessible
        ASSERT_EQ(-1, ::access(actual_path.c_str(), F_OK));
    }
    // no deletion
    {
        const char *path = "/tmp/tmp.XXXXXX";
        std::string actual_path;
        {
            TempFile tmp(path, false);
            // same length
            ASSERT_EQ(::strlen(path), ::strlen(tmp.path()));
            // not equal to the template
            ASSERT_STRNE(path, tmp.path());
            // match to the pattern
            const std::regex pattern(R"(/tmp/tmp\.[0-9a-zA-Z]{6})");
            ASSERT_TRUE(std::regex_match(tmp.path(), pattern)) << tmp.path();
            // accessible
            ASSERT_EQ(0, ::access(tmp.path(), F_OK));
            // save the path
            actual_path = tmp.path();
        }
        // now not accessible
        ASSERT_EQ(0, ::access(actual_path.c_str(), F_OK));
        ASSERT_EQ(0, ::unlink(actual_path.c_str()));
    }
}

TEST(TempFile, Failure) {
    {
        // no permission
        const char *path = "/tmp.XXXXXX";
        ASSERT_THROW({ TempFile tmp(path); }, std::runtime_error);
    }
    {
        // incorrect template
        const char *path = "/tmp.XXXXX";
        ASSERT_THROW({ TempFile tmp(path); }, std::runtime_error);
    }
}

}   // namespace fs
}   // namespace nebula
